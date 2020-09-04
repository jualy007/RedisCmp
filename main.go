package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/urfave/cli/v2"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"
)

var wg sync.WaitGroup

func Connect(redisUrl string) *redis.Client {
	options, _ := redis.ParseURL(redisUrl)
	options.ReadTimeout = 2 * time.Minute
	client := redis.NewClient(options)
	return client
}

func compareKeys(keys []string, s string, t string) {
	ctx := context.Background()
	sourceClient := Connect(s)
	targetClient := Connect(t)

	var flag bool
	for _, key := range keys {
		_type, _ := sourceClient.Type(ctx, key).Result()
		switch _type {
		case "hash":
			svalue, _ := sourceClient.HGetAll(ctx, key).Result()
			tvalue, _ := targetClient.HGetAll(ctx, key).Result()
			flag = reflect.DeepEqual(svalue, tvalue)
		case "zset":
			count, _ := sourceClient.ZCard(ctx, key).Result()
			svalue, _ := sourceClient.ZRange(ctx, key, 1, count).Result()
			tvalue, _ := targetClient.ZRange(ctx, key, 1, count).Result()
			flag = reflect.DeepEqual(svalue, tvalue)
		case "set":
			svalue, _ := sourceClient.SMembers(ctx, key).Result()
			tvalue, _ := targetClient.SMembers(ctx, key).Result()
			flag = reflect.DeepEqual(svalue, tvalue)
		case "list":
			count, _ := sourceClient.LLen(ctx, key).Result()
			svalue, _ := sourceClient.LRange(ctx, key, 1, count).Result()
			tvalue, _ := targetClient.LRange(ctx, key, 1, count).Result()
			flag = reflect.DeepEqual(svalue, tvalue)
		case "string":
			svalue, _ := sourceClient.Get(ctx, key).Result()
			tvalue, _ := targetClient.Get(ctx, key).Result()
			flag = svalue == tvalue
		default:
			svalue, _ := sourceClient.Get(ctx, key).Result()
			tvalue, _ := targetClient.Get(ctx, key).Result()
			flag = svalue == tvalue
		}

		if !flag {
			fmt.Printf("ERROR, Key: %s Key Value in two redis server not same!!!\n", key)
		}
	}
	fmt.Printf("Compare Redis Keys Job Done.\n")
	wg.Done()
}

func main() {
	start := time.Now()

	app := &cli.App{
		Name:  "RedisCmp",
		Usage: "Redis Data Compare Tools",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "server1", Value: "redis://:password@localhost:6379/0", Usage: "Redis Server compare source", Required: true},
			&cli.StringFlag{Name: "server2", Value: "redis://:password@localhost:6379/0", Usage: "Redis Server compare target", Required: true},
		},
		Action: func(c *cli.Context) error {
			ctx := context.Background()

			//compare total keys
			sourceClient := Connect(c.String("server1"))
			targetClient := Connect(c.String("server2"))

			sourceInfo, _ := sourceClient.Info(ctx, "Keyspace").Result()
			targetInfo, _ := targetClient.Info(ctx, "Keyspace").Result()

			sourceDB0 := strings.Split(strings.Split(sourceInfo, "\r\n")[1], ",")[0]
			targetDB0 := strings.Split(strings.Split(targetInfo, "\r\n")[1], ",")[0]

			fmt.Printf("Source: %s, Target: %s\n", sourceDB0, targetDB0)
			if sourceDB0 != targetDB0 {
				fmt.Printf("ERROR!!!DB0 Total keys number not equals.")
			}

			// Scan all keys
			var cursor uint64
			var err error
			for {
				var keys []string
				keys, cursor, err = sourceClient.Scan(ctx, cursor, "", 5000).Result()

				if err != nil {
					fmt.Println("Scan Key ERROR: %s", err)
					os.Exit(2)
				}

				if len(keys) > 0 {
					go compareKeys(keys, c.String("server1"), c.String("server2"))
					wg.Add(1)
				}

				if cursor == 0 {
					break
				}
			}

			wg.Wait()
			return nil
		},
		UsageText: "RedisCmp --server1 redis://...... --server2 redis://......",
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf(err.Error())
	}

	cost := time.Since(start)
	fmt.Printf("\nRunning cost=[%s]\n", cost)
}
