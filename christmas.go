package christmas

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/wanghaha-dev/u"
	"sync"
)

type Task struct {
	Id           string `json:"id"`
	Group        string `json:"group"`
	Name         string `json:"name"`
	Status       int    `json:"status"`
	Data         string `json:"data"`
	ReadTime     string `json:"read_time"`
	CompleteTime string `json:"complete_time"`
}

var _task *Task
var _taskOnce sync.Once

func New() *Task {
	_taskOnce.Do(func() {
		_task = new(Task)
	})
	return _task
}

var _client *redis.Client
var _clientOnce sync.Once
// Connect 连接
func (receiver *Task) Connect(Addr string, Password string, DB int) *redis.Client {
	_clientOnce.Do(func() {
		if Addr == "" {
			Addr = "localhost:6379"
		}
		_client = redis.NewClient(&redis.Options{
			Addr:     Addr,
			Password: Password, // no password set
			DB:       DB,  // use default DB
		})
	})
	return _client
}

// AddTask add task
func (receiver *Task)AddTask(ctx context.Context, client *redis.Client, group string, task *Task) string {
	jsonData, _ := json.Marshal(task)
	client.LPush(ctx, group+"_untreated", jsonData)
	return task.Id
}

// QueryTask query task
func (receiver *Task)QueryTask(ctx context.Context, client *redis.Client, taskId string) *Task {
	taskData := client.HGet(ctx, "completed", taskId)
	var task Task
	_ = json.Unmarshal([]byte(taskData.Val()), &task)
	return &task
}

// AddConsumer add consumer
func (receiver *Task)AddConsumer(ctx context.Context, client *redis.Client, group string, handle func(t *Task)) {
	for {
		tasksCount := client.LLen(ctx, group + "_untreated").Val()
		groupTasks := fmt.Sprintf("[ %v ] total tasks: %v", group, tasksCount)
		if tasksCount == 0 {
			u.NewColor(u.FgLightWhite, u.BgGreen).Println(u.Time().Now().DateTime(), groupTasks ," Waiting to work ...")
		} else {
			u.NewColor(u.FgLightWhite, u.BgBlue).Println(u.Time().Now().DateTime(), groupTasks ," Waiting to work ...")
		}

		getTask := client.BRPop(ctx, 0, group+"_untreated")
		var task Task
		_ = json.Unmarshal([]byte(getTask.Val()[1]), &task)
		task.ReadTime = u.Time().Now().DateTime()

		u.Blue.Println("(1/4)", u.Time().Now().DateTime(), "read task ", task.Id, "...")

		// execute
		u.Magenta.Println("(2/4)", u.Time().Now().DateTime(), "execute task ", task.Id, "...")
		handle(&task)

		// modify status
		u.Cyan.Println("(3/4)", u.Time().Now().DateTime(), "update task status", task.Id, "...")
		task.CompleteTime = u.Time().Now().DateTime()
		task.Status = 200

		// Completed
		completedTask, _ := json.Marshal(task)
		client.HSet(ctx, "completed", task.Id, completedTask)
		u.Green.Println("(4/4)", u.Time().Now().DateTime(), task.Id, "completed ...")
	}
}

// Logo logo chars
func Logo() string {
	logoChars := `
       _           _                             
      | |         (_)       _                    
  ____| |__   ____ _  ___ _| |_ ____  _____  ___ 
 / ___)  _ \ / ___) |/___|_   _)    \(____ |/___)
( (___| | | | |   | |___ | | |_| | | / ___ |___ |
 \____)_| |_|_|   |_(___/   \__)_|_|_\_____(___/ 
                                                 
`
	return logoChars
}
