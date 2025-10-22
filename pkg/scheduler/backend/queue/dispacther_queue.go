package queue

import (
	"container/heap"
	"sync"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// 基于标准库 container/heap 的分片调度优先级队列实现
// dispactherQueue 是一个以 profiles 的 QueueSortFunc 为比较函数的堆
// 与示例中的 nodeScoreHeap 类似，保持排序方法不变
type dispactherQueue struct {
	items []*framework.QueuedPodInfo
	less  framework.LessFunc
	// 为阻塞 Pop/唤醒 Push 增加并发控制
	mu   sync.Mutex
	cond *sync.Cond
}

// 实现 heap.Interface
var _ heap.Interface = &dispactherQueue{}

func (h dispactherQueue) Len() int           { return len(h.items) }
func (h dispactherQueue) Less(i, j int) bool { return h.less(h.items[i], h.items[j]) }
func (h dispactherQueue) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *dispactherQueue) Push(x interface{}) {
	// 追加元素并唤醒等待的 Pop
	h.mu.Lock()
	h.items = append(h.items, x.(*framework.QueuedPodInfo))
	// 如果有等待的 Pop，唤醒一个
	if h.cond != nil {
		h.cond.Signal()
	}
	h.mu.Unlock()
}
func (h *dispactherQueue) Pop() interface{} {
	// 当队列为空时阻塞等待，直到有元素被 Push
	h.mu.Lock()
	for len(h.items) == 0 {
		if h.cond == nil {
			// 惰性初始化，避免外部未通过构造函数创建
			h.cond = sync.NewCond(&h.mu)
		}
		h.cond.Wait()
	}
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[:n-1]
	h.mu.Unlock()
	return x
}

func NewQueuedPodInfoHeap(less framework.LessFunc) heap.Interface {
	h := &dispactherQueue{less: less}
	// 初始化条件变量
	h.cond = sync.NewCond(&h.mu)
	heap.Init(h)
	return h
}

// todo： 线程安全：这个实现不是线程安全的，如果需要在并发环境中使用，需要添加同步机制
