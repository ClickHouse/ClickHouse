#pragma once

#include <Processors/Executors/ExecutionThreadContext.h>
#include <Processors/Executors/PollingQueue.h>
#include <Processors/Executors/ThreadsQueue.h>
#include <Processors/Executors/TasksQueue.h>
#include <stack>

namespace DB
{

/// Manage tasks which are ready for execution. Used in PipelineExecutor.
class ExecutorTasks
{
    /// If query is finished (or cancelled).
    std::atomic_bool finished = false;

    /// Contexts for every executing thread.
    std::vector<std::unique_ptr<ExecutionThreadContext>> executor_contexts;
    /// This mutex protects only executor_contexts vector. Needed to avoid race between init() and finish().
    std::mutex executor_contexts_mutex;

    /// Common mutex for all the following fields.
    std::mutex mutex;

    /// Queue with pointers to tasks. Each thread will concurrently read from it until finished flag is set.
    /// Stores processors need to be prepared. Preparing status is already set for them.
    TaskQueue<ExecutingGraph::Node> task_queue;

    /// Async tasks should be processed with higher priority, but also require task stealing logic.
    /// So we have a separate queue specifically for them.
    TaskQueue<ExecutingGraph::Node> fast_task_queue;
    std::atomic_bool has_fast_tasks = false; // Required only to enable local task optimization

    /// Queue which stores tasks where processors returned Async status after prepare.
    /// If multiple threads are used, main thread will wait for async tasks.
    /// For single thread, will wait for async tasks only when task_queue is empty.
    PollingQueue async_task_queue;

    /// Maximum amount of threads. Constant after initialization, based on `max_threads` setting.
    size_t num_threads = 0;

    /// Started thread count (allocated by `ConcurrencyControl`). Can increase during execution up to `num_threads`.
    size_t use_threads = 0;

    /// Number of idle threads, changed with threads_queue.size().
    std::atomic_size_t idle_threads = 0;

    /// A set of currently waiting threads.
    ThreadsQueue threads_queue;

    /// Threshold found by rolling dice.
    const static size_t TOO_MANY_IDLE_THRESHOLD = 4;

public:
    using Stack = std::stack<UInt64>;
    /// This queue can grow a lot and lead to OOM. That is why we use non-default
    /// allocator for container which throws exceptions in operator new
    using DequeWithMemoryTracker = std::deque<ExecutingGraph::Node *, AllocatorWithMemoryTracking<ExecutingGraph::Node *>>;
    using Queue = std::queue<ExecutingGraph::Node *, DequeWithMemoryTracker>;

    void finish();
    bool isFinished() const { return finished; }

    void rethrowFirstThreadException();

    void tryWakeUpAnyOtherThreadWithTasks(ExecutionThreadContext & self, std::unique_lock<std::mutex> & lock);
    void tryWakeUpAnyOtherThreadWithTasksInQueue(ExecutionThreadContext & self, TaskQueue<ExecutingGraph::Node> & queue, std::unique_lock<std::mutex> & lock);

    /// It sets the task for specified thread `context`.
    /// If task was succeessfully found, one thread is woken up to process the remaining tasks.
    /// If there is no ready task yet, it blocks.
    /// If there are no more tasks, it finishes execution.
    /// Task priorities:
    ///   0. For num_threads == 1 we check async_task_queue directly
    ///   1. Async tasks from fast_task_queue for specified thread
    ///   2. Async tasks from fast_task_queue for other threads
    ///   3. Regular tasks from task_queue for specified thread
    ///   4. Regular tasks from task_queue for other threads
    void tryGetTask(ExecutionThreadContext & context);

    // Adds regular tasks from `queue` and async tasks from `async_queue` into queues for specified thread `context`.
    // Local task optimization: the first regular task could be placed directly into thread to be executed next.
    // For async tasks proessor->schedule() is called.
    // If non-local tasks were added, wake up one thread to process them.
    void pushTasks(Queue & queue, Queue & async_queue, ExecutionThreadContext & context);

    void init(size_t num_threads_, size_t use_threads_, bool profile_processors, bool trace_processors, ReadProgressCallback * callback);
    void fill(Queue & queue, Queue & async_queue);
    void upscale(size_t use_threads_);

    void processAsyncTasks();

    bool shouldSpawn() const { return idle_threads <= TOO_MANY_IDLE_THRESHOLD; }

    ExecutionThreadContext & getThreadContext(size_t thread_num) { return *executor_contexts[thread_num]; }
};

}
