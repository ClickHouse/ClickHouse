#pragma once

#include <Processors/Executors/ExecutionThreadContext.h>
#include <Processors/Executors/PollingQueue.h>
#include <Processors/Executors/ThreadsQueue.h>
#include <Processors/Executors/TasksQueue.h>
#include <Common/ISlotControl.h>
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

    /// Maximum slot_id of currently active slots + 1. Can change during execution in range from 1 to `num_threads`.
    size_t use_threads = 0;

    /// Reference counters for thread CPU slots to handle race conditions between upscale/downscale.
    std::vector<size_t> slot_count;

    /// Total number of non-preempted slots.
    size_t total_slots = 0;

    /// A set of currently waiting threads.
    ThreadsQueue threads_queue;

    /// CPU slots for each thread.
    SlotAllocationPtr cpu_slots;

    /// Threshold found by rolling dice.
    const static size_t TOO_MANY_IDLE_THRESHOLD = 4;

public:
    using Stack = std::stack<UInt64>;
    /// This queue can grow a lot and lead to OOM. That is why we use non-default
    /// allocator for container which throws exceptions in operator new
    using DequeWithMemoryTracker = boost::container::devector<ExecutingGraph::Node *, AllocatorWithMemoryTracking<ExecutingGraph::Node *>>;
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
    // Returns the number of additional threads the caller should try to spawn (0 if idle
    // threads can cover the push, or no new tasks were added).
    size_t pushTasks(Queue & queue, Queue & async_queue, ExecutionThreadContext & context);

    void init(size_t num_threads_, size_t use_threads_, const SlotAllocationPtr & cpu_slots_, bool profile_processors, bool trace_processors, ReadProgressCallback * callback);

    /// Push initial tasks. Returns the count of tasks pushed (regular + async) — used by
    /// `PipelineExecutor::initializeExecution` to size the slot-allocation ceiling via
    /// `cpu_slots->setMax(...)` so the initial parallelism is admitted to the scheduler
    /// without waiting for a later `pushTasks` round to expand it.
    size_t fill(Queue & queue, Queue & async_queue);

    /// Release CPU slots
    void freeCPU();

    /// Upscale to include slot_id. Updates use_threads to max(use_threads, slot_id + 1).
    /// Returns the number of additional threads the caller may try to spawn next (0 means
    /// "stop spawning": either pipeline width is saturated or enough idle threads can absorb
    /// the work). Callers should still spawn one at a time and re-call upscale after each.
    size_t upscale(size_t slot_id);

    /// Downscale by removing slot_id from active slots. Updates use_threads to highest active slot + 1
    void downscale(size_t slot_id);

    /// Temporarily release slot_id without downscale. Later either downscale() or resume() is called.
    void preempt(size_t slot_id);

    /// Resume execution of a previously preempted slot.
    void resume(size_t slot_id);

    void processAsyncTasks();

    ExecutionThreadContext & getThreadContext(size_t thread_num) { return *executor_contexts[thread_num]; }

    String dump();
};

}
