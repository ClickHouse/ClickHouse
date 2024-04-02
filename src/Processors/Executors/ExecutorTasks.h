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

    /// Queue which stores tasks where processors returned Async status after prepare.
    /// If multiple threads are using, main thread will wait for async tasks.
    /// For single thread, will wait for async tasks only when task_queue is empty.
    PollingQueue async_task_queue;

    /// Maximum amount of threads. Constant after initialization, based on `max_threads` setting.
    size_t num_threads = 0;

    /// Started thread count (allocated by `ConcurrencyControl`). Can increase during execution up to `num_threads`.
    size_t use_threads = 0;

    /// This is the total number of waited async tasks which are not executed yet.
    /// sum(executor_contexts[i].async_tasks.size())
    size_t num_waiting_async_tasks = 0;

    /// A set of currently waiting threads.
    ThreadsQueue threads_queue;

public:
    using Stack = std::stack<UInt64>;
    using Queue = std::queue<ExecutingGraph::Node *>;

    void finish();
    bool isFinished() const { return finished; }

    void rethrowFirstThreadException();

    void tryWakeUpAnyOtherThreadWithTasks(ExecutionThreadContext & self, std::unique_lock<std::mutex> & lock);
    void tryGetTask(ExecutionThreadContext & context);
    void pushTasks(Queue & queue, Queue & async_queue, ExecutionThreadContext & context);

    void init(size_t num_threads_, size_t use_threads_, bool profile_processors, bool trace_processors, ReadProgressCallback * callback);
    void fill(Queue & queue);
    void upscale(size_t use_threads_);

    void processAsyncTasks();

    ExecutionThreadContext & getThreadContext(size_t thread_num) { return *executor_contexts[thread_num]; }
};

}
