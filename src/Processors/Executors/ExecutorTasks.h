#pragma once

#include <Processors/Executors/ExecutingGraph.h>
#include <Processors/Executors/PollingQueue.h>
#include <Processors/Executors/ThreadsQueue.h>
#include <Processors/Executors/TasksQueue.h>

#include <stack>
#include <queue>

namespace DB
{

/// Context for each thread.
struct ExecutionThreadContext
{
    /// Things to stop execution to expand pipeline.
    struct ExpandPipelineTask
    {
        std::function<bool()> callback;
        size_t num_waiting_processing_threads = 0;
        std::mutex mutex;
        std::condition_variable condvar;

        explicit ExpandPipelineTask(std::function<bool()> callback_) : callback(callback_) {}
    };

    /// Will store context for all expand pipeline tasks (it's easy and we don't expect many).
    /// This can be solved by using atomic shard ptr.
    std::list<ExpandPipelineTask> task_list;

    std::queue<ExecutingGraph::Node *> async_tasks;
    std::atomic_bool has_async_tasks = false;

    std::condition_variable condvar;
    std::mutex mutex;
    bool wake_flag = false;

    /// Currently processing node.
    ExecutingGraph::Node * node = nullptr;

    /// Exception from executing thread itself.
    std::exception_ptr exception;

#ifndef NDEBUG
    /// Time for different processing stages.
        UInt64 total_time_ns = 0;
        UInt64 execution_time_ns = 0;
        UInt64 processing_time_ns = 0;
        UInt64 wait_time_ns = 0;
#endif

    void wait(std::atomic_bool & finished);
};

class ExecutorTasks
{
    std::atomic_bool finished = false;

    /// Queue with pointers to tasks. Each thread will concurrently read from it until finished flag is set.
    /// Stores processors need to be prepared. Preparing status is already set for them.
    TaskQueue<ExecutingGraph::Node> task_queue;

    /// Queue which stores tasks where processors returned Async status after prepare.
    /// If multiple threads are using, main thread will wait for async tasks.
    /// For single thread, will wait for async tasks only when task_queue is empty.
    PollingQueue async_task_queue;


    size_t num_waiting_async_tasks = 0;

    ThreadsQueue threads_queue;
    std::mutex task_queue_mutex;

    using Stack = std::stack<UInt64>;
    using Queue = std::queue<ExecutingGraph::Node *>;

    std::atomic<size_t> num_processing_executors = 0;
    std::atomic<ExecutionThreadContext::ExpandPipelineTask *> expand_pipeline_task = nullptr;

    std::vector<std::unique_ptr<ExecutionThreadContext>> executor_contexts;
    std::mutex executor_contexts_mutex;

public:
    bool doExpandPipeline(ExecutionThreadContext::ExpandPipelineTask * task, bool processing);
    bool runExpandPipeline(size_t thread_number, std::function<bool()> callback);

    void expandPipelineStart();
    void expandPipelineEnd();

    void finish();
    bool isFinished() const { return finished; }

    void rethrowFirstThreadException();

    void wakeUpExecutor(size_t thread_num);

    ExecutingGraph::Node * tryGetTask(size_t thread_num, size_t num_threads);
    void pushTasks(Queue & queue, Queue & async_queue, size_t thread_num, size_t num_threads);

    void init(size_t num_threads);
    void fill(Queue & queue, size_t num_threads);

    void processAsyncTasks();

    ExecutingGraph::Node *& getNode(size_t thread_num) { return executor_contexts[thread_num]->node; }
};

}
