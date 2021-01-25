#pragma once
#include <Processors/Executors/ExecutingGraph.h>
#include <queue>

namespace DB
{

/// Task to stop execution of all threads (now used to expand pipeline).
/// Is needed to ensure that graph is not changed by any other thread while calling the callback.
class StoppingPipelineTask
{
public:
    /// Data which is needed to manage many StoppingPipelineTasks
    struct Data
    {
        std::atomic<size_t> num_processing_executors = 0;
        std::atomic<StoppingPipelineTask *> task = nullptr;
    };

private:
    std::function<bool()> callback;
    bool result = true;

    size_t num_waiting_processing_threads = 0;
    std::mutex mutex;
    std::condition_variable condvar;

public:
    explicit StoppingPipelineTask(std::function<bool()> callback_) : callback(callback_) {}

    bool executeTask(Data & data);

    /// This methods are called to start/stop concurrent reading of pipeline graph.
    /// Do not use RAII cause exception may be thrown.
    static void enterConcurrentReadSection(Data & data);
    static void exitConcurrentReadSection(Data & data);

private:
    bool executeTask(Data & data, bool on_enter);
};

/// Context for each executing thread of PipelineExecutor.
class ExecutionThreadContext
{
private:
    /// Will store context for all expand pipeline tasks (it's easy and we don't expect many).
    /// This can be solved by using atomic shard ptr.
    std::list<StoppingPipelineTask> task_list;

    /// A queue of async tasks. Task is added to queue when waited.
    std::queue<ExecutingGraph::Node *> async_tasks;
    std::atomic_bool has_async_tasks = false;

    /// This objects are used to wait for next available task.
    std::condition_variable condvar;
    std::mutex mutex;
    bool wake_flag = false;

    /// Currently processing node.
    ExecutingGraph::Node * node = nullptr;

    /// Exception from executing thread itself.
    std::exception_ptr exception;

public:
#ifndef NDEBUG
    /// Time for different processing stages.
    UInt64 total_time_ns = 0;
    UInt64 execution_time_ns = 0;
    UInt64 processing_time_ns = 0;
    UInt64 wait_time_ns = 0;
#endif

    const size_t thread_number;

    void wait(std::atomic_bool & finished);
    void wakeUp();

    /// Methods to access/change currently executing task.
    bool hasTask() const { return node != nullptr; }
    void setTask(ExecutingGraph::Node * task) { node = task; }
    bool executeTask();
    uint64_t getProcessorID() const { return node->processors_id; }

    /// Methods to manage async tasks.
    ExecutingGraph::Node * tryPopAsyncTask();
    void pushAsyncTask(ExecutingGraph::Node * async_task);
    bool hasAsyncTasks() const { return has_async_tasks; }

    StoppingPipelineTask & addStoppingPipelineTask(std::function<bool()> callback) { return task_list.emplace_back(std::move(callback)); }

    std::unique_lock<std::mutex> lockStatus() const { return std::unique_lock(node->status_mutex); }

    void setException(std::exception_ptr exception_) { exception = std::move(exception_); }
    void rethrowExceptionIfHas();

    explicit ExecutionThreadContext(size_t thread_number_) : thread_number(thread_number_) {}
};

}
