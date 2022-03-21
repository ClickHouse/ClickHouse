#pragma once
#include <Processors/Executors/ExecutingGraph.h>
#include <queue>
#include <condition_variable>

namespace DB
{

/// Context for each executing thread of PipelineExecutor.
class ExecutionThreadContext
{
private:
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

    std::unique_lock<std::mutex> lockStatus() const { return std::unique_lock(node->status_mutex); }

    void setException(std::exception_ptr exception_) { exception = std::move(exception_); }
    void rethrowExceptionIfHas();

    explicit ExecutionThreadContext(size_t thread_number_) : thread_number(thread_number_) {}
};

}
