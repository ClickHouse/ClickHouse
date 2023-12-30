#pragma once
#include <Processors/Executors/ExecutingGraph.h>
#include <queue>
#include <condition_variable>

namespace DB
{

class ReadProgressCallback;

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

    /// Callback for read progress.
    ReadProgressCallback * read_progress_callback = nullptr;

public:
#ifndef NDEBUG
    /// Time for different processing stages.
    UInt64 total_time_ns = 0;
    UInt64 execution_time_ns = 0;
    UInt64 processing_time_ns = 0;
    UInt64 wait_time_ns = 0;
#endif

    const size_t thread_number;
    const bool profile_processors;
    const bool trace_processors;

    /// There is a performance optimization that schedules a task to the current thread, avoiding global task queue.
    /// Optimization decreases contention on global task queue but may cause starvation.
    /// See 01104_distributed_numbers_test.sql
    /// This constant tells us that we should skip the optimization
    /// if it was applied more than `max_scheduled_local_tasks` in a row.
    constexpr static size_t max_scheduled_local_tasks = 128;
    size_t num_scheduled_local_tasks = 0;

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

    void setException(std::exception_ptr exception_) { exception = exception_; }
    void rethrowExceptionIfHas();

    explicit ExecutionThreadContext(size_t thread_number_, bool profile_processors_, bool trace_processors_, ReadProgressCallback * callback)
        : read_progress_callback(callback)
        , thread_number(thread_number_)
        , profile_processors(profile_processors_)
        , trace_processors(trace_processors_)
    {}
};

}
