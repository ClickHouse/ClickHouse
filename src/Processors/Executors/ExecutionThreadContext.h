#pragma once
#include <Processors/Executors/ExecutingGraph.h>
#include <Processors/StepWallClockRegistry.h>
#include <queue>
#include <condition_variable>

namespace DB
{

class ReadProgressCallback;
class MemorySpillScheduler;

/// Context for each executing thread of PipelineExecutor.
class ExecutionThreadContext
{
private:
    /// This objects are used to wait for next available task.
    std::condition_variable condvar;
    std::mutex mutex;
    bool wake_flag = false;

    /// Currently processing node.
    ExecutingGraph::Node * node = nullptr;

    /// Exception from executing thread itself.
    std::exception_ptr exception;

    /// The last task ran only its forced-recovery hook. Its prepared processor state must be
    /// preserved and deferred, not passed through prepare() again.
    bool task_consumed_for_recovery = false;

    /// Callback for read progress.
    ReadProgressCallback * read_progress_callback = nullptr;

    /// Retained controller for the complete dependency graph. Never rediscover it from the worker's
    /// current ThreadGroup: nested executors can otherwise register on one controller and execute
    /// recovery against another.
    std::shared_ptr<MemorySpillScheduler> memory_spill_scheduler;

public:
#ifndef NDEBUG
    /// Time for different processing stages.
    UInt64 total_time_ns = 0;
    UInt64 execution_time_ns = 0;
    UInt64 processing_time_ns = 0;
    UInt64 wait_time_ns = 0;
#endif

    /// There is a performance optimization that schedules a task to the current thread, avoiding global task queue.
    /// Optimization decreases contention on global task queue but may cause starvation.
    /// See 01104_distributed_numbers_test.sql
    /// This constant tells us that we should skip the optimization
    /// if it was applied more than `max_scheduled_local_tasks` in a row.
    constexpr static size_t max_scheduled_local_tasks = 128;
    size_t num_scheduled_local_tasks = 0;

    const StepWallClockRegistry * step_to_wall_clock_registry = nullptr;

    const size_t thread_number;
    const bool profile_processors;
    const bool trace_processors;

    void wait(std::atomic_bool & finished);
    void wakeUp();

    /// Methods to access/change currently executing task.
    bool hasTask() const { return node != nullptr; }
    void setTask(ExecutingGraph::Node * task) { node = task; }
    ExecutingGraph::Node * getTask() const { return node; }
    ExecutingGraph::Node * popTask() { return std::exchange(node, nullptr); }
    bool executeTask();
    bool wasTaskConsumedForRecovery() const { return task_consumed_for_recovery; }

    std::unique_lock<std::mutex> lockStatus() const { return std::unique_lock(node->status_mutex); }

    void setException(std::exception_ptr exception_) { exception = exception_; }
    void rethrowExceptionIfHas();

    explicit ExecutionThreadContext(
        size_t thread_number_, bool profile_processors_, bool trace_processors_,
        const StepWallClockRegistry * step_wall_clock_registry_, ReadProgressCallback * callback,
        const std::shared_ptr<MemorySpillScheduler> & memory_spill_scheduler_)
        : read_progress_callback(callback)
        , memory_spill_scheduler(memory_spill_scheduler_)
        , step_to_wall_clock_registry(step_wall_clock_registry_)
        , thread_number(thread_number_)
        , profile_processors(profile_processors_)
        , trace_processors(trace_processors_)
    {}
};

}
