#pragma once

#include <Processors/QueryPlan/Profiling/Execution/WorkInterval.h>
#include <base/types.h>

#include <atomic>
#include <condition_variable>
#include <exception>
#include <utility>

namespace DB
{

class IProcessor;
class ReadProgressCallback;
class StepProfiler;

/// Context for each executing thread of PipelineExecutor.
class ExecutionThreadContext
{
private:
    /// This objects are used to wait for next available task.
    std::condition_variable condvar;
    std::mutex mutex;
    bool wake_flag = false;

    /// Currently processing processor.
    IProcessor * processor = nullptr;

    /// Exception from executing thread itself.
    std::exception_ptr exception;

    /// Callback for read progress.
    ReadProgressCallback * read_progress_callback = nullptr;

    /// EXPLAIN ANALYZE statistics.
    StepProfiler * step_profiler = nullptr;
    std::vector<WorkInterval> work_intervals;

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

    const size_t thread_number;
    const bool profile_processors;
    const bool trace_processors;
    const bool collect_work_intervals;

    void wait(std::atomic_bool & finished);
    void wakeUp();

    /// Methods to access/change currently executing task.
    bool hasTask() const { return processor != nullptr; }
    void setTask(IProcessor * task) { processor = task; }
    IProcessor * getTask() const { return processor; }
    IProcessor * popTask() { return std::exchange(processor, nullptr); }
    bool executeTask();

    void setException(std::exception_ptr exception_);
    std::exception_ptr getException();
    void rethrowExceptionIfHas();

    /// Hands the recorded intervals to the profiler; called once, after the thread finished.
    void flushWorkIntervals();

    ExecutionThreadContext(size_t thread_number_, bool profile_processors_, bool trace_processors_, ReadProgressCallback * callback, StepProfiler * step_profiler_);
};

}
