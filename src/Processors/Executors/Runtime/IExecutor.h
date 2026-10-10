#pragma once

#include <Processors/IProcessor.h>

#include <atomic>
#include <memory>

namespace DB
{

class ReadProgressCallback;
using ReadProgressCallbackPtr = std::unique_ptr<ReadProgressCallback>;

class StepProfiler;
using StepProfilerPtr = std::shared_ptr<StepProfiler>;

/// Runs a pipeline, a set of connected processors, until every processor is finished.
class IExecutor
{
public:
    virtual ~IExecutor() = default;

    /// Execute pipeline in multiple threads.
    virtual void execute(size_t num_threads, bool concurrency_control) = 0;

    /// Execute the pipeline until it is finished or `yield_flag` is set.
    virtual bool executeUntil(std::atomic_bool * yield_flag) = 0;

    /// Cancel execution.
    virtual void cancel(IProcessor::CancelReason reason) = 0;

    /// Cancel processors which only read data from source.
    virtual void cancelReading() = 0;

    /// Set callback for read progress.
    virtual void setReadProgressCallback(ReadProgressCallbackPtr callback) = 0;

    /// Set the profiler of EXPLAIN ANALYZE.
    virtual void setStepProfiler(StepProfilerPtr step_profiler) = 0;
};

using ExecutorPtr = std::shared_ptr<IExecutor>;

}
