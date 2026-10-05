#pragma once

#include <Processors/Executors/Runtime/IExecutor.h>
#include <Processors/Executors/Runtime/V2/Engine/Poller.h>
#include <Processors/Executors/Runtime/V2/Engine/TaskScheduler.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkerPool.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkersCoordinator.h>
#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingPipeline.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>

namespace DB
{

namespace Runtime::V2
{

class Executor : public IExecutor
{
    void start(size_t num_threads, bool concurrency_control);
    void pushInitialTasks();
    void finalize();

public:
    Executor(std::shared_ptr<Processors> processors_, QueryStatusPtr elem);
    ~Executor() override;

    void execute(size_t num_threads, bool concurrency_control) override;
    bool executeUntil(std::atomic_bool * yield_flag) override;

    void cancel(IProcessor::CancelReason reason) override;
    void cancelReading() override;

    void setReadProgressCallback(ReadProgressCallbackPtr callback) override;
    void setStepProfiler(StepProfilerPtr step_profiler_) override;

private:
    const std::shared_ptr<Processors> processors;
    const QueryStatusPtr process_list_element;
    ReadProgressCallbackPtr read_progress_callback;
    StepProfilerPtr step_profiler;

    /// Built by start, when the thread count is known; the mutex orders that against cancel from another thread
    Wyrd::Mutex mutex;
    std::optional<IProcessor::CancelReason> cancel_before_start;
    std::optional<ExecutingPipeline> pipeline;
    std::optional<Poller> poller;
    std::optional<TaskScheduler> scheduler;
    std::optional<WorkersCoordinator> coordinator;
    std::optional<WorkerPool> pool;
};

}

}
