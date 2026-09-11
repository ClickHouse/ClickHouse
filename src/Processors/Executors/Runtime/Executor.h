#pragma once

#include <Processors/Executors/Runtime/Engine/Poller.h>
#include <Processors/Executors/Runtime/Engine/TaskScheduler.h>
#include <Processors/Executors/Runtime/Engine/WorkerPool.h>
#include <Processors/Executors/Runtime/Engine/WorkersCoordinator.h>
#include <Processors/Executors/Runtime/Pipeline/ExecutingPipeline.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>

namespace DB
{

class ReadProgressCallback;
using ReadProgressCallbackPtr = std::unique_ptr<ReadProgressCallback>;

class Executor
{
    void start(size_t num_threads, bool concurrency_control);
    void pushInitialTasks();
    void finalize();

public:
    Executor(std::shared_ptr<Processors> processors_, QueryStatusPtr elem, const StepWallClockRegistry * registry_ = nullptr);
    ~Executor();

    void execute(size_t num_threads, bool concurrency_control);
    bool executeUntil(std::atomic_bool * yield_flag = nullptr);

    void cancel(IProcessor::CancelReason reason);
    void cancelReading();

    void setReadProgressCallback(ReadProgressCallbackPtr callback);

private:
    const std::shared_ptr<Processors> processors;
    const QueryStatusPtr process_list_element;
    const StepWallClockRegistry * const registry;
    ReadProgressCallbackPtr read_progress_callback;

    /// Built by start, when the thread count is known; the mutex orders that against cancel from another thread
    std::mutex mutex;
    std::optional<IProcessor::CancelReason> cancel_before_start;
    std::optional<ExecutingPipeline> pipeline;
    std::optional<Poller> poller;
    std::optional<TaskScheduler> scheduler;
    std::optional<WorkersCoordinator> coordinator;
    std::optional<WorkerPool> pool;
};

}
