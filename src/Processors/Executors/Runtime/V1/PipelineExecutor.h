#pragma once

#include <Common/QueueWithMemoryTracking.h>
#include <Processors/Executors/Runtime/IExecutor.h>
#include <Processors/Executors/Runtime/V1/ExecutorTasks.h>
#include <Common/Logger.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/ISlotControl.h>

#include <queue>
#include <memory>

namespace DB
{

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;

namespace Runtime::V1
{

class ExecutingGraph;
using ExecutingGraphPtr = std::unique_ptr<ExecutingGraph>;
struct WorkloadResources;

/// Executes query pipeline.
class PipelineExecutor : public IExecutor
{
public:
    /// Get pipeline as a set of processors.
    /// Processors should represent full graph. All ports must be connected, all connected nodes are mentioned in set.
    /// During pipeline execution new processors can appear. They will be added to existing set.
    ///
    /// Explicit graph representation is built in constructor. Throws if graph is not correct.
    ///
    /// PipelineExecutor must be destroyed before the corresponding QueryPipeline, because
    /// QueryPlanResourceHolder may hold some resources referenced by processors and used in
    /// processor destructors.
    explicit PipelineExecutor(std::shared_ptr<Processors> processors, QueryStatusPtr elem);
    ~PipelineExecutor() override;

    void execute(size_t num_threads, bool concurrency_control) override;
    bool executeUntil(std::atomic_bool * yield_flag) override;
    void cancel(IProcessor::CancelReason reason) override;
    void cancelReading() override;
    void setReadProgressCallback(ReadProgressCallbackPtr callback) override;
    void setStepProfiler(StepProfilerPtr step_profiler_) override;

private:
    /// Outlives graph registrations, including when construction fails.
    QueryStatusPtr process_list_element;
    ExecutingGraphPtr graph;

    ExecutorTasks tasks;

    /// Concurrency control related
    SlotAllocationPtr cpu_slots;
    AcquiredSlotPtr single_thread_cpu_slot; // cpu slot for single-thread mode to work using executeUntil()
    std::unique_ptr<ThreadPool> pool;

    /// Pipeline's max thread count (captured from execute(num_threads)).
    size_t max_pipeline_threads = 1;

    /// Current `setMax` target. Only grows, see `growDesiredThreads`.
    std::atomic<size_t> desired_threads = 1;

    /// Flag that checks that initializeExecution was called.
    bool is_execution_initialized = false;
    /// system.processors_profile_log
    bool profile_processors = false;
    /// system.opentelemetry_span_log
    bool trace_processors = false;
    bool trace_cpu_scheduling = false;

    std::atomic<IProcessor::CancelReason> cancel_reason = IProcessor::CancelReason::NotCancelled;
    std::atomic_bool cancelled_reading = false;

    LoggerPtr log = getLogger("PipelineExecutor");

    ReadProgressCallbackPtr read_progress_callback;
    StepProfilerPtr step_profiler;

    /// This queue can grow a lot and lead to OOM, so it is allocated through a tracking
    /// allocator that throws instead of overcommitting.
    using Queue = DevectorQueueWithMemoryTracking<IProcessor *>;

    void initializeExecution(size_t num_threads, bool concurrency_control); /// Initialize executor contexts and task_queue.
    void finalizeExecution(); /// Check all processors are finished.

    /// Methods connected to execution.
    void executeImpl(size_t num_threads, bool concurrency_control);
    void executeStepImpl(size_t thread_num, WorkloadResources && resources, std::atomic_bool * yield_flag = nullptr);
    void executeSingleThread(size_t thread_num, WorkloadResources && resources);
    void finish();
    void detachSpillables();

    // Methods for CPU scheduling
    SlotAllocationPtr allocateCPU(size_t num_threads, bool concurrency_control, bool lazy_allocation);
    /// Raise the `setMax` target by `threads`, up to `max_pipeline_threads`.
    void growDesiredThreads(size_t threads);
    /// Start a thread for `slot` (if any) and for every other granted slot, until `tasks` has no capacity left.
    /// Lock-free: any thread calls it after a task while `tasks` has requested threads.
    void spawnThreads(AcquiredSlotPtr slot);
};

}

}
