#pragma once

#include <Processors/Executors/Runtime/IExecutor.h>
#include <Processors/Executors/Runtime/V1/ExecutorTasks.h>
#include <Common/Logger.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/ISlotControl.h>
#include <Common/AllocatorWithMemoryTracking.h>

#include <queue>
#include <memory>

#include <boost/container/devector.hpp>

class MemoryTracker;


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
    ExecutingGraphPtr graph;

    ExecutorTasks tasks;

    /// Concurrency control related
    SlotAllocationPtr cpu_slots;
    AcquiredSlotPtr single_thread_cpu_slot; // cpu slot for single-thread mode to work using executeUntil()

    std::unique_ptr<ThreadPool> pool;
    std::mutex spawn_mutex;

    /// Pipeline's max thread count (captured from execute(num_threads)).
    size_t max_pipeline_threads = 1;

    /// Current setMax target. Written by initializeExecution before any worker thread exists,
    /// then read and updated only under `spawn_mutex` from the upscaling block. Non-atomic
    /// because all mutations are serialized by spawn_mutex and initial write happens-before
    /// any spawned worker via the ThreadPool barrier in initializeExecution.
    size_t desired_threads = 1;

    /// Accumulates spawn_count from pushTasks calls that couldn't acquire `spawn_mutex`
    /// (another thread was already handling the spawn). The thread that owns the mutex
    /// drains this counter before deciding the new setMax target, so no demand is ever
    /// silently dropped.
    std::atomic<size_t> pending_demand{0};

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

    QueryStatusPtr process_list_element;

    ReadProgressCallbackPtr read_progress_callback;
    StepProfilerPtr step_profiler;

    /// This queue can grow a lot and lead to OOM. That is why we use non-default
    /// allocator for container which throws exceptions in operator new
    using DequeWithMemoryTracker = boost::container::devector<IProcessor *, AllocatorWithMemoryTracking<IProcessor *>>;
    using Queue = std::queue<IProcessor *, DequeWithMemoryTracker>;

    void initializeExecution(size_t num_threads, bool concurrency_control); /// Initialize executor contexts and task_queue.
    void finalizeExecution(); /// Check all processors are finished.

    /// Methods connected to execution.
    void executeImpl(size_t num_threads, bool concurrency_control);
    void executeStepImpl(size_t thread_num, WorkloadResources && resources, std::atomic_bool * yield_flag = nullptr);
    void executeSingleThread(size_t thread_num, WorkloadResources && resources);
    void finish();

    // Methods for CPU scheduling
    SlotAllocationPtr allocateCPU(size_t num_threads, bool concurrency_control, bool lazy_allocation);
    void spawnThreads(AcquiredSlotPtr slot) TSA_REQUIRES(spawn_mutex);
};

}

}
