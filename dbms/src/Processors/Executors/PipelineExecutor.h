#pragma once

#include <queue>
#include <stack>
#include <Processors/IProcessor.h>
#include <mutex>
#include <Common/ThreadPool.h>
#include <Common/EventCounter.h>
#include <common/logger_useful.h>

#include <boost/lockfree/queue.hpp>

namespace DB
{


/// Executes query pipeline.
class PipelineExecutor
{
public:
    /// Get pipeline as a set of processors.
    /// Processors should represent full graph. All ports must be connected, all connected nodes are mentioned in set.
    /// Executor doesn't own processors, just stores reference.
    /// During pipeline execution new processors can appear. They will be added to existing set.
    ///
    /// Explicit graph representation is built in constructor. Throws if graph is not correct.
    explicit PipelineExecutor(Processors & processors);

    /// Execute pipeline in multiple threads. Must be called once.
    /// In case of exception during execution throws any occurred.
    void execute(size_t num_threads);

    String getName() const { return "PipelineExecutor"; }

    const Processors & getProcessors() const { return processors; }

    /// Cancel execution. May be called from another thread.
    void cancel()
    {
        cancelled = true;
        finish();
    }

private:
    Processors & processors;

    struct Edge
    {
        UInt64 to = std::numeric_limits<UInt64>::max();

        /// Edge version is increased when port's state is changed (e.g. when data is pushed). See Port.h for details.
        /// To compare version with prev_version we can decide if neighbour processor need to be prepared.
        UInt64 version = 0;
        UInt64 prev_version = 0;
    };

    /// Use std::list because new ports can be added to processor during execution.
    using Edges = std::list<Edge>;

    /// Status for processor.
    /// Can be owning or not. Owning means that executor who set this status can change node's data and nobody else can.
    enum class ExecStatus
    {
        New,  /// prepare wasn't called yet. Initial state. Non-owning.
        Idle,  /// prepare returned NeedData or PortFull. Non-owning.
        Preparing,  /// some executor is preparing processor, or processor is in task_queue. Owning.
        Executing,  /// prepare returned Ready and task is executing. Owning.
        Finished,  /// prepare returned Finished. Non-owning.
        Async  /// prepare returned Async. Owning.
    };

    /// Small structure with context of executing job.
    struct ExecutionState
    {
        std::exception_ptr exception;
        std::function<void()> job;

        IProcessor * processor;
        UInt64 processors_id;

        /// Counters for profiling.
        size_t num_executed_jobs = 0;
        UInt64 execution_time_ns = 0;
        UInt64 preparation_time_ns = 0;
    };

    struct Node
    {
        IProcessor * processor = nullptr;
        Edges directEdges;
        Edges backEdges;

        std::atomic<ExecStatus> status;
        /// This flag can be set by any executor.
        /// When enabled, any executor can try to atomically set Preparing state to status.
        std::atomic_bool need_to_be_prepared;
        /// Last state for profiling.
        IProcessor::Status last_processor_status = IProcessor::Status::NeedData;

        std::unique_ptr<ExecutionState> execution_state;

        Node(IProcessor * processor_, UInt64 processor_id)
            : processor(processor_), status(ExecStatus::New), need_to_be_prepared(false)
        {
            execution_state = std::make_unique<ExecutionState>();
            execution_state->processor = processor;
            execution_state->processors_id = processor_id;
        }

        Node(Node && other) noexcept
            : processor(other.processor), status(ExecStatus::New)
            , need_to_be_prepared(false), execution_state(std::move(other.execution_state))
        {
        }
    };

    using Nodes = std::vector<Node>;

    Nodes graph;

    using Stack = std::stack<UInt64>;
    using TaskQueue = boost::lockfree::queue<ExecutionState *>;

    /// Queue with pointers to tasks. Each thread will concurrently read from it until finished flag is set.
    /// Stores processors need to be prepared. Preparing status is already set for them.
    TaskQueue task_queue;
    static constexpr size_t min_task_queue_size = 8192;
    size_t task_queue_reserved_size = 0;

    /// Monotonically increased counters for task_queue. Helps to check if there is any reason to pull form queue.
    std::atomic<UInt64> num_task_queue_pulls;  /// incremented after successful task_queue.pull
    std::atomic<UInt64> num_task_queue_pushes;  /// incremented before task_queue.push

    std::atomic_bool cancelled;
    std::atomic_bool finished;

    Poco::Logger * log = &Poco::Logger::get("PipelineExecutor");

    /// Context for each thread.
    struct ExecutorContext
    {
        /// Flag is set if thread is waiting condvar.
        std::atomic_bool is_waiting;
        /// Wait this condvar when no tasks to prepare or execute.
        std::condition_variable condvar;
    };

    std::vector<std::unique_ptr<ExecutorContext>> executor_contexts;

    /// Num threads waiting condvar. Last thread finish execution if task_queue is empty.
    std::atomic<size_t> num_waiting_threads;

    /// Things to stop execution to expand pipeline.
    std::atomic<size_t> num_preparing_threads;
    std::atomic<ExecutionState *> node_to_expand;
    std::mutex mutex_to_expand_pipeline;
    std::condition_variable condvar_to_expand_pipeline;
    size_t num_waiting_threads_to_expand_pipeline = 0;

    /// Processor ptr -> node number
    using ProcessorsMap = std::unordered_map<const IProcessor *, UInt64>;
    ProcessorsMap processors_map;

    /// Graph related methods.
    bool addEdges(UInt64 node);
    void buildGraph();
    void expandPipeline(Stack & stack, UInt64 pid);

    /// Pipeline execution related methods.
    void addChildlessProcessorsToStack(Stack & stack);
    bool tryAddProcessorToStackIfUpdated(Edge & edge, Stack & stack);
    static void addJob(ExecutionState * execution_state);
    // TODO: void addAsyncJob(UInt64 pid);
    bool prepareProcessor(size_t pid, Stack & stack, bool async);
    void doExpandPipeline(Stack & stack);

    void executeImpl(size_t num_threads);
    void executeSingleThread(size_t thread_num, size_t num_threads);
    void finish();

    String dumpPipeline() const;
};

using PipelineExecutorPtr = std::shared_ptr<PipelineExecutor>;

}
