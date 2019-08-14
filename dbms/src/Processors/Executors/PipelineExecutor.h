#pragma once

#include <queue>
#include <stack>
#include <Processors/IProcessor.h>
#include <mutex>
#include <Common/ThreadPool.h>
#include <Common/EventCounter.h>
#include <common/logger_useful.h>

#include <boost/lockfree/stack.hpp>

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
    explicit PipelineExecutor(Processors & processors_);

    /// Execute pipeline in multiple threads. Must be called once.
    /// In case of exception during execution throws any occurred.
    void execute(size_t num_threads);

    String getName() const { return "PipelineExecutor"; }

    const Processors & getProcessors() const { return processors; }

    /// Cancel execution. May be called from another thread.
    void cancel();

private:
    Processors & processors;
    std::mutex processors_mutex;

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

        IProcessor * processor = nullptr;
        UInt64 processors_id = 0;

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
            : processor(other.processor), status(other.status.load())
            , need_to_be_prepared(other.need_to_be_prepared.load()), execution_state(std::move(other.execution_state))
        {
        }
    };

    using Nodes = std::vector<Node>;

    Nodes graph;

    using Stack = std::stack<UInt64>;

    using TaskQueue = std::queue<ExecutionState *>;

    /// Queue with pointers to tasks. Each thread will concurrently read from it until finished flag is set.
    /// Stores processors need to be prepared. Preparing status is already set for them.
    TaskQueue task_queue;
    std::mutex task_queue_mutex;
    std::condition_variable task_queue_condvar;

    std::atomic_bool cancelled;
    std::atomic_bool finished;

    Poco::Logger * log = &Poco::Logger::get("PipelineExecutor");

    /// Num threads waiting condvar. Last thread finish execution if task_queue is empty.
    size_t num_waiting_threads = 0;

    /// Things to stop execution to expand pipeline.
    struct ExpandPipelineTask
    {
        ExecutionState * node_to_expand;
        Stack * stack;
        size_t num_waiting_processing_threads = 0;
        std::mutex mutex;
        std::condition_variable condvar;

        ExpandPipelineTask(ExecutionState * node_to_expand_, Stack * stack_)
            : node_to_expand(node_to_expand_), stack(stack_) {}
    };

    std::atomic<size_t> num_processing_executors;
    std::atomic<ExpandPipelineTask *> expand_pipeline_task;

    /// Context for each thread.
    struct ExecutorContext
    {
        /// Will store context for all expand pipeline tasks (it's easy and we don't expect many).
        /// This can be solved by using atomic shard ptr.
        std::list<ExpandPipelineTask> task_list;
    };

    std::vector<std::unique_ptr<ExecutorContext>> executor_contexts;

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

    /// Prepare processor with pid number.
    /// Check parents and children of current processor and push them to stacks if they also need to be prepared.
    /// If processor wants to be expanded, ExpandPipelineTask from thread_number's execution context will be used.
    bool prepareProcessor(UInt64 pid, Stack & children, Stack & parents, size_t thread_number, bool async);
    void doExpandPipeline(ExpandPipelineTask * task, bool processing);

    void executeImpl(size_t num_threads);
    void executeSingleThread(size_t thread_num, size_t num_threads);
    void finish();

    String dumpPipeline() const;
};

using PipelineExecutorPtr = std::shared_ptr<PipelineExecutor>;

}
