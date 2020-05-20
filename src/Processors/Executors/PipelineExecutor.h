#pragma once

#include <Processors/IProcessor.h>
#include <Processors/Executors/ThreadsQueue.h>
#include <Common/ThreadPool.h>
#include <Common/EventCounter.h>
#include <common/logger_useful.h>

#include <queue>
#include <stack>
#include <mutex>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class QueryStatus;

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
    explicit PipelineExecutor(Processors & processors_, QueryStatus * elem = nullptr);

    /// Execute pipeline in multiple threads. Must be called once.
    /// In case of exception during execution throws any occurred.
    void execute(size_t num_threads);

    /// Execute single step. Step will be stopped when yield_flag is true.
    /// Execution is happened in a single thread.
    /// Return true if execution should be continued.
    bool executeStep(std::atomic_bool * yield_flag = nullptr);

    const Processors & getProcessors() const { return processors; }

    /// Cancel execution. May be called from another thread.
    void cancel();

private:
    Processors & processors;
    std::mutex processors_mutex;

    struct Edge
    {
        Edge(UInt64 to_, bool backward_,
             UInt64 input_port_number_, UInt64 output_port_number_, std::vector<void *> * update_list)
            : to(to_), backward(backward_)
            , input_port_number(input_port_number_), output_port_number(output_port_number_)
        {
            update_info.update_list = update_list;
            update_info.id = this;
        }

        UInt64 to = std::numeric_limits<UInt64>::max();
        bool backward;
        UInt64 input_port_number;
        UInt64 output_port_number;

        /// Edge version is increased when port's state is changed (e.g. when data is pushed). See Port.h for details.
        /// To compare version with prev_version we can decide if neighbour processor need to be prepared.
        Port::UpdateInfo update_info;
    };

    /// Use std::list because new ports can be added to processor during execution.
    using Edges = std::list<Edge>;

    /// Status for processor.
    /// Can be owning or not. Owning means that executor who set this status can change node's data and nobody else can.
    enum class ExecStatus
    {
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
        bool has_quota = false;

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

        ExecStatus status;
        std::unique_ptr<std::mutex> status_mutex;

        std::unique_ptr<Port::UpdateInfo::UpdateList> post_updated_input_ports;
        std::unique_ptr<Port::UpdateInfo::UpdateList> post_updated_output_ports;

        /// Last state for profiling.
        IProcessor::Status last_processor_status = IProcessor::Status::NeedData;

        std::unique_ptr<ExecutionState> execution_state;

        IProcessor::PortNumbers updated_input_ports;
        IProcessor::PortNumbers updated_output_ports;

        Node(IProcessor * processor_, UInt64 processor_id)
            : processor(processor_), status(ExecStatus::Idle)
        {
            execution_state = std::make_unique<ExecutionState>();
            execution_state->processor = processor;
            execution_state->processors_id = processor_id;
            execution_state->has_quota = processor->hasQuota();

            status_mutex = std::make_unique<std::mutex>();
            post_updated_input_ports = std::make_unique<Port::UpdateInfo::UpdateList>();
            post_updated_output_ports = std::make_unique<Port::UpdateInfo::UpdateList>();
        }
    };

    using Nodes = std::vector<Node>;

    Nodes graph;

    using Stack = std::stack<UInt64>;

    class TaskQueue
    {
    public:
        void init(size_t num_threads) { queues.resize(num_threads); }

        void push(ExecutionState * state, size_t thread_num)
        {
            queues[thread_num].push(state);

            ++size_;

            if (state->has_quota)
                ++quota_;
        }

        size_t getAnyThreadWithTasks(size_t from_thread = 0)
        {
            if (size_ == 0)
                throw Exception("TaskQueue is empty.", ErrorCodes::LOGICAL_ERROR);

            for (size_t i = 0; i < queues.size(); ++i)
            {
                if (!queues[from_thread].empty())
                    return from_thread;

                ++from_thread;
                if (from_thread >= queues.size())
                    from_thread = 0;
            }

            throw Exception("TaskQueue is empty.", ErrorCodes::LOGICAL_ERROR);
        }

        ExecutionState * pop(size_t thread_num)
        {
            auto thread_with_tasks = getAnyThreadWithTasks(thread_num);

            ExecutionState * state = queues[thread_with_tasks].front();
            queues[thread_with_tasks].pop();

            --size_;

            if (state->has_quota)
                ++quota_;

            return state;
        }

        size_t size() const { return size_; }
        bool empty() const { return size_ == 0; }
        size_t quota() const { return quota_; }

    private:
        using Queue = std::queue<ExecutionState *>;
        std::vector<Queue> queues;
        size_t size_ = 0;
        size_t quota_ = 0;
    };

    /// Queue with pointers to tasks. Each thread will concurrently read from it until finished flag is set.
    /// Stores processors need to be prepared. Preparing status is already set for them.
    TaskQueue task_queue;

    ThreadsQueue threads_queue;
    std::mutex task_queue_mutex;

    /// Flag that checks that initializeExecution was called.
    bool is_execution_initialized = false;
    std::atomic_bool cancelled;
    std::atomic_bool finished;

    Poco::Logger * log = &Poco::Logger::get("PipelineExecutor");

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

        std::condition_variable condvar;
        std::mutex mutex;
        bool wake_flag = false;

        /// Currently processing state.
        ExecutionState * state = nullptr;

#ifndef NDEBUG
        /// Time for different processing stages.
        UInt64 total_time_ns = 0;
        UInt64 execution_time_ns = 0;
        UInt64 processing_time_ns = 0;
        UInt64 wait_time_ns = 0;
#endif

    };

    std::vector<std::unique_ptr<ExecutorContext>> executor_contexts;
    std::mutex executor_contexts_mutex;

    /// Processor ptr -> node number
    using ProcessorsMap = std::unordered_map<const IProcessor *, UInt64>;
    ProcessorsMap processors_map;

    /// Now it's used to check if query was killed.
    QueryStatus * process_list_element = nullptr;

    /// Graph related methods.
    bool addEdges(UInt64 node);
    void buildGraph();
    bool expandPipeline(Stack & stack, UInt64 pid);

    using Queue = std::queue<ExecutionState *>;

    /// Pipeline execution related methods.
    void addChildlessProcessorsToStack(Stack & stack);
    bool tryAddProcessorToStackIfUpdated(Edge & edge, Queue & queue, size_t thread_number);
    static void addJob(ExecutionState * execution_state);
    // TODO: void addAsyncJob(UInt64 pid);

    /// Prepare processor with pid number.
    /// Check parents and children of current processor and push them to stacks if they also need to be prepared.
    /// If processor wants to be expanded, ExpandPipelineTask from thread_number's execution context will be used.
    bool prepareProcessor(UInt64 pid, size_t thread_number, Queue & queue, std::unique_lock<std::mutex> node_lock);
    bool doExpandPipeline(ExpandPipelineTask * task, bool processing);

    /// Continue executor (in case there are tasks in queue).
    void wakeUpExecutor(size_t thread_num);

    void initializeExecution(size_t num_threads); /// Initialize executor contexts and task_queue.
    void finalizeExecution(); /// Check all processors are finished.

    void executeImpl(size_t num_threads);
    void executeStepImpl(size_t thread_num, size_t num_threads, std::atomic_bool * yield_flag = nullptr);
    void executeSingleThread(size_t thread_num, size_t num_threads);
    void finish();

    String dumpPipeline() const;
};

using PipelineExecutorPtr = std::shared_ptr<PipelineExecutor>;

}
