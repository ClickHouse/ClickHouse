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

class PipelineExecutor
{
private:
    Processors processors;

    struct Edge
    {
        UInt64 to = std::numeric_limits<UInt64>::max();
        UInt64 version = 0;
        UInt64 prev_version = 0;
    };

    using Edges = std::list<Edge>;

    enum class ExecStatus
    {
        New,
        Idle,
        Preparing,
        Executing,
        Finished,
        Async
    };

    struct ExecutionState
    {
        std::exception_ptr exception;
        std::function<void()> job;

        IProcessor * processor;
        UInt64 processors_id;

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
        std::atomic_bool need_to_be_prepared;
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
    using FinishedJobsQueue = boost::lockfree::queue<UInt64>;

    /// Queue of processes which we want to call prepare. Is used only in main thread.
    Stack prepare_stack;
    /// Queue with pointers to tasks. Each thread will concurrently read from it until finished flag is set.
    TaskQueue task_queue;

    EventCounter event_counter;

    std::atomic<UInt64> num_waited_tasks;
    std::atomic<UInt64> num_tasks_to_wait;

    std::atomic_bool cancelled;
    std::atomic_bool finished;

    std::vector<std::thread> threads;

    std::mutex task_mutex;
    std::condition_variable task_condvar;

    Poco::Logger * log = &Poco::Logger::get("PipelineExecutor");

    struct ExecutorContext
    {
//        size_t executor_number;
//        std::atomic<ExecutionState *> next_task_to_execute;
        std::atomic_bool is_waiting;
        std::condition_variable condvar;
    };

    std::vector<std::unique_ptr<ExecutorContext>> executor_contexts;

    std::mutex main_executor_mutex;
    std::atomic_bool main_executor_flag;
    /// std::atomic_bool background_executor_flag;
    std::condition_variable main_executor_condvar;

    std::atomic<size_t> num_waiting_threads;
    std::condition_variable finish_condvar;

    std::atomic<size_t> num_preparing_threads;
    std::atomic<ExecutionState *> node_to_expand;
    std::mutex mutex_to_expand_pipeline;
    std::condition_variable condvar_to_expand_pipeline;
    size_t num_waiting_threads_to_expand_pipeline = 0;

public:
    explicit PipelineExecutor(Processors processors);
    void execute(size_t num_threads);

    String getName() const { return "PipelineExecutor"; }

    const Processors & getProcessors() const { return processors; }

    void finish();
    void cancel()
    {
        cancelled = true;
        finish();
    }

private:
    /// Graph related methods.
    using ProcessorsMap = std::unordered_map<const IProcessor *, UInt64>;
    ProcessorsMap processors_map;

    bool addEdges(UInt64 node);
    void buildGraph();
    void expandPipeline(Stack & stack, UInt64 pid);
    void doExpandPipeline(Stack & stack);

    /// Pipeline execution related methods.
    void addChildlessProcessorsToQueue(Stack & stack);
    bool addProcessorToPrepareQueueIfUpdated(Edge & edge, Stack & stack);
    // void processPrepareQueue();
    // void processAsyncQueue();

    void addJob(ExecutionState * execution_state);
    void addAsyncJob(UInt64 pid);
    // bool tryAssignJob(ExecutionState * state);
    // void assignJobs();

    bool prepareProcessor(size_t pid, Stack & stack, bool async);

    void executeImpl(size_t num_threads);
    void executeSingleThread(size_t thread_num, size_t num_threads);

    String dumpPipeline() const;
};

using PipelineExecutorPtr = std::shared_ptr<PipelineExecutor>;

}
