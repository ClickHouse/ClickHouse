#pragma once

#include <queue>
#include <Processors/IProcessor.h>
#include <mutex>
#include <Common/ThreadPool.h>
#include <Common/EventCounter.h>

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
        UInt64 version = 1;
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
        size_t num_executed_jobs = 0;
    };

    struct Node
    {
        IProcessor * processor = nullptr;
        Edges directEdges;
        Edges backEdges;

        ExecStatus status = ExecStatus::New;
        IProcessor::Status last_processor_status = IProcessor::Status::NeedData;
        std::unique_ptr<ExecutionState> execution_state;

        Node() { execution_state = std::make_unique<ExecutionState>(); }
    };

    using Nodes = std::vector<Node>;

    Nodes graph;

    using Queue = std::queue<UInt64>;
    using TaskQueue = boost::lockfree::queue<ExecutionState *>;
    using FinishedJobsQueue = boost::lockfree::queue<UInt64>;

    /// Queue of processes which we want to call prepare. Is used only in main thread.
    Queue prepare_queue;
    /// Queue with pointers to tasks. Each thread will concurrently read from it until finished flag is set.
    TaskQueue task_queue;
    /// Queue with tasks which have finished execution.
    FinishedJobsQueue finished_execution_queue;

    EventCounter event_counter;

    UInt64 num_waited_tasks = 0;
    UInt64 num_tasks_to_wait = 0;

    std::atomic_bool cancelled;
    std::atomic_bool finished;

    std::vector<std::thread> threads;

    std::mutex task_mutex;
    std::condition_variable task_condvar;

public:
    explicit PipelineExecutor(Processors processors);
    void execute(size_t num_threads);

    String getName() const { return "PipelineExecutor"; }

    const Processors & getProcessors() const { return processors; }

    void cancel() { cancelled = true; }

private:
    /// Graph related methods.
    using ProcessorsMap = std::unordered_map<const IProcessor *, UInt64>;
    ProcessorsMap processors_map;

    bool addEdges(UInt64 node);
    void buildGraph();
    void expandPipeline(UInt64 pid);

    /// Pipeline execution related methods.
    void addChildlessProcessorsToQueue();
    void processFinishedExecutionQueue();
    void processFinishedExecutionQueueSafe();
    bool addProcessorToPrepareQueueIfUpdated(Edge & edge);
    void processPrepareQueue();
    void processAsyncQueue();

    void addJob(UInt64 pid);
    void addAsyncJob(UInt64 pid);

    void prepareProcessor(size_t pid, bool async);

    void executeImpl(size_t num_threads);

    String dumpPipeline() const;
};

using PipelineExecutorPtr = std::shared_ptr<PipelineExecutor>;

}
