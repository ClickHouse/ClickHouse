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
    };

    struct Node
    {
        IProcessor * processor = nullptr;
        Edges directEdges;
        Edges backEdges;

        ExecStatus status = ExecStatus::New;
        IProcessor::Status last_processor_status;
        std::unique_ptr<ExecutionState> execution_state;
    };

    using Nodes = std::vector<Node>;

    Nodes graph;

    using Queue = std::queue<UInt64>;
    using TaskQueue = boost::lockfree::queue<ExecutionState *>;

    /// Queue of processes which we want to call prepare. Is used only in main thread.
    Queue prepare_queue;
    TaskQueue task_queue;

    using FinishedJobsQueue = boost::lockfree::queue<UInt64>;
    FinishedJobsQueue finished_execution_queue;

    /// std::mutex finished_execution_mutex;

    EventCounter event_counter;

    UInt64 num_waited_tasks = 0;
    UInt64 num_tasks_to_wait = 0;

    std::atomic_bool cancelled;
    std::atomic_bool finished;

public:
    explicit PipelineExecutor(Processors processors);
    void execute(ThreadPool * pool = nullptr);

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

    template <typename TQueue>
    void processFinishedExecutionQueue(TQueue & queue);

    template <typename TQueue>
    void processFinishedExecutionQueueSafe(TQueue & queue, ThreadPool * pool);

    bool addProcessorToPrepareQueueIfUpdated(Edge & edge);

    template <typename TQueue>
    void processPrepareQueue(TQueue & queue, ThreadPool * pool);

    template <typename TQueue>
    void processAsyncQueue(TQueue & queue, ThreadPool * pool);

    template <typename TQueue>
    void addJob(UInt64 pid, TQueue & queue, ThreadPool * pool);
    void addAsyncJob(UInt64 pid);

    template <typename TQueue>
    void prepareProcessor(size_t pid, bool async, TQueue & queue, ThreadPool * pool);

    void executeImpl(ThreadPool * pool);

    String dumpPipeline() const;
};

using PipelineExecutorPtr = std::shared_ptr<PipelineExecutor>;

}
