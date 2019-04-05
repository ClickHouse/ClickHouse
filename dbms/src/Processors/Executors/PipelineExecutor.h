#pragma once

#include <queue>
#include <Processors/IProcessor.h>
#include <mutex>
#include <Common/ThreadPool.h>
#include <Common/EventCounter.h>

namespace DB
{

class PipelineExecutor
{
private:
    Processors processors;
    ThreadPool * pool;

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

    struct Node
    {
        IProcessor * processor = nullptr;
        Edges directEdges;
        Edges backEdges;

        ExecStatus status = ExecStatus::New;
        IProcessor::Status last_processor_status;
    };

    using Nodes = std::vector<Node>;

    Nodes graph;

    using Queue = std::queue<UInt64>;

    /// Queue of processes which we want to call prepare. Is used only in main thread.
    Queue prepare_queue;
    /// Queue of processes which have finished execution. Must me used with mutex if executing with pool.
    Queue finished_execution_queue;
    std::mutex finished_execution_mutex;
    ExceptionHandler exception_handler;
    EventCounter event_counter;

    UInt64 num_waited_tasks = 0;
    UInt64 num_tasks_to_wait = 0;

    std::atomic_bool cancelled;

public:
    explicit PipelineExecutor(Processors processors, ThreadPool * pool = nullptr);
    void execute();

    String getName() const { return "PipelineExecutor"; }

    const Processors & getProcessors() const { return processors; }

    void cancel() { cancelled = true; }

private:
    /// Graph related methods.
    using ProcessorsMap = std::unordered_map<const IProcessor *, UInt64>;
    void addEdges(const ProcessorsMap & processors_map, UInt64 node, UInt64 from_input, UInt64 from_output);
    void buildGraph();
    void expendPipeline(UInt64 pid);

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
};

using PipelineExecutorPtr = std::shared_ptr<PipelineExecutor>;

}
