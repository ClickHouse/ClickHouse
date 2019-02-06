#pragma once

#include <queue>
#include <Processors/IProcessor.h>

template <typename>
class ThreadPoolImpl;
class ThreadFromGlobalPool;
using ThreadPool = ThreadPoolImpl<ThreadFromGlobalPool>;

namespace DB
{

class PipelineExecutor
{
private:
    Processors processors;
    ThreadPool & pool;

    struct DirectEdge
    {
        size_t to;
    };

    using DirectEdges = std::vector<DirectEdge>;

    struct BackEdge
    {
        size_t from;
    };

    using BackEdges = std::vector<BackEdge>;

    enum class ExecStatus
    {
        None,
        Preparing,
        Executing,
        Finished,
        Idle, /// Prepare was called, but some error status was returned
    };

    struct Node
    {
        IProcessor * processor = nullptr;
        DirectEdges directEdges;
        BackEdges backEdges;

        ExecStatus status = ExecStatus::None;
        IProcessor::Status idle_status; /// What prepare() returned if status is Idle.
    };

    using Nodes = std::vector<Node>;

    Nodes graph;

public:
    PipelineExecutor(const Processors & processors, ThreadPool & pool) : processors(processors), pool(pool) {}
    void execute();

    String getName() const { return "PipelineExecutor"; }

private:
    void buildGraph();
    void prepareProcessor(std::queue<size_t> & jobs, size_t pid);
    void traverse(std::queue<size_t> & preparing, size_t pid);
};

}
