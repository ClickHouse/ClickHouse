#include <Processors/Executors/PipelineExecutor.h>
#include <unordered_map>
#include <queue>

namespace DB
{

namespace
{


}

void PipelineExecutor::buildGraph()
{
    std::unordered_map<const IProcessor *, size_t> proc_map;
    size_t num_processors = processors.size();

    auto throwUnknownProcessor = [](const IProcessor * proc, const IProcessor * parent, bool from_input_port)
    {
        String msg = "Processor " + proc->getName() + " was found as " + (from_input_port ? "input" : "output")
                + " for processor " + parent->getName() + ", but not found in original list or all processors.";

        throw Exception(msg, ErrorCodes::LOGICAL_ERROR);
    };

    graph.resize(num_processors);
    for (size_t node = 0; node < num_processors; ++node)
    {
        IProcessor * proc = processors[node].get();
        proc_map[proc] = node;
        graph[node].processor = proc;
    }

    for (size_t node = 0; node < num_processors; ++node)
    {
        const IProcessor * cur = graph[node].processor;

        for (const InputPort & input_port : processors[node]->getInputs())
        {
            const IProcessor * proc = &input_port.getProcessor();

            auto it = proc_map.find(proc);
            if (it == proc_map.end())
                throwUnknownProcessor(proc, cur, true);

            size_t proc_num = it->second;
            bool new_edge = true;
            for (size_t edge = 0; new_edge && edge < graph[node].backEdges.size(); ++edge)
                if (graph[node].backEdges[edge].from == proc_num)
                    new_edge = false;

            if (new_edge)
            {
                graph[node].backEdges.emplace_back();
                graph[node].backEdges.back().from = proc_num;
            }
        }

        for (const OutputPort & output_port : processors[node]->getOutputs())
        {
            const IProcessor * proc = &output_port.getProcessor();

            auto it = proc_map.find(proc);
            if (it == proc_map.end())
                throwUnknownProcessor(proc, cur, true);

            size_t proc_num = it->second;
            bool new_edge = true;
            for (size_t edge = 0; new_edge && edge < graph[node].directEdges.size(); ++edge)
                if (graph[node].directEdges[edge].to == proc_num)
                    new_edge = false;

            if (new_edge)
            {
                graph[node].directEdges.emplace_back();
                graph[node].directEdges.back().to = proc_num;
            }
        }
    }
}

void PipelineExecutor::prepareProcessor(std::queue<size_t> & jobs, size_t pid)
{
    auto & node = graph[pid];
    auto status = node.processor->prepare();

    switch (status)
    {
        case IProcessor::Status::NeedData:
        case IProcessor::Status::PortFull:
        case IProcessor::Status::Unneeded:
        {
            node.status = ExecStatus::Idle;
            node.idle_status = status;
            break;
        }
        case IProcessor::Status::Finished:
        {
            node.status = ExecStatus::Finished;
            break;
        }
        case IProcessor::Status::Ready:
        {
            jobs.push(pid);
            break;
        }
        case IProcessor::Status::Async:
        {
            throw Exception("Async is not supported for PipelineExecutor", ErrorCodes::LOGICAL_ERROR);
        }
        case IProcessor::Status::Wait:
        {
            throw Exception("Wait is not supported for PipelineExecutor", ErrorCodes::LOGICAL_ERROR);
        }
    }
}

void PipelineExecutor::traverse(std::queue<size_t> & preparing, size_t pid)
{
    for (const auto & edge : graph[pid].directEdges)
    {
        auto & node = graph[edge.to];
        if (node.status == ExecStatus::Idle)
        {
            preparing.push(edge.to);
            node.status = ExecStatus::Preparing;
        }
    }

    for (const auto & edge : graph[pid].backEdges)
    {
        auto & node = graph[edge.from];
        if (node.status == ExecStatus::Idle)
        {
            preparing.push(edge.from);
            node.status = ExecStatus::Preparing;
        }
    }
}

void PipelineExecutor::execute()
{
    std::queue<size_t> jobs;
    std::queue<size_t> preparing;

    size_t num_nodes = graph.size();
    for (size_t i = 0; i < num_nodes; ++i)
    {
        if (graph[i].directEdges.empty())
        {
            preparing.push(i);
            graph[i].status = ExecStatus::Preparing;
        }
    }

    if (preparing.empty())
        throw Exception("No sync processors were found.", ErrorCodes::LOGICAL_ERROR);

    while (!jobs.empty() || !preparing.empty())
    {
        while (!jobs.empty())
        {
            size_t pid = jobs.front();
            jobs.pop();
            addJob(pid);
            /// Make a job
        }

        while (!preparing.empty())
        {
            size_t pid = preparing.front();
            preparing.pop();
            prepareProcessor(jobs, pid);
        }
    }

}

}
