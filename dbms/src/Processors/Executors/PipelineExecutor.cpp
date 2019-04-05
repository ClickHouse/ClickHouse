#include <Processors/Executors/PipelineExecutor.h>
#include <unordered_map>
#include <queue>
#include <IO/WriteBufferFromString.h>
#include <Processors/printPipeline.h>
#include <Common/EventCounter.h>
#include <ext/scope_guard.h>

namespace DB
{

PipelineExecutor::PipelineExecutor(Processors processors, ThreadPool * pool)
    : processors(std::move(processors)), pool(pool), cancelled(false)
{
    buildGraph();
}

void PipelineExecutor::addEdges(const ProcessorsMap & processors_map, UInt64 node, UInt64 from_input, UInt64 from_output)
{
    auto throwUnknownProcessor = [](const IProcessor * proc, const IProcessor * parent, bool from_input_port)
    {
        String msg = "Processor " + proc->getName() + " was found as " + (from_input_port ? "input" : "output")
                     + " for processor " + parent->getName() + ", but not found in list of processors.";

        throw Exception(msg, ErrorCodes::LOGICAL_ERROR);
    };

    const IProcessor * cur = graph[node].processor;

    auto add_edge = [&](auto & from_port, const IProcessor * to_proc, Edges & edges)
    {
        auto it = processors_map.find(to_proc);
        if (it == processors_map.end())
            throwUnknownProcessor(to_proc, cur, true);

        UInt64 proc_num = it->second;
        Edge * edge_ptr = nullptr;

        for (auto & edge : edges)
            if (edge.to == proc_num)
                edge_ptr = &edge;

        if (!edge_ptr)
        {
            edge_ptr = &edges.emplace_back();
            edge_ptr->to = proc_num;
        }

        from_port.setVersion(&edge_ptr->version);
    };

    auto & inputs = processors[node]->getInputs();
    for (auto it = std::next(inputs.begin(), from_input); it != inputs.end(); ++it)
    {
        const IProcessor * proc = &it->getOutputPort().getProcessor();
        add_edge(*it, proc, graph[node].backEdges);
    }

    auto & outputs = processors[node]->getOutputs();
    for (auto it = std::next(outputs.begin(), from_output); it != outputs.end(); ++it)
    {
        const IProcessor * proc = &it->getInputPort().getProcessor();
        add_edge(*it, proc, graph[node].directEdges);
    }
}

void PipelineExecutor::buildGraph()
{
    ProcessorsMap processors_map;
    UInt64 num_processors = processors.size();

    graph.resize(num_processors);
    for (UInt64 node = 0; node < num_processors; ++node)
    {
        IProcessor * proc = processors[node].get();
        processors_map[proc] = node;
        graph[node].processor = proc;
    }

    for (UInt64 node = 0; node < num_processors; ++node)
        addEdges(processors_map, node, 0, 0);
}

void PipelineExecutor::addChildlessProcessorsToQueue()
{
    UInt64 num_processors = processors.size();
    for (UInt64 proc = 0; proc < num_processors; ++proc)
    {
        if (graph[proc].directEdges.empty())
        {
            prepare_queue.push(proc);
            graph[proc].status = ExecStatus::Preparing;
        }
    }
}

void PipelineExecutor::processFinishedExecutionQueue()
{
    while (!finished_execution_queue.empty())
    {
        UInt64 proc = finished_execution_queue.front();
        finished_execution_queue.pop();

        graph[proc].status = ExecStatus::Preparing;
        prepare_queue.push(proc);
    }
}

void PipelineExecutor::processFinishedExecutionQueueSafe()
{
    if (pool)
    {
        exception_handler.throwIfException();
        std::lock_guard lock(finished_execution_mutex);
        processFinishedExecutionQueue();
    }
    else
        processFinishedExecutionQueue();
}

bool PipelineExecutor::addProcessorToPrepareQueueIfUpdated(Edge & edge)
{
    auto & node = graph[edge.to];

    /// Don't add processor if nothing was read from port.
    if (node.status != ExecStatus::New && edge.version == edge.prev_version)
        return false;

    edge.prev_version = edge.version;

    if (node.status == ExecStatus::Idle || node.status == ExecStatus::New)
    {
        prepare_queue.push(edge.to);
        node.status = ExecStatus::Preparing;
        return true;
    }

    return false;
}

void PipelineExecutor::addJob(UInt64 pid)
{
    if (pool)
    {
        auto job = [this, pid]()
        {
            SCOPE_EXIT(event_counter.notify());

            graph[pid].processor->work();

            {
                std::lock_guard lock(finished_execution_mutex);
                finished_execution_queue.push(pid);
            }
        };

        pool->schedule(createExceptionHandledJob(std::move(job), exception_handler));
        ++num_tasks_to_wait;
    }
    else
    {
        /// Execute task in main thread.
        graph[pid].processor->work();
        finished_execution_queue.push(pid);
    }
}

void PipelineExecutor::addAsyncJob(UInt64 pid)
{
    graph[pid].processor->schedule(event_counter);
    graph[pid].status = ExecStatus::Async;
    ++num_tasks_to_wait;
}

void PipelineExecutor::expendPipeline(UInt64 pid)
{
    auto & cur_node = graph[pid];
    UInt64 from_input = cur_node.processor->getInputs().size();
    UInt64 from_output = cur_node.processor->getOutputs().size();
    UInt64 from_processor = processors.size();
    auto new_processors = cur_node.processor->expandPipeline();

    ProcessorsMap processors_map;
    processors_map[cur_node.processor] = pid;
    for (const auto & processor : new_processors)
    {
        processors_map[processor.get()] = graph.size();
        graph.emplace_back();
        graph.back().processor = processor.get();
    }

    processors.insert(processors.end(), new_processors.begin(), new_processors.end());
    UInt64 num_processors = processors.size();

    for (UInt64 node = from_processor; node < num_processors; ++node)
    {
        addEdges(processors_map, node, 0, 0);
        prepare_queue.push(node);
        graph[node].status = ExecStatus::Preparing;
    }

    addEdges(processors_map, pid, from_input, from_output);
}

void PipelineExecutor::prepareProcessor(UInt64 pid, bool async)
{
    auto & node = graph[pid];
    auto status = node.processor->prepare();
    node.last_processor_status = status;

    auto add_neighbours_to_prepare_queue = [&, this]
    {
        for (auto & edge : node.directEdges)
            addProcessorToPrepareQueueIfUpdated(edge);

        for (auto & edge : node.backEdges)
            addProcessorToPrepareQueueIfUpdated(edge);
    };

    switch (status)
    {
        case IProcessor::Status::NeedData:
        {
            add_neighbours_to_prepare_queue();
            node.status = ExecStatus::Idle;
            break;
        }
        case IProcessor::Status::PortFull:
        {
            add_neighbours_to_prepare_queue();
            node.status = ExecStatus::Idle;
            break;
        }
        case IProcessor::Status::Finished:
        {
            add_neighbours_to_prepare_queue();
            node.status = ExecStatus::Finished;
            break;
        }
        case IProcessor::Status::Ready:
        {
            node.status = ExecStatus::Executing;
            addJob(pid);
            break;
        }
        case IProcessor::Status::Async:
        {
            node.status = ExecStatus::Executing;
            addAsyncJob(pid);
            break;
        }
        case IProcessor::Status::Wait:
        {
            if (!async)
                throw Exception("Processor returned status Wait before Async.", ErrorCodes::LOGICAL_ERROR);
            break;
        }
        case IProcessor::Status::ExpandPipeline:
        {
            expendPipeline(pid);
            /// Add node to queue again.
            prepare_queue.push(pid);

            /// node ref is not valid now.
            graph[pid].status = ExecStatus::Preparing;
            break;
        }
    }
}

void PipelineExecutor::processPrepareQueue()
{
    while (!prepare_queue.empty())
    {
        UInt64 proc = prepare_queue.front();
        prepare_queue.pop();

        prepareProcessor(proc, false);

    }
}

void PipelineExecutor::processAsyncQueue()
{
    UInt64 num_processors = processors.size();
    for (UInt64 node = 0; node < num_processors; ++node)
        if (graph[node].status == ExecStatus::Async)
            prepareProcessor(node, true);
}

void PipelineExecutor::execute()
{
    addChildlessProcessorsToQueue();

    while (!cancelled)
    {
        processFinishedExecutionQueueSafe();
        processPrepareQueue();
        processAsyncQueue();

        if (prepare_queue.empty())
        {
            /// For single-thread executor.
            if (!pool && !finished_execution_queue.empty())
                continue;

            if (num_tasks_to_wait > num_waited_tasks)
            {
                /// Try wait anything.
                event_counter.wait();
                ++num_waited_tasks;
            }
            else
            {
                /// Here prepare_queue is empty and we have nobody to wait for. Exiting.
                break;
            }
        }
    }

    bool all_processors_finished = true;
    for (auto & node : graph)
        if (node.status != ExecStatus::Finished)
            all_processors_finished = false;

    if (!all_processors_finished)
    {
        /// It seems that pipeline has stuck.

        std::vector<IProcessor::Status> statuses;
        std::vector<IProcessor *> proc_list;
        statuses.reserve(graph.size());
        proc_list.reserve(graph.size());

        for (auto & proc : graph)
        {
            proc_list.emplace_back(proc.processor);
            statuses.emplace_back(proc.last_processor_status);
        }

        WriteBufferFromOwnString out;
        printPipeline(processors, statuses, out);
        out.finish();

        throw Exception("Pipeline stuck. Current state:\n" + out.str(), ErrorCodes::LOGICAL_ERROR);
    }
}

}
