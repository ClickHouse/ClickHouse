#include <Processors/Executors/ExecutingGraph.h>
#include <stack>
#include <Common/Stopwatch.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ExecutingGraph::ExecutingGraph(Processors & processors_) : processors(processors_)
{
    uint64_t num_processors = processors.size();
    nodes.reserve(num_processors);

    /// Create nodes.
    for (uint64_t node = 0; node < num_processors; ++node)
    {
        IProcessor * proc = processors[node].get();
        processors_map[proc] = node;
        nodes.emplace_back(std::make_unique<Node>(proc, node));
    }

    /// Create edges.
    for (uint64_t node = 0; node < num_processors; ++node)
        addEdges(node);
}

ExecutingGraph::Edge & ExecutingGraph::addEdge(Edges & edges, Edge edge, const IProcessor * from, const IProcessor * to)
{
    auto it = processors_map.find(to);
    if (it == processors_map.end())
    {
        String msg = "Processor " + to->getName() + " was found as " + (edge.backward ? "input" : "output")
                     + " for processor " + from->getName() + ", but not found in list of processors.";

        throw Exception(msg, ErrorCodes::LOGICAL_ERROR);
    }

    edge.to = it->second;
    auto & added_edge = edges.emplace_back(std::move(edge));
    added_edge.update_info.id = &added_edge;
    return added_edge;
}

bool ExecutingGraph::addEdges(uint64_t node)
{
    IProcessor * from = nodes[node]->processor;

    bool was_edge_added = false;

    /// Add backward edges from input ports.
    auto & inputs = from->getInputs();
    auto from_input = nodes[node]->back_edges.size();

    if (from_input < inputs.size())
    {
        was_edge_added = true;

        for (auto it = std::next(inputs.begin(), from_input); it != inputs.end(); ++it, ++from_input)
        {
            const IProcessor * to = &it->getOutputPort().getProcessor();
            auto output_port_number = to->getOutputPortNumber(&it->getOutputPort());
            Edge edge(0, true, from_input, output_port_number, &nodes[node]->post_updated_input_ports);
            auto & added_edge = addEdge(nodes[node]->back_edges, std::move(edge), from, to);
            it->setUpdateInfo(&added_edge.update_info);
        }
    }

    /// Add direct edges form output ports.
    auto & outputs = from->getOutputs();
    auto from_output = nodes[node]->direct_edges.size();

    if (from_output < outputs.size())
    {
        was_edge_added = true;

        for (auto it = std::next(outputs.begin(), from_output); it != outputs.end(); ++it, ++from_output)
        {
            const IProcessor * to = &it->getInputPort().getProcessor();
            auto input_port_number = to->getInputPortNumber(&it->getInputPort());
            Edge edge(0, false, input_port_number, from_output, &nodes[node]->post_updated_output_ports);
            auto & added_edge = addEdge(nodes[node]->direct_edges, std::move(edge), from, to);
            it->setUpdateInfo(&added_edge.update_info);
        }
    }

    return was_edge_added;
}

bool ExecutingGraph::expandPipeline(std::stack<uint64_t> & stack, uint64_t pid)
{
    auto & cur_node = *nodes[pid];
    Processors new_processors;

    try
    {
        new_processors = cur_node.processor->expandPipeline();
    }
    catch (...)
    {
        cur_node.exception = std::current_exception();
        return false;
    }

    {
        std::lock_guard guard(processors_mutex);
        processors.insert(processors.end(), new_processors.begin(), new_processors.end());
    }

    uint64_t num_processors = processors.size();
    std::vector<uint64_t> back_edges_sizes(num_processors, 0);
    std::vector<uint64_t> direct_edge_sizes(num_processors, 0);

    for (uint64_t node = 0; node < nodes.size(); ++node)
    {
        direct_edge_sizes[node] = nodes[node]->direct_edges.size();
        back_edges_sizes[node] = nodes[node]->back_edges.size();
    }

    nodes.reserve(num_processors);

    while (nodes.size() < num_processors)
    {
        auto * processor = processors[nodes.size()].get();
        if (processors_map.count(processor))
            throw Exception("Processor " + processor->getName() + " was already added to pipeline.",
                            ErrorCodes::LOGICAL_ERROR);

        processors_map[processor] = nodes.size();
        nodes.emplace_back(std::make_unique<Node>(processor, nodes.size()));
    }

    std::vector<uint64_t> updated_nodes;

    for (uint64_t node = 0; node < num_processors; ++node)
    {
        if (addEdges(node))
            updated_nodes.push_back(node);
    }

    for (auto updated_node : updated_nodes)
    {
        auto & node = *nodes[updated_node];

        size_t num_direct_edges = node.direct_edges.size();
        size_t num_back_edges = node.back_edges.size();

        std::lock_guard guard(node.status_mutex);

        for (uint64_t edge = back_edges_sizes[updated_node]; edge < num_back_edges; ++edge)
            node.updated_input_ports.emplace_back(edge);

        for (uint64_t edge = direct_edge_sizes[updated_node]; edge < num_direct_edges; ++edge)
            node.updated_output_ports.emplace_back(edge);

        if (node.status == ExecutingGraph::ExecStatus::Idle)
        {
            node.status = ExecutingGraph::ExecStatus::Preparing;
            stack.push(updated_node);
        }
    }

    return true;
}

void ExecutingGraph::initializeExecution(Queue & queue)
{
    std::stack<uint64_t> stack;

    /// Add childless processors to stack.
    uint64_t num_processors = nodes.size();
    for (uint64_t proc = 0; proc < num_processors; ++proc)
    {
        if (nodes[proc]->direct_edges.empty())
        {
            stack.push(proc);
            /// do not lock mutex, as this function is executed in single thread
            nodes[proc]->status = ExecutingGraph::ExecStatus::Preparing;
        }
    }

    Queue async_queue;

    while (!stack.empty())
    {
        uint64_t proc = stack.top();
        stack.pop();

        updateNode(proc, queue, async_queue);

        if (!async_queue.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Async is only possible after work() call. Processor {}",
                            async_queue.front()->processor->getName());
    }
}


bool ExecutingGraph::updateNode(uint64_t pid, Queue & queue, Queue & async_queue)
{
    std::stack<Edge *> updated_edges;
    std::stack<uint64_t> updated_processors;
    updated_processors.push(pid);

    UpgradableMutex::ReadGuard read_lock(nodes_mutex);

    while (!updated_processors.empty() || !updated_edges.empty())
    {
        std::optional<std::unique_lock<std::mutex>> stack_top_lock;

        if (updated_processors.empty())
        {
            auto * edge = updated_edges.top();
            updated_edges.pop();

            /// Here we have ownership on edge, but node can be concurrently accessed.

            auto & node = *nodes[edge->to];

            std::unique_lock lock(node.status_mutex);

            ExecutingGraph::ExecStatus status = node.status;

            if (status != ExecutingGraph::ExecStatus::Finished)
            {
                if (edge->backward)
                    node.updated_output_ports.push_back(edge->output_port_number);
                else
                    node.updated_input_ports.push_back(edge->input_port_number);

                if (status == ExecutingGraph::ExecStatus::Idle)
                {
                    node.status = ExecutingGraph::ExecStatus::Preparing;
                    updated_processors.push(edge->to);
                    stack_top_lock = std::move(lock);
                }
                else
                    nodes[edge->to]->processor->onUpdatePorts();
            }
        }

        if (!updated_processors.empty())
        {
            pid = updated_processors.top();
            updated_processors.pop();

            /// In this method we have ownership on node.
            auto & node = *nodes[pid];

            bool need_expand_pipeline = false;

            if (!stack_top_lock)
                stack_top_lock.emplace(node.status_mutex);

            {
#ifndef NDEBUG
                Stopwatch watch;
#endif

                std::unique_lock<std::mutex> lock(std::move(*stack_top_lock));

                try
                {
                    node.last_processor_status = node.processor->prepare(node.updated_input_ports, node.updated_output_ports);
                }
                catch (...)
                {
                    node.exception = std::current_exception();
                    return false;
                }

#ifndef NDEBUG
                node.preparation_time_ns += watch.elapsed();
#endif

                node.updated_input_ports.clear();
                node.updated_output_ports.clear();

                switch (node.last_processor_status)
                {
                    case IProcessor::Status::NeedData:
                    case IProcessor::Status::PortFull:
                    {
                        node.status = ExecutingGraph::ExecStatus::Idle;
                        break;
                    }
                    case IProcessor::Status::Finished:
                    {
                        node.status = ExecutingGraph::ExecStatus::Finished;
                        break;
                    }
                    case IProcessor::Status::Ready:
                    {
                        node.status = ExecutingGraph::ExecStatus::Executing;
                        queue.push(&node);
                        break;
                    }
                    case IProcessor::Status::Async:
                    {
                        node.status = ExecutingGraph::ExecStatus::Executing;
                        async_queue.push(&node);
                        break;
                    }
                    case IProcessor::Status::ExpandPipeline:
                    {
                        need_expand_pipeline = true;
                        break;
                    }
                }

                if (!need_expand_pipeline)
                {
                    /// If you wonder why edges are pushed in reverse order,
                    /// it is because updated_edges is a stack, and we prefer to get from stack
                    /// input ports firstly, and then outputs, both in-order.
                    ///
                    /// Actually, there should be no difference in which order we process edges.
                    /// However, some tests are sensitive to it (e.g. something like SELECT 1 UNION ALL 2).
                    /// Let's not break this behaviour so far.

                    for (auto it = node.post_updated_output_ports.rbegin(); it != node.post_updated_output_ports.rend(); ++it)
                    {
                        auto * edge = static_cast<ExecutingGraph::Edge *>(*it);
                        updated_edges.push(edge);
                        edge->update_info.trigger();
                    }

                    for (auto it = node.post_updated_input_ports.rbegin(); it != node.post_updated_input_ports.rend(); ++it)
                    {
                        auto * edge = static_cast<ExecutingGraph::Edge *>(*it);
                        updated_edges.push(edge);
                        edge->update_info.trigger();
                    }

                    node.post_updated_input_ports.clear();
                    node.post_updated_output_ports.clear();
                }
            }

            if (need_expand_pipeline)
            {
                {
                    UpgradableMutex::WriteGuard lock(read_lock);
                    if (!expandPipeline(updated_processors, pid))
                        return false;
                }

                /// Add itself back to be prepared again.
                updated_processors.push(pid);
            }
        }
    }

    return true;
}

void ExecutingGraph::cancel()
{
    std::lock_guard guard(processors_mutex);
    for (auto & processor : processors)
        processor->cancel();
}

}
