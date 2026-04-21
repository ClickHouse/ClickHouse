#include <Processors/Executors/ExecutingGraph.h>
#include <Processors/IProcessor.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentThread.h>

#include <shared_mutex>
#include <stack>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ExecutingGraph::ExecutingGraph(std::shared_ptr<Processors> processors_, bool profile_processors_)
    : processors(std::move(processors_))
    , profile_processors(profile_processors_)
{
    /// Create nodes for every processor.
    for (auto it = processors->begin(); it != processors->end(); ++it)
        addNode(it);

    /// Create edges.
    for (auto & node : nodes)
        addEdges(node);
}

ExecutingGraph::Node & ExecutingGraph::addNode(Processors::iterator processor_iter)
{
    IProcessor * processor = processor_iter->get();
    auto & new_node = nodes.emplace_back(processor_iter, next_node_id++);

    const auto [_, inserted] = processors_map.emplace(processor, &new_node);
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} was already added to pipeline", processor->getName());

    return new_node;
}

ExecutingGraph::Node & ExecutingGraph::addNode(ProcessorPtr processor)
{
    processors->push_back(std::move(processor));
    return addNode(std::prev(processors->end()));
}

ExecutingGraph::Edge & ExecutingGraph::addEdge(Edges & edges, Edge edge, const IProcessor * from, const IProcessor * to)
{
    auto it = processors_map.find(to);
    if (it == processors_map.end())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Processor {} was found as {} for processor {}, but not found in list of processors",
            to->getName(),
            edge.backward ? "input" : "output",
            from->getName());

    edge.to = it->second;
    auto & added_edge = edges.emplace_back(std::move(edge));
    added_edge.update_info.id = &added_edge;
    return added_edge;
}

ExecutingGraph::NewEdges ExecutingGraph::addEdges(Node & node)
{
    IProcessor * from = node.processor();
    NewEdges result;

    /// Backward edges from input ports (input_port -> peer's output_port).
    for (auto & input : from->getInputs())
    {
        if (input.hasUpdateInfo() || !input.isConnected())
            continue;

        const IProcessor * to = &input.getOutputPort().getProcessor();
        Edge edge(nullptr, true, &input, &input.getOutputPort(), &node.post_updated_input_ports);
        auto & added_edge = addEdge(node.back_edges, std::move(edge), from, to);
        input.setUpdateInfo(&added_edge.update_info);
        result.back.push_back(&added_edge);
    }

    /// Direct edges from output ports (output_port -> peer's input_port).
    for (auto & output : from->getOutputs())
    {
        if (output.hasUpdateInfo() || !output.isConnected())
            continue;

        const IProcessor * to = &output.getInputPort().getProcessor();
        Edge edge(nullptr, false, &output.getInputPort(), &output, &node.post_updated_output_ports);
        auto & added_edge = addEdge(node.direct_edges, std::move(edge), from, to);
        output.setUpdateInfo(&added_edge.update_info);
        result.direct.push_back(&added_edge);
    }

    return result;
}

ExecutingGraph::UpdateNodeStatus ExecutingGraph::expandPipeline(boost::container::devector<Node *> & stack, Node & cur_node)
{
    Processors new_processors;

    try
    {
        new_processors = cur_node.processor()->expandPipeline();
    }
    catch (...)
    {
        cur_node.exception = std::current_exception();
        return UpdateNodeStatus::Exception;
    }

    {
        std::lock_guard guard(processors_mutex);

        /// Even if the query was already cancelled, we must still add the new processors to the list
        /// to keep them alive, because their ports are already connected to existing processors.
        /// Destroying them here would leave dangling port references, causing use-after-free
        /// (e.g. pure virtual call in `dumpPipeline`).
        for (const auto & new_proc : new_processors)
            addNode(new_proc);

        if (cancel_reason != IProcessor::CancelReason::NotCancelled)
        {
            /// Propagate the cancellation reason to newly added processors. They decide how to react.
            for (auto & processor : new_processors)
                processor->cancel(cancel_reason);

            /// For PartialResult the pipeline must still run to drain all left data, so continue normally.
            if (cancel_reason != IProcessor::CancelReason::PartialResult)
                return UpdateNodeStatus::Cancelled;
        }
    }

    /// Build new edges for every node.
    std::vector<std::pair<Node *, NewEdges>> added_edges;
    for (auto & node : nodes)
        if (auto new_edges = addEdges(node); !new_edges.empty())
            added_edges.emplace_back(&node, std::move(new_edges));

    /// Record updated ports for each newly added edge for each processor and schedule it for prepare if something changed.
    for (auto & [updated_node, new_edges] : added_edges)
    {
        /// Record the updated port pointers so the next prepare() sees them as "updated".
        for (auto * edge : new_edges.back)
            updated_node->updated_input_ports.emplace_back(edge->input_port);

        for (auto * edge : new_edges.direct)
            updated_node->updated_output_ports.emplace_back(edge->output_port);

        if (updated_node->status == ExecutingGraph::ExecStatus::Idle)
        {
            updated_node->status = ExecutingGraph::ExecStatus::Preparing;
            stack.push_front(updated_node);
        }
    }

    return UpdateNodeStatus::Done;
}

void ExecutingGraph::initializeExecution(Queue & queue, Queue & async_queue)
{
    std::stack<Node *> stack;

    /// Add childless processors to stack.
    for (auto & node : nodes)
    {
        if (node.direct_edges.empty())
        {
            stack.push(&node);
            /// do not lock mutex, as this function is executed in single thread
            node.status = ExecutingGraph::ExecStatus::Preparing;
        }
    }

    while (!stack.empty())
    {
        Node * node = stack.top();
        stack.pop();

        updateNode(node, queue, async_queue);
    }
}

ExecutingGraph::UpdateNodeStatus ExecutingGraph::updateNode(Node * start_node, Queue & queue, Queue & async_queue)
{
    boost::container::devector<Edge *> updated_edges;
    boost::container::devector<Node *> updated_processors;
    updated_processors.push_back(start_node);

    std::shared_lock read_lock(nodes_mutex);

    while (!updated_processors.empty() || !updated_edges.empty())
    {
        std::optional<std::unique_lock<std::mutex>> stack_top_lock;

        if (updated_processors.empty())
        {
            auto * edge = updated_edges.front();
            updated_edges.pop_front();

            /// Here we have ownership on edge, but node can be concurrently accessed.

            auto & node = *edge->to;

            std::unique_lock lock(node.status_mutex);

            ExecutingGraph::ExecStatus status = node.status;

            if (status != ExecutingGraph::ExecStatus::Finished)
            {
                if (edge->backward)
                    node.updated_output_ports.push_back(edge->output_port);
                else
                    node.updated_input_ports.push_back(edge->input_port);

                if (status == ExecutingGraph::ExecStatus::Idle)
                {
                    node.status = ExecutingGraph::ExecStatus::Preparing;
                    updated_processors.push_front(edge->to);
                    stack_top_lock = std::move(lock);
                }
                else
                    edge->to->processor()->onUpdatePorts();
            }
        }

        if (!updated_processors.empty())
        {
            Node * current = updated_processors.front();
            updated_processors.pop_front();

            /// In this method we have ownership on node.
            auto & node = *current;

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
                    auto & processor = *node.processor();
                    const auto last_status = node.last_processor_status;
                    IProcessor::Status status = processor.prepare(node.updated_input_ports, node.updated_output_ports);
                    node.last_processor_status = status;
                    if (status == IProcessor::Status::Finished && CurrentThread::getGroup())
                        CurrentThread::getGroup()->memory_spill_scheduler->remove(&processor);

                    if (profile_processors)
                    {
                        /// NeedData
                        if (last_status != IProcessor::Status::NeedData && status == IProcessor::Status::NeedData)
                        {
                            processor.input_wait_watch.restart();
                        }
                        else if (last_status == IProcessor::Status::NeedData && status != IProcessor::Status::NeedData)
                        {
                            processor.input_wait_elapsed_ns += processor.input_wait_watch.elapsedNanoseconds();
                        }

                        /// PortFull
                        if (last_status != IProcessor::Status::PortFull && status == IProcessor::Status::PortFull)
                        {
                            processor.output_wait_watch.restart();
                        }
                        else if (last_status == IProcessor::Status::PortFull && status != IProcessor::Status::PortFull)
                        {
                            processor.output_wait_elapsed_ns += processor.output_wait_watch.elapsedNanoseconds();
                        }
                    }
                }
                catch (...)
                {
                    node.exception = std::current_exception();
                    return UpdateNodeStatus::Exception;
                }

#ifndef NDEBUG
                node.preparation_time_ns += watch.elapsed();
#endif

                node.updated_input_ports.clear();
                node.updated_output_ports.clear();

                switch (*node.last_processor_status)
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
                        updated_edges.push_front(edge);
                        edge->update_info.trigger();
                    }

                    for (auto it = node.post_updated_input_ports.rbegin(); it != node.post_updated_input_ports.rend(); ++it)
                    {
                        auto * edge = static_cast<ExecutingGraph::Edge *>(*it);
                        updated_edges.push_front(edge);
                        edge->update_info.trigger();
                    }

                    node.post_updated_input_ports.clear();
                    node.post_updated_output_ports.clear();
                }
            }

            if (need_expand_pipeline)
            {
                // We do not need to upgrade lock atomically, so we can safely release shared_lock and acquire unique_lock
                read_lock.unlock();
                {
                    std::unique_lock lock(nodes_mutex);
                    auto status = expandPipeline(updated_processors, node);
                    if (status != UpdateNodeStatus::Done)
                        return status;
                }
                read_lock.lock();

                /// Add itself back to be prepared again.
                updated_processors.push_front(current);
            }
        }
    }

    return UpdateNodeStatus::Done;
}

void ExecutingGraph::cancel(IProcessor::CancelReason reason)
{
    std::exception_ptr exception_ptr;

    {
        std::lock_guard guard(processors_mutex);

        if (cancel_reason == IProcessor::CancelReason::NotCancelled)
            cancel_reason = reason;
        else if (cancel_reason == IProcessor::CancelReason::PartialResult && reason != IProcessor::CancelReason::PartialResult)
            cancel_reason = reason;

        for (auto & processor : *processors)
        {
            try
            {
                processor->cancel(cancel_reason);
            }
            catch (...)
            {
                if (!exception_ptr)
                    exception_ptr = std::current_exception();

                /// Log any exception since:
                /// a) they are pretty rare (the only that I know is from
                ///    RemoteQueryExecutor)
                /// b) there can be exception during query execution, and in this
                ///    case, this exception can be ignored (not showed to the user).
                tryLogCurrentException("ExecutingGraph");
            }
        }
    }

    if (exception_ptr)
        std::rethrow_exception(exception_ptr);
}

}
