#include <Processors/Executors/PipelineExecutor.h>
#include <unordered_map>
#include <queue>
#include <IO/WriteBufferFromString.h>
#include <Processors/printPipeline.h>
#include <Common/EventCounter.h>
#include <ext/scope_guard.h>
#include <Common/CurrentThread.h>

#include <boost/lockfree/queue.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int QUOTA_EXPIRED;
    extern const int QUERY_WAS_CANCELLED;
}

static bool checkCanAddAdditionalInfoToException(const DB::Exception & exception)
{
    /// Don't add additional info to limits and quota exceptions, and in case of kill query (to pass tests).
    return exception.code() != ErrorCodes::TOO_MANY_ROWS_OR_BYTES
           && exception.code() != ErrorCodes::QUOTA_EXPIRED
           && exception.code() != ErrorCodes::QUERY_WAS_CANCELLED;
}

PipelineExecutor::PipelineExecutor(Processors processors)
    : processors(std::move(processors)), cancelled(false), finished(false)
{
    buildGraph();
}

bool PipelineExecutor::addEdges(UInt64 node)
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

    bool was_edge_added = false;

    auto & inputs = processors[node]->getInputs();
    auto from_input = graph[node].backEdges.size();

    if (from_input < inputs.size())
    {
        was_edge_added = true;

        for (auto it = std::next(inputs.begin(), from_input); it != inputs.end(); ++it)
        {
            const IProcessor * proc = &it->getOutputPort().getProcessor();
            add_edge(*it, proc, graph[node].backEdges);
        }
    }

    auto & outputs = processors[node]->getOutputs();
    auto from_output = graph[node].directEdges.size();

    if (from_output < outputs.size())
    {
        was_edge_added = true;

        for (auto it = std::next(outputs.begin(), from_output); it != outputs.end(); ++it)
        {
            const IProcessor * proc = &it->getInputPort().getProcessor();
            add_edge(*it, proc, graph[node].directEdges);
        }
    }

    return was_edge_added;
}

void PipelineExecutor::buildGraph()
{
    UInt64 num_processors = processors.size();

    graph.resize(num_processors);
    for (UInt64 node = 0; node < num_processors; ++node)
    {
        IProcessor * proc = processors[node].get();
        processors_map[proc] = node;
        graph[node].processor = proc;
    }

    for (UInt64 node = 0; node < num_processors; ++node)
        addEdges(node);
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
    UInt64 finished_job = graph.size();
    while (finished_execution_queue.pop(finished_job))
    {
        if (graph[finished_job].execution_state->exception)
            std::rethrow_exception(graph[finished_job].execution_state->exception);

        graph[finished_job].status = ExecStatus::Preparing;
        prepare_queue.push(finished_job);
    }
}

void PipelineExecutor::processFinishedExecutionQueueSafe()
{
//    if (pool)
//    {
//        /// std::lock_guard lock(finished_execution_mutex);
//        processFinishedExecutionQueue(queue);
//    }
//    else
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

static void executeJob(IProcessor * processor)
{
    try
    {
        processor->work();
    }
    catch (Exception & exception)
    {
        if (checkCanAddAdditionalInfoToException(exception))
            exception.addMessage("While executing " + processor->getName() + " ("
                                 + toString(reinterpret_cast<std::uintptr_t>(processor)) + ") ");
        throw;
    }
}

void PipelineExecutor::addJob(UInt64 pid)
{
    if (!threads.empty())
    {
        if (!graph[pid].execution_state)
            graph[pid].execution_state = std::make_unique<ExecutionState>();

        auto job = [this, pid, processor = graph[pid].processor, &exception = graph[pid].execution_state->exception]()
        {
            SCOPE_EXIT(
                    while (!finished_execution_queue.push(pid));
                    event_counter.notify()
            );

            try
            {
                executeJob(processor);
            }
            catch (...)
            {
                /// Note: It's important to save exception before pushing pid to finished_execution_queue
                exception = std::current_exception();
            }
        };

        graph[pid].execution_state->job = std::move(job);

        while (!task_queue.push(graph[pid].execution_state.get()));
        ++num_tasks_to_wait;
    }
    else
    {
        /// Execute task in main thread.
        executeJob(graph[pid].processor);
        while (!finished_execution_queue.push(pid));
    }
}

void PipelineExecutor::addAsyncJob(UInt64 pid)
{
    graph[pid].processor->schedule(event_counter);
    graph[pid].status = ExecStatus::Async;
    ++num_tasks_to_wait;
}

void PipelineExecutor::expandPipeline(UInt64 pid)
{
    auto & cur_node = graph[pid];
    auto new_processors = cur_node.processor->expandPipeline();

    for (const auto & processor : new_processors)
    {
        if (processors_map.count(processor.get()))
            throw Exception("Processor " + processor->getName() + " was already added to pipeline.",
                    ErrorCodes::LOGICAL_ERROR);

        processors_map[processor.get()] = graph.size();
        graph.emplace_back();
        graph.back().processor = processor.get();
    }

    processors.insert(processors.end(), new_processors.begin(), new_processors.end());
    UInt64 num_processors = processors.size();

    for (UInt64 node = 0; node < num_processors; ++node)
    {
        if (addEdges(node))
        {
            if (graph[node].status == ExecStatus::Idle || graph[node].status == ExecStatus::New)
            {
                graph[node].status = ExecStatus::Preparing;
                prepare_queue.push(node);
            }
        }
    }
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
            expandPipeline(pid);
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

void PipelineExecutor::execute(size_t num_threads)
{
    addChildlessProcessorsToQueue();

    try
    {
        /// Wait for all tasks to finish in case of exception.
        SCOPE_EXIT(
                finished = true;

                for (auto & thread : threads)
                    thread.join();
        );

        executeImpl(num_threads);
    }
    catch (Exception & exception)
    {
        if (checkCanAddAdditionalInfoToException(exception))
            exception.addMessage("\nCurrent state:\n" + dumpPipeline());

        throw;
    }

    if (cancelled)
        return;

    bool all_processors_finished = true;
    for (auto & node : graph)
        if (node.status != ExecStatus::Finished)
            all_processors_finished = false;

    if (!all_processors_finished)
        throw Exception("Pipeline stuck. Current state:\n" + dumpPipeline(), ErrorCodes::LOGICAL_ERROR);
}

void PipelineExecutor::executeImpl(size_t num_threads)
{
    /// No need to make task_queue longer than num_threads.
    /// Therefore, finished_execution_queue can't be longer than num_threads too.
    task_queue.reserve_unsafe(num_threads);
    finished_execution_queue.reserve_unsafe(num_threads);

    if (num_threads > 1)
    {
        /// For single thread execution will execute jobs in main thread.

        threads.reserve(num_threads);

        for (size_t i = 0; i < num_threads; ++i)
        {
            threads.emplace_back([this]
            {
                ExecutionState * state = nullptr;

                while (!finished)
                {
                    while (task_queue.pop(state))
                        state->job();

                    /// Note: we don't wait in thread like in ordinary thread pool.
                    /// Probably, it's better to add waiting on condition variable.
                    /// But not will avoid it to escape extra locking.
                }
            });
        }
    }

    while (!cancelled)
    {
        processFinishedExecutionQueueSafe();
        processPrepareQueue();
        processAsyncQueue();

        if (prepare_queue.empty())
        {
            /// For single-thread executor.
            if (num_threads == 1)
            {
                if (!finished_execution_queue.empty())
                    continue;
                else
                    break;
            }

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

    finished = true;
}

String PipelineExecutor::dumpPipeline() const
{
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

    return out.str();
}

}
