#include <Processors/Executors/PipelineExecutor.h>
#include <queue>
#include <IO/WriteBufferFromString.h>
#include <Processors/printPipeline.h>
#include <Common/EventCounter.h>
#include <ext/scope_guard.h>
#include <Common/CurrentThread.h>
#include <Processors/ISource.h>
#include <Common/setThreadName.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/OpenTelemetrySpanLog.h>

#ifndef NDEBUG
    #include <Common/Stopwatch.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}


PipelineExecutor::PipelineExecutor(Processors & processors_, QueryStatus * elem)
    : processors(processors_)
    , process_list_element(elem)
{
    try
    {
        graph = std::make_unique<ExecutingGraph>(processors);
    }
    catch (Exception & exception)
    {
        /// If exception was thrown while pipeline initialization, it means that query pipeline was not build correctly.
        /// It is logical error, and we need more information about pipeline.
        WriteBufferFromOwnString buf;
        printPipeline(processors, buf);
        buf.finalize();
        exception.addMessage("Query pipeline:\n" + buf.str());

        throw;
    }
}

void PipelineExecutor::addChildlessProcessorsToStack(Stack & stack)
{
    UInt64 num_processors = processors.size();
    for (UInt64 proc = 0; proc < num_processors; ++proc)
    {
        if (graph->nodes[proc]->direct_edges.empty())
        {
            stack.push(proc);
            /// do not lock mutex, as this function is executed in single thread
            graph->nodes[proc]->status = ExecutingGraph::ExecStatus::Preparing;
        }
    }
}

bool PipelineExecutor::expandPipeline(Stack & stack, UInt64 pid)
{
    auto & cur_node = *graph->nodes[pid];
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

    for (uint64_t node = 0; node < graph->nodes.size(); ++node)
    {
        direct_edge_sizes[node] = graph->nodes[node]->direct_edges.size();
        back_edges_sizes[node] = graph->nodes[node]->back_edges.size();
    }

    auto updated_nodes = graph->expandPipeline(processors);

    for (auto updated_node : updated_nodes)
    {
        auto & node = *graph->nodes[updated_node];

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

bool PipelineExecutor::tryAddProcessorToStackIfUpdated(ExecutingGraph::Edge & edge, Queue & queue, Queue & async_queue, size_t thread_number)
{
    /// In this method we have ownership on edge, but node can be concurrently accessed.

    auto & node = *graph->nodes[edge.to];

    std::unique_lock lock(node.status_mutex);

    ExecutingGraph::ExecStatus status = node.status;

    if (status == ExecutingGraph::ExecStatus::Finished)
        return true;

    if (edge.backward)
        node.updated_output_ports.push_back(edge.output_port_number);
    else
        node.updated_input_ports.push_back(edge.input_port_number);

    if (status == ExecutingGraph::ExecStatus::Idle)
    {
        node.status = ExecutingGraph::ExecStatus::Preparing;
        return prepareProcessor(edge.to, thread_number, queue, async_queue, std::move(lock));
    }
    else
        graph->nodes[edge.to]->processor->onUpdatePorts();

    return true;
}

bool PipelineExecutor::prepareProcessor(UInt64 pid, size_t thread_number, Queue & queue, Queue & async_queue, std::unique_lock<std::mutex> node_lock)
{
    /// In this method we have ownership on node.
    auto & node = *graph->nodes[pid];

    bool need_expand_pipeline = false;

    std::vector<ExecutingGraph::Edge *> updated_back_edges;
    std::vector<ExecutingGraph::Edge *> updated_direct_edges;

    {
#ifndef NDEBUG
        Stopwatch watch;
#endif

        std::unique_lock<std::mutex> lock(std::move(node_lock));

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

        {
            for (auto & edge_id : node.post_updated_input_ports)
            {
                auto * edge = static_cast<ExecutingGraph::Edge *>(edge_id);
                updated_back_edges.emplace_back(edge);
                edge->update_info.trigger();
            }

            for (auto & edge_id : node.post_updated_output_ports)
            {
                auto * edge = static_cast<ExecutingGraph::Edge *>(edge_id);
                updated_direct_edges.emplace_back(edge);
                edge->update_info.trigger();
            }

            node.post_updated_input_ports.clear();
            node.post_updated_output_ports.clear();
        }
    }

    {
        for (auto & edge : updated_direct_edges)
        {
            if (!tryAddProcessorToStackIfUpdated(*edge, queue, async_queue, thread_number))
                return false;
        }

        for (auto & edge : updated_back_edges)
        {
            if (!tryAddProcessorToStackIfUpdated(*edge, queue, async_queue, thread_number))
                return false;
        }
    }

    if (need_expand_pipeline)
    {
        Stack stack;

        auto callback = [this, &stack, pid = node.processors_id]() { return expandPipeline(stack, pid); };

        if (!tasks.runExpandPipeline(thread_number, std::move(callback)))
            return false;

        /// Add itself back to be prepared again.
        stack.push(pid);

        while (!stack.empty())
        {
            auto item = stack.top();
            if (!prepareProcessor(item, thread_number, queue, async_queue, std::unique_lock<std::mutex>(graph->nodes[item]->status_mutex)))
                return false;

            stack.pop();
        }
    }

    return true;
}

void PipelineExecutor::cancel()
{
    cancelled = true;
    finish();

    std::lock_guard guard(processors_mutex);
    for (auto & processor : processors)
        processor->cancel();
}

void PipelineExecutor::finish()
{
    tasks.finish();
}

void PipelineExecutor::execute(size_t num_threads)
{
    try
    {
        executeImpl(num_threads);

        /// Execution can be stopped because of exception. Check and rethrow if any.
        for (auto & node : graph->nodes)
            if (node->exception)
                std::rethrow_exception(node->exception);

        /// Exception which happened in executing thread, but not at processor.
        tasks.rethrowFirstThreadException();
    }
    catch (...)
    {
#ifndef NDEBUG
        LOG_TRACE(log, "Exception while executing query. Current state:\n{}", dumpPipeline());
#endif
        throw;
    }

    finalizeExecution();
}

bool PipelineExecutor::executeStep(std::atomic_bool * yield_flag)
{
    if (tasks.isFinished())
        return false;

    if (!is_execution_initialized)
        initializeExecution(1);

    executeStepImpl(0, yield_flag);

    if (!tasks.isFinished())
        return true;

    /// Execution can be stopped because of exception. Check and rethrow if any.
    for (auto & node : graph->nodes)
        if (node->exception)
            std::rethrow_exception(node->exception);

    finalizeExecution();

    return false;
}

void PipelineExecutor::finalizeExecution()
{
    if (process_list_element && process_list_element->isKilled())
        throw Exception("Query was cancelled", ErrorCodes::QUERY_WAS_CANCELLED);

    if (cancelled)
        return;

    bool all_processors_finished = true;
    for (auto & node : graph->nodes)
    {
        if (node->status != ExecutingGraph::ExecStatus::Finished)
        {
            /// Single thread, do not hold mutex
            all_processors_finished = false;
            break;
        }
    }

    if (!all_processors_finished)
        throw Exception("Pipeline stuck. Current state:\n" + dumpPipeline(), ErrorCodes::LOGICAL_ERROR);
}

void PipelineExecutor::executeSingleThread(size_t thread_num)
{
    executeStepImpl(thread_num);

#ifndef NDEBUG
    auto & context = tasks.getThreadContext(thread_num);
    LOG_TRACE(log,
              "Thread finished. Total time: {} sec. Execution time: {} sec. Processing time: {} sec. Wait time: {} sec.",
              (context.total_time_ns / 1e9),
              (context.execution_time_ns / 1e9),
              (context.processing_time_ns / 1e9),
              (context.wait_time_ns / 1e9));
#endif
}

void PipelineExecutor::executeStepImpl(size_t thread_num, std::atomic_bool * yield_flag)
{
#ifndef NDEBUG
    Stopwatch total_time_watch;
#endif

    // auto & node = tasks.getNode(thread_num);
    auto & context = tasks.getThreadContext(thread_num);
    bool yield = false;

    while (!tasks.isFinished() && !yield)
    {
        /// First, find any processor to execute.
        /// Just travers graph and prepare any processor.
        while (!tasks.isFinished() && !context.hasTask())
            tasks.tryGetTask(context);

        while (context.hasTask() && !yield)
        {
            if (tasks.isFinished())
                break;

            if (!context.executeTask())
                cancel();

            if (tasks.isFinished())
                break;

#ifndef NDEBUG
            Stopwatch processing_time_watch;
#endif

            /// Try to execute neighbour processor.
            {
                Queue queue;
                Queue async_queue;

                tasks.expandPipelineStart();

                /// Prepare processor after execution.
                {
                    auto lock = context.lockStatus();
                    if (!prepareProcessor(context.getProcessorID(), thread_num, queue, async_queue, std::move(lock)))
                        finish();
                }

                /// Push other tasks to global queue.
                tasks.pushTasks(queue, async_queue, context);

                tasks.expandPipelineEnd();
            }

#ifndef NDEBUG
            context.processing_time_ns += processing_time_watch.elapsed();
#endif

            /// We have executed single processor. Check if we need to yield execution.
            if (yield_flag && *yield_flag)
                yield = true;
        }
    }

#ifndef NDEBUG
    context.total_time_ns += total_time_watch.elapsed();
    context.wait_time_ns = context.total_time_ns - context.execution_time_ns - context->processing_time_ns;
#endif
}

void PipelineExecutor::initializeExecution(size_t num_threads)
{
    is_execution_initialized = true;

    Stack stack;
    addChildlessProcessorsToStack(stack);

    tasks.init(num_threads);

    Queue queue;
    Queue async_queue;

    while (!stack.empty())
    {
        UInt64 proc = stack.top();
        stack.pop();

        prepareProcessor(proc, 0, queue, async_queue, std::unique_lock<std::mutex>(graph->nodes[proc]->status_mutex));

        if (!async_queue.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Async is only possible after work() call. Processor {}",
                            async_queue.front()->processor->getName());
    }

    tasks.fill(queue);
}

void PipelineExecutor::executeImpl(size_t num_threads)
{
    OpenTelemetrySpanHolder span("PipelineExecutor::executeImpl()");

    initializeExecution(num_threads);

    using ThreadsData = std::vector<ThreadFromGlobalPool>;
    ThreadsData threads;
    threads.reserve(num_threads);

    bool finished_flag = false;

    SCOPE_EXIT(
        if (!finished_flag)
        {
            finish();

            for (auto & thread : threads)
                if (thread.joinable())
                    thread.join();
        }
    );

    if (num_threads > 1)
    {
        auto thread_group = CurrentThread::getGroup();

        for (size_t i = 0; i < num_threads; ++i)
        {
            threads.emplace_back([this, thread_group, thread_num = i]
            {
                /// ThreadStatus thread_status;

                setThreadName("QueryPipelineEx");

                if (thread_group)
                    CurrentThread::attachTo(thread_group);

                SCOPE_EXIT(
                        if (thread_group)
                            CurrentThread::detachQueryIfNotDetached();
                );

                try
                {
                    executeSingleThread(thread_num);
                }
                catch (...)
                {
                    /// In case of exception from executor itself, stop other threads.
                    finish();
                    tasks.getThreadContext(thread_num).setException(std::current_exception());
                }
            });
        }

        tasks.processAsyncTasks();

        for (auto & thread : threads)
            if (thread.joinable())
                thread.join();
    }
    else
        executeSingleThread(0);

    finished_flag = true;
}

String PipelineExecutor::dumpPipeline() const
{
    for (const auto & node : graph->nodes)
    {
        {
            WriteBufferFromOwnString buffer;
            buffer << "(" << node->num_executed_jobs << " jobs";

#ifndef NDEBUG
            buffer << ", execution time: " << node->execution_time_ns / 1e9 << " sec.";
            buffer << ", preparation time: " << node->preparation_time_ns / 1e9 << " sec.";
#endif

            buffer << ")";
            node->processor->setDescription(buffer.str());
        }
    }

    std::vector<IProcessor::Status> statuses;
    std::vector<IProcessor *> proc_list;
    statuses.reserve(graph->nodes.size());
    proc_list.reserve(graph->nodes.size());

    for (const auto & node : graph->nodes)
    {
        proc_list.emplace_back(node->processor);
        statuses.emplace_back(node->last_processor_status);
    }

    WriteBufferFromOwnString out;
    printPipeline(processors, statuses, out);
    out.finalize();

    return out.str();
}

}
