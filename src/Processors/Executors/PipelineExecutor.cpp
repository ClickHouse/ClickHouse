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

PipelineExecutor::PipelineExecutor(Processors & processors_, QueryStatus * elem)
    : processors(processors_)
    , cancelled(false)
    , finished(false)
    , num_processing_executors(0)
    , expand_pipeline_task(nullptr)
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

static void executeJob(IProcessor * processor)
{
    try
    {
        processor->work();
    }
    catch (Exception & exception)
    {
        if (checkCanAddAdditionalInfoToException(exception))
            exception.addMessage("While executing " + processor->getName());
        throw;
    }
}

void PipelineExecutor::addJob(ExecutingGraph::Node * execution_state)
{
    auto job = [execution_state]()
    {
        try
        {
            // Stopwatch watch;
            executeJob(execution_state->processor);
            // execution_state->execution_time_ns += watch.elapsed();

            ++execution_state->num_executed_jobs;
        }
        catch (...)
        {
            execution_state->exception = std::current_exception();
        }
    };

    execution_state->job = std::move(job);
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

bool PipelineExecutor::tryAddProcessorToStackIfUpdated(ExecutingGraph::Edge & edge, Queue & queue, size_t thread_number)
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
        return prepareProcessor(edge.to, thread_number, queue, std::move(lock));
    }
    else
        graph->nodes[edge.to]->processor->onUpdatePorts();

    return true;
}

bool PipelineExecutor::prepareProcessor(UInt64 pid, size_t thread_number, Queue & queue, std::unique_lock<std::mutex> node_lock)
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
                throw Exception("Async is temporary not supported.", ErrorCodes::LOGICAL_ERROR);

//            node.status = ExecStatus::Executing;
//            addAsyncJob(pid);
//            break;
            }
            case IProcessor::Status::Wait:
            {
                throw Exception("Wait is temporary not supported.", ErrorCodes::LOGICAL_ERROR);
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
            if (!tryAddProcessorToStackIfUpdated(*edge, queue, thread_number))
                return false;
        }

        for (auto & edge : updated_back_edges)
        {
            if (!tryAddProcessorToStackIfUpdated(*edge, queue, thread_number))
                return false;
        }
    }

    if (need_expand_pipeline)
    {
        Stack stack;

        executor_contexts[thread_number]->task_list.emplace_back(&node, &stack);

        ExpandPipelineTask * desired = &executor_contexts[thread_number]->task_list.back();
        ExpandPipelineTask * expected = nullptr;

        while (!expand_pipeline_task.compare_exchange_strong(expected, desired))
        {
            if (!doExpandPipeline(expected, true))
                return false;

            expected = nullptr;
        }

        if (!doExpandPipeline(desired, true))
            return false;

        /// Add itself back to be prepared again.
        stack.push(pid);

        while (!stack.empty())
        {
            auto item = stack.top();
            if (!prepareProcessor(item, thread_number, queue, std::unique_lock<std::mutex>(graph->nodes[item]->status_mutex)))
                return false;

            stack.pop();
        }
    }

    return true;
}

bool PipelineExecutor::doExpandPipeline(ExpandPipelineTask * task, bool processing)
{
    std::unique_lock lock(task->mutex);

    if (processing)
        ++task->num_waiting_processing_threads;

    task->condvar.wait(lock, [&]()
    {
        return task->num_waiting_processing_threads >= num_processing_executors || expand_pipeline_task != task;
    });

    bool result = true;

    /// After condvar.wait() task may point to trash. Can change it only if it is still in expand_pipeline_task.
    if (expand_pipeline_task == task)
    {
        result = expandPipeline(*task->stack, task->node_to_expand->processors_id);

        expand_pipeline_task = nullptr;

        lock.unlock();
        task->condvar.notify_all();
    }

    return result;
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
    {
        std::lock_guard lock(task_queue_mutex);
        finished = true;
    }

    std::lock_guard guard(executor_contexts_mutex);

    for (auto & context : executor_contexts)
    {
        {
            std::lock_guard lock(context->mutex);
            context->wake_flag = true;
        }

        context->condvar.notify_one();
    }
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
        for (auto & executor_context : executor_contexts)
            if (executor_context->exception)
                std::rethrow_exception(executor_context->exception);
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
    if (finished)
        return false;

    if (!is_execution_initialized)
        initializeExecution(1);

    executeStepImpl(0, 1, yield_flag);

    if (!finished)
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

void PipelineExecutor::wakeUpExecutor(size_t thread_num)
{
    std::lock_guard guard(executor_contexts[thread_num]->mutex);
    executor_contexts[thread_num]->wake_flag = true;
    executor_contexts[thread_num]->condvar.notify_one();
}

void PipelineExecutor::executeSingleThread(size_t thread_num, size_t num_threads)
{
    executeStepImpl(thread_num, num_threads);

#ifndef NDEBUG
    auto & context = executor_contexts[thread_num];
    LOG_TRACE(log, "Thread finished. Total time: {} sec. Execution time: {} sec. Processing time: {} sec. Wait time: {} sec.", (context->total_time_ns / 1e9), (context->execution_time_ns / 1e9), (context->processing_time_ns / 1e9), (context->wait_time_ns / 1e9));
#endif
}

void PipelineExecutor::executeStepImpl(size_t thread_num, size_t num_threads, std::atomic_bool * yield_flag)
{
#ifndef NDEBUG
    Stopwatch total_time_watch;
#endif

    auto & context = executor_contexts[thread_num];
    auto & node = context->node;
    bool yield = false;

    while (!finished && !yield)
    {
        /// First, find any processor to execute.
        /// Just travers graph and prepare any processor.
        while (!finished && node == nullptr)
        {
            {
                std::unique_lock lock(task_queue_mutex);

                if (!task_queue.empty())
                {
                    node = task_queue.pop(thread_num);

                    if (!task_queue.empty() && !threads_queue.empty() /*&& task_queue.quota() > threads_queue.size()*/)
                    {
                        auto thread_to_wake = task_queue.getAnyThreadWithTasks(thread_num + 1 == num_threads ? 0 : (thread_num + 1));

                        if (threads_queue.has(thread_to_wake))
                            threads_queue.pop(thread_to_wake);
                        else
                            thread_to_wake = threads_queue.popAny();

                        lock.unlock();
                        wakeUpExecutor(thread_to_wake);
                    }

                    break;
                }

                if (threads_queue.size() + 1 == num_threads)
                {
                    lock.unlock();
                    finish();
                    break;
                }

                threads_queue.push(thread_num);
            }

            {
                std::unique_lock lock(context->mutex);

                context->condvar.wait(lock, [&]
                {
                    return finished || context->wake_flag;
                });

                context->wake_flag = false;
            }
        }

        if (finished)
            break;

        while (node && !yield)
        {
            if (finished)
                break;

            addJob(node);

            {
#ifndef NDEBUG
                Stopwatch execution_time_watch;
#endif

                node->job();

#ifndef NDEBUG
                context->execution_time_ns += execution_time_watch.elapsed();
#endif
            }

            if (node->exception)
                cancel();

            if (finished)
                break;

#ifndef NDEBUG
            Stopwatch processing_time_watch;
#endif

            /// Try to execute neighbour processor.
            {
                Queue queue;

                ++num_processing_executors;
                while (auto * task = expand_pipeline_task.load())
                    doExpandPipeline(task, true);

                /// Prepare processor after execution.
                {
                    auto lock = std::unique_lock<std::mutex>(node->status_mutex);
                    if (!prepareProcessor(node->processors_id, thread_num, queue, std::move(lock)))
                        finish();
                }

                node = nullptr;

                /// Take local task from queue if has one.
                if (!queue.empty())
                {
                    node = queue.front();
                    queue.pop();
                }

                /// Push other tasks to global queue.
                if (!queue.empty())
                {
                    std::unique_lock lock(task_queue_mutex);

                    while (!queue.empty() && !finished)
                    {
                        task_queue.push(queue.front(), thread_num);
                        queue.pop();
                    }

                    if (!threads_queue.empty() && !finished /* && task_queue.quota() > threads_queue.size()*/)
                    {
                        auto thread_to_wake = task_queue.getAnyThreadWithTasks(thread_num + 1 == num_threads ? 0 : (thread_num + 1));

                        if (threads_queue.has(thread_to_wake))
                            threads_queue.pop(thread_to_wake);
                        else
                            thread_to_wake = threads_queue.popAny();

                        lock.unlock();

                        wakeUpExecutor(thread_to_wake);
                    }
                }

                --num_processing_executors;
                while (auto * task = expand_pipeline_task.load())
                    doExpandPipeline(task, false);
            }

#ifndef NDEBUG
            context->processing_time_ns += processing_time_watch.elapsed();
#endif

            /// We have executed single processor. Check if we need to yield execution.
            if (yield_flag && *yield_flag)
                yield = true;
        }
    }

#ifndef NDEBUG
    context->total_time_ns += total_time_watch.elapsed();
    context->wait_time_ns = context->total_time_ns - context->execution_time_ns - context->processing_time_ns;
#endif
}

void PipelineExecutor::initializeExecution(size_t num_threads)
{
    is_execution_initialized = true;

    threads_queue.init(num_threads);
    task_queue.init(num_threads);

    {
        std::lock_guard guard(executor_contexts_mutex);

        executor_contexts.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i)
            executor_contexts.emplace_back(std::make_unique<ExecutorContext>());
    }

    Stack stack;
    addChildlessProcessorsToStack(stack);

    {
        std::lock_guard lock(task_queue_mutex);

        Queue queue;
        size_t next_thread = 0;

        while (!stack.empty())
        {
            UInt64 proc = stack.top();
            stack.pop();

            prepareProcessor(proc, 0, queue, std::unique_lock<std::mutex>(graph->nodes[proc]->status_mutex));

            while (!queue.empty())
            {
                task_queue.push(queue.front(), next_thread);
                queue.pop();

                ++next_thread;
                if (next_thread >= num_threads)
                    next_thread = 0;
            }
        }
    }
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
            threads.emplace_back([this, thread_group, thread_num = i, num_threads]
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
                    executeSingleThread(thread_num, num_threads);
                }
                catch (...)
                {
                    /// In case of exception from executor itself, stop other threads.
                    finish();
                    executor_contexts[thread_num]->exception = std::current_exception();
                }
            });
        }

        for (auto & thread : threads)
            if (thread.joinable())
                thread.join();
    }
    else
        executeSingleThread(0, num_threads);

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
