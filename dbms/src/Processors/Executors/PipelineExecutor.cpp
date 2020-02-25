#include <Processors/Executors/PipelineExecutor.h>
#include <unordered_map>
#include <queue>
#include <IO/WriteBufferFromString.h>
#include <Processors/printPipeline.h>
#include <Common/EventCounter.h>
#include <ext/scope_guard.h>
#include <Common/CurrentThread.h>

#include <Common/Stopwatch.h>
#include <Processors/ISource.h>
#include <Common/setThreadName.h>
#include <Interpreters/ProcessList.h>


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

PipelineExecutor::PipelineExecutor(Processors & processors_, QueryStatus * elem)
    : processors(processors_)
    , cancelled(false)
    , finished(false)
    , num_processing_executors(0)
    , expand_pipeline_task(nullptr)
    , process_list_element(elem)
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

    auto add_edge = [&](auto & from_port, const IProcessor * to_proc, Edges & edges,
                        bool is_backward, UInt64 input_port_number, UInt64 output_port_number,
                        std::vector<void *> * update_list)
    {
        auto it = processors_map.find(to_proc);
        if (it == processors_map.end())
            throwUnknownProcessor(to_proc, cur, true);

        UInt64 proc_num = it->second;
        auto & edge = edges.emplace_back(proc_num, is_backward, input_port_number, output_port_number, update_list);

        from_port.setUpdateInfo(&edge.update_info);
    };

    bool was_edge_added = false;

    auto & inputs = processors[node]->getInputs();
    auto from_input = graph[node].backEdges.size();

    if (from_input < inputs.size())
    {
        was_edge_added = true;

        for (auto it = std::next(inputs.begin(), from_input); it != inputs.end(); ++it, ++from_input)
        {
            const IProcessor * proc = &it->getOutputPort().getProcessor();
            auto output_port_number = proc->getOutputPortNumber(&it->getOutputPort());
            add_edge(*it, proc, graph[node].backEdges, true, from_input, output_port_number, &graph[node].post_updated_input_ports);
        }
    }

    auto & outputs = processors[node]->getOutputs();
    auto from_output = graph[node].directEdges.size();

    if (from_output < outputs.size())
    {
        was_edge_added = true;

        for (auto it = std::next(outputs.begin(), from_output); it != outputs.end(); ++it, ++from_output)
        {
            const IProcessor * proc = &it->getInputPort().getProcessor();
            auto input_port_number = proc->getInputPortNumber(&it->getInputPort());
            add_edge(*it, proc, graph[node].directEdges, false, input_port_number, from_output, &graph[node].post_updated_output_ports);
        }
    }

    return was_edge_added;
}

void PipelineExecutor::buildGraph()
{
    UInt64 num_processors = processors.size();

    graph.reserve(num_processors);
    for (UInt64 node = 0; node < num_processors; ++node)
    {
        IProcessor * proc = processors[node].get();
        processors_map[proc] = node;
        graph.emplace_back(proc, node);
    }

    for (UInt64 node = 0; node < num_processors; ++node)
        addEdges(node);
}

void PipelineExecutor::addChildlessProcessorsToStack(Stack & stack)
{
    UInt64 num_processors = processors.size();
    for (UInt64 proc = 0; proc < num_processors; ++proc)
    {
        if (graph[proc].directEdges.empty())
        {
            stack.push(proc);
            /// do not lock mutex, as this function is executedin single thread
            graph[proc].status = ExecStatus::Preparing;
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

void PipelineExecutor::addJob(ExecutionState * execution_state)
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
    auto & cur_node = graph[pid];
    Processors new_processors;

    try
    {
        new_processors = cur_node.processor->expandPipeline();
    }
    catch (...)
    {
        cur_node.execution_state->exception = std::current_exception();
        return false;
    }

    for (const auto & processor : new_processors)
    {
        if (processors_map.count(processor.get()))
            throw Exception("Processor " + processor->getName() + " was already added to pipeline.",
                    ErrorCodes::LOGICAL_ERROR);

        processors_map[processor.get()] = graph.size();
        graph.emplace_back(processor.get(), graph.size());
    }

    {
        std::lock_guard guard(processors_mutex);
        processors.insert(processors.end(), new_processors.begin(), new_processors.end());
    }

    UInt64 num_processors = processors.size();
    for (UInt64 node = 0; node < num_processors; ++node)
    {
        size_t num_direct_edges = graph[node].directEdges.size();
        size_t num_back_edges = graph[node].backEdges.size();

        if (addEdges(node))
        {
            std::lock_guard guard(graph[node].status_mutex);

            for (; num_back_edges < graph[node].backEdges.size(); ++num_back_edges)
                graph[node].updated_input_ports.emplace_back(num_back_edges);

            for (; num_direct_edges < graph[node].directEdges.size(); ++num_direct_edges)
                graph[node].updated_output_ports.emplace_back(num_direct_edges);

            if (graph[node].status == ExecStatus::Idle)
            {
                graph[node].status = ExecStatus::Preparing;
                stack.push(node);
            }
        }
    }

    return true;
}

bool PipelineExecutor::tryAddProcessorToStackIfUpdated(Edge & edge, Queue & queue, size_t thread_number)
{
    /// In this method we have ownership on edge, but node can be concurrently accessed.

    auto & node = graph[edge.to];

    std::unique_lock lock(node.status_mutex);

    ExecStatus status = node.status;

    if (status == ExecStatus::Finished)
        return true;

    if (edge.backward)
        node.updated_output_ports.push_back(edge.output_port_number);
    else
        node.updated_input_ports.push_back(edge.input_port_number);

    if (status == ExecStatus::Idle)
    {
        node.status = ExecStatus::Preparing;
        return prepareProcessor(edge.to, thread_number, queue, std::move(lock));
    }

    return true;
}

bool PipelineExecutor::prepareProcessor(UInt64 pid, size_t thread_number, Queue & queue, std::unique_lock<std::mutex> node_lock)
{
    /// In this method we have ownership on node.
    auto & node = graph[pid];

    bool need_expand_pipeline = false;

    std::vector<Edge *> updated_back_edges;
    std::vector<Edge *> updated_direct_edges;

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
            node.execution_state->exception = std::current_exception();
            return false;
        }

#ifndef NDEBUG
        node.execution_state->preparation_time_ns += watch.elapsed();
#endif

        node.updated_input_ports.clear();
        node.updated_output_ports.clear();

        switch (node.last_processor_status)
        {
            case IProcessor::Status::NeedData:
            case IProcessor::Status::PortFull:
            {
                node.status = ExecStatus::Idle;
                break;
            }
            case IProcessor::Status::Finished:
            {
                node.status = ExecStatus::Finished;
                break;
            }
            case IProcessor::Status::Ready:
            {
                node.status = ExecStatus::Executing;
                queue.push(node.execution_state.get());
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
                auto edge = static_cast<Edge *>(edge_id);
                updated_back_edges.emplace_back(edge);
                edge->update_info.trigger();
            }

            for (auto & edge_id : node.post_updated_output_ports)
            {
                auto edge = static_cast<Edge *>(edge_id);
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

        executor_contexts[thread_number]->task_list.emplace_back(
                node.execution_state.get(),
                &stack
        );

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
            if (!prepareProcessor(item, thread_number, queue, std::unique_lock<std::mutex>(graph[item].status_mutex)))
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
        for (auto & node : graph)
            if (node.execution_state->exception)
                std::rethrow_exception(node.execution_state->exception);
    }
    catch (...)
    {
#ifndef NDEBUG
        LOG_TRACE(log, "Exception while executing query. Current state:\n" << dumpPipeline());
#endif
        throw;
    }

    if (process_list_element && process_list_element->isKilled())
        throw Exception("Query was cancelled", ErrorCodes::QUERY_WAS_CANCELLED);

    if (cancelled)
        return;

    bool all_processors_finished = true;
    for (auto & node : graph)
        if (node.status != ExecStatus::Finished)  /// Single thread, do not hold mutex
            all_processors_finished = false;

    if (!all_processors_finished)
        throw Exception("Pipeline stuck. Current state:\n" + dumpPipeline(), ErrorCodes::LOGICAL_ERROR);
}

void PipelineExecutor::executeSingleThread(size_t thread_num, size_t num_threads)
{
#ifndef NDEBUG
    UInt64 total_time_ns = 0;
    UInt64 execution_time_ns = 0;
    UInt64 processing_time_ns = 0;
    UInt64 wait_time_ns = 0;

    Stopwatch total_time_watch;
#endif

    ExecutionState * state = nullptr;

    auto prepare_processor = [&](UInt64 pid, Queue & queue)
    {
        if (!prepareProcessor(pid, thread_num, queue, std::unique_lock<std::mutex>(graph[pid].status_mutex)))
            finish();
    };

    auto wake_up_executor = [&](size_t executor)
    {
        std::lock_guard guard(executor_contexts[executor]->mutex);
        executor_contexts[executor]->wake_flag = true;
        executor_contexts[executor]->condvar.notify_one();
    };

    while (!finished)
    {
        /// First, find any processor to execute.
        /// Just travers graph and prepare any processor.
        while (!finished)
        {
            {
                std::unique_lock lock(task_queue_mutex);

                if (!task_queue.empty())
                {
                    state = task_queue.pop(thread_num);

                    if (!task_queue.empty() && !threads_queue.empty() /*&& task_queue.quota() > threads_queue.size()*/)
                    {
                        auto thread_to_wake = task_queue.getAnyThreadWithTasks(thread_num + 1 == num_threads ? 0 : (thread_num + 1));

                        if (threads_queue.has(thread_to_wake))
                            threads_queue.pop(thread_to_wake);
                        else
                            thread_to_wake = threads_queue.pop_any();

                        lock.unlock();
                        wake_up_executor(thread_to_wake);
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
                std::unique_lock lock(executor_contexts[thread_num]->mutex);

                executor_contexts[thread_num]->condvar.wait(lock, [&]
                {
                    return finished || executor_contexts[thread_num]->wake_flag;
                });

                executor_contexts[thread_num]->wake_flag = false;
            }
        }

        if (finished)
            break;

        while (state)
        {
            if (finished)
                break;

            addJob(state);

            {
#ifndef NDEBUG
                Stopwatch execution_time_watch;
#endif

                state->job();

#ifndef NDEBUG
                execution_time_ns += execution_time_watch.elapsed();
#endif
            }

            if (state->exception)
                finish();

            if (finished)
                break;

#ifndef NDEBUG
            Stopwatch processing_time_watch;
#endif

            /// Try to execute neighbour processor.
            {
                Queue queue;

                ++num_processing_executors;
                while (auto task = expand_pipeline_task.load())
                    doExpandPipeline(task, true);

                /// Execute again if can.
                prepare_processor(state->processors_id, queue);
                state = nullptr;

                /// Take local task from queue if has one.
                if (!queue.empty())
                {
                    state = queue.front();
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
                            thread_to_wake = threads_queue.pop_any();

                        lock.unlock();

                        wake_up_executor(thread_to_wake);
                    }
                }

                --num_processing_executors;
                while (auto task = expand_pipeline_task.load())
                    doExpandPipeline(task, false);
            }

#ifndef NDEBUG
            processing_time_ns += processing_time_watch.elapsed();
#endif
        }
    }

#ifndef NDEBUG
    total_time_ns = total_time_watch.elapsed();
    wait_time_ns = total_time_ns - execution_time_ns - processing_time_ns;

    LOG_TRACE(log, std::fixed << std::setprecision(3)
        << "Thread finished."
        << " Total time: " << (total_time_ns / 1e9) << " sec."
        << " Execution time: " << (execution_time_ns / 1e9) << " sec."
        << " Processing time: " << (processing_time_ns / 1e9) << " sec."
        << " Wait time: " << (wait_time_ns / 1e9) << " sec.");
#endif
}

void PipelineExecutor::executeImpl(size_t num_threads)
{
    Stack stack;

    threads_queue.init(num_threads);
    task_queue.init(num_threads);

    {
        std::lock_guard guard(executor_contexts_mutex);

        executor_contexts.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i)
            executor_contexts.emplace_back(std::make_unique<ExecutorContext>());
    }

    auto thread_group = CurrentThread::getGroup();

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

    addChildlessProcessorsToStack(stack);

    {
        std::lock_guard lock(task_queue_mutex);

        Queue queue;
        size_t next_thread = 0;

        while (!stack.empty())
        {
            UInt64 proc = stack.top();
            stack.pop();

            prepareProcessor(proc, 0, queue, std::unique_lock<std::mutex>(graph[proc].status_mutex));

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

    if (num_threads > 1)
    {

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

                executeSingleThread(thread_num, num_threads);
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
    for (auto & node : graph)
    {
        if (node.execution_state)
        {
            WriteBufferFromOwnString buffer;
            buffer << "(" << node.execution_state->num_executed_jobs << " jobs";

#ifndef NDEBUG
            buffer << ", execution time: " << node.execution_state->execution_time_ns / 1e9 << " sec.";
            buffer << ", preparation time: " << node.execution_state->preparation_time_ns / 1e9 << " sec.";
#endif

            buffer << ")";
            node.processor->setDescription(buffer.str());
        }
    }

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
    out.finalize();

    return out.str();
}

}
