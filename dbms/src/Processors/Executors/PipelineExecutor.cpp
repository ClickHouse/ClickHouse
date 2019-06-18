#include <Processors/Executors/PipelineExecutor.h>
#include <unordered_map>
#include <queue>
#include <IO/WriteBufferFromString.h>
#include <Processors/printPipeline.h>
#include <Common/EventCounter.h>
#include <ext/scope_guard.h>
#include <Common/CurrentThread.h>

#include <boost/lockfree/queue.hpp>
#include <Common/Stopwatch.h>

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
    : processors(std::move(processors)), cancelled(false), finished(false), num_waiting_threads(0)
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
            prepare_stack.push(proc);
            graph[proc].status = ExecStatus::Preparing;
        }
    }
}

void PipelineExecutor::processFinishedExecutionQueue()
{
    UInt64 finished_job = graph.size();
    while (!finished_execution_queue.empty())
    {
        /// Should be successful as single consumer is used.
        while (!finished_execution_queue.pop(finished_job));

        auto & state = graph[finished_job].execution_state;

        /// ++num_waited_tasks;
        ++state->num_executed_jobs;
        state->need_update_stream = true;

        if (graph[finished_job].execution_state->exception)
            std::rethrow_exception(graph[finished_job].execution_state->exception);

        graph[finished_job].status = ExecStatus::Preparing;
        prepare_stack.push(finished_job);
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

bool PipelineExecutor::tryAssignJob(ExecutionState * state)
{
    auto current_stream = state->current_stream;
    for (auto & executor_context : executor_contexts)
    {
        if (executor_context->current_stream == current_stream)
        {
            ExecutionState * expected = nullptr;
            if (executor_context->next_task_to_execute.compare_exchange_strong(expected, state))
            {
                ++num_tasks_to_wait;
                return true;
            }
        }
    }

    return false;
}

void PipelineExecutor::addJob(IProcessor * processor, ExecutionState * execution_state)
{
///    if (!threads.empty())
    {
        auto job = [processor, execution_state]()
        {
            // SCOPE_EXIT(
                    /// while (!finished_execution_queue.push(pid));
                    /// event_counter.notify()
            // );

            try
            {
                Stopwatch watch;
                executeJob(processor);
                execution_state->execution_time_ns += watch.elapsed();

                ++execution_state->num_executed_jobs;
            }
            catch (...)
            {
                /// Note: It's important to save exception before pushing pid to finished_execution_queue
                execution_state->exception = std::current_exception();
            }
        };

        execution_state->job = std::move(job);
        /// auto * state = graph[pid].execution_state.get();

//        bool is_stream_updated = false;
//        if (state->need_update_stream)
//        {
//            is_stream_updated = true;
//            state->current_stream = next_stream;
//            ++next_stream;
//        }

        /// Try assign job to executor right now.
//        if (is_stream_updated || !tryAssignJob(state))
//            execution_states_queue.emplace_back(state);

        /// while (!task_queue.push(graph[pid].execution_state.get()))
        ///    sleep(0);
    }
//    else
//    {
//        /// Execute task in main thread.
//        executeJob(graph[pid].processor);
//        while (!finished_execution_queue.push(pid));
//    }
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
                prepare_stack.push(node);
            }
        }
    }
}

bool PipelineExecutor::addProcessorToPrepareQueueIfUpdated(Edge & edge, bool update_stream_number, UInt64 stream_number)
{
    auto & node = graph[edge.to];

    /// Don't add processor if nothing was read from port.
    if (node.status != ExecStatus::New && edge.version == edge.prev_version)
        return false;

    edge.prev_version = edge.version;

    if (node.status == ExecStatus::Idle || node.status == ExecStatus::New)
    {
        prepare_stack.push(edge.to);
        node.status = ExecStatus::Preparing;

        if (update_stream_number)
        {
            node.execution_state->current_stream = stream_number;
            node.execution_state->need_update_stream = false;
        }

        return true;
    }

    return false;
}

void PipelineExecutor::prepareProcessor(UInt64 pid, bool async)
{
    auto & node = graph[pid];

    {
        Stopwatch watch;
        auto status = node.processor->prepare();
        node.execution_state->preparation_time_ns += watch.elapsed();

        node.last_processor_status = status;
    }

    auto add_neighbours_to_prepare_queue = [&, this]
    {
        auto stream_number = node.execution_state->current_stream;

        for (auto & edge : node.backEdges)
            addProcessorToPrepareQueueIfUpdated(edge, false, stream_number);

        for (auto & edge : node.directEdges)
            addProcessorToPrepareQueueIfUpdated(edge, true, stream_number);
    };

    switch (node.last_processor_status)
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
            /// addJob(pid);
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
            prepare_stack.push(pid);

            /// node ref is not valid now.
            graph[pid].status = ExecStatus::Preparing;
            break;
        }
    }
}

void PipelineExecutor::assignJobs()
{
    for (auto * state : execution_states_queue)
    {
        if (!tryAssignJob(state))
        {
            while (!task_queue.push(state))
                sleep(0);

            task_condvar.notify_one();
            ++num_tasks_to_wait;
        }
    }

    execution_states_queue.clear();
}

void PipelineExecutor::processPrepareQueue()
{
    while (!prepare_stack.empty())
    {
        UInt64 proc = prepare_stack.top();
        prepare_stack.pop();

        prepareProcessor(proc, false);
    }

    assignJobs();
}

void PipelineExecutor::processAsyncQueue()
{
    UInt64 num_processors = processors.size();
    for (UInt64 node = 0; node < num_processors; ++node)
        if (graph[node].status == ExecStatus::Async)
            prepareProcessor(node, true);

    assignJobs();
}

void PipelineExecutor::execute(size_t num_threads)
{
    addChildlessProcessorsToQueue();

    try
    {
        /// Wait for all tasks to finish in case of exception.
        SCOPE_EXIT(
                finished = true;

                task_condvar.notify_all();

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

void PipelineExecutor::executeSingleThread(size_t num_threads)
{
    UInt64 total_time_ns = 0;
    UInt64 execution_time_ns = 0;
    UInt64 processing_time_ns = 0;
    UInt64 wait_time_ns = 0;

    Stopwatch total_time_watch;

    ExecutionState * state = nullptr;
    IProcessor * processor = nullptr;
    UInt64 processor_to_execute = 0;
    bool found_processor_to_execute = false;

    while (!finished)
    {
        /// First, find any processor to execute.
        /// Just travers graph and prepare any processor.
        {
            Stopwatch processing_time_watch;

            {
                std::unique_lock lock(main_executor_mutex);

                while (!found_processor_to_execute)
                {
                    if (finished)
                        break;


                    while (!prepare_stack.empty())
                    {
                        UInt64 proc = prepare_stack.top();
                        prepare_stack.pop();

                        prepareProcessor(proc, false);

                        if (graph[proc].status == ExecStatus::Executing)
                        {
                            found_processor_to_execute = true;
                            processor_to_execute = proc;
                            state = graph[processor_to_execute].execution_state.get();
                            processor = graph[processor_to_execute].processor;

                            break;
                        }
                    }

                    if (found_processor_to_execute)
                    {
                        break;
                    }

                    if (num_waiting_threads.fetch_add(1) + 1 == num_threads)
                    {
                        finished = true;
                        main_executor_condvar.notify_all();
                        finish_condvar.notify_one();
                        break;
                    }

                    main_executor_condvar.wait(lock, [&]() { return finished || !prepare_stack.empty(); });

                    num_waiting_threads.fetch_sub(1);
                }
            }

            processing_time_ns += processing_time_watch.elapsed();
        }

        if (finished)
            break;

        /// In case if somebody is sleeping and prepare_queue is not empty.
        main_executor_condvar.notify_one();

        while (found_processor_to_execute)
        {
            if (finished)
                break;

            addJob(processor, state);

            {
                Stopwatch execution_time_watch;
                state->job();
                execution_time_ns += execution_time_watch.elapsed();
            }

            if (state->exception)
                finished = true;

            if (finished)
                break;

            Stopwatch processing_time_watch;
            found_processor_to_execute = false;

            /// Try to execute neighbour processor.
            {
                std::unique_lock lock(main_executor_mutex);

                auto queue_size = prepare_stack.size();

                prepareProcessor(processor_to_execute, false);

                /// Execute again if can.
                if (graph[processor_to_execute].status == ExecStatus::Executing)
                {
                    found_processor_to_execute = true;
                }

                std::queue<UInt64> neighbours;

                while (!found_processor_to_execute && prepare_stack.size() > queue_size)
                {
                    neighbours.push(prepare_stack.top());
                    prepare_stack.pop();
                }

                /// Process all neighbours. Children will be on the top of stack, then parents.
                while (!found_processor_to_execute && !neighbours.empty())
                {
                    auto current_processor = neighbours.front();
                    neighbours.pop();

                    prepareProcessor(current_processor, false);

                    if (graph[current_processor].status == ExecStatus::Executing)
                    {
                        found_processor_to_execute = true;
                        processor_to_execute = current_processor;
                        state = graph[processor_to_execute].execution_state.get();
                        processor = graph[processor_to_execute].processor;
                    }
                }

                while (!neighbours.empty())
                {
                    prepare_stack.push(neighbours.front());
                    neighbours.pop();
                }
            }

            /// Let another thread to continue.
            main_executor_condvar.notify_one();

            processing_time_ns += processing_time_watch.elapsed();
        }
    }

    total_time_ns = total_time_watch.elapsed();
    wait_time_ns = total_time_ns - execution_time_ns - processing_time_ns;

    LOG_TRACE(log, "Thread finished."
                     << " Total time: " << (total_time_ns / 1e9) << " sec."
                     << " Execution time: " << (execution_time_ns / 1e9) << " sec."
                     << " Processing time: " << (processing_time_ns / 1e9) << " sec."
                     << " Wait time: " << (wait_time_ns / 1e9) << "sec.");
}

void PipelineExecutor::executeImpl(size_t num_threads)
{
    /// No need to make task_queue longer than num_threads.
    /// Therefore, finished_execution_queue can't be longer than num_threads too.
    task_queue.reserve_unsafe(num_threads);
    finished_execution_queue.reserve_unsafe(num_threads);


    threads.reserve(num_threads);
    executor_contexts.reserve(num_threads);

    auto thread_group = CurrentThread::getGroup();

    for (size_t i = 0; i < num_threads; ++i)
    {
        executor_contexts.emplace_back(std::make_unique<ExecutorContext>());
        auto * executor_context = executor_contexts.back().get();

        executor_context->executor_number = i;
        executor_context->next_task_to_execute = nullptr;

        threads.emplace_back([this, thread_group, num_threads]
        {
            ThreadStatus thread_status;

            if (thread_group)
                CurrentThread::attachTo(thread_group);

            SCOPE_EXIT(
                    if (thread_group)
                        CurrentThread::detachQueryIfNotDetached();
            );

            executeSingleThread(num_threads);
        });
    }

    {
        std::mutex finish_mutex;
        std::unique_lock lock(finish_mutex);
        finish_condvar.wait(lock, [&]() -> bool { return finished; });
    }

    {
        std::lock_guard lock(main_executor_mutex);

        for (auto & node : graph)
            if (node.execution_state->exception)
                std::rethrow_exception(node.execution_state->exception);
    }
//    while (!cancelled)
//    {
//        processFinishedExecutionQueueSafe();
//        processPrepareQueue();
//        processAsyncQueue();
//
//        if (prepare_stack.empty())
//        {
//            /// For single-thread executor.
//            if (num_threads == 1)
//            {
//                if (!finished_execution_queue.empty())
//                    continue;
//                else
//                    break;
//            }
//
//            if (num_tasks_to_wait > num_waited_tasks)
//            {
//                /// Try wait anything.
//                event_counter.wait();
//                ++num_waited_tasks;
//            }
//            else
//            {
//                /// Here prepare_queue is empty and we have nobody to wait for. Exiting.
//                break;
//            }
//        }
//    }
}

String PipelineExecutor::dumpPipeline() const
{
    for (auto & node : graph)
    {
        if (node.execution_state)
            node.processor->setDescription(
                    "(" + std::to_string(node.execution_state->num_executed_jobs) + " jobs, execution time: "
                    + std::to_string(node.execution_state->execution_time_ns / 1e9) + " sec., preparation time: "
                    + std::to_string(node.execution_state->preparation_time_ns / 1e9) + " sec.)");
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
    out.finish();

    return out.str();
}

}
