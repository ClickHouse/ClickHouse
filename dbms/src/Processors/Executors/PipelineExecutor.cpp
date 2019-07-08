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

PipelineExecutor::PipelineExecutor(Processors & processors)
    : processors(processors)
    , num_tasks_and_active_threads(0)
    , cancelled(false)
    , finished(false)
    , num_waiting_threads(0)
    , num_processing_executors(0)
    , expand_pipeline_task(nullptr)
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
            exception.addMessage("While executing " + processor->getName() + " ("
                                 + toString(reinterpret_cast<std::uintptr_t>(processor)) + ") ");
        throw;
    }
}

void PipelineExecutor::addJob(ExecutionState * execution_state)
{
    auto job = [execution_state]()
    {
        try
        {
            Stopwatch watch;
            executeJob(execution_state->processor);
            execution_state->execution_time_ns += watch.elapsed();

            ++execution_state->num_executed_jobs;
        }
        catch (...)
        {
            execution_state->exception = std::current_exception();
        }
    };

    execution_state->job = std::move(job);
}

void PipelineExecutor::expandPipeline(Stack & stack, UInt64 pid)
{
    auto & cur_node = graph[pid];
    auto new_processors = cur_node.processor->expandPipeline();

    for (const auto & processor : new_processors)
    {
        if (processors_map.count(processor.get()))
            throw Exception("Processor " + processor->getName() + " was already added to pipeline.",
                    ErrorCodes::LOGICAL_ERROR);

        processors_map[processor.get()] = graph.size();
        graph.emplace_back(processor.get(), graph.size());
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
                stack.push(node);
            }
        }
    }
}

bool PipelineExecutor::tryAddProcessorToStackIfUpdated(Edge & edge, Stack & stack)
{
    /// In this method we have ownership on edge, but node can be concurrently accessed.

    auto & node = graph[edge.to];

    ExecStatus status = node.status.load();

    /// Don't add processor if nothing was read from port.
    if (status != ExecStatus::New && edge.version == edge.prev_version)
        return false;

    if (status == ExecStatus::Finished)
        return false;

    /// Signal that node need to be prepared.
    node.need_to_be_prepared = true;
    edge.prev_version = edge.version;

    /// Try to get ownership for node.

    /// Assume that current status is New or Idle. Otherwise, can't prepare node.
    if (status != ExecStatus::New)
        status = ExecStatus::Idle;

    /// Statuses but New and Idle are not interesting because they own node.
    /// Prepare will be called in owning thread before changing status.
    while (!node.status.compare_exchange_weak(status, ExecStatus::Preparing))
        if (!(status == ExecStatus::New || status == ExecStatus::Idle) || !node.need_to_be_prepared)
            return false;

    stack.push(edge.to);
    return true;

}

bool PipelineExecutor::prepareProcessor(UInt64 pid, Stack & stack, size_t thread_number, bool async)
{
    /// In this method we have ownership on node.
    auto & node = graph[pid];

    {
        /// Stopwatch watch;

        /// Disable flag before prepare call. Otherwise, we can skip prepare request.
        /// Prepare can be called more times than needed, but it's ok.
        node.need_to_be_prepared = false;

        auto status = node.processor->prepare();

        /// node.execution_state->preparation_time_ns += watch.elapsed();
        node.last_processor_status = status;
    }

    auto add_neighbours_to_prepare_queue = [&] ()
    {
        for (auto & edge : node.backEdges)
            tryAddProcessorToStackIfUpdated(edge, stack);

        for (auto & edge : node.directEdges)
            tryAddProcessorToStackIfUpdated(edge, stack);
    };

    auto try_release_ownership = [&] ()
    {
        /// This function can be called after expand pipeline, where node from outer scope is not longer valid.
        auto & node_ = graph[pid];
        ExecStatus expected = ExecStatus::Idle;
        node_.status = ExecStatus::Idle;

        if (node_.need_to_be_prepared)
        {
            while (!node_.status.compare_exchange_weak(expected, ExecStatus::Preparing))
                if (!(expected == ExecStatus::Idle) || !node_.need_to_be_prepared)
                    return;

            stack.push(pid);
        }
    };

    switch (node.last_processor_status)
    {
        case IProcessor::Status::NeedData:
        {
            add_neighbours_to_prepare_queue();
            try_release_ownership();

            break;
        }
        case IProcessor::Status::PortFull:
        {
            add_neighbours_to_prepare_queue();
            try_release_ownership();

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
            return true;
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
            if (!async)
                throw Exception("Processor returned status Wait before Async.", ErrorCodes::LOGICAL_ERROR);
            break;
        }
        case IProcessor::Status::ExpandPipeline:
        {
            executor_contexts[thread_number]->task_list.emplace_back(
                node.execution_state.get(),
                &stack
            );

            ExpandPipelineTask * desired = &executor_contexts[thread_number]->task_list.back();
            ExpandPipelineTask * expected = nullptr;

            while (!expand_pipeline_task.compare_exchange_strong(expected, desired))
            {
                doExpandPipeline(expected);
                expected = nullptr;
            }

            doExpandPipeline(desired);

            /// node is not longer valid after pipeline was expanded
            graph[pid].need_to_be_prepared = true;
            try_release_ownership();
            break;
        }
    }

    return false;
}

void PipelineExecutor::doExpandPipeline(ExpandPipelineTask * task)
{
    std::unique_lock lock(task->mutex);
    ++task->num_waiting_threads;

    task->condvar.wait(lock, [&]()
    {
        return task->num_waiting_threads >= num_processing_executors || expand_pipeline_task != task;
    });

    /// After condvar.wait() task may point to trash. Can change it only if it is still in expand_pipeline_task.
    if (expand_pipeline_task == task)
    {
        expandPipeline(*task->stack, task->node_to_expand->processors_id);

        if (graph.size() > task_queue_reserved_size)
        {
            task_queue_reserved_size = graph.size();
            task_queue.reserve(graph.size());
        }

        expand_pipeline_task = nullptr;

        lock.unlock();
        task->condvar.notify_all();
    }
}

void PipelineExecutor::finish()
{
    bool expected = false;
    if (finished.compare_exchange_strong(expected, true))
    {
        for (auto & context : executor_contexts)
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

namespace
{

struct AtomicCounter
{
    std::atomic<uint64_t> & counter;

    constexpr static uint64_t num_bits_for_thread_counter = 16;
    constexpr static uint64_t num_bits_for_task_counter = 8 * sizeof(uint64_t) - num_bits_for_thread_counter;
    constexpr static uint64_t task_mask = (uint64_t(1) << num_bits_for_task_counter) - 1;
    constexpr static uint64_t thread_mask = ~task_mask;

    template <typename Updater>
    void update(Updater updater)
    {
        uint64_t expected = counter.load();
        uint64_t desired = updater(expected);

        while (!counter.compare_exchange_weak(expected, desired))
            desired = updater(expected);
    }

    uint64_t getNumTasks() const { return (counter.load() & task_mask); }
    /// uint64_t getNumThreads() const { return ((counter.load() & thread_mask)) >> num_bits_for_task_counter; }

    void addTask()
    {
        update([](uint64_t value) { return ((value & task_mask) + 1) | (value & thread_mask); });
    }

    void removeTask()
    {
        update([](uint64_t value) { return ((value & task_mask) - 1) | (value & thread_mask); });
    }

    void addActiveThread()
    {
        update([](uint64_t value)
        {
            return (value & task_mask) | ((value & thread_mask) + (uint64_t(1) << num_bits_for_task_counter));
        });
    }

    void removeActiveThread()
    {
        update([](uint64_t value)
        {
            return (value & task_mask) | ((value & thread_mask) - (uint64_t(1) << num_bits_for_task_counter));
        });
    }
};

}

void PipelineExecutor::executeSingleThread(size_t thread_num, size_t)
{
    UInt64 total_time_ns = 0;
    UInt64 execution_time_ns = 0;
    UInt64 processing_time_ns = 0;
    UInt64 wait_time_ns = 0;

    Stopwatch total_time_watch;

    std::mutex mutex;
    ExecutionState * state = nullptr;
    AtomicCounter threads_and_tasks_counter { .counter = num_tasks_and_active_threads };

    threads_and_tasks_counter.addActiveThread();

    auto prepare_processor = [&](UInt64 pid, Stack & stack)
    {
        try
        {
            return prepareProcessor(pid, stack, thread_num, false);
        }
        catch (...)
        {
            graph[pid].execution_state->exception = std::current_exception();
            finish();
        }

        return false;
    };

    while (!finished)
    {

        /// First, find any processor to execute.
        /// Just travers graph and prepare any processor.
        while (!finished)
        {

            while (threads_and_tasks_counter.getNumTasks())
            {
                if (task_queue.pop(state))
                {
                    threads_and_tasks_counter.removeTask();
                    break;
                }
                else
                    state = nullptr;
            }

            if (state)
                break;

            std::unique_lock lock(mutex);

            threads_and_tasks_counter.removeActiveThread();

            if (threads_and_tasks_counter.counter.load() == 0)
                finish();

            executor_contexts[thread_num]->is_waiting = true;
            executor_contexts[thread_num]->condvar.wait_for(lock, std::chrono::microseconds(1), [&]()
            {
                return finished || threads_and_tasks_counter.getNumTasks();
            });
            executor_contexts[thread_num]->is_waiting = false;

            threads_and_tasks_counter.addActiveThread();
        }

        if (finished)
            break;

        while (state)
        {
            if (finished)
                break;

            Stopwatch processing_time_watch;

            /// Try to execute neighbour processor.
            {
                Stack stack;

                ++num_processing_executors;
                while (auto task = expand_pipeline_task.load())
                    doExpandPipeline(task);

                /// Execute again if can.
                if (!prepare_processor(state->processors_id, stack))
                    state = nullptr;

                /// Process all neighbours. Children will be on the top of stack, then parents.
                while (!state && !stack.empty() && !finished)
                {
                    auto current_processor = stack.top();
                    stack.pop();

                    if (prepare_processor(current_processor, stack))
                        state = graph[current_processor].execution_state.get();
                }

                bool do_wake_up_threads = !stack.empty();

                while (!stack.empty() && !finished)
                {
                    auto cur_state = graph[stack.top()].execution_state.get();
                    stack.pop();

                    threads_and_tasks_counter.addTask();
                    while (!task_queue.push(cur_state));
                }

                if (do_wake_up_threads)
                {
                    for (auto & context : executor_contexts)
                        if (context->is_waiting)
                            context->condvar.notify_one();
                }

                --num_processing_executors;
                while (auto task = expand_pipeline_task.load())
                    doExpandPipeline(task);
            }

            processing_time_ns += processing_time_watch.elapsed();

            if (!state)
                continue;

            addJob(state);

            {
                Stopwatch execution_time_watch;
                state->job();
                execution_time_ns += execution_time_watch.elapsed();
            }

            if (state->exception)
                finish();

            if (finished)
                break;
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
    task_queue_reserved_size = std::max<size_t>(min_task_queue_size, graph.size());
    task_queue.reserve_unsafe(task_queue_reserved_size);

    Stack stack;

    addChildlessProcessorsToStack(stack);

    AtomicCounter threads_and_tasks_counter { .counter = num_tasks_and_active_threads };

    while (!stack.empty())
    {
        UInt64 proc = stack.top();
        stack.pop();

        auto cur_state = graph[proc].execution_state.get();
        threads_and_tasks_counter.addTask();
        while (!task_queue.push(cur_state));
    }

    ThreadPool pool(num_threads);

    SCOPE_EXIT(
            finish();
            pool.wait()
    );

    executor_contexts.reserve(num_threads);

    for (size_t i = 0; i < num_threads; ++i)
    {
        executor_contexts.emplace_back(std::make_unique<ExecutorContext>());
        auto * executor_context = executor_contexts.back().get();
        executor_context->is_waiting = false;
    }

    auto thread_group = CurrentThread::getGroup();

    for (size_t i = 0; i < num_threads; ++i)
    {
        pool.schedule([this, thread_group, thread_num = i, num_threads]
        {
            ThreadStatus thread_status;

            if (thread_group)
                CurrentThread::attachTo(thread_group);

            SCOPE_EXIT(
                    if (thread_group)
                        CurrentThread::detachQueryIfNotDetached();
            );

            executeSingleThread(thread_num, num_threads);
        });
    }

    pool.wait();
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
