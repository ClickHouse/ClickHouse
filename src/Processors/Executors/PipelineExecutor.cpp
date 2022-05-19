#include <queue>
#include <IO/WriteBufferFromString.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/MemoryTracker.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/ExecutingGraph.h>
#include <QueryPipeline/printPipeline.h>
#include <Processors/ISource.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Context.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Common/scope_guard_safe.h>

#ifndef NDEBUG
    #include <Common/Stopwatch.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


PipelineExecutor::PipelineExecutor(Processors & processors, QueryStatus * elem)
    : process_list_element(elem)
{
    if (process_list_element)
        profile_processors = process_list_element->getContext()->getSettingsRef().log_processors_profiles;

    try
    {
        graph = std::make_unique<ExecutingGraph>(processors, profile_processors);
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
    if (process_list_element)
    {
        // Add the pipeline to the QueryStatus at the end to avoid issues if other things throw
        // as that would leave the executor "linked"
        process_list_element->addPipelineExecutor(this);
    }
}

PipelineExecutor::~PipelineExecutor()
{
    if (process_list_element)
        process_list_element->removePipelineExecutor(this);
}

const Processors & PipelineExecutor::getProcessors() const
{
    return graph->getProcessors();
}

void PipelineExecutor::cancel()
{
    cancelled = true;
    finish();
    graph->cancel();
}

void PipelineExecutor::finish()
{
    tasks.finish();
}

void PipelineExecutor::execute(size_t num_threads)
{
    checkTimeLimit();
    if (num_threads < 1)
        num_threads = 1;

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
    if (!is_execution_initialized)
    {
        initializeExecution(1);

        if (yield_flag && *yield_flag)
            return true;
    }

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

bool PipelineExecutor::checkTimeLimitSoft()
{
    if (process_list_element)
    {
        bool continuing = process_list_element->checkTimeLimitSoft();
        // We call cancel here so that all processors are notified and tasks waken up
        // so that the "break" is faster and doesn't wait for long events
        if (!continuing)
            cancel();
        return continuing;
    }

    return true;
}

bool PipelineExecutor::checkTimeLimit()
{
    bool continuing = checkTimeLimitSoft();
    if (!continuing)
        process_list_element->checkTimeLimit(); // Will throw if needed

    return continuing;
}

void PipelineExecutor::finalizeExecution()
{
    checkTimeLimit();

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
              context.total_time_ns / 1e9,
              context.execution_time_ns / 1e9,
              context.processing_time_ns / 1e9,
              context.wait_time_ns / 1e9);
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
        /// Just traverse graph and prepare any processor.
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

            if (!checkTimeLimitSoft())
                break;

#ifndef NDEBUG
            Stopwatch processing_time_watch;
#endif

            /// Try to execute neighbour processor.
            {
                Queue queue;
                Queue async_queue;

                /// Prepare processor after execution.
                if (!graph->updateNode(context.getProcessorID(), queue, async_queue))
                    finish();

                /// Push other tasks to global queue.
                tasks.pushTasks(queue, async_queue, context);
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
    context.wait_time_ns = context.total_time_ns - context.execution_time_ns - context.processing_time_ns;
#endif
}

void PipelineExecutor::initializeExecution(size_t num_threads)
{
    is_execution_initialized = true;

    Queue queue;
    graph->initializeExecution(queue);

    tasks.init(num_threads, profile_processors);
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

    SCOPE_EXIT_SAFE(
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
    printPipeline(graph->getProcessors(), statuses, out);
    out.finalize();

    return out.str();
}

}
