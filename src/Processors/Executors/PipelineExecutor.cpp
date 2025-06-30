#include <memory>
#include <IO/WriteBufferFromString.h>
#include <Common/ISlotControl.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentThread.h>
#include <Common/CurrentMetrics.h>
#include <Common/ConcurrencyControl.h>
#include <Common/Scheduler/CPUSlotsAllocation.h>
#include <Common/Scheduler/IResourceManager.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Common/setThreadName.h>
#include <Common/logger_useful.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/ExecutingGraph.h>
#include <QueryPipeline/printPipeline.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Processors/ISource.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Context.h>
#include <Common/scope_guard_safe.h>
#include <Common/Exception.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Core/Settings.h>

#ifndef NDEBUG
    #include <Common/Stopwatch.h>
#endif


namespace CurrentMetrics
{
    extern const Metric QueryPipelineExecutorThreads;
    extern const Metric QueryPipelineExecutorThreadsActive;
    extern const Metric QueryPipelineExecutorThreadsScheduled;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool log_processors_profiles;
    extern const SettingsBool opentelemetry_trace_processors;
    extern const SettingsSeconds max_execution_time;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


PipelineExecutor::PipelineExecutor(std::shared_ptr<Processors> & processors, QueryStatusPtr elem)
    : process_list_element(std::move(elem))
{
    if (process_list_element)
    {
        profile_processors = process_list_element->getContext()->getSettingsRef()[Setting::log_processors_profiles]
            && process_list_element->getContext()->getProcessorsProfileLog();
        trace_processors = process_list_element->getContext()->getSettingsRef()[Setting::opentelemetry_trace_processors];
    }
    try
    {
        graph = std::make_unique<ExecutingGraph>(processors, profile_processors);
    }
    catch (Exception & exception)
    {
        /// If exception was thrown while pipeline initialization, it means that query pipeline was not build correctly.
        /// It is logical error, and we need more information about pipeline.
        WriteBufferFromOwnString buf;
        printPipeline(*processors, buf);
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

void PipelineExecutor::cancel(ExecutionStatus reason)
{
    /// It is allowed to cancel not started query by user.
    if (reason == ExecutionStatus::CancelledByUser)
        tryUpdateExecutionStatus(ExecutionStatus::NotStarted, reason);

    tryUpdateExecutionStatus(ExecutionStatus::Executing, reason);
    finish();
    graph->cancel();
}

void PipelineExecutor::cancelReading()
{
    if (!cancelled_reading)
    {
        cancelled_reading = true;
        graph->cancel(/*cancel_all_processors*/ false);
    }
}

void PipelineExecutor::finish()
{
    tasks.finish();
}

bool PipelineExecutor::tryUpdateExecutionStatus(ExecutionStatus expected, ExecutionStatus desired)
{
    return execution_status.compare_exchange_strong(expected, desired);
}

void PipelineExecutor::execute(size_t num_threads, bool concurrency_control)
{
    checkTimeLimit();
    num_threads = std::max<size_t>(num_threads, 1);

    OpenTelemetry::SpanHolder span("PipelineExecutor::execute()");
    span.addAttribute("clickhouse.thread_num", num_threads);

    try
    {
        executeImpl(num_threads, concurrency_control);

        /// Log all of the LOGICAL_ERROR exceptions.
        for (auto & node : graph->nodes)
            if (node->exception && getExceptionErrorCode(node->exception) == ErrorCodes::LOGICAL_ERROR)
                tryLogException(node->exception, log);

        /// Rethrow the first exception.
        for (auto & node : graph->nodes)
            if (node->exception)
                std::rethrow_exception(node->exception);

        /// Exception which happened in executing thread, but not at processor.
        tasks.rethrowFirstThreadException();
    }
    catch (...)
    {
        span.addAttribute(DB::ExecutionStatus::fromCurrentException());

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
        initializeExecution(1, true);

        // Acquire slot until we are done
        single_thread_cpu_slot = cpu_slots->acquire();
        chassert(single_thread_cpu_slot && "Unable to allocate cpu slot for the first thread, but we just allocated at least one slot");

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
            cancel(ExecutionStatus::CancelledByTimeout);

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

void PipelineExecutor::setReadProgressCallback(ReadProgressCallbackPtr callback)
{
    read_progress_callback = std::move(callback);
}

void PipelineExecutor::finalizeExecution()
{
    single_thread_cpu_slot.reset();
    {
        std::lock_guard lock(spawn_mutex);
        cpu_slots.reset();
    }

    checkTimeLimit();

    auto status = execution_status.load();
    if (status == ExecutionStatus::CancelledByTimeout || status == ExecutionStatus::CancelledByUser)
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
        if (node->processor && read_progress_callback)
        {
            /// Some executors might have reported progress as part of their finish() call
            /// For example, when reading from parallel replicas the coordinator will cancel the queries as soon as it
            /// enough data (on LIMIT), but as the progress report is asynchronous it might not be reported until the
            /// connection is cancelled and all packets drained
            /// To cover these cases we check if there is any pending progress in the processors to report
            if (auto read_progress = node->processor->getReadProgress())
            {
                if (read_progress->counters.total_rows_approx)
                    read_progress_callback->addTotalRowsApprox(read_progress->counters.total_rows_approx);

                if (read_progress->counters.total_bytes)
                    read_progress_callback->addTotalBytes(read_progress->counters.total_bytes);

                /// We are finalizing the execution, so no need to call onProgress if there is nothing to report
                if (read_progress->counters.read_rows || read_progress->counters.read_bytes)
                    read_progress_callback->onProgress(
                        read_progress->counters.read_rows, read_progress->counters.read_bytes, read_progress->limits);
            }
        }
    }

    if (!all_processors_finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline stuck. Current state:\n{}", dumpPipeline());
}

void PipelineExecutor::executeSingleThread(size_t thread_num)
{
    executeStepImpl(thread_num);

#ifndef NDEBUG
    auto & context = tasks.getThreadContext(thread_num);
    LOG_TEST(log,
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

    auto & context = tasks.getThreadContext(thread_num);
    bool yield = false;

    while (!tasks.isFinished() && !yield)
    {
        /// First, find any processor to execute.
        while (!tasks.isFinished() && !context.hasTask())
            tasks.tryGetTask(context);

        while (!tasks.isFinished() && context.hasTask() && !yield)
        {
            if (!context.executeTask())
                cancel(ExecutionStatus::Exception);

            if (tasks.isFinished())
                break;

            if (!checkTimeLimitSoft())
                break;

#ifndef NDEBUG
            Stopwatch processing_time_watch;
#endif

            /// Try to execute neighbour processor.
            ExecutorTasks::SpawnStatus spawn_status = ExecutorTasks::DO_NOT_SPAWN;
            {
                Queue queue;
                Queue async_queue;

                /// Prepare processor after execution.
                auto status = graph->updateNode(context.getProcessorID(), queue, async_queue);
                if (status == ExecutingGraph::UpdateNodeStatus::Exception)
                    cancel(ExecutionStatus::Exception);

                /// Push other tasks to global queue.
                if (status == ExecutingGraph::UpdateNodeStatus::Done)
                    spawn_status = tasks.pushTasks(queue, async_queue, context);
            }

#ifndef NDEBUG
            context.processing_time_ns += processing_time_watch.elapsed();
#endif

            if (spawn_status == ExecutorTasks::SHOULD_SPAWN)
            {
                try
                {
                    /// Upscale if possible.
                    spawnThreads();
                }
                catch (...)
                {
                    /// spawnThreads can throw an exception, for example CANNOT_SCHEDULE_TASK.
                    /// We should cancel execution properly before rethrow.
                    cancel(ExecutionStatus::Exception);
                    throw;
                }
            }

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

// This function properly allocates CPU slots for the thread pool
// There are a few possible modes of concurrency control.
// 1) If we have `CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)`:
//    all thread should be allocated through scheduler using the same resource link
// 2) If we have `CREATE RESOURCE cpu (WORKER THREAD)`:
//    the first (master) thread uses "free" noncompeting CPU slot, the other threads are allocated through resource scheduler
// 3) If we have two resources `CREATE RESOURCE master_cpu (MASTER THREAD)` and `CREATE RESOURCE worker_cpu (WORKER THREAD)`:
//    all thread should be allocated through scheduler using different resource links
// 4) If we have no cpu-related resources:
//    the ConcurrencyControl class is used instead of resource scheduler
// NOTE: With enabled workload CPU scheduling, both links could be empty in case of unknown workload.
static SlotAllocationPtr allocateCPUSlots(size_t num_threads, bool concurrency_control)
{
    if (concurrency_control)
    {
        auto query_context = CurrentThread::getQueryContext();
        ResourceLink master_thread_link;
        ResourceLink worker_thread_link;
        bool workload_cpu_scheduling_is_enabled = false;

        if (query_context)
        {
            String master_thread_resource_name = query_context->getWorkloadEntityStorage().getMasterThreadResourceName();
            if (!master_thread_resource_name.empty())
                master_thread_link = query_context->getWorkloadClassifier()->get(master_thread_resource_name);
            String worker_thread_resource_name = query_context->getWorkloadEntityStorage().getWorkerThreadResourceName();
            if (!worker_thread_resource_name.empty())
                worker_thread_link = query_context->getWorkloadClassifier()->get(worker_thread_resource_name);
            workload_cpu_scheduling_is_enabled = !master_thread_resource_name.empty() || !worker_thread_resource_name.empty();
        }

        if (workload_cpu_scheduling_is_enabled)
        {
            /// Allocate CPU slots through resource scheduler
            constexpr size_t master_threads = 1uz;
            return std::make_shared<CPUSlotsAllocation>(master_threads, num_threads - master_threads, master_thread_link, worker_thread_link);
        }
        else
        {
            /// Allocate CPU slots from concurrency control
            constexpr size_t min_threads = 1uz; // Number of threads that should be granted to every query no matter how many threads are already running in other queries
            return ConcurrencyControl::instance().allocate(min_threads, num_threads);
        }
    }
    else
    {
        /// If concurrency control is not used we should not even count threads as competing.
        /// To avoid counting them in ConcurrencyControl, we create dummy slot allocation.
        return std::make_shared<GrantedAllocation>(num_threads);
    }
}

void PipelineExecutor::initializeExecution(size_t num_threads, bool concurrency_control)
{
    is_execution_initialized = true;
    tryUpdateExecutionStatus(ExecutionStatus::NotStarted, ExecutionStatus::Executing);

    cpu_slots = allocateCPUSlots(num_threads, concurrency_control);

    Queue queue;
    Queue async_queue;
    graph->initializeExecution(queue, async_queue);

    /// use_threads should reflect number of thread spawned and can grow with tasks.upscale(...).
    /// Starting from 1 instead of 0 is to tackle the single thread scenario, where no upscale() will
    /// be invoked but actually 1 thread used.
    tasks.init(num_threads, 1, profile_processors, trace_processors, read_progress_callback.get());
    tasks.fill(queue, async_queue);

    if (num_threads > 1)
        pool = std::make_unique<ThreadPool>(CurrentMetrics::QueryPipelineExecutorThreads, CurrentMetrics::QueryPipelineExecutorThreadsActive, CurrentMetrics::QueryPipelineExecutorThreadsScheduled, num_threads);
}

void PipelineExecutor::spawnThreads()
{
    /// Only allow one thread to spawn, if someone is already spawning threads, just skip.
    if (spawn_mutex.try_lock())
    {
        std::lock_guard lock(spawn_mutex, std::adopt_lock);
        spawnThreadsImpl({});
    }
}

void PipelineExecutor::spawnThreadsImpl(AcquiredSlotPtr slot)
{
    while (cpu_slots)
    {
        if (!slot)
            slot = cpu_slots->tryAcquire();
        if (!slot)
            return;

        size_t thread_num = threads.fetch_add(1);

        /// Count of threads in use should be updated for proper finish() condition.
        /// NOTE: this will not decrease `use_threads` below initially granted count
        const auto spawn_status = tasks.upscale(thread_num + 1);

        /// Start new thread
        pool->scheduleOrThrowOnError([this, thread_num, thread_group = CurrentThread::getGroup(), my_slot = std::move(slot)]
        {
            ThreadGroupSwitcher switcher(thread_group, "QueryPipelineEx");

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

        chassert(!slot); // Just to make sure. Slot should be empty after we moved it to the thread lambda

        if (spawn_status == ExecutorTasks::DO_NOT_SPAWN)
            return;
    }
}

void PipelineExecutor::executeImpl(size_t num_threads, bool concurrency_control)
{
    initializeExecution(num_threads, concurrency_control);

    try
    {
        if (num_threads > 1)
        {
            {
                std::lock_guard lock(spawn_mutex);
                // Start at least one thread, could block to acquire the first CPU slot
                spawnThreadsImpl(cpu_slots->acquire());
            }
            tasks.processAsyncTasks();
            pool->wait();
        }
        else
        {
            auto slot = cpu_slots->tryAcquire();
            executeSingleThread(0);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        cancel(ExecutionStatus::Exception);
        if (pool)
            pool->wait();

        throw;
    }
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

    std::vector<std::optional<IProcessor::Status>> statuses;
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
