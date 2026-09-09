#include <Processors/Executors/Runtime/Engine/WorkerPool.h>
#include <Processors/Executors/Runtime/Engine/Worker.h>
#include <Common/ConcurrencyControl.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Scheduler/CPULeaseAllocation.h>
#include <Common/Scheduler/CPUSlotsAllocation.h>
#include <Common/Scheduler/IResourceManager.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>

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
    extern const SettingsBool opentelemetry_trace_cpu_scheduling;
    extern const SettingsString workload;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

SlotAllocationPtr allocateCPU(size_t max_threads, bool concurrency_control, SlotCount initial_max, WorkersCoordinator & coordinator)
{
    const SlotCount master_threads = 1;
    const SlotCount worker_threads = max_threads - master_threads;

    if (!concurrency_control)
        return std::make_shared<GrantedAllocation>(max_threads);

    auto query_context = CurrentThread::tryGetQueryContext();
    ResourceLink master_thread_link;
    ResourceLink worker_thread_link;
    bool workload_cpu_scheduling_is_enabled = false;

    if (query_context)
    {
        if (auto workload_storage = query_context->getWorkloadEntityStoragePtr())
        {
            String master_thread_resource_name = workload_storage->getMasterThreadResourceName();
            if (!master_thread_resource_name.empty())
                master_thread_link = query_context->getWorkloadClassifier()->get(master_thread_resource_name);
            String worker_thread_resource_name = workload_storage->getWorkerThreadResourceName();
            if (!worker_thread_resource_name.empty())
                worker_thread_link = query_context->getWorkloadClassifier()->get(worker_thread_resource_name);
            workload_cpu_scheduling_is_enabled = !master_thread_resource_name.empty() || !worker_thread_resource_name.empty();
        }
    }

    if (!workload_cpu_scheduling_is_enabled)
        return ConcurrencyControl::instance().allocate(master_threads, initial_max);

    if (!master_thread_link && !worker_thread_link)
        return std::make_shared<GrantedAllocation>(max_threads);

    if (!query_context->getCPUSlotPreemption())
        return std::make_shared<CPUSlotsAllocation>(master_threads, worker_threads, master_thread_link, worker_thread_link);

    const auto quantum_ns = std::max<UInt64>(10, query_context->getCPUSlotQuantum());
    return std::make_shared<CPULeaseAllocation>(
        max_threads,
        master_thread_link,
        worker_thread_link,
        CPULeaseSettings
        {
            .quantum_ns = static_cast<ResourceCost>(quantum_ns),
            .report_ns = static_cast<ResourceCost>(quantum_ns / 10),
            .preemption_timeout = std::chrono::milliseconds(query_context->getCPUSlotPreemptionTimeout()),
            .on_preempt = [&coordinator](size_t slot_id) { coordinator.leave(slot_id); },
            .on_resume = [&coordinator](size_t slot_id) { coordinator.enter(slot_id); },
            .workload = query_context->getSettingsRef()[Setting::workload],
            .trace_cpu_scheduling = query_context->getSettingsRef()[Setting::opentelemetry_trace_cpu_scheduling],
        },
        initial_max);
}

}

WorkerPool::WorkerPool(TaskScheduler & scheduler_, WorkersCoordinator & coordinator_, ExecutingPipeline & pipeline_, size_t max_threads_, bool concurrency_control)
    : scheduler(scheduler_)
    , coordinator(coordinator_)
    , pipeline(pipeline_)
    , max_threads(max_threads_)
    , requested_threads(concurrency_control && ConcurrencyControl::instance().getLazyAllocation() ? 1 : max_threads)
    , cpu_slots(allocateCPU(max_threads, concurrency_control, requested_threads, coordinator))
{
    if (max_threads > 1)
        pool = std::make_unique<ThreadPool>(
            CurrentMetrics::QueryPipelineExecutorThreads,
            CurrentMetrics::QueryPipelineExecutorThreadsActive,
            CurrentMetrics::QueryPipelineExecutorThreadsScheduled,
            max_threads);

    if (pipeline.process_list_element)
    {
        reservation = pipeline.process_list_element->getMemoryReservation();
        tracker = pipeline.process_list_element->getMemoryTracker();
    }
}

WorkerPool::~WorkerPool()
{
    chassert(!pool || pool->active() == 0);
}

void WorkerPool::runSlot(AcquiredSlotPtr slot, std::atomic_bool * yield_flag)
{
    const size_t worker_id = slot->slot_id;

    coordinator.enter(worker_id);
    try
    {
        WorkerSlot worker_slot(std::move(slot), reservation, tracker);
        Worker(worker_id, scheduler, coordinator, pipeline, *this).run(worker_slot, yield_flag);
    }
    catch (...)
    {
        pipeline.fail(std::current_exception());
        coordinator.stop();
    }
    coordinator.leave(worker_id);
    --workers_count;
}

void WorkerPool::run()
{
    auto slot = cpu_slots->acquire();
    ++workers_count;
    runSlot(std::move(slot), nullptr);
}

void WorkerPool::runUntil(std::atomic_bool * yield_flag)
{
    if (max_threads != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Step by step execution needs a pool of one thread, got {}", max_threads);

    if (!single_slot)
        single_slot = cpu_slots->acquire();

    ++workers_count;
    runSlot(single_slot, yield_flag);
}

void WorkerPool::grow()
{
    if (!pool || workers_count >= max_threads || coordinator.idle() > 0 || scheduler.queued() == 0)
        return;

    std::unique_lock lock(spawn_mutex, std::try_to_lock);
    if (!lock || workers_count >= max_threads)
        return;

    if (requested_threads < workers_count + 1)
    {
        requested_threads = workers_count + 1;
        cpu_slots->setMax(requested_threads);
    }

    auto slot = cpu_slots->tryAcquire();
    if (!slot)
        return;

    ++workers_count;
    try
    {
        pool->scheduleOrThrowOnError([this, my_slot = std::move(slot), thread_group = CurrentThread::getGroup()]
        {
            ThreadGroupSwitcher switcher(thread_group, ThreadName::QUERY_ASYNC_EXECUTOR);
            runSlot(my_slot, nullptr);
        });
    }
    catch (...)
    {
        --workers_count;
        throw;
    }
}

void WorkerPool::stop()
{
    {
        std::lock_guard lock(spawn_mutex);
        cpu_slots->free();
    }

    if (pool)
        pool->wait();

    single_slot.reset();
    cpu_slots.reset();
}

}
