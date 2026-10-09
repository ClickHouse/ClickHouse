#include <Processors/Executors/Runtime/V2/Engine/WorkerPool.h>
#include <Common/ConcurrencyControl.h>
#include <Common/CurrentThread.h>
#include <Common/Scheduler/CPULeaseAllocation.h>
#include <Common/Scheduler/CPUSlotsAllocation.h>
#include <Common/Scheduler/IResourceManager.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/setThreadName.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>

#include <algorithm>

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

namespace Runtime::V2
{

SlotAllocationPtr WorkerPool::allocateCPU(bool concurrency_control, SlotCount initial_max)
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
            .on_preempt = [this](size_t slot_id) { preempt(slot_id); },
            .on_resume = [this](size_t slot_id) { resume(slot_id); },
            .workload = query_context->getSettingsRef()[Setting::workload],
            .trace_cpu_scheduling = query_context->getSettingsRef()[Setting::opentelemetry_trace_cpu_scheduling],
        },
        initial_max);
}

WorkerPool::WorkerPool(ExecutingPipeline & pipeline_, size_t max_threads_, bool concurrency_control)
    : pipeline(pipeline_)
    , max_threads(max_threads_)
    , coordinator(*this, max_threads)
    , requested_threads(concurrency_control && ConcurrencyControl::instance().getLazyAllocation() ? 1 : max_threads)
    , cpu_slots(allocateCPU(concurrency_control, requested_threads))
    , threads(max_threads)
{
    for (size_t worker_id = 0; worker_id < max_threads; ++worker_id)
        workers.emplace_back(*this, worker_id);

    if (pipeline.process_list_element)
    {
        reservation = pipeline.process_list_element->getMemoryReservation();
        tracker = pipeline.process_list_element->getMemoryTracker();
    }
}

WorkerPool::~WorkerPool()
{
    chassert(std::ranges::none_of(threads, [](const auto & thread) { return thread.has_value(); }));
}

void WorkerPool::runSlot(AcquiredSlotPtr slot, std::atomic_bool * yield_flag)
{
    const size_t worker_id = slot->slot_id;

    coordinator.enter(worker_id);
    try
    {
        WorkerSlot worker_slot(std::move(slot), reservation, tracker);
        workers[worker_id].run(worker_slot, yield_flag);
    }
    catch (...)
    {
        pipeline.fail(std::current_exception());
        coordinator.stop();
    }
    coordinator.leave(worker_id);
}

void WorkerPool::initializeExecution()
{
    std::vector<Task> root_tasks;
    for (auto * sink : pipeline.sinks())
    {
        sink->lock.setExecuting();
        root_tasks.push_back(Task::create(*sink, Task::Step::Prepare));
    }
    global_tasks.pushManyFifo(root_tasks);
}

void WorkerPool::preempt(size_t worker_id)
{
    workers[worker_id].handOverTasks();
    coordinator.leave(worker_id);
}

void WorkerPool::resume(size_t worker_id)
{
    coordinator.enter(worker_id);
}

void WorkerPool::run()
{
    auto slot = cpu_slots->acquire();
    workers_count.fetch_add(1);

    try
    {
        initializeExecution();
        if (const size_t initial_tasks = global_tasks.size(); initial_tasks > 1)
            grow(initial_tasks - 1);
    }
    catch (...)
    {
        pipeline.fail(std::current_exception());
        coordinator.stop();
    }

    runSlot(std::move(slot), nullptr);
    workers_count.fetch_sub(1);
}

void WorkerPool::runUntil(std::atomic_bool * yield_flag)
{
    if (max_threads != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Step by step execution needs a pool of one thread, got {}", max_threads);

    if (!single_slot)
    {
        initializeExecution();
        single_slot = cpu_slots->acquire();
    }

    workers_count.fetch_add(1);
    runSlot(single_slot, yield_flag);
    workers_count.fetch_sub(1);
}

void WorkerPool::grow(size_t threads_needed)
{
    if (max_threads == 1 || threads_needed == 0 || workers_count.load() >= max_threads)
        return;

    std::lock_guard lock(spawn_mutex);

    const size_t target = std::min(max_threads, workers_count.load() + threads_needed);
    if (requested_threads < target)
    {
        requested_threads = target;
        cpu_slots->setMax(requested_threads);
    }

    while (workers_count.load() < target)
    {
        if (auto slot = cpu_slots->tryAcquire())
            spawn(std::move(slot));
        else
            break;
    }
}

void WorkerPool::spawn(AcquiredSlotPtr slot)
{
    auto & thread = threads[slot->slot_id];
    if (thread)
    {
        thread->join();
        thread.reset();
    }

    workers_count.fetch_add(1);
    try
    {
        thread.emplace([this, my_slot = std::move(slot), thread_group = CurrentThread::getGroup()]
        {
            ThreadGroupSwitcher switcher(thread_group, ThreadName::QUERY_ASYNC_EXECUTOR);
            runSlot(my_slot, nullptr);
            workers_count.fetch_sub(1);
        });
    }
    catch (...)
    {
        workers_count.fetch_sub(1);
        throw;
    }
}

void WorkerPool::stop()
{
    coordinator.stop();
}

bool WorkerPool::isStopped() const
{
    return coordinator.isStopped();
}

void WorkerPool::join()
{
    {
        std::lock_guard lock(spawn_mutex);
        cpu_slots->free();
    }

    for (auto & thread : threads)
    {
        if (!thread)
            continue;
        thread->join();
        thread.reset();
    }

    single_slot.reset();
    cpu_slots.reset();
}

}

}
