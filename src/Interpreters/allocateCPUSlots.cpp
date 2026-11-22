#include <Interpreters/allocateCPUSlots.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/ConcurrencyControl.h>
#include <Common/CurrentThread.h>
#include <Common/Scheduler/CPULeaseAllocation.h>
#include <Common/Scheduler/CPUSlotsAllocation.h>
#include <Common/Scheduler/IResourceManager.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool opentelemetry_trace_cpu_scheduling;
    extern const SettingsString workload;
}

SlotAllocationPtr allocateCPUSlots(size_t num_threads, bool lazy_allocation, CPUSlotPreemptionCallbacks callbacks)
{
    // The first thread is called master thread.
    // It is NOT the thread that handles async tasks (unless query has max_threads=1).
    // Master thread is different from other threads due to special role in scheduling:
    //  1. During query start, master thread spawns worker threads, so starting it fast is important.
    //  2. It should never be downscaled to avoid query deadlock (0 threads to process tasks), although it can be preempted.
    //  3. When using ConcurrencyControl, master thread is granted immediately and may not request CPU slot (see 'fair_round_robin').
    //  4. When using resource scheduler, master thread may use different resource link than worker threads.
    const auto master_threads = 1uz;

    // Other threads are called worker threads.
    // They are competing for CPU slots with other queries and may not start or can be preempted and downscaled after they start.
    // Worker threads are spawned by master thread or other worker threads.
    const auto worker_threads = num_threads - master_threads;

    // Under lazy allocation, request only `master_threads` at startup and let the consumer
    // grow the ceiling via `setMax` as it actually has parallelizable work.
    const SlotCount initial_max = lazy_allocation ? master_threads : num_threads;

    // There are a few possible modes of concurrency control.
    // 1) If we have `CREATE RESOURCE cpu (MASTER THREAD, WORKER THREAD)`:
    //    all thread should be allocated through scheduler using the same resource link
    // 2) If we have `CREATE RESOURCE cpu (WORKER THREAD)`:
    //    master thread uses "free" noncompeting CPU slot, the other threads are allocated through resource scheduler
    // 3) If we have two resources `CREATE RESOURCE master_cpu (MASTER THREAD)` and `CREATE RESOURCE worker_cpu (WORKER THREAD)`:
    //    all thread should be allocated through scheduler using different resource links
    // 4) If we have no cpu-related resources:
    //    the ConcurrencyControl class is used instead of resource scheduler
    auto query_context = CurrentThread::tryGetQueryContext();
    ResourceLink master_thread_link;
    ResourceLink worker_thread_link;
    bool workload_cpu_scheduling_is_enabled = false;

    if (query_context)
    {
        /// Hold a shared_ptr to keep the storage alive for the duration of this call.
        /// The storage can be destroyed during server shutdown while a query is still
        /// in pipeline initialization, causing a use-after-free of the internal mutex.
        auto workload_storage = query_context->getWorkloadEntityStoragePtr();
        if (workload_storage)
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
    {
        /// Allocate CPU slots from concurrency control with guaranteed master thread slot.
        return ConcurrencyControl::instance().allocate(master_threads, initial_max);
    }

    if (!master_thread_link && !worker_thread_link) // Only use resource scheduler if at least one resource link is specified, otherwise unlimited
        return std::make_shared<GrantedAllocation>(num_threads);

    /// Allocate CPU slots through resource scheduler
    if (!query_context->getCPUSlotPreemption())
        return std::make_shared<CPUSlotsAllocation>(master_threads, worker_threads, master_thread_link, worker_thread_link);

    const auto & settings = query_context->getSettingsRef();
    auto quantum_ns = std::max<UInt64>(10, query_context->getCPUSlotQuantum());
    return std::make_shared<CPULeaseAllocation>(num_threads, master_thread_link, worker_thread_link,
        CPULeaseSettings
        {
            .quantum_ns = static_cast<ResourceCost>(quantum_ns),
            .report_ns = static_cast<ResourceCost>(quantum_ns / 10),
            .preemption_timeout = std::chrono::milliseconds(query_context->getCPUSlotPreemptionTimeout()),
            .on_preempt = std::move(callbacks.on_preempt),
            .on_resume = std::move(callbacks.on_resume),
            .workload = settings[Setting::workload],
            .parking_enabled = query_context->getCPUSlotParking(),
            .trace_cpu_scheduling = settings[Setting::opentelemetry_trace_cpu_scheduling],
        },
        initial_max);
}

}
