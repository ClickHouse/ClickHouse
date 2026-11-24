#include <Interpreters/WorkloadCPUScheduling.h>

#include <Common/Scheduler/IResourceManager.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool opentelemetry_trace_cpu_scheduling;
    extern const SettingsString workload;
}

WorkloadCPULinks getWorkloadCPULinks(const ContextPtr & query_context)
{
    WorkloadCPULinks links;

    /// Hold a shared_ptr to keep the storage alive for the duration of this call.
    /// The storage can be destroyed during server shutdown while a query is still
    /// in pipeline initialization, causing a use-after-free of the internal mutex.
    auto workload_storage = query_context->getWorkloadEntityStoragePtr();
    if (workload_storage)
    {
        String master_thread_resource_name = workload_storage->getMasterThreadResourceName();
        if (!master_thread_resource_name.empty())
            links.master_thread = query_context->getWorkloadClassifier()->get(master_thread_resource_name);
        String worker_thread_resource_name = workload_storage->getWorkerThreadResourceName();
        if (!worker_thread_resource_name.empty())
            links.worker_thread = query_context->getWorkloadClassifier()->get(worker_thread_resource_name);
        links.enabled = !master_thread_resource_name.empty() || !worker_thread_resource_name.empty();
    }
    return links;
}

CPULeaseSettings getCPULeaseSettings(const ContextPtr & query_context)
{
    auto quantum_ns = std::max<UInt64>(10, query_context->getCPUSlotQuantum());
    CPULeaseSettings settings;
    settings.quantum_ns = static_cast<ResourceCost>(quantum_ns);
    settings.report_ns = static_cast<ResourceCost>(quantum_ns / 10);
    settings.preemption_timeout = std::chrono::milliseconds(query_context->getCPUSlotPreemptionTimeout());
    settings.workload = query_context->getSettingsRef()[Setting::workload];
    settings.parking_enabled = query_context->getCPUSlotParking();
    settings.trace_cpu_scheduling = query_context->getSettingsRef()[Setting::opentelemetry_trace_cpu_scheduling];
    return settings;
}

}
