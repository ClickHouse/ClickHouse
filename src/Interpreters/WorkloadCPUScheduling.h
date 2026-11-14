#pragma once

#include <Common/Scheduler/CPULeaseAllocation.h>
#include <Common/Scheduler/ResourceLink.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// The resources of the threads of a query, see `CREATE RESOURCE ... (MASTER THREAD, WORKER THREAD)`
struct WorkloadCPULinks
{
    ResourceLink master_thread;
    ResourceLink worker_thread;
    /// A resource is declared for MASTER THREAD or WORKER THREAD, so `ConcurrencyControl` is not used
    bool enabled = false;
};

WorkloadCPULinks getWorkloadCPULinks(const ContextPtr & query_context);

/// Settings of a preemptive CPU lease of the query (`cpu_slot_preemption`), without callbacks
CPULeaseSettings getCPULeaseSettings(const ContextPtr & query_context);

}
