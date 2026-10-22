#pragma once

#include <base/types.h>

#include <Common/Priority.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>

#include <limits>

namespace DB
{

// Represent parsed version for SETTINGS clause in CREATE WORKLOAD query
// Note that it is parsed for specific resource name and unit
struct WorkloadSettings
{
    static constexpr Int64 unlimited = std::numeric_limits<Int64>::max();
    static constexpr Float64 default_burst_seconds = 1.0;

    /// Weight, priority and precedence among siblings
    Float64 weight = 1.0;
    Priority priority;
    Priority precedence;

    /// IO throttling constraints
    Float64 max_bytes_per_second = 0; // Zero means unlimited
    Float64 max_burst_bytes = 0; // default is `default_burst_seconds * max_bytes_per_second`

    /// CPU throttling constraints
    Float64 max_cpus = 0; // Zero means unlimited
    Float64 max_burst_cpu_seconds = 1.0;

    /// Query throttling constraints
    Float64 max_queries_per_second = 0; // Zero means unlimited
    Float64 max_burst_queries = 0; // default is `default_burst_seconds * max_queries_per_second`

    /// Limits total number of concurrent resource requests that are allowed to consume
    Int64 max_io_requests = unlimited;

    /// Limits total bytes in-inflight for concurrent IO resource requests
    Int64 max_bytes_inflight = unlimited;

    /// Limits total number of query threads
    Int64 max_concurrent_threads = unlimited;

    /// Limits total number of queries
    Int64 max_concurrent_queries = unlimited;

    /// Limits total number of waiting queries
    Int64 max_waiting_queries = unlimited;

    /// Limits total memory reservation
    Int64 max_memory = unlimited;

    // Throttler (time-shared resource)
    bool hasThrottler(CostUnit unit) const;
    Float64 getThrottlerMaxSpeed(CostUnit unit) const;
    Float64 getThrottlerMaxBurst(CostUnit unit) const;

    // Semaphore (time-shared resource)
    bool hasSemaphore(CostUnit unit) const;
    Int64 getSemaphoreMaxRequests(CostUnit unit) const;
    Int64 getSemaphoreMaxCost(CostUnit unit) const;

    // Queue (both time-shared and space-shared resources)
    bool hasQueueLimit(CostUnit unit) const;
    Int64 getQueueLimit(CostUnit unit) const;

    // Allocation Limit (space-shared resource)
    bool hasAllocationLimit(CostUnit unit) const;
    Int64 getAllocationLimit(CostUnit unit) const;

    // Should be called after default constructor
    void initFromChanges(const ASTCreateWorkloadQuery::SettingsChanges & changes, const String & resource_name = {}, bool throw_on_unknown_setting = true);
};

}
