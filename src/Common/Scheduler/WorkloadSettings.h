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

    /// Priority and weight among siblings
    Float64 weight = 1.0;
    Priority priority;

    /// IO throttling constraints
    Float64 max_bytes_per_second = 0; // Zero means unlimited
    Float64 max_burst_bytes = 0; // default is `default_burst_seconds * max_bytes_per_second`

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

    /// Settings that are applied depend on cost unit
    CostUnit unit = CostUnit::IOByte;

    // Throttler
    bool hasThrottler() const;
    Float64 getThrottlerMaxSpeed() const;
    Float64 getThrottlerMaxBurst() const;

    // Semaphore
    bool hasSemaphore() const;
    Int64 getSemaphoreMaxRequests() const;
    Int64 getSemaphoreMaxCost() const;

    // Should be called after default constructor
    void initFromChanges(CostUnit unit_, const ASTCreateWorkloadQuery::SettingsChanges & changes, const String & resource_name = {}, bool throw_on_unknown_setting = true);
};

}
