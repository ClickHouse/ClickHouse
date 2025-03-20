#pragma once

#include <base/types.h>

#include <Common/Priority.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>

#include <limits>

namespace DB
{

struct SchedulingSettings
{
    static constexpr Int64 unlimited = std::numeric_limits<Int64>::max();
    static constexpr Float64 default_burst_seconds = 1.0;

    /// Priority and weight among siblings
    Float64 weight = 1.0;
    Priority priority;

    /// Throttling constraints
    Float64 max_bytes_per_second = 0; // Zero means unlimited
    Float64 max_burst_bytes = 0; // default is `default_burst_seconds * max_bytes_per_second`

    /// Limits total number of concurrent resource requests that are allowed to consume
    Int64 max_io_requests = unlimited;

    /// Limits total bytes in-inflight for concurrent IO resource requests
    Int64 max_bytes_inflight = unlimited;

    /// Limits total number of additional query threads (the first main thread is not counted)
    Int64 max_concurrent_threads = unlimited;

    /// Settings that are applied depend on cost unit
    using Unit = ASTCreateResourceQuery::CostUnit;
    Unit unit = Unit::IOByte;

    // Throttler
    bool hasThrottler() const;
    Float64 getThrottlerMaxSpeed() const;
    Float64 getThrottlerMaxBurst() const;

    // Semaphore
    bool hasSemaphore() const;
    Int64 getSemaphoreMaxRequests() const;
    Int64 getSemaphoreMaxCost() const;

    void updateFromChanges(Unit unit_, const ASTCreateWorkloadQuery::SettingsChanges & changes, const String & resource_name = {});
};

}
