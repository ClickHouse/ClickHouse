#pragma once

#include <base/types.h>

#include <Common/Priority.h>
#include <Parsers/IAST_fwd.h>

#include <limits>

namespace DB
{

struct SchedulingSettings
{
    /// Priority and weight among siblings
    Float64 weight = 1.0;
    Priority priority;

    /// Throttling constraints.
    /// Up to 2 independent throttlers: one for average speed and one for peek speed.
    static constexpr Float64 default_burst_seconds = 1.0;
    Float64 max_speed = 0; // Zero means unlimited
    Float64 max_burst = 0; // default is `default_burst_seconds * max_speed`

    /// Limits total number of concurrent resource requests that are allowed to consume
    static constexpr Int64 default_max_requests = std::numeric_limits<Int64>::max();
    Int64 max_requests = default_max_requests;

    /// Limits total cost of concurrent resource requests that are allowed to consume
    static constexpr Int64 default_max_cost = std::numeric_limits<Int64>::max();
    Int64 max_cost = default_max_cost;

    bool hasThrottler() const { return max_speed != 0; }
    bool hasSemaphore() const { return max_requests != default_max_requests || max_cost != default_max_cost; }

    void updateFromAST(const ASTPtr & settings, const String & resource_name);
};

}
