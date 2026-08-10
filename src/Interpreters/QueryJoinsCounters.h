#pragma once

#include <base/types.h>

#include <atomic>
#include <memory>

namespace DB
{

/// Metrics about the JOINs executed by a query, dumped into `system.query_log`.
struct QueryJoinsCounters
{
    /// Number of physical joins executed by the query.
    std::atomic<UInt64> number_of_joins = 0;
};

using QueryJoinsCountersPtr = std::shared_ptr<QueryJoinsCounters>;

}
