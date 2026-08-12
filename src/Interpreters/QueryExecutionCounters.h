#pragma once

#include <Core/Joins.h>
#include <base/defines.h>
#include <base/types.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <string_view>
#include <utility>
#include <vector>

namespace DB
{

/// Metrics about the execution of a query, dumped into `system.query_log`: the joins it ran and
/// the operators that spilled to disk.
struct QueryExecutionCounters
{
    void addJoin(JoinKind kind, JoinStrictness strictness, std::string_view algorithm);

    UInt64 getNumberOfJoins() const;
    std::vector<String> getJoinKinds() const;
    std::vector<String> getJoinStrictness() const;
    std::set<String> getJoinAlgorithms() const;

    static void addUsedJoinAlgorithm(JoinAlgorithm algorithm);

    /// Records that `operator_name` of the query the calling thread belongs to wrote temporary data
    /// to disk, which is reported in `spilled_to_disk`. The values are `join`, `aggregation` and
    /// `sort` so far; an operator that starts spilling later only has to name itself in the
    /// `TemporaryDataMetrics` of the scope it already creates.
    /// Best effort: nothing is recorded when the calling thread is not attached to a query.
    static void markSpilledToDisk(std::string_view operator_name);

    std::set<String> getSpilledToDisk() const;

private:

    /// Counters of the query the calling thread belongs to, or nullptr when it is not attached to one.
    static std::shared_ptr<QueryExecutionCounters> getForCurrentQuery();

    mutable std::mutex mutex;

    /// Algorithms that were really used, which is not the same as the `join_algorithm` setting: the
    /// choice among the allowed algorithms is made at run time, and an algorithm can be replaced by
    /// another one in the middle of execution.
    std::set<String> used_join_algorithms TSA_GUARDED_BY(mutex);

    /// Number of physical joins executed by the query.
    std::atomic<UInt64> number_of_joins = 0;

    /// Keeps both elements together, to avoid mis-aligned items, and keeps
    /// it in a sorted container to not depend on order of execution
    std::set<std::pair<String, String>> used_joins TSA_GUARDED_BY(mutex);

    /// Operators that wrote temporary data to disk, sorted and deduplicated by the container.
    std::set<String> spilled_to_disk TSA_GUARDED_BY(mutex);
};

using QueryExecutionCountersPtr = std::shared_ptr<QueryExecutionCounters>;

}
