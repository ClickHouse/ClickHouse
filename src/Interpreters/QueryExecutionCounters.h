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

class IJoin;

/// Metrics about the execution of a query, dumped into `system.query_log`.
/// All of them are best effort and record nothing when the thread is not attached to a query.
struct QueryExecutionCounters
{
    /// Records one physical join, taking its kind, strictness and algorithm from the join itself.
    static void addExecutedJoin(const IJoin & join);

    /// For a join whose algorithm the `IJoin` alone cannot name, because it is decided while
    /// the pipeline is assembled, e.g. `full_sorting_merge` and `parallel_full_sorting_merge` build the same
    /// `FullSortingMergeJoin`.
    static void addExecutedJoin(const IJoin & join, std::string_view algorithm);

    /// Records an algorithm a join switched to while the query was already running, so that both the
    /// original one and this one are reported.
    static void addUsedJoinAlgorithm(JoinAlgorithm algorithm);

    /// Records that `operator_name` wrote temporary data to disk, e.g. `join` or `aggregation`.
    static void markSpilledToDisk(std::string_view operator_name);

    UInt64 getNumberOfJoins() const;
    std::vector<String> getJoinKinds() const;
    std::vector<String> getJoinStrictness() const;
    std::set<String> getJoinAlgorithms() const;
    std::set<String> getSpilledToDisk() const;

private:

    mutable std::mutex mutex;

    /// Counters of the query the calling thread belongs to, or nullptr when it is not attached to one.
    static std::shared_ptr<QueryExecutionCounters> getForCurrentQuery();

    /// Algorithms that were used in the query
    std::set<String> used_join_algorithms TSA_GUARDED_BY(mutex);

    /// Number of physical joins executed by the query.
    std::atomic<UInt64> number_of_joins = 0;

    /// Keeps both elements together, to avoid mis-aligned items
    std::set<std::pair<String, String>> used_joins TSA_GUARDED_BY(mutex);

    /// Operators that wrote temporary data to disk
    std::set<String> spilled_to_disk TSA_GUARDED_BY(mutex);
};

using QueryExecutionCountersPtr = std::shared_ptr<QueryExecutionCounters>;

}
