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

/// Metrics about the JOINs executed by a query, dumped into `system.query_log`.
struct QueryJoinsCounters
{
    void addJoin(JoinKind kind, JoinStrictness strictness, std::string_view algorithm);

    UInt64 getNumberOfJoins() const;
    std::vector<String> getJoinKinds() const;
    std::vector<String> getJoinStrictness() const;
    std::set<String> getJoinAlgorithms() const;

    static void addUsedJoinAlgorithm(JoinAlgorithm algorithm);

    static void markJoinAsSpilled();

    bool getJoinSpilledToDisk() const;

private:

    /// Counters of the query the calling thread belongs to, or nullptr when it is not attached to one.
    static std::shared_ptr<QueryJoinsCounters> getForCurrentQuery();

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

    /// True if any join spilled to disk
    std::atomic<bool> spilled_to_disk{false};
};

using QueryJoinsCountersPtr = std::shared_ptr<QueryJoinsCounters>;

}
