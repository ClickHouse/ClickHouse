#pragma once

#include <Core/Joins.h>
#include <base/defines.h>
#include <base/types.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <utility>
#include <vector>

namespace DB
{

/// Metrics about the JOINs executed by a query, dumped into `system.query_log`.
struct QueryJoinsCounters
{
    void addJoin(JoinKind kind, JoinStrictness strictness);

    UInt64 getNumberOfJoins() const;
    std::vector<String> getJoinKinds() const;
    std::vector<String> getJoinStrictness() const;

    /// Best effort: nothing is recorded when the calling thread is not attached to a query.
    static void markJoinAsSpilled();

    bool getJoinSpilledToDisk() const;

private:

    mutable std::mutex mutex;

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
