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
    /// Number of physical joins executed by the query.
    std::atomic<UInt64> number_of_joins = 0;

    void addJoin(JoinKind kind, JoinStrictness strictness)
    {
        std::lock_guard lock(mutex);
        used_joins.emplace(
            toString(kind),
            toString(strictness));
    }

    std::vector<String> getJoinKinds() const
    {
        std::lock_guard lock(mutex);

        std::vector<String> kinds;
        kinds.reserve(used_joins.size());
        for (const auto & [kind, _] : used_joins)
            kinds.push_back(kind);
        return kinds;
    }

    std::vector<String> getJoinStrictness() const
    {
        std::lock_guard lock(mutex);

        std::vector<String> strictness;
        strictness.reserve(used_joins.size());
        for (const auto & [_, join_strictness] : used_joins)
            strictness.push_back(join_strictness);
        return strictness;
    }

private:

    mutable std::mutex mutex;

    /// Keeps both elements together, to avoid mis-aligned items, and keeps
    /// it in a sorted container to not depend on order of execution
    std::set<std::pair<String, String>> used_joins TSA_GUARDED_BY(mutex);
};

using QueryJoinsCountersPtr = std::shared_ptr<QueryJoinsCounters>;

}
