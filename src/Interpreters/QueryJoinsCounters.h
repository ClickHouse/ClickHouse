#pragma once

#include <Core/Joins.h>
#include <base/defines.h>
#include <base/types.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <set>

namespace DB
{

/// Metrics about the JOINs executed by a query, dumped into `system.query_log`.
struct QueryJoinsCounters
{
    /// Number of physical joins executed by the query.
    std::atomic<UInt64> number_of_joins = 0;

    void addJoinKind(JoinKind kind, JoinStrictness strictness) {
        /// Strictness is meaningless for CROSS, COMMA and PASTE
        const bool with_strictness = strictness != JoinStrictness::Unspecified
            && !isCrossOrComma(kind) && !isPaste(kind);

        String join = with_strictness
            ? String(toString(strictness)) + " " + toString(kind)
            : String(toString(kind));

        std::lock_guard lock(mutex);
        used_joins.insert(std::move(join));
    }

    std::set<String> getUsedJoins() {
        std::lock_guard lock(mutex);
        return used_joins;
    }

private:

    mutable std::mutex mutex;

    /// Set of join strictness and kind used in the query
    std::set<String> used_joins TSA_GUARDED_BY(mutex);
};

using QueryJoinsCountersPtr = std::shared_ptr<QueryJoinsCounters>;

}
