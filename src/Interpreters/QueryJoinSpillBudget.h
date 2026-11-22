#pragma once

#include <base/types.h>

#include <atomic>
#include <memory>
#include <mutex>

namespace DB
{

class ThreadGroup;
using ThreadGroupPtr = std::shared_ptr<ThreadGroup>;

/// The spill budget the spill-capable hash joins of a query share: `max_bytes_ratio_before_external_join` of the
/// memory available when the query was planned, never below the join's own threshold. While the memory of the query
/// is at or over it, each join may keep an equal share of it in memory.
class QueryJoinSpillBudget
{
public:
    QueryJoinSpillBudget(size_t budget_bytes_, size_t own_threshold_bytes_);
    ~QueryJoinSpillBudget();

    /// The bytes the join may keep now: 0 when the memory of its query is under the budget, or when an equal share
    /// would not be below the join's own threshold. Counts the join in the thread group of the caller on the first call.
    size_t getShareWhenOverBudget();

    /// The join holds nothing that it could spill any more (switched to `MergeJoin`, or failed). Idempotent.
    void leave();

    /// The memory of the query, the budget and the number of joins sharing it, for logs.
    String describe() const;

private:
    const size_t budget_bytes;
    const size_t own_threshold_bytes;

    std::once_flag joined;
    /// The thread group of the first caller, set inside `joined`; null outside of a query.
    ThreadGroupPtr group;
    std::atomic<bool> counted{false};
};

}
