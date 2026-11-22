#include <Interpreters/QueryJoinSpillBudget.h>

#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/formatReadable.h>

#include <fmt/format.h>

#include <algorithm>

namespace DB
{

QueryJoinSpillBudget::QueryJoinSpillBudget(size_t budget_bytes_, size_t own_threshold_bytes_)
    : budget_bytes(std::max(budget_bytes_, own_threshold_bytes_))
    , own_threshold_bytes(own_threshold_bytes_)
{
}

QueryJoinSpillBudget::~QueryJoinSpillBudget()
{
    leave();
}

size_t QueryJoinSpillBudget::getShareWhenOverBudget()
{
    std::call_once(joined, [this]
    {
        group = CurrentThread::getGroup();
        if (group)
        {
            group->spill_capable_hash_joins.fetch_add(1, std::memory_order_relaxed);
            counted.store(true, std::memory_order_release);
        }
    });

    if (!counted.load(std::memory_order_acquire))
        return 0;

    const size_t share = budget_bytes / std::max<size_t>(1, group->spill_capable_hash_joins.load(std::memory_order_relaxed));
    if (share >= own_threshold_bytes)
        return 0;

    if (group->memory_tracker.get() < static_cast<Int64>(budget_bytes))
        return 0;

    return share;
}

void QueryJoinSpillBudget::leave()
{
    /// A join that leaves before its first insert must not be counted by a later call.
    std::call_once(joined, [] {});
    if (counted.exchange(false, std::memory_order_acq_rel))
        group->spill_capable_hash_joins.fetch_sub(1, std::memory_order_relaxed);
}

String QueryJoinSpillBudget::describe() const
{
    if (!counted.load(std::memory_order_acquire))
        return fmt::format("budget {}", ReadableSize(budget_bytes));

    return fmt::format(
        "query memory {}, budget {}, shared by {} joins",
        ReadableSize(group->memory_tracker.get()),
        ReadableSize(budget_bytes),
        group->spill_capable_hash_joins.load(std::memory_order_relaxed));
}

}
