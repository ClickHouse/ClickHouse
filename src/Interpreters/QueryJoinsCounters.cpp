#include <Interpreters/QueryJoinsCounters.h>

#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>

namespace DB
{

void QueryJoinsCounters::addJoin(JoinKind kind, JoinStrictness strictness)
{
    number_of_joins.fetch_add(1, std::memory_order_relaxed);

    /// Strictness does not matter for these kinds, see `JoinKind` in Core/Joins.h, and the two
    /// analyzers fill it in differently: for a CROSS join the old one leaves it unspecified while
    /// the new one takes `join_default_strictness`. Report nothing instead of a value that depends
    /// on the analyzer.
    if (isCrossOrComma(kind) || isPaste(kind))
        strictness = JoinStrictness::Unspecified;

    std::lock_guard lock(mutex);
    used_joins.emplace(
        toString(kind),
        toString(strictness));
}

UInt64 QueryJoinsCounters::getNumberOfJoins() const
{
    return number_of_joins.load(std::memory_order_relaxed);
}

std::vector<String> QueryJoinsCounters::getJoinKinds() const
{
    std::lock_guard lock(mutex);

    std::vector<String> kinds;
    kinds.reserve(used_joins.size());
    for (const auto & [kind, _] : used_joins)
        kinds.push_back(kind);
    return kinds;
}

std::vector<String> QueryJoinsCounters::getJoinStrictness() const
{
    std::lock_guard lock(mutex);

    std::vector<String> strictness;
    strictness.reserve(used_joins.size());
    for (const auto & [_, join_strictness] : used_joins)
        strictness.push_back(join_strictness);
    return strictness;
}

void QueryJoinsCounters::markJoinAsSpilled()
{
    auto query_context = CurrentThread::tryGetQueryContext();
    if (!query_context)
        return;

    if (auto counters = query_context->getQueryJoinsCounters())
        counters->spilled_to_disk.store(true, std::memory_order_relaxed);
}

bool QueryJoinsCounters::getJoinSpilledToDisk() const
{
    return spilled_to_disk;
}

}
