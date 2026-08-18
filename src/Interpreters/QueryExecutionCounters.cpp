#include <Interpreters/QueryExecutionCounters.h>

#include <Interpreters/Context.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Common/CurrentThread.h>

namespace DB
{

void QueryExecutionCounters::addExecutedJoin(const IJoin & join)
{
    addExecutedJoin(join, join.getAlgorithm());
}

void QueryExecutionCounters::addExecutedJoin(const IJoin & join, std::string_view algorithm)
{
    const auto & table_join = join.getTableJoin();
    addExecutedJoin(table_join.kind(), table_join.strictness(), algorithm);
}

void QueryExecutionCounters::addExecutedJoin(JoinKind kind, JoinStrictness strictness, std::string_view algorithm)
{
    auto counters = getForCurrentQuery();
    if (!counters)
        return;

    counters->number_of_joins.fetch_add(1, std::memory_order_relaxed);

    std::lock_guard lock(counters->mutex);
    counters->used_joins.emplace(
        toString(kind),
        toString(strictness));

    counters->used_join_algorithms.emplace(algorithm);
}

UInt64 QueryExecutionCounters::getNumberOfJoins() const
{
    return number_of_joins.load(std::memory_order_relaxed);
}

std::vector<String> QueryExecutionCounters::getJoinKinds() const
{
    std::lock_guard lock(mutex);

    std::vector<String> kinds;
    kinds.reserve(used_joins.size());
    for (const auto & [kind, _] : used_joins)
        kinds.push_back(kind);
    return kinds;
}

std::vector<String> QueryExecutionCounters::getJoinStrictness() const
{
    std::lock_guard lock(mutex);

    std::vector<String> strictness;
    strictness.reserve(used_joins.size());
    for (const auto & [_, join_strictness] : used_joins)
        strictness.push_back(join_strictness);
    return strictness;
}

std::set<String> QueryExecutionCounters::getJoinAlgorithms() const
{
    std::lock_guard lock(mutex);
    return used_join_algorithms;
}

std::shared_ptr<QueryExecutionCounters> QueryExecutionCounters::getForCurrentQuery()
{
    auto query_context = CurrentThread::tryGetQueryContext();
    if (!query_context)
        return nullptr;

    return query_context->getQueryExecutionCounters();
}

void QueryExecutionCounters::addUsedJoinAlgorithm(JoinAlgorithm algorithm)
{
    if (auto counters = getForCurrentQuery())
    {
        std::lock_guard lock(counters->mutex);
        counters->used_join_algorithms.emplace(toString(algorithm));
    }
}

void QueryExecutionCounters::markSpilledToDisk(std::string_view operator_name)
{
    if (operator_name.empty())
        return;

    if (auto counters = getForCurrentQuery())
    {
        std::lock_guard lock(counters->mutex);
        counters->spilled_to_disk.emplace(operator_name);
    }
}

std::set<String> QueryExecutionCounters::getSpilledToDisk() const
{
    std::lock_guard lock(mutex);
    return spilled_to_disk;
}

}
