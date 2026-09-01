#include <Interpreters/QueryExecutionCounters.h>

#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Common/CurrentThread.h>

#include <utility>

namespace DB
{

namespace
{

/// The pipeline that is being built by this thread, when it is one that is built more than once for the
/// same query, empty otherwise, which is the usual case. See `RepeatedPipelineBuildScope`.
thread_local String repeated_pipeline_build_scope;

/// The key of a join inside a `RepeatedPipelineBuildScope`: the scope, and the shape of the join, i.e. the
/// columns and the types of its sides. The shape is the same every time the same pipeline is built, and it
/// differs between the joins of one pipeline, because the analyzer qualifies the columns of every table
/// expression of a query with a name of its own.
String makeJoinKey(std::string_view scope, const SharedHeaders & input_headers)
{
    WriteBufferFromOwnString key;
    key << scope;
    for (const auto & header : input_headers)
        key << '\n' << header->dumpStructure();
    return key.str();
}

}

QueryExecutionCounters::RepeatedPipelineBuildScope::RepeatedPipelineBuildScope(String scope)
    : scope_to_restore(std::exchange(repeated_pipeline_build_scope, std::move(scope)))
{
}

QueryExecutionCounters::RepeatedPipelineBuildScope::~RepeatedPipelineBuildScope()
{
    repeated_pipeline_build_scope = std::move(scope_to_restore);
}

void QueryExecutionCounters::addExecutedJoin(const IJoin & join, const SharedHeaders & input_headers)
{
    addExecutedJoin(join, join.getAlgorithm(), input_headers);
}

void QueryExecutionCounters::addExecutedJoin(const IJoin & join, std::string_view algorithm, const SharedHeaders & input_headers)
{
    const auto & table_join = join.getTableJoin();
    addExecutedJoin(table_join.kind(), table_join.strictness(), algorithm, input_headers);
}

void QueryExecutionCounters::addExecutedJoin(
    JoinKind kind, JoinStrictness strictness, std::string_view algorithm, const SharedHeaders & input_headers)
{
    auto counters = getForCurrentQuery();
    if (!counters)
        return;

    std::lock_guard lock(counters->mutex);

    /// The algorithm is recorded even for a join that an earlier build of the pipeline already counted:
    /// the algorithms are a set, and a build is free to pick another algorithm than the one before it,
    /// in which case both of them were used.
    counters->used_join_algorithms.emplace(algorithm);

    if (counters->isCountedByAnEarlierBuild(input_headers))
        return;

    counters->used_joins.emplace(
        toString(kind),
        toString(strictness));
}

bool QueryExecutionCounters::isCountedByAnEarlierBuild(const SharedHeaders & input_headers)
{
    if (repeated_pipeline_build_scope.empty())
        return false;

    return !joins_of_repeated_builds.emplace(makeJoinKey(repeated_pipeline_build_scope, input_headers)).second;
}

QueryExecutionCounters::Snapshot QueryExecutionCounters::getSnapshot() const
{
    std::lock_guard lock(mutex);

    Snapshot snapshot;
    snapshot.number_of_joins = used_joins.size();
    snapshot.join_algorithms = used_join_algorithms;
    snapshot.spilled_to_disk = spilled_to_disk;

    snapshot.join_kinds.reserve(used_joins.size());
    snapshot.join_strictness.reserve(used_joins.size());
    for (const auto & [kind, strictness] : used_joins)
    {
        snapshot.join_kinds.push_back(kind);
        snapshot.join_strictness.push_back(strictness);
    }

    return snapshot;
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

}
