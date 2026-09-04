#include <Interpreters/QueryExecutionCounters.h>

#include <Interpreters/Context.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Common/CurrentThread.h>

#include <fmt/format.h>

#include <utility>

namespace DB
{

namespace
{

/// The pipeline that is being built by this thread, when it is one that is built more than once for the
/// same query, empty otherwise, which is the usual case. See `RepeatedPipelineBuildScope`.
thread_local String repeated_pipeline_build_scope;

/// How many joins the build of that pipeline that is running on this thread has registered so far. The
/// ordinal it hands out identifies a join inside the scope: the pipeline is assembled by walking the query
/// plan in a fixed order, and the plan is the same one every time the same `SELECT` is planned again, so
/// the n-th join a build registers is the same physical join of the same query as the n-th join of every
/// other build of that pipeline.
///
/// The ordinal is what tells two joins apart, and not the shape of their inputs: two joins of one query
/// can have exactly the same input columns and types, for instance the joins of two identical `UNION ALL`
/// branches, or two `Join`-engine or dictionary joins over the same stream, whose `FilledJoinStep` sees
/// only the left side.
thread_local size_t joins_registered_by_current_build = 0;

/// How many pipelines that are assembled later the build running on this thread has named so far, see
/// `makeScopeForPipelineBuiltLater`. A counter of its own, so that naming one does not move the ordinals
/// of the joins around it.
///
/// It is reset by a scope, exactly like the ordinals of the joins, which is what makes the names stable
/// when a pipeline is assembled more than once. Outside a scope it simply keeps counting: there the
/// holding pipeline is assembled once, so the only thing the name has to do is tell the operators of
/// that one assembly apart.
thread_local size_t pipelines_named_by_current_build = 0;

}

QueryExecutionCounters::RepeatedPipelineBuildScope::RepeatedPipelineBuildScope(String scope)
    : scope_to_restore(std::exchange(repeated_pipeline_build_scope, std::move(scope)))
    , registered_joins_to_restore(std::exchange(joins_registered_by_current_build, 0))
    , named_pipelines_to_restore(std::exchange(pipelines_named_by_current_build, 0))
{
}

QueryExecutionCounters::RepeatedPipelineBuildScope::~RepeatedPipelineBuildScope()
{
    repeated_pipeline_build_scope = std::move(scope_to_restore);
    joins_registered_by_current_build = registered_joins_to_restore;
    pipelines_named_by_current_build = named_pipelines_to_restore;
}

String QueryExecutionCounters::makeScopeForPipelineBuiltLater(std::string_view kind)
{
    const size_t ordinal = pipelines_named_by_current_build++;

    /// Inside a scope the name is qualified by it, so that a `loop` of one materialized view is not the
    /// `loop` of another one that happens to sit in the same position of its own pipeline.
    if (repeated_pipeline_build_scope.empty())
        return fmt::format("{}#{}", kind, ordinal);

    return fmt::format("{}/{}#{}", repeated_pipeline_build_scope, kind, ordinal);
}

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

    std::lock_guard lock(counters->mutex);

    /// The algorithm is recorded even for a join that an earlier build of the pipeline already counted:
    /// the algorithms are a set, and a build is free to pick another algorithm than the one before it,
    /// in which case both of them were used.
    counters->used_join_algorithms.emplace(algorithm);

    if (counters->isCountedByAnEarlierBuild())
        return;

    counters->used_joins.emplace(
        toString(kind),
        toString(strictness));
}

bool QueryExecutionCounters::isCountedByAnEarlierBuild()
{
    if (repeated_pipeline_build_scope.empty())
        return false;

    const size_t ordinal = joins_registered_by_current_build++;
    return !joins_of_repeated_builds.emplace(repeated_pipeline_build_scope, ordinal).second;
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
