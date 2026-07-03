#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Core/Settings.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/PreparedSets.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/ReadFromRemotePlanStep.h>
#include <QueryPipeline/UnavailableShardTracker.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool skip_unavailable_shards;
    extern const SettingsUInt64 max_skip_unavailable_shards_num;
    extern const SettingsFloat max_skip_unavailable_shards_ratio;
}

namespace QueryPlanOptimizations
{

bool planReadsFromRemote(QueryPlan::Node & root)
{
    std::vector<QueryPlan::Node *> stack = {&root};
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        auto * step = node->step.get();
        if (typeid_cast<const ReadFromRemotePlanStep *>(step) || typeid_cast<const ReadFromRemote *>(step))
            return true;

        for (auto * child : node->children)
            stack.push_back(child);

        /// Steps like `ReadFromMerge` hold whole child plans instead of plan children; a remote
        /// read nested there decides the distributed split just the same (e.g. a `Merge` table
        /// over a `Distributed` one), so the MPP conversion must be skipped for it too.
        for (auto * child_plan : step->getChildPlans())
            stack.push_back(child_plan->getRootNode());
    }
    return false;
}

/// Replace a `ReadFromRemotePlanStep` placeholder with a regular `ReadFromRemote` step whose shards
/// carry the inner query plan (per-shard stage becomes `QueryProcessingStage::QueryPlan` automatically
/// because `Shard::query_plan` is set).
static void finalizeNode(QueryPlan::Node & node, ReadFromRemotePlanStep & placeholder)
{
    auto remote_context = placeholder.getRemoteContext();
    const auto & cluster = placeholder.getCluster();
    const size_t shard_count = cluster->getShardCount();

    /// Tracker is shared between all shards so that max_skip_unavailable_shards_num
    /// and max_skip_unavailable_shards_ratio are enforced uniformly.
    UnavailableShardTrackerPtr unavailable_shard_tracker;
    {
        const auto & settings = remote_context->getSettingsRef();
        if (settings[Setting::skip_unavailable_shards])
        {
            size_t max_num = settings[Setting::max_skip_unavailable_shards_num];
            Float64 max_ratio = static_cast<double>(settings[Setting::max_skip_unavailable_shards_ratio]);
            if (max_num > 0 || max_ratio > 0)
                unavailable_shard_tracker = std::make_shared<UnavailableShardTracker>(shard_count, max_num, max_ratio);
        }
    }

    Scalars scalars = remote_context->hasQueryContext() ? remote_context->getQueryContext()->getScalars() : Scalars{};
    scalars.emplace(
        "_shard_count", Block{{DataTypeUInt32().createColumnConst(1, shard_count), std::make_shared<DataTypeUInt32>(), "_shard_count"}});
    auto external_tables = remote_context->getExternalTables();

    /// One shared inner plan serves all shards: its leaf names the remote table either fully
    /// qualified or as a bare identifier that each shard resolves against the connection's
    /// database, which is `ShardInfo::default_database`.
    std::shared_ptr<QueryPlan> shared_plan = placeholder.extractInnerPlan();
    auto shard_header = shared_plan->getCurrentHeader();

    ClusterProxy::SelectStreamFactory::Shards shards;
    shards.reserve(cluster->getShardsInfo().size());
    for (const auto & shard_info : cluster->getShardsInfo())
    {
        shards.emplace_back(ClusterProxy::SelectStreamFactory::Shard{
            .query = placeholder.getQueryForLogging(),
            .query_tree = nullptr,
            .planner_context = nullptr,
            .query_plan = shared_plan,
            .main_table = StorageID::createEmpty(),
            .header = shard_header,
            .shard_info = shard_info,
            .lazy = false,
            .shard_filter_generator = {},
        });
    }

    auto read_from_remote = std::make_unique<ReadFromRemote>(
        std::move(shards),
        placeholder.getOutputHeader(),
        QueryProcessingStage::FetchColumns,
        placeholder.getMainTable(),
        placeholder.getTableFunctionPtr(),
        remote_context,
        ClusterProxy::getThrottler(remote_context),
        std::move(scalars),
        std::move(external_tables),
        placeholder.getLog(),
        static_cast<UInt32>(shard_count),
        placeholder.getStorageLimits(),
        placeholder.getClusterName(),
        std::move(unavailable_shard_tracker));

    read_from_remote->setStepDescription("Read from remote replica");

    node.step = std::move(read_from_remote);
    node.children.clear();
}

/// Copy an outer `LimitStep` down into the per-shard plan so each shard emits at most `limit + offset`
/// rows, while the global (outer) `LimitStep` stays in place — a per-shard `LIMIT n` is not a global
/// `LIMIT n`. Mirrors the per-branch LIMIT copy that `limitPushDown.cpp` performs for `UNION ALL`.
static void tryCopyLimitToRemotePlan(
    LimitStep & limit, ReadFromRemotePlanStep & placeholder, const QueryPlanOptimizationSettings & optimization_settings)
{
    /// The user disabled per-shard LIMIT application (parity with the legacy path over Distributed).
    if (!optimization_settings.distributed_push_down_limit)
        return;

    /// Copy the limit at most once. The bottom-up traversal visits each node once, so this only
    /// guards against a hypothetical re-traversal; absorption is tracked in the placeholder.
    if (placeholder.isLimitCopied())
        return;

    /// WITH TIES needs a global sort order to be meaningful: a per-shard tie set is not composable
    /// into the global one, so the limit can only be applied on the initiator.
    /// Defensive: a real WITH TIES always has a `SortingStep` between the limit and the placeholder,
    /// so this pattern never matches in practice.
    if (limit.withTies())
        return;

    /// `always_read_till_end` means the step must consume all input regardless of the limit. It is set
    /// for `exact_rows_before_limit` and for WITH TOTALS. In both cases a per-shard limit is either
    /// pointless (the shard would still read everything) or breaks exact `rows_before_limit_at_least`
    /// reporting — the same reason `DistributedCreateLocalPlan.cpp` avoids limit pushdown for the
    /// `WithMergeableStateAfterAggregationAndLimit` stage. Leave the shard reading everything.
    if (limit.alwaysReadTillEnd())
        return;

    /// `getLimitForSorting` returns `limit + offset` with overflow protection, yielding 0 when the sum
    /// would overflow `UInt64` or when the limit itself is 0. In either case a per-shard limit is
    /// unrepresentable or useless, so leave the outer step alone and absorb nothing.
    if (limit.getLimitForSorting() == 0)
        return;

    placeholder.absorbLimitCopy(limit);
}

/// A step may be executed on the shards only if every set referenced by its actions travels with
/// the serialized plan. Literal `IN` sets (`FutureSetFromTuple`) are serialized with their values
/// and are self-contained. A set from a subquery cannot go: the initiator builds it via
/// `DelayedCreatingSetsStep` in the third optimization pass — after this rule — which consumes the
/// subquery's source plan, so a later serialization of the shard plan would throw a logical error
/// (`Cannot serialize FutureSetFromSubquery with no query plan`); and shipping the subquery plan
/// instead would make every shard re-execute it, changing semantics. A set from a `Set` storage is
/// serialized as a table name that the shard would resolve against its own catalog — also a
/// semantics change. Keep steps referencing such sets on the initiator.
static bool dagReferencesOnlyInlineSets(const ActionsDAG & dag)
{
    for (const auto & dag_node : dag.getNodes())
    {
        /// Scan any node carrying a column (matching what `ActionsDAG::serialize` serializes), not
        /// just `COLUMN` nodes, unwrapping `ColumnConst` to reach the underlying column.
        if (!dag_node.column)
            continue;

        const IColumn * inner = dag_node.column.get();
        if (const auto * column_const = checkAndGetColumn<const ColumnConst>(inner))
            inner = &column_const->getDataColumn();

        const auto * column_set = checkAndGetColumn<const ColumnSet>(inner);
        if (!column_set)
            continue;

        const auto & future_set = column_set->getData();
        if (!future_set || !typeid_cast<const FutureSetFromTuple *>(future_set.get()))
            return false;
    }
    return true;
}

void tryPushDownToRemotePlan(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings & optimization_settings)
{
    /// Pattern: a step whose single child is a `ReadFromRemotePlanStep` placeholder. Depending on the
    /// step type it is either moved into the serialized per-shard plan (Expression/Filter) or copied
    /// there while staying on the initiator (Limit).
    auto * step = node.step.get();

    if (node.children.size() != 1)
        return;

    auto * child_node = node.children.front();
    auto * placeholder = typeid_cast<ReadFromRemotePlanStep *>(child_node->step.get());
    if (!placeholder)
        return;

    /// A `LimitStep` is copied (not moved) down: the outer step stays as the global limit and the
    /// traversal simply continues, so there is no node swap here.
    if (auto * limit = typeid_cast<LimitStep *>(step))
    {
        tryCopyLimitToRemotePlan(*limit, *placeholder, optimization_settings);
        return;
    }

    /// Otherwise only an `ExpressionStep` or `FilterStep` can be evaluated on the shards.
    const ActionsDAG * dag = nullptr;
    if (const auto * expression = typeid_cast<ExpressionStep *>(step))
        dag = &expression->getExpression();
    else if (const auto * filter = typeid_cast<FilterStep *>(step))
        dag = &filter->getExpression();
    else
        return;

    /// Graceful degradation: only move a step that can be serialized to the shard and that carries no
    /// correlated expressions (`PLACEHOLDER` action nodes referencing outer-query columns absent on the
    /// shard). Otherwise leave the step on the initiator.
    if (!step->isSerializable() || step->hasCorrelatedExpressions())
        return;

    /// Sets from subqueries or `Set` storages cannot travel with the serialized shard plan.
    if (!dagReferencesOnlyInlineSets(*dag))
        return;

    /// A shard plan that outputs zero columns cannot carry its row count across `ReadFromRemote`:
    /// an empty block loses `num_rows` over the wire, which would make e.g. `count()` return 0. This
    /// happens when the pushed step projects everything away (bare `count()` needs no columns). Keep
    /// such a step on the initiator so the shard still emits at least one column and the initiator does
    /// the empty projection itself.
    if (step->getOutputHeader()->columns() == 0)
        return;

    /// Move the step into the inner (per-shard) plan, then reconnect. Mirrors the
    /// `ReadFromLocalParallelReplicaStep` idiom in `filterPushDown.cpp`: after the swap the parent node
    /// holds the placeholder (now carrying the absorbed step) with empty children, and the orphaned node
    /// stays in the `QueryPlan::Nodes` list harmlessly. Repeated application chains naturally in the
    /// bottom-up traversal: a `Filter` above an `Expression` above a placeholder absorbs both, one at a
    /// time, because after each swap the parent's child points at the node now holding the placeholder.
    placeholder->absorbStep(std::move(node.step));
    std::swap(node, *child_node);
}

void finalizeReadFromRemotePlan(QueryPlan::Node & root)
{
    std::vector<QueryPlan::Node *> stack = {&root};
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        if (auto * placeholder = typeid_cast<ReadFromRemotePlanStep *>(node->step.get()))
        {
            finalizeNode(*node, *placeholder);
            continue;
        }

        for (auto * child : node->children)
            stack.push_back(child);
    }
}

}

}
