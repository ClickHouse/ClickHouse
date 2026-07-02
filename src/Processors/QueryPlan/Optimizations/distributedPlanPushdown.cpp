#include <Columns/ColumnConst.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
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

bool planReadsFromRemote(const QueryPlan::Node & root)
{
    std::vector<const QueryPlan::Node *> stack = {&root};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        const auto * step = node->step.get();
        if (typeid_cast<const ReadFromRemotePlanStep *>(step) || typeid_cast<const ReadFromRemote *>(step))
            return true;

        for (const auto * child : node->children)
            stack.push_back(child);
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

void tryPushDownToRemotePlan(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &)
{
    /// Pattern: an `ExpressionStep` or `FilterStep` whose single child is a `ReadFromRemotePlanStep`
    /// placeholder. Such a step can be evaluated on the shards inside the serialized inner plan.
    auto * step = node.step.get();
    if (!typeid_cast<ExpressionStep *>(step) && !typeid_cast<FilterStep *>(step))
        return;

    if (node.children.size() != 1)
        return;

    auto * child_node = node.children.front();
    auto * placeholder = typeid_cast<ReadFromRemotePlanStep *>(child_node->step.get());
    if (!placeholder)
        return;

    /// Graceful degradation: only move a step that can be serialized to the shard and that carries no
    /// correlated expressions (`PLACEHOLDER` action nodes referencing outer-query columns absent on the
    /// shard). Otherwise leave the step on the initiator.
    if (!step->isSerializable() || step->hasCorrelatedExpressions())
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
