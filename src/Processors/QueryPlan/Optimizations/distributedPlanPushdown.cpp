#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnSet.h>
#include <Core/Settings.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsMiscellaneous.h>
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
#include <Processors/QueryPlan/SortingStep.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
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
        /// A plain `ReadFromRemote` is distributed by `make_distributed_plan` itself.
        if (typeid_cast<const ReadFromRemotePlanStep *>(step))
            return true;

        for (auto * child : node->children)
            stack.push_back(child);

        /// A plan-level remote read in a child plan also fixes the distributed split.
        for (auto * child_plan : step->getChildPlans(/*for_explain=*/ false))
            stack.push_back(child_plan->getRootNode());
    }
    return false;
}

static void finalizeNode(QueryPlan::Node & node, ReadFromRemotePlanStep & placeholder)
{
    auto remote_context = placeholder.getRemoteContext();
    const auto & cluster = placeholder.getCluster();
    const size_t shard_count = cluster->getShardCount();

    /// All shards must share one unavailable-shard budget.
    /// Needed even with no limit set: it is also what notices that every shard was skipped.
    UnavailableShardTrackerPtr unavailable_shard_tracker;
    {
        const auto & settings = remote_context->getSettingsRef();
        if (settings[Setting::skip_unavailable_shards])
        {
            size_t max_num = settings[Setting::max_skip_unavailable_shards_num];
            Float64 max_ratio = static_cast<double>(settings[Setting::max_skip_unavailable_shards_ratio]);
            unavailable_shard_tracker = std::make_shared<UnavailableShardTracker>(shard_count, max_num, max_ratio);
        }
    }

    Scalars scalars = remote_context->hasQueryContext() ? remote_context->getQueryContext()->getScalars() : Scalars{};
    scalars.emplace(
        "_shard_count", Block{{DataTypeUInt32().createColumnConst(1, shard_count), std::make_shared<DataTypeUInt32>(), "_shard_count"}});
    auto external_tables = remote_context->getExternalTables();

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
        getThrottler(remote_context),
        std::move(scalars),
        std::move(external_tables),
        placeholder.getLog(),
        static_cast<UInt32>(shard_count),
        placeholder.getStorageLimits(),
        placeholder.getClusterName(),
        cluster->getShardScopeIdentity(),
        std::move(unavailable_shard_tracker));

    if (const auto & coordination = placeholder.getDistributedTopKCoordination())
    {
        read_from_remote->setDistributedTopKCoordination(
            coordination->limit,
            coordination->sort_description);
    }

    read_from_remote->setStepDescription("Read from remote replica");

    node.step = std::move(read_from_remote);
    node.children.clear();
}

#if defined(OS_LINUX) || defined(OS_DARWIN)
static void tryEnableDistributedLazyMaterialization(
    LimitStep & limit,
    SortingStep & sorting,
    ReadFromRemotePlanStep & placeholder,
    const QueryPlanOptimizationSettings & optimization_settings)
{
    if (!optimization_settings.optimize_distributed_lazy_materialization
        || !optimization_settings.serialize_query_plan
        || !optimization_settings.optimize_lazy_materialization
        || !optimization_settings.distributed_push_down_limit
        || optimization_settings.skip_unavailable_shards
        || optimization_settings.parallel_replicas_enabled
        || placeholder.isLimitCopied()
        || limit.getLimit() == 0
        || limit.withTies()
        || limit.alwaysReadTillEnd())
        return;

    const size_t candidate_limit = limit.getLimitForSorting();
    if (candidate_limit == 0
        || (optimization_settings.max_limit_for_lazy_materialization != 0
            && candidate_limit > optimization_settings.max_limit_for_lazy_materialization))
        return;

    if (sorting.getType() != SortingStep::Type::Full || sorting.hasPartitions())
        return;

    SortDescription sort_description = sorting.getSortDescription();
    if (sort_description.empty())
        return;
    for (const auto & column : sort_description)
        if (column.with_fill)
            return;

    placeholder.absorbStep(std::make_unique<SortingStep>(
        placeholder.getOutputHeader(),
        sort_description,
        /* limit_= */ 0,
        sorting.getSettings()));

    auto shard_limit = std::make_unique<LimitStep>(
        placeholder.getOutputHeader(),
        candidate_limit,
        /* offset_= */ 0,
        /* always_read_till_end_= */ false,
        /* with_ties_= */ false,
        /* description_= */ SortDescription{});
    shard_limit->setDistributedTopKCandidateLimit(sort_description);
    placeholder.absorbStep(std::move(shard_limit));
    placeholder.setDistributedTopKCoordination(candidate_limit, std::move(sort_description));
}
#endif

/// Keep the outer limit because per-shard limits do not enforce a global limit.
static void tryCopyLimitToRemotePlan(
    LimitStep & limit, ReadFromRemotePlanStep & placeholder, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (!optimization_settings.distributed_push_down_limit)
        return;

    if (placeholder.isLimitCopied())
        return;

    /// Per-shard tie sets cannot be combined into the global tie set.
    if (limit.withTies())
        return;

    /// A shard limit would break exact rows-before-limit reporting when all input must be read.
    if (limit.alwaysReadTillEnd())
        return;

    /// Zero also represents overflow of `limit + offset`.
    if (limit.getLimitForSorting() == 0)
        return;

    placeholder.absorbLimitCopy(limit);
}

/// Sort each shard's rows and keep only its first `limit + offset` of them, as the classic path does at
/// `WithMergeableState`, so that a shard sends at most that many rows instead of all of them.
/// The initiator keeps its own sorting and limit: per-shard results are not globally sorted or limited.
static void tryCopySortingAndLimitToRemotePlan(
    LimitStep & limit, SortingStep & sorting, ReadFromRemotePlanStep & placeholder, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (!optimization_settings.distributed_push_down_limit)
        return;

    if (placeholder.isLimitCopied())
        return;

    /// Per-shard tie sets cannot be combined into the global tie set.
    if (limit.withTies())
        return;

    /// A shard limit would break exact rows-before-limit reporting when all input must be read.
    if (limit.alwaysReadTillEnd())
        return;

    /// Zero also represents overflow of `limit + offset`.
    const size_t shard_limit = limit.getLimitForSorting();
    if (shard_limit == 0)
        return;

    if (sorting.getType() != SortingStep::Type::Full || sorting.hasPartitions())
        return;

    const SortDescription & sort_description = sorting.getSortDescription();
    if (sort_description.empty())
        return;
    for (const auto & column : sort_description)
        if (column.with_fill)
            return;

    placeholder.absorbStep(std::make_unique<SortingStep>(
        placeholder.getOutputHeader(),
        sort_description,
        shard_limit,
        sorting.getSettings()));
    placeholder.absorbLimitCopy(limit);
}

static bool dagReferencesOnlyInlineSets(const ActionsDAG & dag);

static bool columnReferencesOnlyInlineSets(const IColumn & column)
{
    const IColumn * inner = &column;
    if (const auto * column_const = checkAndGetColumn<const ColumnConst>(inner))
        inner = &column_const->getDataColumn();

    if (const auto * column_set = checkAndGetColumn<const ColumnSet>(inner))
    {
        const auto & future_set = column_set->getData();
        return future_set && typeid_cast<const FutureSetFromTuple *>(future_set.get());
    }

    /// A constant-folded lambda keeps the sets it uses in its captured columns and in its body,
    /// and `ActionsDAG::serialize` sends both to the shards.
    if (const auto * column_function = checkAndGetColumn<const ColumnFunction>(inner))
    {
        for (const auto & captured : column_function->getCapturedColumns())
        {
            if (captured.column && !columnReferencesOnlyInlineSets(*captured.column))
                return false;
        }

        if (const auto * function_expression = typeid_cast<const FunctionExpression *>(column_function->getFunction().get()))
            return dagReferencesOnlyInlineSets(function_expression->getAcionsDAG());
    }

    return true;
}

/// Only literal `IN` sets are self-contained. Subquery sets would run once per shard, and `Set`
/// storage names would resolve in the shard's catalog. Lambda bodies are inspected as well.
static bool dagReferencesOnlyInlineSets(const ActionsDAG & dag)
{
    for (const auto & dag_node : dag.getNodes())
    {
        if (dag_node.column && !columnReferencesOnlyInlineSets(*dag_node.column))
            return false;

        if (const auto * function_capture = typeid_cast<const FunctionCapture *>(dag_node.function_base.get()))
        {
            if (!dagReferencesOnlyInlineSets(function_capture->getAcionsDAG()))
                return false;
        }
    }
    return true;
}

void tryPushDownToRemotePlan(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings & optimization_settings)
{
    auto * step = node.step.get();

    if (node.children.size() != 1)
        return;

    auto * child_node = node.children.front();

    if (auto * limit = typeid_cast<LimitStep *>(step))
    {
        if (auto * sorting = typeid_cast<SortingStep *>(child_node->step.get());
            sorting && child_node->children.size() == 1)
        {
            if (auto * placeholder = typeid_cast<ReadFromRemotePlanStep *>(child_node->children.front()->step.get()))
            {
#if defined(OS_LINUX) || defined(OS_DARWIN)
                tryEnableDistributedLazyMaterialization(*limit, *sorting, *placeholder, optimization_settings);
#endif
                /// No-op if the coordinated Top-K above has already copied the sorting and the limit.
                tryCopySortingAndLimitToRemotePlan(*limit, *sorting, *placeholder, optimization_settings);
            }
            return;
        }
    }

    auto * placeholder = typeid_cast<ReadFromRemotePlanStep *>(child_node->step.get());
    if (!placeholder)
        return;

    if (auto * limit = typeid_cast<LimitStep *>(step))
    {
        tryCopyLimitToRemotePlan(*limit, *placeholder, optimization_settings);
        return;
    }

    const ActionsDAG * dag = nullptr;
    if (const auto * expression = typeid_cast<ExpressionStep *>(step))
        dag = &expression->getExpression();
    else if (const auto * filter = typeid_cast<FilterStep *>(step))
        dag = &filter->getExpression();
    else
        return;

    /// Correlated expressions may reference columns absent on the shard.
    if (!step->isSerializable() || step->hasCorrelatedExpressions())
        return;

    if (!dagReferencesOnlyInlineSets(*dag))
        return;

    /// Empty blocks lose their row count over the wire, so an empty projection must run locally.
    if (step->getOutputHeader()->columns() == 0)
        return;

    /// The old node remains in `QueryPlan::Nodes`; the bottom-up traversal may absorb the next parent.
    placeholder->absorbStep(std::move(node.step));
    std::swap(node, *child_node);
}

void finalizeReadFromRemotePlan(QueryPlan::Node & root, bool walk_child_plans)
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

        /// Do not call `getChildPlans` when disabled because it may materialize lazy child plans.
        if (walk_child_plans)
        {
            for (auto * child_plan : node->step->getChildPlans(/*for_explain=*/ false))
                stack.push_back(child_plan->getRootNode());
        }
    }
}

}

}
