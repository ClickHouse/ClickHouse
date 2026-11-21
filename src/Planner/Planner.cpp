#include <Analyzer/IQueryTreeNode.h>
#include <Planner/Planner.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypesNumber.h>

#include <Core/Names.h>
#include <Core/ProtocolDefines.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Processors/QueryPlan/BlocksMarshallingStep.h>
#include <Common/Exception.h>
#include <Common/FieldVisitors.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/saturatedDuration.h>
#include <base/scope_guard.h>

#include <Access/AccessControl.h>
#include <Access/SettingsProfile.h>
#include <Access/SettingsProfilesInfo.h>

#include <Processors/QueryPlan/FractionalLimitStep.h>
#include <Processors/QueryPlan/FractionalOffsetStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/keyTypeBreaksHashSharding.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/StreamInQueryResultCacheStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/LimitRangeStep.h>
#include <Processors/QueryPlan/NegativeLimitStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/ShuffleStep.h>
#include <Processors/QueryPlan/NegativeOffsetStep.h>
#include <Processors/QueryPlan/BypassExtremesSteps.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/NegativeLimitByStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/ReadFromRecursiveCTEStep.h>
#include <Processors/QueryPlan/ReadFromQueryResultCacheStep.h>
#include <Processors/QueryPlan/Streaming/hasUnboundedSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/QueryConsumedObjectSets.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/Cache/QueryResultCacheOnDisk.h>
#include <Interpreters/getEffectiveMaxThreads.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/IStorageCluster.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageView.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/Utils.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InterpolateNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TrivialGroupByLimit.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Analyzer/traverseQueryTree.h>

#include <Planner/CollectColumnIdentifiers.h>
#include <Planner/CollectMaterializedCTE.h>
#include <Planner/CollectSets.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/findQueryForParallelReplicas.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Planner/PlannerCorrelatedSubqueries.h>
#include <Planner/PlannerExpressionAnalysis.h>
#include <Planner/PlannerJoins.h>
#include <Planner/PlannerJoinTree.h>
#include <Planner/PlannerQueryProcessingInfo.h>
#include <Planner/PlannerSorting.h>
#include <Planner/PlannerSteps.h>
#include <Planner/PlannerStreaming.h>
#include <Planner/PlannerWindowFunctions.h>
#include <Planner/QueryAnalysisResult.h>
#include <Planner/Utils.h>
#include <base/types.h>
#include <Common/VectorWithMemoryTracking.h>


namespace ProfileEvents
{
    extern const Event SelectQueriesWithSubqueries;
    extern const Event QueriesWithSubqueries;
    extern const Event QueryPlanBuildMicroseconds;
}

namespace DB
{
namespace Setting
{
    extern const SettingsMap additional_table_filters;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool enable_reads_from_query_cache;
    extern const SettingsBool query_cache_for_subqueries;
    extern const SettingsBool query_cache_use_only_when_data_was_not_changed;
    extern const SettingsBool use_query_cache;
    extern const SettingsBool enable_group_by_top_k_optimization;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 max_subquery_depth;
    extern const SettingsBool parallel_replicas_allow_in_with_subquery;
    extern const SettingsString parallel_replicas_custom_key;
    extern const SettingsBool parallel_replicas_plan_based;
    extern const SettingsBool query_cache_compress_entries;
    extern const SettingsUInt64 query_cache_max_entries;
    extern const SettingsUInt64 query_cache_max_size_in_bytes;
    extern const SettingsMilliseconds query_cache_min_query_duration;
    extern const SettingsUInt64 query_cache_min_query_runs;
    extern const SettingsBool query_cache_share_between_users;
    extern const SettingsBool query_cache_squash_partial_results;
    extern const SettingsSeconds query_cache_ttl;
    extern const SettingsBool serialize_query_plan;
    extern const SettingsBool throw_on_unsupported_query_inside_transaction;
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const SettingsBool use_variant_as_common_type;
    extern const SettingsBool make_distributed_plan;
    extern const SettingsUInt64 query_plan_max_limit_for_top_k_optimization;
}

namespace ServerSetting
{
}

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int BAD_ARGUMENTS;
    extern const int TOO_DEEP_SUBQUERIES;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int ILLEGAL_STREAM;
}

namespace
{

/** Check that table and table function table expressions from planner context support transactions.
  *
  * There is precondition that table expression data for table expression nodes is collected in planner context.
  */
void checkStoragesSupportTransactions(const PlannerContextPtr & planner_context)
{
    const auto & query_context = planner_context->getQueryContext();
    if (!query_context->getSettingsRef()[Setting::throw_on_unsupported_query_inside_transaction])
        return;

    if (!query_context->getCurrentTransaction())
        return;

    for (const auto & [table_expression, _] : planner_context->getTableExpressionNodeToData())
    {
        StoragePtr storage;
        if (auto * table_node = table_expression->as<TableNode>())
            storage = table_node->getStorage();
        else if (auto * table_function_node = table_expression->as<TableFunctionNode>())
            storage = table_function_node->getStorage();

        if (storage && !storage->supportsTransactions())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Storage {} (table {}) does not support transactions",
                storage->getName(),
                storage->getStorageID().getNameForLogs());
    }
}

/// Extend lifetime of query context, storages, and table locks
void extendQueryContextAndStoragesLifetime(QueryPlan & query_plan, const PlannerContextPtr & planner_context)
{
    query_plan.addInterpreterContext(planner_context->getQueryContext());
    query_plan.addDistributedPlanDecisionContext(planner_context->getMutableQueryContext());
    for (const auto & [table_expression, _] : planner_context->getTableExpressionNodeToData())
    {
        if (auto * table_node = table_expression->as<TableNode>())
        {
            query_plan.addStorageHolder(table_node->getStorage());
            query_plan.addTableLock(table_node->getStorageLock());
        }
        else if (auto * table_function_node = table_expression->as<TableFunctionNode>())
        {
            query_plan.addStorageHolder(table_function_node->getStorage());
        }
    }
}

/// The `GROUP BY` top-K heap (`enable_group_by_top_k_optimization`) and the shared kept-keys
/// cutoff target the same `GROUP BY ... LIMIT n` shape and exclude each other: both top-K entry
/// points (`applyTopKPushdownToPartialAggregation` and `tryOptimizeGroupByTopK`) bail out on
/// `max_rows_to_group_by > 0`, so arming the cutoff takes the heap away from the query. Measured
/// head to head on the same build, the heap is as fast or faster on every key type: 1.4-1.6x on
/// `GROUP BY number % 100000000 LIMIT 10` (CI performance comparison), up to 1.3x on `String`
/// and `LowCardinality(Nullable(UInt64))` keys. So the cutoff only serves the queries the heap
/// does not apply to.
/// Returns true when the heap would apply to this query and should be left in charge.
bool preferGroupByTopKOverKeptKeysCutoff(const Settings & settings, UInt64 limit)
{
    if (!settings[Setting::enable_group_by_top_k_optimization])
        return false;

    /// A user-set `max_rows_to_group_by` (already known to be looser than the cutoff here) makes
    /// both top-K entry points bail out, so the heap would not apply and must not take the
    /// cutoff away from the query.
    if (settings[Setting::max_rows_to_group_by] != 0)
        return false;

    if (settings[Setting::query_plan_max_limit_for_top_k_optimization] != 0
        && limit > settings[Setting::query_plan_max_limit_for_top_k_optimization])
        return false;

    if (limit > Aggregator::Params::TopKParams::max_k)
        return false;

    return true;
}

void addReadFromQueryResultCacheStep(
    QueryPlan & query_plan,
    std::unique_ptr<SourceFromChunks> source,
    std::unique_ptr<SourceFromChunks> source_totals,
    std::unique_ptr<SourceFromChunks> source_extremes)
{
    auto read_from_query_result_cache_step = std::make_unique<ReadFromQueryResultCacheStep>(
        std::shared_ptr<SourceFromChunks>(std::move(source)),
        std::shared_ptr<SourceFromChunks>(std::move(source_totals)),
        std::shared_ptr<SourceFromChunks>(std::move(source_extremes)));
    query_plan.addStep(std::move(read_from_query_result_cache_step));
}

}

static PlannerContextPtr buildPlannerContext(const QueryTreeNodePtr & query_tree_node,
    const SelectQueryOptions & select_query_options,
    GlobalPlannerContextPtr global_planner_context)
{
    auto * query_node = query_tree_node->as<QueryNode>();
    auto * union_node = query_tree_node->as<UnionNode>();

    if (!query_node && !union_node)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected QUERY or UNION node. Actual {}",
            query_tree_node->formatASTForErrorMessage());

    auto & mutable_context = query_node ? query_node->getMutableContext() : union_node->getMutableContext();
    size_t max_subquery_depth = mutable_context->getSettingsRef()[Setting::max_subquery_depth];
    if (max_subquery_depth && select_query_options.subquery_depth > max_subquery_depth)
        throw Exception(ErrorCodes::TOO_DEEP_SUBQUERIES, "Too deep subqueries. Maximum: {}", max_subquery_depth);

    const auto & client_info = mutable_context->getClientInfo();

    bool need_to_disable_two_level_aggregation = client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY &&
        client_info.connection_tcp_protocol_version < DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD;

    if (need_to_disable_two_level_aggregation)
    {
        /// Disable two-level aggregation due to version incompatibility
        mutable_context->setSetting("group_by_two_level_threshold", Field(0));
        mutable_context->setSetting("group_by_two_level_threshold_bytes", Field(0));
    }

    if (select_query_options.is_subquery)
        updateContextForSubqueryExecution(mutable_context);

    return std::make_shared<PlannerContext>(mutable_context, std::move(global_planner_context), select_query_options);
}

Planner::Planner(const QueryTreeNodePtr & query_tree_,
    SelectQueryOptions & select_query_options_,
    const ActionsDAG * post_filter_)
    : query_tree(query_tree_)
    , select_query_options(select_query_options_)
    , planner_context(buildPlannerContext(query_tree, select_query_options,
        std::make_shared<GlobalPlannerContext>(
            findQueryForParallelReplicas(query_tree, select_query_options),
            findTableForParallelReplicas(query_tree, select_query_options),
            findTableUnionForParallelReplicas(query_tree, select_query_options),
            collectFiltersForAnalysis(query_tree, select_query_options, post_filter_))))
{
    markDistributedPlanContext();
}

Planner::Planner(const QueryTreeNodePtr & query_tree_,
    SelectQueryOptions & select_query_options_,
    GlobalPlannerContextPtr global_planner_context_)
    : query_tree(query_tree_)
    , select_query_options(select_query_options_)
    , planner_context(buildPlannerContext(query_tree_, select_query_options, std::move(global_planner_context_)))
{
    markDistributedPlanContext();
}

Planner::Planner(const QueryTreeNodePtr & query_tree_,
    SelectQueryOptions & select_query_options_,
    PlannerContextPtr planner_context_)
    : query_tree(query_tree_)
    , select_query_options(select_query_options_)
    , planner_context(std::move(planner_context_))
{
    markDistributedPlanContext();
}

/// Make the distributed-planning context sticky: once any query in the planning recursion runs
/// with `make_distributed_plan` enabled, every nested plan (subqueries inherit the options via
/// `SelectQueryOptions::subquery`) knows it may end up inside a plan that
/// `QueryPlan::convertToDistributed` splits into fragments, even when the subquery's own SETTINGS
/// clause turned the setting off for its local context. See `shouldReadFromQueryCacheForSubquery`.
void Planner::markDistributedPlanContext()
{
    if (planner_context->getQueryContext()->getSettingsRef()[Setting::make_distributed_plan])
        select_query_options.building_distributed_plan = true;
}

void Planner::buildQueryPlanIfNeeded()
{
    if (query_plan.isInitialized())
        return;

    /// Measure only the outermost plan build. buildQueryPlanIfNeeded recurses through
    /// nested planners (union branches, subqueries, CTEs) on the same thread, so without
    /// a guard each nested build would add its own time to the same event and double-count
    /// subplans (the event could then exceed the real plan-build wall time).
    static thread_local size_t query_plan_build_depth = 0;
    std::optional<ProfileEventTimeIncrement<Microseconds>> plan_build_time_watch;
    if (query_plan_build_depth == 0)
        plan_build_time_watch.emplace(ProfileEvents::QueryPlanBuildMicroseconds);
    ++query_plan_build_depth;
    SCOPE_EXIT({ --query_plan_build_depth; });

    LOG_TRACE(
        log,
        "Query to stage {}{}",
        QueryProcessingStage::toString(select_query_options.to_stage),
        select_query_options.only_analyze ? " only analyze" : "");

    if (query_tree->getNodeType() == QueryTreeNodeType::UNION)
        buildPlanForUnionNode();
    else
        buildPlanForQueryNode();

    /// The consistency check of the query result cache (or of `REFRESH ... IF CHANGED`) looks for
    /// non-deterministic functions in the query text, where the determinism of some functions is not known
    /// yet: `groupArraySample(1)(x)` or `arrayReduce('groupArraySample(1)', ...)` are deterministic only with
    /// a seed. Check the resolved functions, after a subquery-level cache may have installed the capture.
    /// A `STREAM` read (including the `STREAM BOUNDED UNORDERED` that an incremental refresh injects) reads
    /// only the safe streaming prefix, which can stop before a committed part (e.g. behind the block number
    /// of an unfinished insert). That prefix can later grow without any change of the active parts that
    /// the modification hash covers, so the result is not determined by the hash either.
    if (auto consumed_object_sets = planner_context->getQueryContext()->getQueryConsumedObjectSets();
        consumed_object_sets && (queryTreeHasNonDeterministicFunction(query_tree) || queryTreeHasStreamRead(query_tree)))
        consumed_object_sets->markNonDeterministic();

    extendQueryContextAndStoragesLifetime(query_plan, planner_context);
}

/// Gate every Planner-level cache use by the query kind, so an explicit `SETTINGS use_query_cache`
/// on a subquery cannot bypass the safety check that `executeQuery` performs for the outer query.
/// Caching internal queries (e.g. background system queries) or non-initial queries (e.g. a
/// `SECONDARY_QUERY` executed by a replica as part of a distributed query) would write/read
/// per-replica or per-internal entries that the outer query already manages or that bypass the
/// safety gate `executeQuery` applies via `Context::setCanUseQueryResultCache`.
static bool isQueryKindEligibleForSubqueryCache(const ContextPtr & query_context)
{
    return !query_context->isInternalQuery()
        && query_context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY;
}

/// Never use the cache in plans that are serialized for remote execution: logical plans are
/// shipped to another node by parallel replicas with `serialize_query_plan = 1` (see
/// `createRemotePlanForParallelReplicas`). The cache steps hold node-local state - a
/// `QueryResultCacheWriter` or the cached chunks themselves - which has no serialized
/// representation. The cache is populated and read by the plan the initiator executes itself.
///
/// Plan-based parallel replicas (`parallel_replicas_plan_based`) are another such case: the plan is
/// built locally, and `QueryPlanOptimizations::applyParallelReplicas` later serializes a fragment of
/// it - including the plans of `IN` subquery sets - for the replicas. A fragment holding a cache step
/// is kept local instead, so the cache would silently disable parallel replicas, and on a warm cache
/// the remote/local decision would depend on cache contents. The setting is checked on both the local
/// and the query-level context, so a subquery cannot escape the ban with its own SETTINGS clause.
static bool isPlanEligibleForSubqueryCache(const SelectQueryOptions & select_query_options, const ContextPtr & query_context)
{
    if (select_query_options.build_logical_plan)
        return false;

    const auto uses_plan_based_parallel_replicas = [](const ContextPtr & context)
    {
        return context->getSettingsRef()[Setting::parallel_replicas_plan_based] && context->canUseParallelReplicasOnInitiator();
    };
    return !uses_plan_based_parallel_replicas(query_context)
        && !(query_context->hasQueryContext() && uses_plan_based_parallel_replicas(query_context->getQueryContext()));
}

/// Returns the explicit `use_query_cache` opt-in / opt-out carried by a node's own query-level
/// `SETTINGS` clause, or `std::nullopt` when the clause does not set the setting, neither literally
/// nor through a `profile` it applies. `QueryNode` and `UnionNode` store the clause the same way,
/// so a plain subquery and a `UNION` subquery are handled alike.
///
/// The clause is only inspected for the *presence* of the setting; the value is then read from the
/// node's own context, which already has the whole clause applied in order (and clamped against the
/// constraints). `ParserSetQuery` keeps every occurrence, so `SETTINGS use_query_cache = 1,
/// use_query_cache = 0` - or an assignment followed by `= DEFAULT`, or a literal followed by a
/// profile that sets it the other way - has a last-wins effective value, which the first entry of
/// `settings_changes` does not report. Returning that first entry would make this path disagree with
/// every other consumer of the setting, e.g. taking the cache path (and possibly raising
/// `QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS`) for a query that opted out.
///
/// A profile stays in the clause as written (`profile = 'p'`), so that every shard of a distributed
/// query applies its own profile under its own constraints; it is resolved here instead. A profile
/// that sets `use_query_cache` is this clause's opt-in / opt-out exactly like the literal, while a
/// profile that does not mention it contributes nothing, so an outer `use_query_cache` still does
/// not propagate into the subquery (#119019).
template <typename Node>
static std::optional<bool> explicitUseQueryCacheSetting(const Node & node)
{
    const ContextPtr & node_context = node.getContext();
    const SettingsChanges & settings_changes = node.getSettingsChanges();

    bool mentioned_in_clause = settings_changes.tryGet("use_query_cache") != nullptr
        || std::ranges::contains(node.getDefaultSettings(), "use_query_cache");

    if (!mentioned_in_clause)
    {
        /// The profile lookup needs the mutable `AccessControl`, which only the global context exposes.
        AccessControl & access_control = node_context->getGlobalContext()->getAccessControl();
        for (const SettingChange & change : settings_changes)
        {
            if (change.name != "profile")
                continue;
            /// The profile resolved when the clause was applied to the node's context; one dropped since is not consulted.
            auto profile_id = access_control.find<SettingsProfile>(change.value.safeGet<String>());
            if (profile_id && access_control.getSettingsProfileInfo(*profile_id)->settings.tryGet("use_query_cache"))
            {
                mentioned_in_clause = true;
                break;
            }
        }
    }

    if (!mentioned_in_clause)
        return {};

    return node_context->getSettingsRef()[Setting::use_query_cache];
}

/// Returns true if the Planner-level query result cache (with `is_subquery = true` key) should be used.
/// For actual subqueries (is_subquery=true): explicit SETTINGS `use_query_cache` on the node wins.
/// For all Planner invocations: `query_cache_for_subqueries` propagation from outer query.
/// By default, `use_query_cache` does NOT propagate.
static bool shouldUseQueryCacheForSubquery(
    const QueryNode & query_node,
    bool outer_can_use_cache,
    const Settings & settings,
    const SelectQueryOptions & select_query_options,
    bool allow_propagation,
    const ContextPtr & query_context)
{
    if (!isQueryKindEligibleForSubqueryCache(query_context) || !isPlanEligibleForSubqueryCache(select_query_options, query_context))
        return false;

    const bool is_subquery = select_query_options.is_subquery;

    /// Only check explicit per-node `use_query_cache` for actual subqueries.
    /// For the top-level query, this setting is handled by `executeQuery` (with `is_subquery = false` key).
    if (is_subquery)
    {
        if (auto explicit_setting = explicitUseQueryCacheSetting(query_node))
            return *explicit_setting;
    }

    /// `query_cache_for_subqueries` enables the Planner-level (`is_subquery = true`) cache
    /// only for actual subqueries. The top-level query is cached separately by `executeQuery`
    /// using the `is_subquery = false` key; allowing it through this path here would write
    /// a duplicate entry under the subquery namespace.
    if (is_subquery && allow_propagation && settings[Setting::query_cache_for_subqueries] && outer_can_use_cache)
        return true;

    return false;
}

/// Whether the query tree contains the body of a view inlined by the analyzer (`analyzer_inline_views`),
/// or any other query or union planned under its own view-inner context (e.g. the definer's one of a
/// `SQL SECURITY DEFINER` view). The AST of the tree keeps only the explicit `SETTINGS` of such a node,
/// not its context, so hashing the tables of the AST under the context of the root misses the
/// dependencies of that context (e.g. `additional_table_filters` of the definer that read other tables).
static bool queryTreeHasNodeWithOwnViewInnerContext(const QueryTreeNodePtr & root)
{
    auto get_context = [](const IQueryTreeNode & node) -> ContextPtr
    {
        if (const auto * query_node = node.as<QueryNode>())
            return query_node->getContext();
        if (const auto * union_node = node.as<UnionNode>())
            return union_node->getContext();
        return nullptr;
    };

    const ContextPtr root_context = get_context(*root);

    std::vector<const IQueryTreeNode *> nodes_to_visit{root.get()};
    while (!nodes_to_visit.empty())
    {
        const auto * node = nodes_to_visit.back();
        nodes_to_visit.pop_back();

        if (node != root.get())
        {
            if (auto context = get_context(*node); context && context != root_context && context->isViewInnerQuery())
                return true;
        }

        for (const auto & child : node->getChildren())
            if (child)
                nodes_to_visit.push_back(child.get());
    }

    return false;
}

/// Whether the query tree contains a constant folded from a scalar subquery. The analyzer executes such a
/// subquery while it resolves the tree, before the planner of an enclosing subquery cached with
/// `query_cache_use_only_when_data_was_not_changed` installs the object-set capture that keeps an
/// unvalidated cache entry of the scalar subquery from being served. The rows of the scalar can therefore be
/// older than the hashes taken here, and the constant that replaced the subquery hides its tables from them.
static bool queryTreeHasConstantFoldedFromSubquery(const QueryTreeNodePtr & root)
{
    std::vector<const IQueryTreeNode *> nodes_to_visit{root.get()};
    while (!nodes_to_visit.empty())
    {
        const auto * node = nodes_to_visit.back();
        nodes_to_visit.pop_back();

        if (const auto * constant_node = node->as<ConstantNode>(); constant_node && constant_node->hasSourceExpression())
        {
            auto source_type = constant_node->getSourceExpression()->getNodeType();
            if (source_type == QueryTreeNodeType::QUERY || source_type == QueryTreeNodeType::UNION)
                return true;
        }

        for (const auto & child : node->getChildren())
            if (child)
                nodes_to_visit.push_back(child.get());
    }

    return false;
}

/// When the query cache is restricted to consistent results, returns the combined modification hash of the
/// referenced tables to fold into the (subquery) cache key. If consistency cannot be guaranteed, resets
/// `should_cache`, so the subquery is not cached.
static std::optional<UInt128> prepareSubqueryCacheReferencedTablesModificationHash(
    bool & should_cache,
    const QueryTreeNodePtr & query_tree,
    const ASTPtr & ast,
    const ContextPtr & query_context,
    const Settings & settings)
{
    /// A consistency consumer of the whole query - an outer query cached with
    /// `query_cache_use_only_when_data_was_not_changed`, or a `REFRESH ... IF CHANGED` - installs the
    /// object-set capture before the plan is built and certifies the result with the current hashes of
    /// the tables it reads. An entry of a subquery that does not validate its own sources could be older
    /// than those hashes, and the consumer would certify the stale rows as current. Such a subquery must
    /// not be served from (nor written to) the cache. A capture installed for a consistent sibling
    /// subquery below disables it as well, which only costs a cache miss.
    if (should_cache && !settings[Setting::query_cache_use_only_when_data_was_not_changed] && query_context->getQueryConsumedObjectSets())
        should_cache = false;

    std::optional<UInt128> referenced_tables_modification_hash;
    /// The modification hash is computed from the AST under `query_context`. An inlined view body is
    /// planned under its own context, whose dependencies the AST does not carry: fail closed.
    if (should_cache && settings[Setting::query_cache_use_only_when_data_was_not_changed]
        && queryTreeHasNodeWithOwnViewInnerContext(query_tree))
        should_cache = false;

    /// A scalar subquery was executed during analysis, possibly from an unvalidated cache entry, and its
    /// tables are not in the AST hashed below: fail closed. See `queryTreeHasConstantFoldedFromSubquery`.
    if (should_cache && settings[Setting::query_cache_use_only_when_data_was_not_changed]
        && queryTreeHasConstantFoldedFromSubquery(query_tree))
        should_cache = false;

    if (should_cache && settings[Setting::query_cache_use_only_when_data_was_not_changed])
    {
        referenced_tables_modification_hash = computeQueryReferencedTablesModificationHash(ast, query_context);
        if (!referenced_tables_modification_hash)
            should_cache = false;
    }

    /// Object-storage tables list their objects independently of the read, so the finalization
    /// consistency check in `addStreamInQueryResultCacheStepIfNeeded` could re-list a different object
    /// set than the read actually consumed (a listing `A -> B -> A` race). This Planner-level subquery cache can reach the consistency check
    /// without the `executeQuery` level having created the capture - e.g. an explicit per-subquery
    /// `use_query_cache = 1` opt-in while the outer query does not use the (consistent) cache, so
    /// `can_use_query_result_cache` is false there and no capture is allocated. Create it on the query
    /// context here too (reusing an existing one so the read path and both finalization checks share a
    /// single capture), so `StorageObjectStorage::getModificationHash` hashes the consumed set instead of
    /// re-listing. See `QueryConsumedObjectSets`.
    if (should_cache && referenced_tables_modification_hash.has_value()
        && query_context->hasQueryContext() && !query_context->getQueryConsumedObjectSets())
        query_context->getQueryContext()->setQueryConsumedObjectSets(std::make_shared<QueryConsumedObjectSets>());

    return referenced_tables_modification_hash;
}

/// Probes the Planner-level (`is_subquery = true`) query result cache and, on a hit, replaces the
/// plan with a step reading the cached result. Returns true when the plan was served from the cache.
/// If reads are enabled for both the in-memory and the on-disk cache, the (slower) on-disk cache is
/// consulted only on a miss in memory.
static bool tryAddReadFromQueryResultCacheStep(
    QueryPlan & query_plan,
    const ASTPtr & ast,
    const ContextPtr & query_context,
    const Settings & settings,
    const Settings & settings_for_key,
    const QueryResultCachePtr & query_result_cache,
    const QueryResultCacheOnDiskPtr & query_result_cache_on_disk,
    const std::optional<UInt128> & referenced_tables_modification_hash)
{
    const bool reads_from_memory = settings[Setting::enable_reads_from_query_cache];
    const bool reads_from_disk = query_result_cache_on_disk && query_result_cache_on_disk->readsEnabled();
    if (!reads_from_memory && !reads_from_disk)
        return false;

    QueryResultCache::Key key(ast, query_context->getCurrentDatabase(), settings_for_key, query_context->getCurrentQueryId(), query_context->getUserID(), query_context->getCurrentRoles(), /* is_subquery = */ true, referenced_tables_modification_hash);

    std::optional<QueryResultCacheReader> reader;
    if (reads_from_memory)
    {
        reader.emplace(query_result_cache->createReader(key));
        if (!reader->hasCacheEntryForKey())
            reader.reset();
    }
    if (!reader && reads_from_disk)
    {
        reader.emplace(query_result_cache_on_disk->createReader(key));
        if (!reader->hasCacheEntryForKey())
            reader.reset();
    }

    QueryResultCacheReader::recordProbeResult(reader.has_value());

    if (!reader)
        return false;

    addReadFromQueryResultCacheStep(query_plan, reader->getSource(), reader->getSourceTotals(), reader->getSourceExtremes());
    return true;
}

/// If active (write) use of the query cache is enabled, appends a step which stores the result of
/// `query_plan` in the Planner-level (`is_subquery = true`) query result cache.
/// `skip_context_check` is for the explicit per-subquery opt-in, where the outer query did not set
/// `use_query_cache` itself: the context flag check in `checkCanWriteQueryResultCache` is skipped
/// while all the other safety checks still apply.
static void addStreamInQueryResultCacheStepIfNeeded(
    QueryPlan & query_plan,
    const ASTPtr & ast,
    const ContextPtr & query_context,
    const Settings & settings,
    const Settings & settings_for_key,
    const QueryResultCachePtr & query_result_cache,
    const QueryResultCacheOnDiskPtr & query_result_cache_on_disk,
    bool skip_context_check,
    const std::optional<UInt128> & referenced_tables_modification_hash)
{
    if (!checkCanWriteQueryResultCache(ast, query_context, query_result_cache_on_disk, skip_context_check))
        return;

    auto created_at = std::chrono::system_clock::now();
    auto expires_at = saturatedSecondsFrom(created_at, settings[Setting::query_cache_ttl].totalSeconds());

    QueryResultCache::Key key(
        ast, query_context->getCurrentDatabase(), settings_for_key, query_plan.getRootNode()->step->getOutputHeader(),
        query_context->getCurrentQueryId(), query_context->getUserID(), query_context->getCurrentRoles(),
        settings[Setting::query_cache_share_between_users],
        created_at, expires_at,
        settings[Setting::query_cache_compress_entries],
        /* is_subquery = */ true,
        referenced_tables_modification_hash);

    const size_t num_query_runs = settings[Setting::query_cache_min_query_runs] ? query_result_cache->recordQueryRun(key) : 1; /// try to avoid locking a mutex in recordQueryRun()
    if (num_query_runs <= settings[Setting::query_cache_min_query_runs])
    {
        LOG_TRACE(getLogger("QueryResultCache"),
                "Skipped insert because the query ran {} times but the minimum required number of query runs to cache the query result is {}",
                num_query_runs, settings[Setting::query_cache_min_query_runs].value);
        return;
    }

    const bool write_to_memory_cache = canWriteToQueryResultCacheInMemory(query_context);
    QueryResultCacheOnDiskPtr write_to_on_disk_cache
        = canWriteToQueryResultCacheOnDisk(query_context, query_result_cache_on_disk) ? query_result_cache_on_disk : nullptr;

    if (!write_to_memory_cache && !write_to_on_disk_cache)
        return;

    auto query_result_cache_writer = std::make_shared<QueryResultCacheWriter>(query_result_cache->createWriter(
                        key,
                        std::chrono::milliseconds(settings[Setting::query_cache_min_query_duration].totalMilliseconds()),
                        settings[Setting::query_cache_squash_partial_results],
                        settings[Setting::max_block_size],
                        settings[Setting::query_cache_max_size_in_bytes],
                        settings[Setting::query_cache_max_entries],
                        write_to_memory_cache,
                        write_to_on_disk_cache));

    /// Drop the entry at finalization if a referenced table changed during execution (see the
    /// executeQuery-level cache for details).
    if (referenced_tables_modification_hash.has_value())
        query_result_cache_writer->setConsistencyValidator(
            [ast, query_context, expected = referenced_tables_modification_hash]()
            {
                auto current = computeQueryReferencedTablesModificationHash(ast, query_context);
                return current.has_value() && current == expected;
            });

    auto stream_into_query_result_cache_step = std::make_unique<StreamInQueryResultCacheStep>(query_plan.getRootNode()->step->getOutputHeader(), query_result_cache_writer);
    query_plan.addStep(std::move(stream_into_query_result_cache_step));
}

/// A cache hit replaces the whole subquery plan with a `ReadFromQueryResultCacheStep`, a node-local
/// source of the cached chunks. Unlike the write step, it cannot be dropped when the plan is split
/// into fragments - there is no subplan left behind it to run instead - so a hit would either fail
/// serialization or make the query fall back to local execution depending on what the cache happens
/// to hold. Do not read the cache while a distributed plan is being built, so the plan shape never
/// depends on cache contents.
///
/// The ban is keyed to the sticky `building_distributed_plan` option rather than to the local
/// `make_distributed_plan` setting: the subquery context applies the subquery's own SETTINGS
/// changes (see `QueryTreeBuilder::buildSelectExpression`), so a subquery could otherwise escape
/// the ban - and make the outer distributed plan's shape depend on cache contents again - just by
/// specifying `SETTINGS make_distributed_plan = 0` while the outer query is still planned
/// distributed.
///
/// There is no exception for the in-process local plans of a distributed query
/// (`is_local_plan_for_distributed_query`, e.g. the local shard plan of a `Distributed` table): that
/// flag only means nothing unmarshalls the blocks of the plan. The local plan is still united into
/// the enclosing plan, which `QueryPlan::convertToDistributed` may split into fragments, so a cache
/// hit there would make the shape of the outer plan depend on cache contents as well.
///
/// The sticky option only travels through `SelectQueryOptions::subquery`, but some storages plan a
/// generated query with fresh options (e.g. `StorageView`, `loop`, `StorageExecutable`, `TimeSeries`
/// and PromQL storages). So the setting of the query-level context is consulted as well: it is the
/// one the outer interpreter optimizes the top-level plan under, and no SETTINGS clause of a subquery
/// or of a generated query can change it.
static bool shouldReadFromQueryCacheForSubquery(
    const Settings & settings,
    const SelectQueryOptions & select_query_options,
    const ContextPtr & query_context)
{
    if (settings[Setting::make_distributed_plan] || select_query_options.building_distributed_plan)
        return false;
    return !query_context->hasQueryContext() || !query_context->getQueryContext()->getSettingsRef()[Setting::make_distributed_plan];
}

void Planner::buildPlanForUnionNode()
{
    const auto & union_node = query_tree->as<UnionNode &>();
    auto union_mode = union_node.getUnionMode();
    if (union_mode == SelectUnionMode::UNION_DEFAULT || union_mode == SelectUnionMode::EXCEPT_DEFAULT
        || union_mode == SelectUnionMode::INTERSECT_DEFAULT)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "UNION mode must be initialized");

    if (union_node.hasRecursiveCTETable())
    {
        const auto & recursive_cte_table = *union_node.getRecursiveCTETable();

        ColumnsWithTypeAndName recursive_cte_columns;
        recursive_cte_columns.reserve(recursive_cte_table.columns.size());
        for (const auto & recursive_cte_table_column : recursive_cte_table.columns)
            recursive_cte_columns.emplace_back(recursive_cte_table_column.type, recursive_cte_table_column.name);

        auto read_from_recursive_cte_step = std::make_unique<ReadFromRecursiveCTEStep>(std::make_shared<const Block>(Block(std::move(recursive_cte_columns))), query_tree);
        read_from_recursive_cte_step->setStepDescription(query_tree->toAST()->formatForErrorMessage(), select_query_options.max_step_description_length);
        query_plan.addStep(std::move(read_from_recursive_cte_step));
        return;
    }

    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    /// A `UNION` subquery opts into (or out of) the Planner-level query result cache with its own
    /// query-level `SETTINGS use_query_cache`, exactly like a plain subquery does in
    /// `buildPlanForQueryNode`. Only the explicit per-node setting is honored here: the
    /// `query_cache_for_subqueries` propagation is already applied to every arm of the union, and
    /// caching the union on top of that would only duplicate the same rows under a second key.
    QueryResultCachePtr query_result_cache = planner_context->getMutableQueryContext()->getQueryResultCache();
    bool can_use_query_result_cache = query_context->getCanUseQueryResultCache();
    std::optional<bool> explicit_use_query_cache;
    if (select_query_options.is_subquery)
        explicit_use_query_cache = explicitUseQueryCacheSetting(union_node);

    bool should_cache = select_query_options.is_subquery && query_result_cache != nullptr
        && isQueryKindEligibleForSubqueryCache(query_context)
        && isPlanEligibleForSubqueryCache(select_query_options, query_context)
        && explicit_use_query_cache.value_or(false);

    /// An explicit opt-out covers the whole set operation: its arms must not be cached through the
    /// outer `query_cache_for_subqueries` propagation either. Only the last arm carries the clause
    /// itself, so without this the earlier arms would still populate and read the cache.
    bool allow_propagation_to_arms = allow_query_cache_for_subqueries_propagation && explicit_use_query_cache.value_or(true);

    /// The settings are internally modified in some places during query execution, which would make
    /// the read and the write use different keys. Take a copy for both, as `buildPlanForQueryNode` does.
    ASTPtr ast;
    std::optional<Settings> settings_copy;
    QueryResultCacheOnDiskPtr query_result_cache_on_disk;
    std::optional<UInt128> referenced_tables_modification_hash;
    if (should_cache)
    {
        ast = query_tree->toAST({.qualify_indentifiers_with_database = false});
        settings_copy = settings;
        referenced_tables_modification_hash
            = prepareSubqueryCacheReferencedTablesModificationHash(should_cache, query_tree, ast, query_context, settings);
    }

    if (should_cache)
    {
        /// The query result cache on disk (backed by a preconfigured filesystem cache), see setting `query_cache_on_disk_cache_name`.
        query_result_cache_on_disk = QueryResultCacheOnDisk::getFromSettings(settings);

        if (shouldReadFromQueryCacheForSubquery(settings, select_query_options, query_context)
            && tryAddReadFromQueryResultCacheStep(
                query_plan, ast, query_context, settings, *settings_copy, query_result_cache, query_result_cache_on_disk,
                referenced_tables_modification_hash))
            return;
    }

    /// Each arm is checked only against its own context, so compare the arms with the union here.
    checkParallelReplicasImplementationOfSubqueries(query_tree, query_context);

    const auto & union_queries_nodes = union_node.getQueries().getNodes();
    size_t queries_size = union_queries_nodes.size();

    VectorWithMemoryTracking<std::unique_ptr<QueryPlan>> query_plans;
    query_plans.reserve(queries_size);

    SharedHeaders query_plans_headers;
    query_plans_headers.reserve(queries_size);

    for (const auto & query_node : union_queries_nodes)
    {
        /// Every branch of a `UNION` has its own context with its own SETTINGS clause, so branches
        /// must stay isolated from each other: the `Planner` mutates the options it is given (see
        /// `markDistributedPlanContext`), and sharing one object would let a branch-local
        /// `make_distributed_plan` of an earlier branch ban the query result cache reads of its
        /// siblings. Sticky options still travel downwards, because the copy carries them in.
        SelectQueryOptions branch_select_query_options = select_query_options;
        Planner query_planner(query_node, branch_select_query_options, planner_context->getGlobalPlannerContext());
        if (!allow_propagation_to_arms)
            query_planner.disableQueryCacheForSubqueriesPropagation();

        query_planner.buildQueryPlanIfNeeded();
        for (const auto & row_policy : query_planner.getUsedRowPolicies())
            used_row_policies.insert(row_policy);
        const auto & mapping = query_planner.getQueryNodeToPlanStepMapping();
        query_node_to_plan_step_mapping.insert(mapping.begin(), mapping.end());
        auto query_node_plan = std::make_unique<QueryPlan>(std::move(query_planner).extractQueryPlan());
        query_plans_headers.push_back(query_node_plan->getCurrentHeader());
        query_plans.push_back(std::move(query_node_plan));

        /// The quota and limit exemptions of the system tables are a property of the whole query -
        /// `InterpreterSelectQueryAnalyzer` reads them back from the options after the plan is
        /// built - so they keep propagating upwards out of the branch.
        select_query_options.ignore_quota |= branch_select_query_options.ignore_quota;
        select_query_options.ignore_limits |= branch_select_query_options.ignore_limits;
    }

    bool has_unbounded_arm = std::ranges::any_of(query_plans, [](const auto & plan) { return hasUnboundedSource(*plan); });
    if (has_unbounded_arm && union_mode != SelectUnionMode::UNION_ALL && union_mode != SelectUnionMode::UNION_DISTINCT)
        throw Exception(ErrorCodes::ILLEGAL_STREAM, "INTERSECT and EXCEPT are not supported over an unbounded STREAM");
    if (has_unbounded_arm && settings[Setting::use_query_cache])
        throw Exception(ErrorCodes::ILLEGAL_STREAM, "The setting use_query_cache is not supported over an unbounded STREAM");

    Block union_common_header = buildCommonHeaderForUnion(
        query_plans_headers, union_mode, union_node.getContext()->getSettingsRef()[Setting::use_variant_as_common_type]);
    addConvertingToCommonHeaderActionsIfNeeded(query_plans, union_common_header, query_plans_headers, query_context);
    auto max_threads = getEffectiveMaxThreads(*query_context);

    bool is_distinct = union_mode == SelectUnionMode::UNION_DISTINCT || union_mode == SelectUnionMode::INTERSECT_DISTINCT
        || union_mode == SelectUnionMode::EXCEPT_DISTINCT;

    if (union_mode == SelectUnionMode::UNION_ALL || union_mode == SelectUnionMode::UNION_DISTINCT)
    {
        auto union_step = std::make_unique<UnionStep>(std::move(query_plans_headers), max_threads, /* allow_narrowing = */ !has_unbounded_arm);
        query_plan.unitePlans(std::move(union_step), std::move(query_plans));
    }
    else if (union_mode == SelectUnionMode::INTERSECT_ALL || union_mode == SelectUnionMode::INTERSECT_DISTINCT
        || union_mode == SelectUnionMode::EXCEPT_ALL || union_mode == SelectUnionMode::EXCEPT_DISTINCT)
    {
        IntersectOrExceptStep::Operator intersect_or_except_operator = IntersectOrExceptStep::Operator::UNKNOWN;

        if (union_mode == SelectUnionMode::INTERSECT_ALL)
            intersect_or_except_operator = IntersectOrExceptStep::Operator::INTERSECT_ALL;
        else if (union_mode == SelectUnionMode::INTERSECT_DISTINCT)
            intersect_or_except_operator = IntersectOrExceptStep::Operator::INTERSECT_DISTINCT;
        else if (union_mode == SelectUnionMode::EXCEPT_ALL)
            intersect_or_except_operator = IntersectOrExceptStep::Operator::EXCEPT_ALL;
        else if (union_mode == SelectUnionMode::EXCEPT_DISTINCT)
            intersect_or_except_operator = IntersectOrExceptStep::Operator::EXCEPT_DISTINCT;

        auto union_step
            = std::make_unique<IntersectOrExceptStep>(std::move(query_plans_headers), intersect_or_except_operator, max_threads);
        query_plan.unitePlans(std::move(union_step), std::move(query_plans));
    }

    if (is_distinct)
    {
        /// Add distinct transform
        DistinctStep::Settings distinct_settings(settings);

        /// UNION concatenates its branches' streams instead of merging them, so a preliminary DISTINCT
        /// runs in parallel and shrinks what the final single-stream DISTINCT must merge. INTERSECT/EXCEPT
        /// already narrow their output to one stream, so a preliminary step there is pure overhead.
        const bool add_pre_distinct = !has_unbounded_arm && union_mode == SelectUnionMode::UNION_DISTINCT && preliminaryDistinctIsUseful(max_threads);

        if (add_pre_distinct)
        {
            auto pre_distinct_step = std::make_unique<DistinctStep>(
                query_plan.getCurrentHeader(),
                distinct_settings,
                0 /*limit hint*/,
                query_plan.getCurrentHeader()->getNames(),
                true /*pre distinct*/);
            pre_distinct_step->setStepDescription("Preliminary DISTINCT");
            query_plan.addStep(std::move(pre_distinct_step));
        }

        auto distinct_step = std::make_unique<DistinctStep>(
            query_plan.getCurrentHeader(),
            std::move(distinct_settings),
            0 /*limit hint*/,
            query_plan.getCurrentHeader()->getNames(),
            false /*pre distinct*/);
        if (add_pre_distinct)
            distinct_step->setStepDescription("DISTINCT");
        if (has_unbounded_arm)
            distinct_step->disableExternalDistinct();
        query_plan.addStep(std::move(distinct_step));
    }

    /// Each child of the UNION/INTERSECT/EXCEPT may independently add a DelayedMaterializingCTEsStep
    /// for the same CTE. During optimization only one child wins the atomic is_materialization_planned
    /// flag, leaving the other children without CTE materialization. Because IntersectOrExceptStep
    /// runs all children concurrently, the losing child may read from the CTE StorageMemory before the
    /// winning child has finished materializing it.
    ///
    /// Fix: add a DelayedMaterializingCTEsStep at the UNION level so that resolveMaterializingCTEs
    /// (which walks pre-order) claims the CTE here first, ensuring materialization completes before
    /// any child starts reading.
    if (!select_query_options.only_analyze && !select_query_options.build_logical_plan)
    {
        auto materialized_ctes = collectMaterializedCTEs(query_tree, select_query_options);
        addBuildSubqueriesForMaterializedCTEsIfNeeded(query_plan, select_query_options, materialized_ctes);
    }

    if (should_cache)
        addStreamInQueryResultCacheStepIfNeeded(
            query_plan, ast, query_context, settings, *settings_copy, query_result_cache, query_result_cache_on_disk,
            /* skip_context_check = */ !can_use_query_result_cache, referenced_tables_modification_hash);
}

void Planner::buildPlanForQueryNode()
{
    ProfileEvents::increment(ProfileEvents::SelectQueriesWithSubqueries);
    ProfileEvents::increment(ProfileEvents::QueriesWithSubqueries);

    auto & query_node = query_tree->as<QueryNode &>();
    const auto & query_context = planner_context->getQueryContext();
    if (query_node.hasWhere())
    {
        auto condition_constant = tryExtractConstantFromConditionNode(query_node.getWhere());
        if (condition_constant.has_value() && *condition_constant)
            query_node.getWhere() = {};
    }

    SelectQueryInfo select_query_info = buildSelectQueryInfo();
    const Settings & settings = query_context->getSettingsRef();

    QueryResultCachePtr query_result_cache = planner_context->getMutableQueryContext()->getQueryResultCache();
    bool can_use_query_result_cache = query_context->getCanUseQueryResultCache();
    ASTPtr ast = select_query_info.query;

    /// Determine if the query result cache should be used for this query node (with `is_subquery = true` key).
    /// The Planner-level cache is separate from the `executeQuery`-level cache (`is_subquery = false`).
    /// Rules:
    /// 1. For actual subqueries: explicit SETTINGS `use_query_cache` on the node wins
    /// 2. `query_cache_for_subqueries` propagates from outer query (applies to all Planner invocations)
    /// 3. By default, `use_query_cache` does NOT propagate from outer query to subqueries
    bool should_cache = shouldUseQueryCacheForSubquery(query_node, can_use_query_result_cache, settings,
        select_query_options, allow_query_cache_for_subqueries_propagation, query_context);

    /// Query result cache must actually exist
    if (should_cache && !query_result_cache)
        should_cache = false;

    /// For explicit per-subquery opt-in, we track cache eligibility locally
    /// without mutating the shared query context (which would leak to the outer query).
    bool local_can_use_cache = can_use_query_result_cache || should_cache;

    /// If the query runs with "use_query_cache = 1", we first probe if the query cache already contains the query result (if yes:
    /// return result from cache). If doesn't, we execute the query normally and write the result into the query cache. Both steps use a
    /// hash of the AST, the current database and the settings as cache key. Unfortunately, the settings are in some places internally
    /// modified between steps 1 and 2 (= during query execution) - this is silly but hard to forbid. As a result, the hashes no longer
    /// match and the cache is rendered ineffective. Therefore make a copy of the settings and use it for steps 1 and 2.
    std::optional<Settings> settings_copy;
    if (local_can_use_cache)
        settings_copy = settings;

    std::optional<UInt128> referenced_tables_modification_hash
        = prepareSubqueryCacheReferencedTablesModificationHash(should_cache, query_tree, ast, query_context, settings);

    /// The query result cache on disk (backed by a preconfigured filesystem cache), see setting `query_cache_on_disk_cache_name`.
    QueryResultCacheOnDiskPtr query_result_cache_on_disk;
    if (should_cache)
        query_result_cache_on_disk = QueryResultCacheOnDisk::getFromSettings(settings);

    /// If it is a non-internal SELECT, and passive (read) use of the query cache is enabled, and the cache knows the query, then add a ReadFromQueryResultCacheStep instead of building the rest of the plan.
    if (should_cache && shouldReadFromQueryCacheForSubquery(settings, select_query_options, query_context)
        && tryAddReadFromQueryResultCacheStep(
            query_plan, ast, query_context, settings, *settings_copy, query_result_cache, query_result_cache_on_disk,
            referenced_tables_modification_hash))
        return;

    StorageLimitsList current_storage_limits = storage_limits;
    select_query_info.local_storage_limits = buildStorageLimits(*query_context, select_query_options);
    current_storage_limits.push_back(select_query_info.local_storage_limits);
    select_query_info.storage_limits = std::make_shared<StorageLimitsList>(current_storage_limits);
    /// `SHUFFLE` reorders the rows like `ORDER BY`, so the storage must not stop reading after `LIMIT` rows.
    select_query_info.has_order_by = query_node.hasOrderBy() || query_node.isShuffle();
    select_query_info.has_window = hasWindowFunctionNodes(query_tree);
    select_query_info.has_aggregates = hasAggregateFunctionNodes(query_tree);
    select_query_info.need_aggregate = query_node.hasGroupBy() || select_query_info.has_aggregates;

    if (!select_query_info.has_window && query_node.hasQualify())
    {
        if (query_node.hasHaving())
            query_node.getHaving() = mergeConditionNodes({query_node.getHaving(), query_node.getQualify()}, query_context);
        else
            query_node.getHaving() = query_node.getQualify();

        query_node.getQualify() = {};
    }

    if (!select_query_info.need_aggregate && query_node.hasHaving())
    {
        if (query_node.hasWhere())
            query_node.getWhere() = mergeConditionNodes({query_node.getWhere(), query_node.getHaving()}, query_context);
        else
            query_node.getWhere() = query_node.getHaving();

        query_node.getHaving() = {};
    }

    /// Before collecting sets and materialized CTEs, whose subqueries are planned separately.
    checkParallelReplicasImplementationOfSubqueries(query_tree, query_context);

    collectSets(query_tree, *planner_context);
    auto materialized_ctes = collectMaterializedCTEs(query_tree, select_query_options);

    /// The kill switch for a query joining multiple tables runs first: the checks below throw when
    /// `enable_parallel_replicas = 2`, and a query for which parallel replicas are already disabled
    /// must be executed without them instead of failing with a parallel-replicas-only exception.
    /// It runs after `collectSets` so that the prepared sets it has to reach are already collected.
    disableParallelReplicasForMultipleTablesQueryIfNeeded(query_tree, planner_context);

    if (query_context->canUseTaskBasedParallelReplicas())
    {
        if (!settings[Setting::parallel_replicas_allow_in_with_subquery] && planner_context->getPreparedSets().hasSubqueries())
        {
            if (settings[Setting::allow_experimental_parallel_reading_from_replicas] >= 2)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "IN with subquery is not supported with parallel replicas");

            auto & mutable_context = planner_context->getMutableQueryContext();
            mutable_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
            LOG_DEBUG(log, "Disabling parallel replicas to execute a query with IN with subquery");
        }
    }

    /// `additional_table_filters` keys are resolved against the initiator's session current database,
    /// but on followers the rewritten `SELECT` uses fully qualified names and the follower's current
    /// database is the initiator's user-default DB, so the filter match is unreliable. Rather than
    /// patch the match (which differs case by case), disable the combination on the analyzer path.
    /// With `serialize_query_plan` the initiator lowers `additional_table_filters` into an explicit
    /// `FilterStep` and ships the serialized plan, so the follower never re-resolves the setting —
    /// the combination works there and the check is skipped. Plan-based parallel replicas ships the
    /// plan the same way, whatever `serialize_query_plan` says, so the check is skipped there too.
    if (query_context->canUseParallelReplicasOnInitiator()
        && !settings[Setting::serialize_query_plan]
        && !settings[Setting::parallel_replicas_plan_based]
        && !settings[Setting::additional_table_filters].value.empty())
    {
        if (settings[Setting::allow_experimental_parallel_reading_from_replicas] >= 2)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "additional_table_filters is not supported with parallel replicas unless the plan is "
                "shipped (parallel_replicas_plan_based=1 or serialize_query_plan=1)");

        auto & mutable_context = planner_context->getMutableQueryContext();
        mutable_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
        LOG_DEBUG(log, "Disabling parallel replicas to execute a query with additional_table_filters");
    }

    collectTableExpressionData(query_tree, planner_context);
    checkStoragesSupportTransactions(planner_context);

    const auto & table_filters = planner_context->getGlobalPlannerContext()->filters_for_table_expressions;
    if (!select_query_options.only_analyze && !table_filters.empty())
    {
        for (auto & [table_node, table_expression_data] : planner_context->getTableExpressionNodeToData())
        {
            auto it = table_filters.find(table_node);
            if (it != table_filters.end())
            {
                const auto & filters = it->second;
                table_expression_data.setFilterActions(filters.filter_actions->clone());
                table_expression_data.setPrewhereInfo(filters.prewhere_info);
            }
        }
    }

    if (query_context->canUseTaskBasedParallelReplicas())
    {
        auto & query_node_typed = query_tree->as<QueryNode &>();
        const auto & table_expression_nodes = extractTableExpressions(query_node_typed.getJoinTreeNodeTyped(), true, true);
        for (const auto & it : table_expression_nodes)
        {
            const std::optional<TableExpressionModifiers> * modifiers_ptr = nullptr;
            if (const auto * table_node = it->as<TableNode>())
                modifiers_ptr = &table_node->getTableExpressionModifiers();
            else if (const auto * table_function_node = it->as<TableFunctionNode>())
                modifiers_ptr = &table_function_node->getTableExpressionModifiers();

            if (!modifiers_ptr)
                continue;

            const auto & modifiers = *modifiers_ptr;
            /// A follower must keep the setting on for its own read-side `STREAM` refusal to fire.
            if (modifiers.has_value()
                && (modifiers->hasFinal()
                    || (modifiers->hasStream() && query_context->canUseParallelReplicasOnInitiator())))
            {
                const auto * modifier = modifiers->hasFinal() ? "FINAL" : "STREAM";
                if (settings[Setting::allow_experimental_parallel_reading_from_replicas] >= 2)
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "{} modifier is not supported with parallel replicas", modifier);

                LOG_DEBUG(log, "{} modifier is not supported with parallel replicas. Query will be executed without using them.", modifier);
                auto & mutable_context = planner_context->getMutableQueryContext();
                mutable_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
                break;
            }
        }
    }

    if (!settings[Setting::parallel_replicas_custom_key].value.empty())
    {
        /// Check support for JOIN for parallel replicas with custom key
        if (planner_context->getTableExpressionNodeToData().size() > 1)
        {
            if (settings[Setting::allow_experimental_parallel_reading_from_replicas] >= 2)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "JOINs are not supported with parallel replicas");

            LOG_DEBUG(log, "JOINs are not supported with parallel replicas. Query will be executed without using them.");

            auto & mutable_context = planner_context->getMutableQueryContext();
            mutable_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
            mutable_context->setSetting("parallel_replicas_custom_key", String{""});
        }
    }

    JoinTreeQueryPlan join_tree_query_plan;
    /// With plan-based parallel replicas the planner builds a plain, normal local plan; distribution is
    /// applied later as a plan transformation (QueryPlanOptimizations::applyParallelReplicas). So skip the
    /// old parallel-replicas planning path here (it would emit ReadFromLocalReplica /
    /// ReadFromRemoteParallelReplicas, e.g. inside a view/union inner query) and use the normal plan.
    /// Same for a plan that is shipped to a shard: those steps are local to the server that planned
    /// them and have no serialized form, and the shard decides on parallel replicas on its own.
    if (planner_context->getMutableQueryContext()->canUseTaskBasedParallelReplicas()
        && planner_context->getGlobalPlannerContext()->parallel_replicas_node == &query_node
        && !settings[Setting::parallel_replicas_plan_based]
        && !select_query_options.build_logical_plan)
    {
        join_tree_query_plan = buildQueryPlanForParallelReplicas(query_node, planner_context, select_query_info.storage_limits);
    }
    else
    {
        auto top_level_identifiers = collectTopLevelColumnIdentifiers(query_tree, planner_context);
        join_tree_query_plan = buildJoinTreeQueryPlan(query_tree,
            select_query_info,
            select_query_options,
            top_level_identifiers,
            planner_context);
    }

    auto from_stage = join_tree_query_plan.stage;
    query_plan = std::move(join_tree_query_plan.query_plan);
    used_row_policies = std::move(join_tree_query_plan.used_row_policies);
    auto & mapping = join_tree_query_plan.query_node_to_plan_step_mapping;
    query_node_to_plan_step_mapping.insert(mapping.begin(), mapping.end());

    LOG_TRACE(
        log,
        "Query from stage {} to stage {}{}",
        QueryProcessingStage::toString(from_stage),
        QueryProcessingStage::toString(select_query_options.to_stage),
        select_query_options.only_analyze ? " only analyze" : "");

    auto useful_sets = std::move(join_tree_query_plan.useful_sets);

    for (auto & [_, table_expression_data] : planner_context->getTableExpressionNodeToData())
    {
        if (table_expression_data.getPrewhereFilterActions())
            appendSetsFromActionsDAG(*table_expression_data.getPrewhereFilterActions(), useful_sets);

        if (table_expression_data.getRowLevelFilterActions())
            appendSetsFromActionsDAG(*table_expression_data.getRowLevelFilterActions(), useful_sets);
    }

    if (select_query_options.to_stage == QueryProcessingStage::FetchColumns)
    {
        /// The reader evaluates PREWHERE and row-level filter expressions itself, so the sets they
        /// reference need their sources attached even though no expression step is added past here.
        if (!select_query_options.only_analyze)
            addBuildSubqueriesForSetsStepIfNeeded(query_plan, select_query_options, planner_context, useful_sets);
        return;
    }

    PlannerQueryProcessingInfo query_processing_info(from_stage, select_query_options.to_stage);
    auto query_analysis_result = QueryAnalysisResult::create(query_tree, query_processing_info, planner_context);
    auto expression_analysis_result = buildExpressionAnalysisResult(query_tree,
        query_plan.getCurrentHeader()->getColumnsWithTypeAndName(),
        planner_context,
        query_processing_info,
        join_tree_query_plan.source_constants);

    if (hasUnboundedSource(query_plan))
    {
        buildStreamingPlan(
            query_plan,
            planner_context,
            expression_analysis_result,
            useful_sets,
            query_tree,
            select_query_options,
            query_processing_info,
            query_analysis_result,
            materialized_ctes);

        query_node_to_plan_step_mapping[&query_node] = query_plan.getRootNode();
        return;
    }

    if (query_processing_info.isIntermediateStage())
    {
        addPreliminarySortOrDistinctOrLimitStepsIfNeeded(
            query_plan,
            expression_analysis_result,
            query_analysis_result,
            planner_context,
            query_processing_info,
            query_tree,
            select_query_options,
            useful_sets);

        if (expression_analysis_result.hasAggregation())
        {
            const auto & aggregation_analysis_result = expression_analysis_result.getAggregation();
            addMergingAggregatedStep(query_plan, aggregation_analysis_result, query_analysis_result, planner_context, join_tree_query_plan.shard_collapse_duplicate_keys);
        }
    }

    if (query_processing_info.isFirstStage())
    {
        if (expression_analysis_result.hasWhere())
            addFilterStep(planner_context, query_plan, expression_analysis_result.getWhere(), select_query_options, "WHERE", useful_sets);

        if (expression_analysis_result.hasAggregation())
        {
            auto & aggregation_analysis_result = expression_analysis_result.getAggregation();
            if (aggregation_analysis_result.before_aggregation_actions)
                addExpressionStep(
                    planner_context,
                    query_plan,
                    aggregation_analysis_result.before_aggregation_actions,
                    /*correlated_subtrees=*/{},
                    select_query_options,
                    "Before GROUP BY",
                    useful_sets);

            /// For the trivial `GROUP BY ... LIMIT` shape, cap the aggregation at `LIMIT + OFFSET`
            /// keys. Unlike the settings-based rewrite of `OptimizeTrivialGroupByLimitPass`
            /// (which is restricted to aggregate-free projections), the shared kept-keys cutoff
            /// keeps the aggregate values of the kept keys exact, so it also fires with aggregate
            /// functions in the projection. It is only sound when this node performs the complete
            /// aggregation itself (both stages): partial states sent to a remote initiator would
            /// be merged with other nodes' states, whose kept keys differ, and the values of the
            /// kept keys would be undercounted again — one level up, across nodes instead of
            /// across threads. On the shards of distributed queries and on the replicas of
            /// parallel-replicas reading, `isSecondStage` is false, so the cutoff stays off there.
            /// `settings` holds this node's SETTINGS clause as constrained and clamped. A nested change equal to the inherited
            /// value is not applied there, so an explicit `group_by_overflow_mode` is not marked as changed.
            Settings query_settings = settings;
            if (query_node.getSettingsChanges().tryGet("group_by_overflow_mode"))
                query_settings[Setting::group_by_overflow_mode].setChanged(true);

            std::optional<UInt64> trivial_group_by_limit;
            if (!query_settings[Setting::make_distributed_plan]
                && query_processing_info.isFirstStage() && query_processing_info.isSecondStage()
                && hasAggregateFunctionNodes(query_node.getProjectionNode()))
            {
                trivial_group_by_limit = getTrivialGroupByLimit(query_node, query_settings);

                /// Respect a user-set tighter `max_rows_to_group_by`. Equality is safe only with
                /// the approximate ANY-mode semantics; otherwise forcing ANY below would replace
                /// the user's `throw` or `break` contract.
                const UInt64 user_max_rows = query_settings[Setting::max_rows_to_group_by];
                if (trivial_group_by_limit && user_max_rows != 0
                    && (user_max_rows < *trivial_group_by_limit
                        || (user_max_rows == *trivial_group_by_limit && query_settings[Setting::group_by_overflow_mode] != OverflowMode::ANY)))
                    trivial_group_by_limit.reset();

                /// Leave the `GROUP BY` top-K heap in charge wherever it applies: it is faster.
                if (trivial_group_by_limit && preferGroupByTopKOverKeptKeysCutoff(query_settings, *trivial_group_by_limit))
                    trivial_group_by_limit.reset();
            }

            addAggregationStep(query_plan, query_node, expression_analysis_result, query_analysis_result, planner_context, query_settings, trivial_group_by_limit);
        }

        /** If we have aggregation, we can't execute any later-stage
          * expressions on shards, neither "Before WINDOW" nor "Before ORDER BY"
          */
        if (!expression_analysis_result.hasAggregation())
        {
            if (expression_analysis_result.hasWindow())
            {
                /** Window functions must be executed on initiator (second_stage).
                  * ORDER BY and DISTINCT might depend on them, so if we have
                  * window functions, we can't execute ORDER BY and DISTINCT
                  * now, on shard (first_stage).
                  */
                auto & window_analysis_result = expression_analysis_result.getWindow();
                if (window_analysis_result.before_window_actions)
                    addExpressionStep(
                        planner_context,
                        query_plan,
                        window_analysis_result.before_window_actions,
                        /*correlated_subtrees=*/{},
                        select_query_options,
                        "Before WINDOW",
                        useful_sets);
            }
            else
            {
                /** There are no window functions, so we can execute the
                  * Projection expressions, preliminary DISTINCT and before ORDER BY expressions
                  * now, on shards (first_stage).
                  */
                auto & projection_analysis_result = expression_analysis_result.getProjection();
                addExpressionStep(
                    planner_context,
                    query_plan,
                    projection_analysis_result.projection_actions,
                    projection_analysis_result.correlated_subtrees,
                    select_query_options,
                    "Projection",
                    useful_sets);

                if (query_node.isDistinct())
                {
                    addDistinctStep(query_plan,
                        query_analysis_result,
                        planner_context,
                        expression_analysis_result.getProjection().projection_column_names,
                        query_node,
                        true /*before_order*/,
                        true /*pre_distinct*/);
                }

                if (expression_analysis_result.hasSort())
                {
                    auto & sort_analysis_result = expression_analysis_result.getSort();
                    addExpressionStep(
                        planner_context,
                        query_plan,
                        sort_analysis_result.before_order_by_actions,
                        /*correlated_subtrees=*/{},
                        select_query_options,
                        "Before ORDER BY",
                        useful_sets);
                }
            }
        }

        addPreliminarySortOrDistinctOrLimitStepsIfNeeded(
            query_plan,
            expression_analysis_result,
            query_analysis_result,
            planner_context,
            query_processing_info,
            query_tree,
            select_query_options,
            useful_sets);
    }

    if (query_processing_info.isSecondStage() || query_processing_info.isFromAggregationState())
    {
        if (query_processing_info.isFromAggregationState())
        {
            /// Aggregation was performed on remote shards
        }
        else if (expression_analysis_result.hasAggregation())
        {
            const auto & aggregation_analysis_result = expression_analysis_result.getAggregation();

            if (!query_processing_info.isFirstStage())
            {
                addMergingAggregatedStep(query_plan, aggregation_analysis_result, query_analysis_result, planner_context, join_tree_query_plan.shard_collapse_duplicate_keys);
            }

            bool having_executed = false;

            if (query_node.isGroupByWithTotals())
            {
                addTotalsHavingStep(query_plan, expression_analysis_result, query_analysis_result, planner_context, select_query_options, query_node, useful_sets);
                having_executed = true;
            }

            addCubeOrRollupStepIfNeeded(query_plan, aggregation_analysis_result, query_analysis_result, planner_context, query_node);

            if (!having_executed && expression_analysis_result.hasHaving())
                addFilterStep(planner_context, query_plan, expression_analysis_result.getHaving(), select_query_options, "HAVING", useful_sets);
        }

        if (query_processing_info.isFromAggregationState())
        {
            if (expression_analysis_result.hasWindow())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Window functions does not support processing from WithMergeableStateAfterAggregation");
        }
        else if (expression_analysis_result.hasWindow() || expression_analysis_result.hasAggregation())
        {
            if (expression_analysis_result.hasWindow())
            {
                auto & window_analysis_result = expression_analysis_result.getWindow();
                if (expression_analysis_result.hasAggregation())
                    addExpressionStep(
                        planner_context,
                        query_plan,
                        window_analysis_result.before_window_actions,
                        /*correlated_subtrees=*/{},
                        select_query_options,
                        "Before window functions",
                        useful_sets);

                addWindowSteps(query_plan, planner_context, window_analysis_result, select_query_options.max_step_description_length);
            }

            if (expression_analysis_result.hasQualify())
                addFilterStep(planner_context, query_plan, expression_analysis_result.getQualify(), select_query_options, "QUALIFY", useful_sets);

            auto & projection_analysis_result = expression_analysis_result.getProjection();
            addExpressionStep(
                planner_context,
                query_plan,
                projection_analysis_result.projection_actions,
                projection_analysis_result.correlated_subtrees,
                select_query_options,
                "Projection",
                useful_sets);

            if (query_node.isDistinct())
            {
                addDistinctStep(query_plan,
                    query_analysis_result,
                    planner_context,
                    expression_analysis_result.getProjection().projection_column_names,
                    query_node,
                    true /*before_order*/,
                    true /*pre_distinct*/);
            }

            if (expression_analysis_result.hasSort())
            {
                auto & sort_analysis_result = expression_analysis_result.getSort();
                addExpressionStep(
                    planner_context,
                    query_plan,
                    sort_analysis_result.before_order_by_actions,
                    /*correlated_subtrees=*/{},
                    select_query_options,
                    "Before ORDER BY",
                    useful_sets);
            }
        }
        else
        {
            /// There are no aggregation or windows, all expressions before ORDER BY executed on shards
        }

        const bool distinct_shuffle = query_node.isShuffle() && query_node.isDistinct();
        const bool finalize_distinct_shuffle = distinct_shuffle && query_processing_info.isFinalizingStage();

        if (finalize_distinct_shuffle)
        {
            /// Sample distinct result rows, not occurrences in individual streams or shards.
            /// Incoming shard keys are not part of the projection columns used for deduplication.
            addDistinctStep(query_plan,
                query_analysis_result,
                planner_context,
                expression_analysis_result.getProjection().projection_column_names,
                query_node,
                true /*before_order*/,
                false /*pre_distinct*/);
            addShuffleStep(query_plan, query_analysis_result, planner_context, query_processing_info);
        }
        else if (expression_analysis_result.hasSort() || query_node.isShuffle())
        {
            /** If there is an ORDER BY for distributed query processing,
              * but there is no aggregation, then on the remote servers ORDER BY was made
              * and we merge the sorted streams from remote servers.
              *
              * Also in case of remote servers was process the query up to WithMergeableStateAfterAggregationAndLimit
              * (distributed_group_by_no_merge=2 or optimize_distributed_group_by_sharding_key=1 takes place),
              * then merge the sorted streams is enough, since remote servers already did full ORDER BY.
              */
            if (query_processing_info.isFromAggregationState())
                addMergeSortingStep(query_plan, query_analysis_result, planner_context, "Merge sorted streams after aggregation stage for ORDER BY");
            else if (!query_processing_info.isFirstStage() &&
                !expression_analysis_result.hasAggregation() &&
                !expression_analysis_result.hasWindow() &&
                !(query_node.isGroupByWithTotals() && !query_analysis_result.aggregate_final))
                addMergeSortingStep(query_plan, query_analysis_result, planner_context, "Merge sorted streams for ORDER BY, without aggregation");
            else if (query_node.isShuffle())
                addShuffleStep(query_plan, query_analysis_result, planner_context, query_processing_info);
            else
                addSortingStep(query_plan, query_analysis_result, planner_context);
        }

        /** Optimization if there are several sources and there is LIMIT, then first apply the preliminary LIMIT,
          * limiting the number of rows in each up to `offset + limit`.
          */
        bool applied_prelimit = addPreliminaryLimitOptimizationStepIfNeeded(query_plan,
            query_analysis_result,
            planner_context,
            query_processing_info,
            query_tree,
            select_query_options);

        //// If there was more than one stream, then DISTINCT needs to be performed once again after merging all streams.
        if (!finalize_distinct_shuffle && !query_processing_info.isFromAggregationState() && query_node.isDistinct())
        {
            addDistinctStep(query_plan,
                query_analysis_result,
                planner_context,
                expression_analysis_result.getProjection().projection_column_names,
                query_node,
                false /*before_order*/,
                false /*pre_distinct*/);
        }

        if ((!query_processing_info.isFromAggregationState() || finalize_distinct_shuffle)
            && (!distinct_shuffle || finalize_distinct_shuffle) && expression_analysis_result.hasLimitBy())
        {
            auto & limit_by_analysis_result = expression_analysis_result.getLimitBy();
            preserveShuffleKeyColumn(limit_by_analysis_result.before_limit_by_actions->dag, query_plan);
            addExpressionStep(
                planner_context,
                query_plan,
                limit_by_analysis_result.before_limit_by_actions,
                /*correlated_subtrees=*/{},
                select_query_options,
                "Before LIMIT BY",
                useful_sets);
            addLimitByStep(
                query_plan,
                limit_by_analysis_result,
                query_analysis_result,
                planner_context,
                query_node,
                false /*do_not_skip_offset*/);
        }

        /// WITH FILL / INTERPOLATE must run only on the finalizing node, over the merged stream,
        /// not per shard (which would duplicate fill rows and break the INTERPOLATE column on merge).
        if (query_node.hasOrderBy() && query_processing_info.isFinalizingStage())
            addWithFillStepIfNeeded(query_plan, query_analysis_result, expression_analysis_result.getSort(), planner_context, query_node, select_query_options, useful_sets);

        const bool apply_limit = query_processing_info.getToStage() != QueryProcessingStage::WithMergeableStateAfterAggregation;
        /// `isToAggregationState` covers both `WithMergeableStateAfterAggregation` (stage 3) and
        /// `WithMergeableStateAfterAggregationAndLimit` (stage 4). OFFSET must not be applied at
        /// either stage because OFFSET means skipping rows from the entire query result, not from each
        /// shard individually.
        const bool apply_offset = !query_processing_info.isToAggregationState();
        const bool has_limit_range = query_node.hasLimitAfter() || query_node.hasLimitUntil();

        /// WITH TIES needs ORDER BY columns that are removed by projection, so it must run before extremes.
        if (!has_limit_range && query_node.hasLimit() && query_node.isLimitWithTies() && apply_limit && apply_offset)
            addLimitStep(query_plan, query_analysis_result, planner_context, query_node);

        /// Extremes are computed before the final LIMIT, matching normal LIMIT semantics.
        addExtremesStepIfNeeded(query_plan, planner_context);

        bool limit_applied = applied_prelimit || (has_limit_range && apply_limit && apply_offset) || (query_node.isLimitWithTies() && apply_offset);

        /** Limit is no longer needed if there is prelimit.
          *
          * That LIMIT cannot be applied if OFFSET should not be applied, since LIMIT will apply OFFSET too.
          * This is the case for various optimizations for distributed queries,
          * and when LIMIT cannot be applied it will be applied on the initiator anyway.
          */
        if (has_limit_range && apply_limit && apply_offset)
        {
            /// Keep the AFTER/UNTIL boundary columns (which may not be selected) available for the range
            /// step, since it runs before "Project names" would drop them.
            if (expression_analysis_result.hasLimitRange())
                addExpressionStep(
                    planner_context,
                    query_plan,
                    expression_analysis_result.getLimitRange().before_limit_range_actions,
                    /*correlated_subtrees=*/{},
                    select_query_options,
                    "Before LIMIT range (AFTER/UNTIL)",
                    useful_sets);

            addLimitRangeStep(
                query_plan,
                query_analysis_result,
                planner_context,
                query_node,
                expression_analysis_result.hasSort() ? expression_analysis_result.getSort().interpolate_target_column_names : NameToNameMap{},
                useful_sets);
        }
        else if (query_node.hasLimit() && !has_limit_range && apply_limit && !limit_applied && apply_offset)
            addLimitStep(query_plan, query_analysis_result, planner_context, query_node);
        else if (!limit_applied && apply_offset && query_node.hasOffset())
            addOffsetStep(query_plan, query_analysis_result);

        /// Project names is not done on shards, because initiator will not find columns in blocks
        if (!query_processing_info.isToAggregationState())
        {
            auto & projection_analysis_result = expression_analysis_result.getProjection();
            addExpressionStep(
                planner_context,
                query_plan,
                projection_analysis_result.project_names_actions,
                /*correlated_subtrees=*/{},
                select_query_options,
                "Project names",
                useful_sets);
        }

        // For additional_result_filter setting
        addAdditionalFilterStepIfNeeded(query_plan, query_node, select_query_options, planner_context, useful_sets);
    }

    /// A header carrying nothing but row-count-only columns cannot express "N rows" across a
    /// mergeable-stage boundary, and both sides must derive the same header.
    if (!query_processing_info.isFinalizingStage() && query_plan.isInitialized())
        addRowCountMarkerStepIfNeeded(query_plan, planner_context);

    const auto & client_info = query_context->getClientInfo();

    // Not all cases are supported here yet. E.g. for this query:
    // select * from remote('127.0.0.{1,2}', numbers_mt(1e6)) group by number
    // we will have `BlocksMarshallingStep` added to the query plan, but not for
    // select * from remote('127.0.0.{1,2}', numbers_mt(1e6))
    // because `to_stage` for it will be `QueryProcessingStage::Complete`.
    if (contextAllowsBlocksMarshalling(*query_context)
        && client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY
        && select_query_options.to_stage != QueryProcessingStage::Complete // Don't do it for INSERT SELECT, for example
        // A local shard/replica plan is united into the parent pipeline in this process, where
        // nothing unmarshalls the blocks.
        && !select_query_options.is_local_plan_for_distributed_query
    )
        query_plan.addStep(std::make_unique<BlocksMarshallingStep>(query_plan.getCurrentHeader()));

    if (!select_query_options.only_analyze)
    {
        addBuildSubqueriesForSetsStepIfNeeded(query_plan, select_query_options, planner_context, useful_sets);
        addBuildSubqueriesForMaterializedCTEsIfNeeded(query_plan, select_query_options, materialized_ctes);
    }

    /// If it is a non-internal SELECT query, and active (write) use of the query cache is enabled,
    /// then add a step which stores the result in the query cache.
    if (should_cache)
        addStreamInQueryResultCacheStepIfNeeded(
            query_plan, ast, query_context, settings, *settings_copy, query_result_cache, query_result_cache_on_disk,
            /* skip_context_check = */ !can_use_query_result_cache, referenced_tables_modification_hash);

    query_node_to_plan_step_mapping[&query_node] = query_plan.getRootNode();
}

SelectQueryInfo Planner::buildSelectQueryInfo() const
{
    auto select_query_info = ::DB::buildSelectQueryInfo(query_tree, planner_context);
    select_query_info.is_secondary_query_root = select_query_options.is_secondary_query_root;
    return select_query_info;
}

void Planner::addStorageLimits(const StorageLimitsList & limits)
{
    for (const auto & limit : limits)
        storage_limits.push_back(limit);
}

}
