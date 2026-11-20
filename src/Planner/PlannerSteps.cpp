#include <Planner/PlannerSteps.h>

#include <Planner/CollectColumnIdentifiers.h>
#include <Planner/CollectSets.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/Planner.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Planner/PlannerCorrelatedSubqueries.h>
#include <Planner/PlannerExpressionAnalysis.h>
#include <Planner/PlannerJoinTree.h>
#include <Planner/PlannerJoins.h>
#include <Planner/PlannerQueryProcessingInfo.h>
#include <Planner/PlannerSorting.h>
#include <Planner/PlannerWindowFunctions.h>
#include <Planner/QueryAnalysisResult.h>
#include <Planner/Utils.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/InterpolateNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Analyzer/traverseQueryTree.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/BypassExtremesSteps.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/FractionalLimitStep.h>
#include <Processors/QueryPlan/FractionalOffsetStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitRangeStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/NegativeLimitByStep.h>
#include <Processors/QueryPlan/NegativeLimitStep.h>
#include <Processors/QueryPlan/NegativeOffsetStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/keyTypeBreaksHashSharding.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/ShuffleStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/WindowStep.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

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

#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/getEffectiveMaxThreads.h>

#include <DataTypes/DataTypesNumber.h>

#include <Columns/IColumn.h>

#include <Core/Names.h>
#include <Core/ProtocolDefines.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>

#include <Common/Exception.h>
#include <Common/FieldVisitors.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/logger_useful.h>

#include <base/scope_guard.h>
#include <base/types.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_constant_columns_after_filter;
    extern const SettingsUInt64 aggregation_in_order_max_block_bytes;
    extern const SettingsUInt64 aggregation_memory_efficient_merge_threads;
    extern const SettingsBool collect_hash_table_stats_during_aggregation;
    extern const SettingsBool distributed_aggregation_memory_efficient;
    extern const SettingsBool enable_memory_bound_merging_of_aggregation_results;
    extern const SettingsBool empty_result_for_aggregation_by_constant_keys_on_empty_set;
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
    extern const SettingsBool enable_group_by_top_k_optimization;
    extern const SettingsUInt64 group_by_top_k_optimization_observation_rows;
    extern const SettingsBool group_by_top_k_optimization_shared_boundary;
    extern const SettingsBool exact_rows_before_limit;
    extern const SettingsBool extremes;
    extern const SettingsBool force_aggregation_in_order;
    extern const SettingsUInt64 group_by_two_level_threshold;
    extern const SettingsUInt64 group_by_two_level_threshold_bytes;
    extern const SettingsBool group_by_use_nulls;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 max_size_to_preallocate_for_aggregation;
    extern const SettingsUInt64 parallel_replicas_min_number_of_rows_per_replica;
    extern const SettingsBool query_plan_enable_multithreading_after_window_functions;
    extern const SettingsFloat totals_auto_threshold;
    extern const SettingsTotalsMode totals_mode;
    extern const SettingsBool use_with_fill_by_sorting_prefix;
    extern const SettingsFloat min_hit_rate_to_use_consecutive_keys_optimization;
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const SettingsUInt64 max_bytes_before_external_group_by;
    extern const SettingsDouble max_bytes_ratio_before_external_group_by;
    extern const SettingsUInt64 min_bytes_before_external_group_by;
    extern const SettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const SettingsBool compile_aggregate_expressions;
    extern const SettingsUInt64 min_count_to_compile_aggregate_expression;
    extern const SettingsBool enable_software_prefetch_in_aggregation;
    extern const SettingsBool optimize_group_by_constant_keys;
    extern const SettingsBool enable_adaptive_aggregator;
    extern const SettingsUInt64 adaptive_aggregator_freeze_threshold;
    extern const SettingsUInt64 adaptive_aggregator_freeze_threshold_bytes;
    extern const SettingsBool adaptive_aggregator_disable_thaw;
    extern const SettingsBool use_aggregation_memory_tracker;
    extern const SettingsUInt64 max_bytes_to_transfer;
    extern const SettingsUInt64 max_rows_to_transfer;
    extern const SettingsOverflowMode transfer_overflow_mode;
    extern const SettingsBool enable_packed_string_keys_in_aggregation;
    extern const SettingsBool enable_fixed_and_packed_string_keys_in_aggregation;
    extern const SettingsBool enable_parallel_single_level_merge;
    extern const SettingsBool enable_parallel_single_level_merge_by_index;
    extern const SettingsBool enable_producing_buckets_out_of_order_in_aggregation;
    extern const SettingsBool serialize_string_in_memory_with_zero_byte;
    extern const SettingsString temporary_files_codec;
    extern const SettingsNonZeroUInt64 temporary_files_buffer_size;
    extern const SettingsBool make_distributed_plan;
    extern const SettingsUInt64 query_plan_max_limit_for_top_k_optimization;
}

namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_entries_for_hash_table_stats;
}

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int INVALID_WITH_FILL_EXPRESSION;
}

namespace
{

/** Storages can rely that filters that for storage will be available for analysis before
  * getQueryProcessingStage method will be called.
  *
  * StorageDistributed skip unused shards optimization relies on this.
  * Parallel replicas estimation relies on this too.
  * StorageMerge common header calculation relies on this too.
  *
  * To collect filters that will be applied to specific table in case we have JOINs requires
  * to run query plan optimization pipeline.
  *
  * Algorithm:
  * 1. Replace all table expressions in query tree with dummy tables.
  * 2. Build query plan.
  * 3. Optimize query plan.
  * 4. Extract filters from ReadFromDummy query plan steps from query plan leaf nodes.
  */

FiltersForTableExpressionMap collectFiltersForAnalysis(const QueryTreeNodePtr & query_tree, const TableExpressionNodes & table_nodes, const ContextPtr & query_context, const ActionsDAG * post_filter)
{
    bool collect_filters = false;

    /// `table_nodes` also holds the nested query and union scopes, and the settings may be set only in one
    /// of them, so the top-level context alone is not enough to tell whether the estimation will run.
    const bool parallel_replicas_estimation_enabled = std::ranges::any_of(table_nodes, [](const TableExpressionNodePtr & scope)
    {
        const auto * scope_query = scope->as<QueryNode>();
        const auto * scope_union = scope->as<UnionNode>();
        if (!scope_query && !scope_union)
            return false;

        const auto & scope_context = scope_query ? scope_query->getContext() : scope_union->getContext();
        return scope_context->canUseParallelReplicasOnInitiator()
            && scope_context->getSettingsRef()[Setting::parallel_replicas_min_number_of_rows_per_replica] > 0
            && !scope_context->isAutomaticParallelReplicasProbe();
    });

    auto storage_requires_filter_collection = [parallel_replicas_estimation_enabled](const StoragePtr & storage_ptr)
    {
        const auto * raw = storage_ptr.get();
        if (typeid_cast<const StorageDistributed *>(raw))
            return true;
        if (parallel_replicas_estimation_enabled && castStorage<MergeTreeData>(storage_ptr, DeferredTable::Load))
            return true;
        /// Every cluster engine hands paths out to replicas through `getTaskIteratorExtension`, which
        /// prunes them with this predicate. The initiator's plan for such a read stops at
        /// `WithMergeableState` (`IStorageCluster::getQueryProcessingStage`), so there is no `Filter` step
        /// above `ReadFromCluster` to collect the `WHERE` from - without this the predicate is null and
        /// no `_path` / `_file` pruning happens at all.
        if (dynamic_cast<const IStorageCluster *>(raw))
            return true;
        if (typeid_cast<const StorageView *>(raw))
            return true;
        return false;
    };

    for (const auto & table_expression : table_nodes)
    {
        auto * table_node = table_expression->as<TableNode>();
        auto * table_function_node = table_expression->as<TableFunctionNode>();
        if (!table_node && !table_function_node)
            continue;

        const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
        if (storage_requires_filter_collection(storage))
        {
            collect_filters = true;
            break;
        }
        /// `Merge` is itself agnostic to shard skipping, but if any of the underlying
        /// tables would itself trigger filter collection at the top level
        /// (`Distributed`, `View`, `ObjectStorageCluster`, ...), the predicate from
        /// above the `Merge` must still reach those storages via `query_info`.
        /// The dummy-plan trick captures the WHERE clause for the `Merge` table
        /// expression, which `StorageMerge` then forwards to each child storage.
        if (const auto * storage_merge = typeid_cast<const StorageMerge *>(storage.get()))
        {
            if (storage_merge->hasChildTable(storage_requires_filter_collection))
            {
                collect_filters = true;
                break;
            }
        }
    }

    if (!collect_filters)
        return {};

    ResultReplacementMap replacement_map;

    auto updated_query_tree = replaceTableExpressionsWithDummyTables(query_tree, table_nodes, query_context, &replacement_map);
    /// Disable parallel replicas to collect filters, in every (sub)query: each one is planned with its own context,
    /// which the clone still shares with the original tree, and an `IN` subquery keeps its real tables.
    /// A materialized CTE keeps its contexts: the plan built for it here is stored in the handle the query shares and executes.
    auto not_materialized_cte = [](const QueryTreeNodePtr & parent, const QueryTreeNodePtr & child)
    {
        const auto * table_node = parent->as<TableNode>();
        return !table_node || child != table_node->getMaterializedCTESubquery();
    };
    traverseQueryTree(updated_query_tree, not_materialized_cte, [](const QueryTreeNodePtr & node)
    {
        if (auto * query_node = node->as<QueryNode>())
        {
            auto new_context = Context::createCopy(query_node->getMutableContext());
            new_context->setSetting("enable_parallel_replicas", false);
            query_node->getMutableContext() = new_context;
        }
        else if (auto * union_node = node->as<UnionNode>())
        {
            auto new_context = Context::createCopy(union_node->getMutableContext());
            new_context->setSetting("enable_parallel_replicas", false);
            union_node->getMutableContext() = new_context;
        }
    });

    std::unordered_map<const IStorage *, QueryTreeNodePtr> dummy_storage_to_table;

    for (auto & [from_table_expression, dummy_table_expression] : replacement_map)
    {
        auto * dummy_storage = dummy_table_expression->as<TableNode &>().getStorage().get();
        dummy_storage_to_table.emplace(dummy_storage, from_table_expression);
    }

    SelectQueryOptions select_query_options;
    Planner planner(updated_query_tree, select_query_options);
    planner.buildQueryPlanIfNeeded();

    auto & result_query_plan = planner.getQueryPlan();

    /// This part is needed to support "skip unused shards" for the View over Distributed.
    /// When reading through a View, the outer WHERE clause is not visible to the inner query.
    /// Inject it as a FilterStep on top of the dummy plan so that predicate-push-down works.
    if (post_filter)
    {
        const auto & header = *result_query_plan.getCurrentHeader();
        bool all_inputs_present = true;
        for (const auto & input : post_filter->getInputs())
        {
            if (!header.has(input->result_name))
            {
                all_inputs_present = false;
                break;
            }
            if (!input->result_type->equals(*header.getByName(input->result_name).type))
            {
                all_inputs_present = false;
                break;
            }
        }

        if (all_inputs_present)
        {
            auto filter_dag = post_filter->clone();
            // filter_dag.appendInputsForUnusedColumns(header);
            String filter_column_name = filter_dag.getOutputs().at(0)->result_name;
            auto filter_step = std::make_unique<FilterStep>(
                result_query_plan.getCurrentHeader(),
                std::move(filter_dag),
                filter_column_name,
                true /* remove_filter_column */);
            result_query_plan.addStep(std::move(filter_step));
        }
    }

    QueryPlanOptimizationSettings optimization_settings(query_context);
    optimization_settings.build_sets = false; // no need to build sets to collect filters
    optimization_settings.materialize_ctes = false; // no need to materialize CTEs to collect filters
    /// This plan collects pushed-down filters and is never executed
    optimization_settings.make_distributed_plan = false;
    result_query_plan.optimize(optimization_settings);

    FiltersForTableExpressionMap res;

    std::vector<QueryPlan::Node *> nodes_to_process;
    nodes_to_process.push_back(result_query_plan.getRootNode());

    while (!nodes_to_process.empty())
    {
        const auto * node_to_process = nodes_to_process.back();
        nodes_to_process.pop_back();
        nodes_to_process.insert(nodes_to_process.end(), node_to_process->children.begin(), node_to_process->children.end());

        auto * read_from_dummy = typeid_cast<ReadFromDummy *>(node_to_process->step.get());
        if (!read_from_dummy)
            continue;

        if (auto filter_actions = read_from_dummy->detachFilterActionsDAG())
        {
            const auto & table_node = dummy_storage_to_table.at(&read_from_dummy->getStorage());
            res[table_node] = FiltersForTableExpression{filter_actions, read_from_dummy->getPrewhereInfo()};
        }
    }

    return res;
}

struct LimitRangeConditions
{
    ActionsDAG actions;
    std::optional<String> start_column_name;
    std::optional<String> end_column_name;
};

/// Builds the boundary conditions of a `LIMIT` range as one DAG over the columns of `header`, so a
/// subexpression shared by `AFTER` and `UNTIL` is computed once; the DAG keeps only the inputs the
/// conditions read. With `INTERPOLATE`, `FillingStep` writes the interpolated values only into the target's
/// column (see `addWithFillStepIfNeeded`), while the projection expression the
/// alias was computed from keeps default values on the filled rows. A boundary that refers to such an alias
/// is therefore redirected to the alias column, as `Project names` does for the query result.
LimitRangeConditions buildLimitRangeConditions(
    const SharedHeader & header,
    const QueryNode & query_node,
    const PlannerContextPtr & planner_context,
    const NameToNameMap & interpolate_target_column_names)
{
    ActionsDAG actions;
    for (const auto & column : header->getColumnsWithTypeAndName())
        actions.addInput(column);

    const auto correlated_columns_set = query_node.getCorrelatedColumnsSet();
    PlannerActionsVisitor actions_visitor(planner_context, correlated_columns_set);
    auto add_boundary = [&](const QueryTreeNodePtr & boundary_node, const String & description) -> std::optional<String>
    {
        if (!boundary_node)
            return std::nullopt;

        auto [boundary_nodes, correlated_subtrees] = actions_visitor.visit(actions, boundary_node);
        correlated_subtrees.assertEmpty("in " + description + " expression");

        const auto * output = boundary_nodes.at(0);
        if (!output->result_type->canBeUsedInBooleanContext())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "{} expression must be boolean, got {}",
                description,
                output->result_type->getName());
        }

        /// `AFTER` and `UNTIL` with the same expression share one output column.
        if (!actions.tryFindInOutputs(output->result_name))
            actions.getOutputs().push_back(output);
        return output->result_name;
    };

    auto start_column_name = add_boundary(query_node.getLimitAfter(), "LIMIT AFTER");
    auto end_column_name = add_boundary(query_node.getLimitUntil(), "LIMIT UNTIL");

    Names condition_names;
    for (const auto & column_name : {start_column_name, end_column_name})
        if (column_name)
            condition_names.push_back(*column_name);

    if (query_node.hasInterpolate())
    {
        ActionsDAG interpolated_columns(header->getColumnsWithTypeAndName());
        for (const auto & interpolate_node : query_node.getInterpolate()->as<ListNode &>().getNodes())
        {
            const auto & interpolate_node_typed = interpolate_node->as<InterpolateNode &>();
            const auto & alias_column = interpolated_columns.findInOutputs(
                interpolate_target_column_names.at(interpolate_node_typed.getExpressionName()));
            auto expression_name = calculateActionNodeName(interpolate_node_typed.getExpression(), *planner_context);
            interpolated_columns.addOrReplaceInOutputs(interpolated_columns.addAlias(alias_column, expression_name));
        }

        actions = ActionsDAG::merge(std::move(interpolated_columns), std::move(actions));
    }

    actions.removeUnusedActions(condition_names);
    return {std::move(actions), std::move(start_column_name), std::move(end_column_name)};
}

Aggregator::Params getAggregatorParams(const PlannerContextPtr & planner_context,
    const AggregationAnalysisResult & aggregation_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const Settings & settings,
    bool aggregate_descriptions_remove_arguments = false,
    std::optional<UInt64> trivial_group_by_limit = {})
{
    const auto & query_context = planner_context->getQueryContext();

    /// The cache key is computed later from the query plan in setAggregationHashTableCacheKeys
    /// (key == 0 keeps preallocation disabled until the optimization pass stamps the real key).
    /// max_size_to_preallocate is still carried here because it is consumed at runtime.
    const auto stats_collecting_params = StatsCollectingParams(
        /*key_=*/ 0,
        settings[Setting::collect_hash_table_stats_during_aggregation],
        query_context->getServerSettings()[ServerSetting::max_entries_for_hash_table_stats],
        settings[Setting::max_size_to_preallocate_for_aggregation]);

    auto aggregate_descriptions = aggregation_analysis_result.aggregate_descriptions;
    if (aggregate_descriptions_remove_arguments)
    {
        for (auto & aggregate_description : aggregate_descriptions)
            aggregate_description.argument_names.clear();
    }

    auto tmp_data_scope = query_context->getTempDataOnDisk();
    if (tmp_data_scope)
        tmp_data_scope = tmp_data_scope->childScope(/* metrics */{}, settings[Setting::temporary_files_buffer_size], settings[Setting::temporary_files_codec]);
    /// For the trivial `GROUP BY ... LIMIT` shape, cap the aggregation at `LIMIT + OFFSET` keys.
    /// With aggregate functions, enable the shared kept-keys cutoff, which keeps the values of the kept keys
    /// exact under parallel aggregation (see `Aggregator::Params::shared_kept_keys_for_overflow_any`).
    /// External aggregation stays configured as usual: before the kept keys are frozen a spill
    /// abandons the cutoff, and after the freeze a table already rebuilt to the kept keys still
    /// spills and is re-seeded with them (see `Aggregator::Params::SharedKeptKeysControl`), so a
    /// query that needs to spill keeps spilling exactly as without the optimization.
    Aggregator::Params aggregator_params = Aggregator::Params(
        aggregation_analysis_result.aggregation_keys,
        aggregate_descriptions,
        query_analysis_result.aggregate_overflow_row,
        trivial_group_by_limit.value_or(settings[Setting::max_rows_to_group_by]),
        trivial_group_by_limit ? OverflowMode::ANY : settings[Setting::group_by_overflow_mode].value,
        settings[Setting::group_by_two_level_threshold],
        settings[Setting::group_by_two_level_threshold_bytes],
        Aggregator::Params::getMaxBytesBeforeExternalGroupBy(
            settings[Setting::max_bytes_before_external_group_by], settings[Setting::max_bytes_ratio_before_external_group_by]),
        settings[Setting::min_bytes_before_external_group_by],
        settings[Setting::empty_result_for_aggregation_by_empty_set]
            || (settings[Setting::empty_result_for_aggregation_by_constant_keys_on_empty_set]
                && aggregation_analysis_result.aggregation_keys.empty() && aggregation_analysis_result.group_by_with_constant_keys),
        tmp_data_scope,
        getEffectiveMaxThreads(*query_context),
        settings[Setting::min_free_disk_space_for_temporary_data],
        settings[Setting::compile_aggregate_expressions],
        settings[Setting::min_count_to_compile_aggregate_expression],
        settings[Setting::max_block_size],
        settings[Setting::enable_software_prefetch_in_aggregation],
        /* only_merge */ false,
        settings[Setting::optimize_group_by_constant_keys],
        settings[Setting::min_hit_rate_to_use_consecutive_keys_optimization],
        stats_collecting_params,
        settings[Setting::enable_producing_buckets_out_of_order_in_aggregation],
        settings[Setting::serialize_string_in_memory_with_zero_byte],
        settings[Setting::enable_parallel_single_level_merge],
        settings[Setting::enable_parallel_single_level_merge_by_index],
        settings[Setting::enable_packed_string_keys_in_aggregation],
        settings[Setting::enable_fixed_and_packed_string_keys_in_aggregation],
        settings[Setting::enable_adaptive_aggregator],
        settings[Setting::adaptive_aggregator_freeze_threshold],
        settings[Setting::adaptive_aggregator_freeze_threshold_bytes],
        settings[Setting::adaptive_aggregator_disable_thaw],
        settings[Setting::use_aggregation_memory_tracker]);
    aggregator_params.shared_kept_keys_for_overflow_any = trivial_group_by_limit.has_value() && !aggregate_descriptions.empty();

    return aggregator_params;
}

SortDescription getSortDescriptionFromNames(const Names & names)
{
    SortDescription order_descr;
    order_descr.reserve(names.size());

    for (const auto & name : names)
        order_descr.emplace_back(name, 1, 1);

    return order_descr;
}

void applyTopKPushdownToPartialAggregation(
    AggregatingStep & aggregating_step,
    const QueryNode & query_node,
    PlannerExpressionsAnalysisResult & expression_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const Settings & settings)
{
    if (!settings[Setting::enable_group_by_top_k_optimization])
        return;

    /// The distributed planner splits aggregation itself; see the matching gate
    /// in `tryOptimizeGroupByTopK`.
    if (settings[Setting::make_distributed_plan])
        return;

    /// Pruning undercounts `rows_before_limit_at_least` in exact mode.
    if (settings[Setting::exact_rows_before_limit])
        return;

    /// `partial_sorting_limit` is `limit + offset` and is already zero for
    /// DISTINCT, LIMIT WITH TIES, LIMIT BY, ARRAY JOIN and fractional or
    /// negative values (see `QueryAnalysisResult`).
    const UInt64 limit = query_analysis_result.partial_sorting_limit;
    if (limit == 0)
        return;
    if (settings[Setting::query_plan_max_limit_for_top_k_optimization] != 0
        && limit > settings[Setting::query_plan_max_limit_for_top_k_optimization])
        return;

    if (limit > Aggregator::Params::TopKParams::max_k)
    {
        LOG_DEBUG(
            getLogger("Planner"),
            "Skipping GROUP BY top-K optimization: the requested heap size {} is larger than the maximum {}",
            limit, Aggregator::Params::TopKParams::max_k);
        return;
    }

    /// These consume all groups on the initiator, so local pruning would change
    /// their input: HAVING / QUALIFY filter ranked groups back in, window
    /// functions and TOTALS/ROLLUP/CUBE aggregate across all of them.
    if (expression_analysis_result.hasHaving() || expression_analysis_result.hasWindow() || expression_analysis_result.hasQualify())
        return;
    if (query_node.isGroupByWithTotals() || query_analysis_result.aggregation_with_rollup_or_cube_or_grouping_sets)
        return;
    if (query_node.hasInterpolate())
        return;

    /// An arrayJoin in the projection (or ORDER BY expressions) changes row
    /// multiplicity after the aggregation: a group can expand to zero rows, so
    /// the smallest N groups no longer guarantee N result rows and pruning
    /// loses groups the limit still needs.
    const auto & projection_actions = expression_analysis_result.getProjection().projection_actions;
    if (projection_actions && projection_actions->dag.hasArrayJoin())
        return;
    if (expression_analysis_result.hasSort() && expression_analysis_result.getSort().before_order_by_actions->dag.hasArrayJoin())
        return;

    const auto & params = aggregating_step.getParams();
    if (aggregating_step.isGroupingSets() || params.overflow_row || params.max_rows_to_group_by > 0 || params.keys.empty())
        return;

    if (aggregating_step.inOrder())
        return;

    /// ORDER BY must be a leading prefix of the GROUP BY keys, by action name.
    /// Both name sets come from the same `PlannerContext`, so a name match means
    /// the same expression; anything else (a function of a key, a projection
    /// alias reusing the name) produces a different action name and is rejected.
    const auto & sort_description = query_analysis_result.sort_description;
    if (sort_description.empty() || sort_description.size() > params.keys.size())
        return;

    /// The heap ranks the keys as the aggregation produced them, while the sort runs above the projection and the
    /// ORDER BY expressions. Matching names is not enough on its own: require that both DAGs forward the key
    /// untouched, the same invariant `tryOptimizeGroupByTopK` enforces on the plan-level expression chain.
    const auto * before_order_by_dag
        = expression_analysis_result.hasSort() ? &expression_analysis_result.getSort().before_order_by_actions->dag : nullptr;

    VectorWithMemoryTracking<int> directions;
    VectorWithMemoryTracking<int> nulls_directions;
    directions.reserve(sort_description.size());
    nulls_directions.reserve(sort_description.size());

    for (size_t i = 0; i < sort_description.size(); ++i)
    {
        if (sort_description[i].column_name != params.keys[i])
            return;

        if (projection_actions && !isSortKeyPassThrough(projection_actions->dag, params.keys[i]))
            return;

        if (before_order_by_dag && !isSortKeyPassThrough(*before_order_by_dag, params.keys[i]))
            return;

        if (sort_description[i].collator || sort_description[i].with_fill)
            return;

        directions.push_back(sort_description[i].direction);
        nulls_directions.push_back(sort_description[i].nulls_direction);
    }

    aggregating_step.applyTopKOptimization(
        Aggregator::Params::TopKParams{
            .k = limit,
            .directions = std::move(directions),
            .nulls_directions = std::move(nulls_directions),
            .key_columns = sort_description.size(),
            .observation_rows = settings[Setting::group_by_top_k_optimization_observation_rows],
            .shared_boundary = settings[Setting::group_by_top_k_optimization_shared_boundary],
            .threshold_tracker = nullptr,
        });
}

/// Whether the final LimitStep is built with `always_read_till_end`, i.e. it must not cancel its
/// input early. Shared by `addDistinctStep` and `addLimitStep` so the two cannot drift apart.
bool limitAlwaysReadsTillEnd(
    const QueryAnalysisResult & query_analysis_result, const Settings & settings, const QueryNode & query_node)
{
    if (settings[Setting::exact_rows_before_limit])
        return true;

    if (query_node.isGroupByWithTotals())
        return !query_node.hasOrderBy() && !query_node.isShuffle();

    return query_analysis_result.query_has_with_totals_in_any_subquery_in_join_tree;
}

/// LIMIT BY can be pushed into the sorted-stream pipeline before the final merge, so a direct
/// WITH TOTALS query must drain its input even when it has ORDER BY. The final LIMIT has a weaker
/// condition because sorting itself may already consume the full input before LIMIT runs.
bool limitByAlwaysReadsTillEnd(
    const QueryAnalysisResult & query_analysis_result, const Settings & settings, const QueryNode & query_node)
{
    if (settings[Setting::exact_rows_before_limit]
        && (query_node.hasLimit() || query_node.hasLimitAfter() || query_node.hasLimitUntil()))
        return true;

    if (query_node.isGroupByWithTotals())
        return true;

    return query_analysis_result.query_has_with_totals_in_any_subquery_in_join_tree;
}

void addPreliminaryLimitStep(
    QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    bool do_not_skip_offset,
    bool is_shard_limit)
{
    UInt64 limit_offset = query_analysis_result.limit_offset;
    UInt64 limit_length = query_analysis_result.limit_length;
    bool is_limit_length_negative = query_analysis_result.is_limit_length_negative;
    bool is_limit_offset_negative = query_analysis_result.is_limit_offset_negative;

    if (do_not_skip_offset)
    {
        if (limit_length > std::numeric_limits<UInt64>::max() - limit_offset)
            return;

        limit_length += limit_offset;
        limit_offset = 0;
    }

    const auto & query_context = planner_context->getQueryContext();
    const Settings & settings = query_context->getSettingsRef();

    if (!is_limit_length_negative && !is_limit_offset_negative) [[likely]]
    {
        auto limit = std::make_unique<LimitStep>(
            query_plan.getCurrentHeader(), limit_length, limit_offset, settings[Setting::exact_rows_before_limit]);
        if (is_shard_limit)
            limit->markAsShardLimit();
        if (do_not_skip_offset)
            limit->setStepDescription("preliminary LIMIT (with OFFSET)");
        else
            limit->setStepDescription("preliminary LIMIT");
        query_plan.addStep(std::move(limit));
    }
    else if (is_limit_length_negative && is_limit_offset_negative)
    {
        auto limit = std::make_unique<NegativeLimitStep>(query_plan.getCurrentHeader(), limit_length, limit_offset);
        if (is_shard_limit)
            limit->markAsShardLimit();

        query_plan.addStep(std::move(limit));
    }
    else if (is_limit_length_negative && !is_limit_offset_negative)
    {
        auto offset = std::make_unique<OffsetStep>(query_plan.getCurrentHeader(), limit_offset);

        query_plan.addStep(std::move(offset));

        auto limit = std::make_unique<NegativeLimitStep>(query_plan.getCurrentHeader(), limit_length, 0);
        if (is_shard_limit)
            limit->markAsShardLimit();
        query_plan.addStep(std::move(limit));
    }
    else // if (!is_limit_length_negative && is_limit_offset_negative)
    {
        auto offset = std::make_unique<NegativeOffsetStep>(query_plan.getCurrentHeader(), limit_offset);

        query_plan.addStep(std::move(offset));

        auto limit = std::make_unique<LimitStep>(
            query_plan.getCurrentHeader(), limit_length, 0, settings[Setting::exact_rows_before_limit]);
        if (is_shard_limit)
            limit->markAsShardLimit();
        query_plan.addStep(std::move(limit));
    }
}

}

FiltersForTableExpressionMap collectFiltersForAnalysis(const QueryTreeNodePtr & query_tree_node, const SelectQueryOptions & select_query_options, const ActionsDAG * post_filter)
{
    if (select_query_options.only_analyze)
        return {};

    auto * query_node = query_tree_node->as<QueryNode>();
    auto * union_node = query_tree_node->as<UnionNode>();

    if (!query_node && !union_node)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected QUERY or UNION node. Actual {}",
            query_tree_node->formatASTForErrorMessage());

    auto context = query_node ? query_node->getContext() : union_node->getContext();

    auto table_expressions_nodes
        = extractTableExpressions(static_pointer_cast<ITableExpressionNode>(query_tree_node), false /* add_array_join */, true /* recursive */);

    return collectFiltersForAnalysis(query_tree_node, table_expressions_nodes, context, post_filter);
}

void addExpressionStep(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    ActionsAndProjectInputsFlagPtr & expression_actions,
    const CorrelatedSubtrees & correlated_subtrees,
    const SelectQueryOptions & select_query_options,
    std::string_view step_description,
    UsefulSets & useful_sets)
{
    NameSet input_columns_set;
    for (const auto & column : query_plan.getCurrentHeader()->getColumnsWithTypeAndName())
        input_columns_set.insert(column.name);
    for (const auto & correlated_subquery : correlated_subtrees.subqueries)
    {
        for (const auto & identifier : correlated_subquery.correlated_column_identifiers)
        {
            if (!input_columns_set.contains(identifier))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Current query is not supported yet, because can't find correlated column '{}' in current header: {}",
                    identifier,
                    query_plan.getCurrentHeader()->dumpNames());
        }
        buildQueryPlanForCorrelatedSubquery(planner_context, query_plan, correlated_subquery, select_query_options);
    }

    auto actions = std::move(expression_actions->dag);
    if (expression_actions->project_input)
        actions.appendInputsForUnusedColumns(*query_plan.getCurrentHeader());

    auto expression_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(actions));
    appendSetsFromActionsDAG(expression_step->getExpression(), useful_sets);
    expression_step->setStepDescription(String(step_description), step_description.size());
    query_plan.addStep(std::move(expression_step));
}

void addFilterStep(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    FilterAnalysisResult & filter_analysis_result,
    const SelectQueryOptions & select_query_options,
    std::string_view step_description,
    UsefulSets & useful_sets)
{
    for (const auto & correlated_subquery : filter_analysis_result.correlated_subtrees.subqueries)
    {
        buildQueryPlanForCorrelatedSubquery(planner_context, query_plan, correlated_subquery, select_query_options);
    }

    auto actions = std::move(filter_analysis_result.filter_actions->dag);
    if (filter_analysis_result.filter_actions->project_input)
        actions.appendInputsForUnusedColumns(*query_plan.getCurrentHeader());

    auto where_step = std::make_unique<FilterStep>(query_plan.getCurrentHeader(),
        std::move(actions),
        filter_analysis_result.filter_column_name,
        filter_analysis_result.remove_filter_column);
    where_step->setOptimizeConstantColumnsAfterFilter(
        planner_context->getQueryContext()->getSettingsRef()[Setting::optimize_constant_columns_after_filter]);
    appendSetsFromActionsDAG(where_step->getExpression(), useful_sets);
    where_step->setStepDescription(String(step_description), step_description.size());
    query_plan.addStep(std::move(where_step));
}

void addAggregationStep(QueryPlan & query_plan,
    const QueryNode & query_node,
    PlannerExpressionsAnalysisResult & expression_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const Settings & settings,
    std::optional<UInt64> trivial_group_by_limit)
{
    auto aggregation_analysis_result = expression_analysis_result.getAggregation();
    auto aggregator_params = getAggregatorParams(
        planner_context,
        aggregation_analysis_result,
        query_analysis_result,
        settings,
        /*aggregate_descriptions_remove_arguments=*/false,
        trivial_group_by_limit);

    SortDescription sort_description_for_merging;
    SortDescription group_by_sort_description;

    /// With GROUPING SETS, `AggregatingStep::transformPipeline` returns from the grouping-sets branch
    /// before any `AggregatingInOrderTransform` is built, so in-order state here only misleads later
    /// steps: with `enable_memory_bound_merging_of_aggregation_results` a distributed query would take
    /// `MergingAggregatedStep::applyOrder` and then fail in `MergingAggregatedStep::transformPipeline`.
    const bool force_aggregation_in_order
        = settings[Setting::force_aggregation_in_order] && aggregation_analysis_result.grouping_sets_parameters_list.empty();

    if (force_aggregation_in_order)
    {
        group_by_sort_description = getSortDescriptionFromNames(aggregation_analysis_result.aggregation_keys);
        sort_description_for_merging = group_by_sort_description;
    }

    const size_t effective_max_threads = getEffectiveMaxThreads(*planner_context->getQueryContext());
    auto merge_threads = effective_max_threads;
    auto temporary_data_merge_threads = settings[Setting::aggregation_memory_efficient_merge_threads]
        ? static_cast<size_t>(settings[Setting::aggregation_memory_efficient_merge_threads])
        : effective_max_threads;

    bool storage_has_evenly_distributed_read = false;
    const auto & table_expression_node_to_data = planner_context->getTableExpressionNodeToData();

    if (table_expression_node_to_data.size() == 1)
    {
        auto it = table_expression_node_to_data.begin();
        const auto & table_expression_node = it->first;
        if (const auto * table_node = table_expression_node->as<TableNode>())
            storage_has_evenly_distributed_read = table_node->getStorage()->hasEvenlyDistributedRead();
        else if (const auto * table_function_node = table_expression_node->as<TableFunctionNode>())
            storage_has_evenly_distributed_read = table_function_node->getStorageOrThrow()->hasEvenlyDistributedRead();
    }

    auto aggregating_step = std::make_unique<AggregatingStep>(
        query_plan.getCurrentHeader(),
        aggregator_params,
        aggregation_analysis_result.grouping_sets_parameters_list,
        query_analysis_result.aggregate_final,
        settings[Setting::max_block_size],
        settings[Setting::aggregation_in_order_max_block_bytes],
        merge_threads,
        temporary_data_merge_threads,
        storage_has_evenly_distributed_read,
        settings[Setting::group_by_use_nulls],
        std::move(sort_description_for_merging),
        std::move(group_by_sort_description),
        query_analysis_result.aggregation_should_produce_results_in_order_of_bucket_number,
        settings[Setting::enable_memory_bound_merging_of_aggregation_results],
        force_aggregation_in_order);

    if (!query_analysis_result.aggregate_final)
        applyTopKPushdownToPartialAggregation(*aggregating_step, query_node, expression_analysis_result, query_analysis_result, settings);

    query_plan.addStep(std::move(aggregating_step));
}

void addMergingAggregatedStep(QueryPlan & query_plan,
    const AggregationAnalysisResult & aggregation_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const std::unordered_map<String, String> & shard_collapse_duplicate_keys)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    /** There are two modes of distributed aggregation.
      *
      * 1. In different threads read from the remote servers blocks.
      * Save all the blocks in the RAM. Merge blocks.
      * If the aggregation is two-level - parallelize to the number of buckets.
      *
      * 2. In one thread, read blocks from different servers in order.
      * RAM stores only one block from each server.
      * If the aggregation is a two-level aggregation, we consistently merge the blocks of each next level.
      *
      * The second option consumes less memory (up to 256 times less)
      * in the case of two-level aggregation, which is used for large results after GROUP BY,
      * but it can work more slowly.
      */

    const auto & analysis_keys = aggregation_analysis_result.aggregation_keys;

    /// When the query reads from a Distributed table whose duplicate-ALIAS GROUP BY keys the shard collapsed into a
    /// single key before computing two-level bucket numbers (see `buildShardCollapseFanOut`), the initiator must merge
    /// by only the representative key columns. Otherwise the initiator would bucket by more key columns than the shard
    /// did, so equal groups from different shards could land in different two-level buckets and never merge.
    /// `merge_keys` drops the duplicate keys; the dropped columns are reconstructed (aliased to their representative)
    /// after the merge so the output header is unchanged.
    Names merge_keys;
    std::vector<std::pair<String /*duplicate*/, String /*representative*/>> dropped_duplicate_keys;
    if (!shard_collapse_duplicate_keys.empty() && aggregation_analysis_result.grouping_sets_parameters_list.empty())
    {
        const NameSet analysis_keys_set(analysis_keys.begin(), analysis_keys.end());
        merge_keys.reserve(analysis_keys.size());
        for (const auto & key : analysis_keys)
        {
            auto it = shard_collapse_duplicate_keys.find(key);
            /// Drop `key` only when it is a recognized duplicate AND its representative is itself a merge key, so the
            /// representative remains available to reconstruct the dropped column after merging.
            if (it != shard_collapse_duplicate_keys.end() && analysis_keys_set.contains(it->second))
                dropped_duplicate_keys.emplace_back(key, it->second);
            else
                merge_keys.push_back(key);
        }
        if (dropped_duplicate_keys.empty())
            merge_keys = analysis_keys;
    }
    else
    {
        /// A grouping set holding two keys that the shard collapsed onto the same column is rejected: the shard buckets
        /// that set by one key while the initiator buckets it by both, so a single/two-level partial mix splits the same
        /// group across buckets (the wrong-results defect the non-grouping-sets branch fixes by merging on the collapsed
        /// set). Carrying the collapse through the grouping-sets merge machinery is unsupported, so reject rather than
        /// silently return wrong results. Each used key is canonicalized through the collapse map (a duplicate maps to
        /// its representative; any other key maps to itself), and a set is rejected when two of its keys share a
        /// canonical representative -- this covers shapes where the representative itself is absent from the set, e.g.
        /// ((a2, a3)) with a1, a2, a3 all collapsing onto a1. Sets that keep one key each (e.g. ((a1), (a2))) are
        /// unaffected. With both two-level thresholds at 0 no shard can build a bucketed state, so the mismatch is
        /// unreachable and the query keeps working.
        const bool two_level_aggregation_possible = settings[Setting::group_by_two_level_threshold] != 0
            || settings[Setting::group_by_two_level_threshold_bytes] != 0;
        if (!shard_collapse_duplicate_keys.empty() && !aggregation_analysis_result.grouping_sets_parameters_list.empty()
            && two_level_aggregation_possible)
        {
            for (const auto & grouping_set : aggregation_analysis_result.grouping_sets_parameters_list)
            {
                std::unordered_map<String, String> representative_to_used_key;
                for (const auto & key : grouping_set.used_keys)
                {
                    auto it = shard_collapse_duplicate_keys.find(key);
                    const String & representative = it != shard_collapse_duplicate_keys.end() ? it->second : key;
                    auto [pos, inserted] = representative_to_used_key.emplace(representative, key);
                    if (!inserted)
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "GROUP BY GROUPING SETS with duplicate ALIAS columns that expand to the same expression "
                            "({} and {}) over a Distributed table is not supported, as it can produce incorrect results "
                            "under two-level aggregation. Please use distinct expressions in the grouping sets.",
                            pos->second, key);
                }
            }
        }
        merge_keys = analysis_keys;
    }

    const auto & keys = merge_keys;

    /// Drop the duplicate key columns from the input before merging so the merge sees the same key layout the shard
    /// produced (representative keys followed by aggregate states). They are reconstructed right after the merge.
    /// The aggregate columns are unaffected: they are matched by name in the merge, not by position.
    if (!dropped_duplicate_keys.empty())
    {
        NameSet drop_set;
        for (const auto & [duplicate_name, representative_name] : dropped_duplicate_keys)
            drop_set.insert(duplicate_name);

        ActionsDAG drop_dag(query_plan.getCurrentHeader()->getColumnsWithTypeAndName());
        ActionsDAG::NodeRawConstPtrs kept_outputs;
        kept_outputs.reserve(drop_dag.getOutputs().size());
        for (const auto * output : drop_dag.getOutputs())
            if (!drop_set.contains(output->result_name))
                kept_outputs.push_back(output);
        drop_dag.getOutputs() = std::move(kept_outputs);

        auto drop_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(drop_dag));
        drop_step->setStepDescription("Drop collapsed duplicate-ALIAS GROUP BY keys before merge");
        query_plan.addStep(std::move(drop_step));
    }

    /// For count() without parameters try to use just one thread
    /// Typically this will either be a trivial count or a really small number of states
    size_t max_threads = getEffectiveMaxThreads(*query_context);
    if (keys.empty() && aggregation_analysis_result.aggregate_descriptions.size() == 1
        && aggregation_analysis_result.aggregate_descriptions[0].function->getName() == String{"count"}
        && aggregation_analysis_result.grouping_sets_parameters_list.empty())
        max_threads = 1;

    Aggregator::Params params(
        keys,
        aggregation_analysis_result.aggregate_descriptions,
        query_analysis_result.aggregate_overflow_row,
        max_threads,
        settings[Setting::max_block_size],
        settings[Setting::min_hit_rate_to_use_consecutive_keys_optimization],
        settings[Setting::serialize_string_in_memory_with_zero_byte],
        settings[Setting::enable_packed_string_keys_in_aggregation],
        settings[Setting::enable_fixed_and_packed_string_keys_in_aggregation]);

    bool is_remote_storage = false;
    bool parallel_replicas_from_merge_tree = false;

    const auto & table_expression_node_to_data = planner_context->getTableExpressionNodeToData();
    if (table_expression_node_to_data.size() == 1)
    {
        auto it = table_expression_node_to_data.begin();
        is_remote_storage = it->second.isRemote();
        parallel_replicas_from_merge_tree = it->second.isMergeTree() && query_context->canUseParallelReplicasOnInitiator();
    }

    auto merging_aggregated = std::make_unique<MergingAggregatedStep>(
        query_plan.getCurrentHeader(),
        params,
        aggregation_analysis_result.grouping_sets_parameters_list,
        query_analysis_result.aggregate_final,
        /// Grouping sets don't work with distributed_aggregation_memory_efficient enabled (#43989)
        settings[Setting::distributed_aggregation_memory_efficient] && (is_remote_storage || parallel_replicas_from_merge_tree)
            && !query_analysis_result.aggregation_with_rollup_or_cube_or_grouping_sets,
        settings[Setting::aggregation_memory_efficient_merge_threads],
        query_analysis_result.aggregation_should_produce_results_in_order_of_bucket_number,
        settings[Setting::max_block_size],
        settings[Setting::aggregation_in_order_max_block_bytes],
        settings[Setting::enable_memory_bound_merging_of_aggregation_results]);
    query_plan.addStep(std::move(merging_aggregated));

    /// Reconstruct the canonical aggregated layout the rest of the plan expects: all original aggregation keys first,
    /// in their declared order, then the aggregate-state columns. The merge produced [representative keys, aggregate
    /// states] (dropping the duplicate keys); each dropped key is identical to its representative (which the merge
    /// kept), so it is restored as an alias and placed back in its original key position. Emitting the full
    /// [keys..., aggregate states...] layout (rather than appending the duplicates at the end) is required because a
    /// following ROLLUP/CUBE step reads the merged block positionally - the first keys_size columns as the GROUP BY
    /// keys and the remaining columns as aggregate states - and ROLLUP/CUBE reach this path with an empty
    /// grouping_sets_parameters_list, so they are not excluded by the guard above.
    if (!dropped_duplicate_keys.empty())
    {
        std::unordered_map<std::string_view, String> representative_by_duplicate;
        for (const auto & [duplicate_name, representative_name] : dropped_duplicate_keys)
            representative_by_duplicate.emplace(duplicate_name, representative_name);

        ActionsDAG reconstruct_dag(query_plan.getCurrentHeader()->getColumnsWithTypeAndName());
        std::unordered_map<std::string_view, const ActionsDAG::Node *> output_by_name;
        for (const auto * output : reconstruct_dag.getOutputs())
            output_by_name.emplace(output->result_name, output);

        const NameSet analysis_keys_set(analysis_keys.begin(), analysis_keys.end());

        ActionsDAG::NodeRawConstPtrs new_outputs;
        new_outputs.reserve(reconstruct_dag.getOutputs().size() + dropped_duplicate_keys.size());

        /// 1. All original aggregation keys, in their declared order: a representative is kept as-is, a dropped
        ///    duplicate is restored as an alias of its representative.
        for (const auto & key : analysis_keys)
        {
            auto dup_it = representative_by_duplicate.find(key);
            if (dup_it != representative_by_duplicate.end())
            {
                auto rep_it = output_by_name.find(dup_it->second);
                if (rep_it != output_by_name.end())
                    new_outputs.push_back(&reconstruct_dag.addAlias(*rep_it->second, key));
            }
            else
            {
                auto it = output_by_name.find(key);
                if (it != output_by_name.end())
                    new_outputs.push_back(it->second);
            }
        }

        /// 2. The aggregate-state columns (everything in the merge output that is not a GROUP BY key), in order.
        for (const auto * output : reconstruct_dag.getOutputs())
            if (!analysis_keys_set.contains(output->result_name))
                new_outputs.push_back(output);

        reconstruct_dag.getOutputs() = std::move(new_outputs);

        auto reconstruct_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(reconstruct_dag));
        reconstruct_step->setStepDescription("Reconstruct collapsed duplicate-ALIAS GROUP BY keys");
        query_plan.addStep(std::move(reconstruct_step));
    }
}

void addTotalsHavingStep(QueryPlan & query_plan,
    PlannerExpressionsAnalysisResult & expression_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const SelectQueryOptions & select_query_options,
    const QueryNode & query_node,
    UsefulSets & useful_sets)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    auto & aggregation_analysis_result = expression_analysis_result.getAggregation();
    auto & having_analysis_result = expression_analysis_result.getHaving();
    bool need_finalize = !query_node.isGroupByWithRollup() && !query_node.isGroupByWithCube();

    /// `TotalsHavingStep` evaluates `HAVING` itself, so a correlated subquery in `HAVING` has to be
    /// decorrelated into the plan before the step, the same way `addFilterStep` does it.
    /// The decorrelation joins the aggregated stream, which drops the `AggregatedChunkInfo` of its chunks
    /// and mixes the overflow row (the keys not included in `max_rows_to_group_by`) into the ordinary rows,
    /// so it cannot be combined with the overflow row that `TotalsHavingStep` expects.
    if (query_analysis_result.aggregate_overflow_row && having_analysis_result.correlated_subtrees.notEmpty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Correlated subqueries in HAVING are not supported yet with WITH TOTALS, max_rows_to_group_by, "
            "group_by_overflow_mode = 'any' and totals_mode other than 'after_having_exclusive'");

    for (const auto & correlated_subquery : having_analysis_result.correlated_subtrees.subqueries)
        buildQueryPlanForCorrelatedSubquery(planner_context, query_plan, correlated_subquery, select_query_options);

    std::optional<ActionsDAG> actions;
    if (having_analysis_result.filter_actions)
    {
        actions = std::move(having_analysis_result.filter_actions->dag);
        if (having_analysis_result.filter_actions->project_input)
            actions->appendInputsForUnusedColumns(*query_plan.getCurrentHeader());
    }

    auto totals_having_step = std::make_unique<TotalsHavingStep>(
        query_plan.getCurrentHeader(),
        aggregation_analysis_result.aggregate_descriptions,
        query_analysis_result.aggregate_overflow_row,
        std::move(actions),
        having_analysis_result.filter_column_name,
        having_analysis_result.remove_filter_column,
        settings[Setting::totals_mode],
        settings[Setting::totals_auto_threshold],
        need_finalize);

    if (having_analysis_result.filter_actions)
        appendSetsFromActionsDAG(*totals_having_step->getActions(), useful_sets);

    query_plan.addStep(std::move(totals_having_step));
}

void addCubeOrRollupStepIfNeeded(QueryPlan & query_plan,
    const AggregationAnalysisResult & aggregation_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const QueryNode & query_node)
{
    if (!query_node.isGroupByWithCube() && !query_node.isGroupByWithRollup())
        return;

    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    auto aggregator_params = getAggregatorParams(planner_context,
        aggregation_analysis_result,
        query_analysis_result,
        settings,
        true /*aggregate_descriptions_remove_arguments*/);

    if (query_node.isGroupByWithRollup())
    {
        auto rollup_step = std::make_unique<RollupStep>(
            query_plan.getCurrentHeader(), std::move(aggregator_params), true /*final*/, settings[Setting::group_by_use_nulls]);
        query_plan.addStep(std::move(rollup_step));
    }
    else if (query_node.isGroupByWithCube())
    {
        auto cube_step = std::make_unique<CubeStep>(
            query_plan.getCurrentHeader(), std::move(aggregator_params), true /*final*/, settings[Setting::group_by_use_nulls]);
        query_plan.addStep(std::move(cube_step));
    }
}

void addDistinctStep(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const Names & column_names,
    const QueryNode & query_node,
    bool before_order,
    bool pre_distinct)
{
    const Settings & settings = planner_context->getQueryContext()->getSettingsRef();

    UInt64 limit_offset = query_analysis_result.limit_offset;
    UInt64 limit_length = query_analysis_result.limit_length;

    UInt64 limit_hint_for_distinct = 0;

    /** If after this stage of DISTINCT
      * 1. ORDER BY is not executed.
      * 2. There is no LIMIT BY.
      * 3. There is a non-zero LIMIT (a bare OFFSET without a LIMIT still populates limit_offset, but
      *    limit_length + limit_offset would then bound the head by the offset alone and drop the tail
      *    that OFFSET must return).
      * 4. LIMIT is not negative (a negative LIMIT takes rows from the tail, so it cannot bound
      *    the number of distinct rows collected from the head).
      * 5. LIMIT/OFFSET is not fractional (a fraction of the total row count is only resolved after
      *    all rows are read, so it cannot bound the number of distinct rows either).
      * 6. LIMIT is not WITH TIES (the tie suffix of the last row is unbounded).
      * 7. The LIMIT does not read till end: such a LIMIT needs the whole stream, either to count
      *    `exact_rows_before_limit` or to accumulate WITH TOTALS, and an early stop makes both short.
      * Then you can get no more than limit_length + limit_offset of different rows.
      */
    if (!query_node.isShuffle() && (!query_node.hasOrderBy() || !before_order) && !query_node.hasLimitBy()
        && !query_node.hasLimitAfter() && !query_node.hasLimitUntil()
        && limit_length != 0
        && !query_analysis_result.is_limit_length_negative
        && query_analysis_result.fractional_limit == 0 && query_analysis_result.fractional_offset == 0
        && !query_node.isLimitWithTies()
        && !limitAlwaysReadsTillEnd(query_analysis_result, settings, query_node))
    {
        if (limit_length <= std::numeric_limits<UInt64>::max() - limit_offset)
            limit_hint_for_distinct = limit_length + limit_offset;
    }

    auto distinct_step = std::make_unique<DistinctStep>(
        query_plan.getCurrentHeader(),
        DistinctStep::Settings(settings),
        limit_hint_for_distinct,
        column_names,
        pre_distinct);

    if (pre_distinct)
        distinct_step->setStepDescription("Preliminary DISTINCT");
    else
        distinct_step->setStepDescription("DISTINCT");

    /// The `DISTINCT` that runs after the `ORDER BY` sits above the sort in the plan: the sorted order has
    /// to survive it up to the result.
    if (!before_order && (query_node.hasOrderBy() || query_node.isShuffle()))
        distinct_step->preserveInputOrder();

    query_plan.addStep(std::move(distinct_step));
}

void addSortingStep(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context)
{
    const auto & sort_description = query_analysis_result.sort_description;
    const auto & query_context = planner_context->getQueryContext();
    SortingStep::Settings sort_settings(query_context->getSettingsRef());

    auto sorting_step = std::make_unique<SortingStep>(
        query_plan.getCurrentHeader(),
        sort_description,
        query_analysis_result.partial_sorting_limit,
        sort_settings);
    sorting_step->setStepDescription("Sorting for ORDER BY");
    query_plan.addStep(std::move(sorting_step));
}

/// `SHUFFLE`: shuffle the rows. The random choices are made just in time, no random values are stored for the rows.
/// If the result is not final (it is sent from a shard to the initiator), `ShuffleStep` appends an internal key column
/// with sorted random keys, so that the streams of the shards can be merged by `addMergeSortingStep`.
void addShuffleStep(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const PlannerQueryProcessingInfo & query_processing_info)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    std::optional<ShuffleStep::Key> key;
    if (query_processing_info.getToStage() != QueryProcessingStage::Complete)
    {
        const String key_column_name = ShuffleStep::internal_key_column_name;
        if (query_plan.getCurrentHeader()->has(key_column_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The column name `{}` is reserved for `SHUFFLE`", key_column_name);
        key = ShuffleStep::Key{.column_name = key_column_name, .replace = false, .direction = 1};
    }

    auto shuffle_step = std::make_unique<ShuffleStep>(
        query_plan.getCurrentHeader(),
        std::move(key),
        query_analysis_result.partial_sorting_limit,
        settings[Setting::max_block_size]);
    shuffle_step->setStepDescription("Shuffle for SHUFFLE");
    query_plan.addStep(std::move(shuffle_step));
}

/// On a shard, `addShuffleStep` appends the internal key column of `SHUFFLE`, by which the initiator merges the streams
/// of the shards. The expression analysis does not know about this column, so keep it in the output of `actions`
/// that are applied after the shuffle (for example, before `LIMIT BY`).
void preserveShuffleKeyColumn(ActionsDAG & actions, const QueryPlan & query_plan)
{
    const auto * key_column = query_plan.getCurrentHeader()->findByName(String(ShuffleStep::internal_key_column_name));
    if (!key_column)
        return;

    for (const auto * output : actions.getOutputs())
        if (output->result_name == key_column->name)
            return;

    const auto & input = actions.addInput(key_column->name, key_column->type);
    actions.getOutputs().push_back(&input);
}

void addMergeSortingStep(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    std::string_view description)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    const auto & sort_description = query_analysis_result.sort_description;

    SortingStep::Settings sort_settings(settings);

    auto merging_sorted = std::make_unique<SortingStep>(
        query_plan.getCurrentHeader(),
        sort_description,
        sort_settings,
        query_analysis_result.partial_sorting_limit,
        settings[Setting::exact_rows_before_limit]);
    merging_sorted->setStepDescription(String(description), description.size());

    /// Buffer incoming pre-sorted streams to decouple the readers from the merger.
    /// Mirrors the single-node read-in-order case in optimizeReadInOrder.
    /// If a limit is later pushed down into this step, `updateLimit` will turn buffering back off.
    if (query_analysis_result.partial_sorting_limit == 0
        && sort_settings.read_in_order_use_buffering
        && !sort_settings.read_in_order_use_virtual_row_per_block)
        merging_sorted->enableBuffering();

    query_plan.addStep(std::move(merging_sorted));
}

void addWithFillStepIfNeeded(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    SortAnalysisResult & sort_analysis_result,
    const PlannerContextPtr & planner_context,
    const QueryNode & query_node,
    const SelectQueryOptions & select_query_options,
    UsefulSets & useful_sets)
{
    NameSet order_by_column_names;
    /// `FillingStep` derives the columns to fill from the sort description; this only has to know whether
    /// there is anything to fill at all, and that every such column is readable here.
    bool has_fill = false;

    const auto & header = query_plan.getCurrentHeader();

    for (const auto & description : query_analysis_result.sort_description)
    {
        order_by_column_names.insert(description.column_name);
        if (description.with_fill)
        {
            if (!header->findByName(description.column_name))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Filling column {} is not present in the block {}", description.column_name, header->dumpNames());
            has_fill = true;
        }
    }

    if (!has_fill)
        return;

    InterpolateDescriptionPtr interpolate_description;

    if (query_node.hasInterpolate())
    {
        auto & before_interpolate_actions = sort_analysis_result.before_interpolate_actions;
        addExpressionStep(planner_context, query_plan, before_interpolate_actions, CorrelatedSubtrees(), select_query_options, "Before INTERPOLATE", useful_sets);

        ActionsDAG interpolate_actions_dag;
        auto query_plan_columns = query_plan.getCurrentHeader()->getColumnsWithTypeAndName();
        for (auto & query_plan_column : query_plan_columns)
        {
            /// INTERPOLATE actions dag input columns must be non constant
            query_plan_column.column = nullptr;
            interpolate_actions_dag.addInput(query_plan_column);
        }

        auto & interpolate_list_node = query_node.getInterpolate()->as<ListNode &>();
        auto & interpolate_list_nodes = interpolate_list_node.getNodes();

        if (interpolate_list_nodes.empty())
        {
            for (const auto * input_node : interpolate_actions_dag.getInputs())
            {
                if (order_by_column_names.contains(input_node->result_name))
                    continue;

                interpolate_actions_dag.getOutputs().push_back(input_node);
            }
        }
        else
        {
            ActionsDAG rename_dag;
            const auto & target_column_names = sort_analysis_result.interpolate_target_column_names;

            /// `FillingTransform` rejects an ORDER BY column as an INTERPOLATE output by name, which a renamed target column no longer has.
            for (const auto & description : query_analysis_result.sort_description)
                for (const auto & name : {description.alias, description.column_name})
                    if (auto it = target_column_names.find(name); !name.empty() && it != target_column_names.end() && it->second != name)
                        throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                            "Column '{}' is participating in ORDER BY expression and can't be INTERPOLATE output", name);

            /// A later item resolves names against every node of the DAG, so the conversions and outputs are added after all items are visited.
            std::vector<std::tuple<const ActionsDAG::Node *, DataTypePtr, String>> interpolated_columns;

            for (auto & interpolate_node : interpolate_list_nodes)
            {
                auto & interpolate_node_typed = interpolate_node->as<InterpolateNode &>();
                const auto & column_name = target_column_names.at(interpolate_node_typed.getExpressionName());

                /// The expression reads the previous value of its target through a column named by the target name.
                QueryTreeNodePtr interpolate_expression_node = interpolate_node_typed.getInterpolateExpression();
                if (column_name != interpolate_node_typed.getExpressionName())
                {
                    interpolate_expression_node = interpolate_expression_node->clone();
                    auto target_column_node = std::make_shared<ColumnNode>(
                        NameAndTypePair(column_name, interpolate_node_typed.getExpression()->getResultType()),
                        std::static_pointer_cast<ITableExpressionNode>(interpolate_node));
                    replaceColumns(interpolate_expression_node, interpolate_node, {{interpolate_node_typed.getExpressionName(), target_column_node}});
                }

                ColumnNodePtrWithHashSet empty_correlated_columns_set;
                PlannerActionsVisitor planner_actions_visitor(planner_context, empty_correlated_columns_set);
                auto [expression_to_interpolate_expression_nodes, expression_to_interpolate_correlated_subtrees] = planner_actions_visitor.visit(interpolate_actions_dag,
                    interpolate_node_typed.getExpression());
                expression_to_interpolate_correlated_subtrees.assertEmpty("in expression to interpolate");
                if (expression_to_interpolate_expression_nodes.size() != 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expression to interpolate expected to have single action node");

                auto [interpolate_expression_nodes, interpolate_correlated_subtrees] = planner_actions_visitor.visit(interpolate_actions_dag,
                    interpolate_expression_node);
                interpolate_correlated_subtrees.assertEmpty("in interpolate expression");
                if (interpolate_expression_nodes.size() != 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Interpolate expression expected to have single action node");

                const auto * expression_to_interpolate = expression_to_interpolate_expression_nodes[0];
                const auto * interpolate_expression = interpolate_expression_nodes[0];
                interpolated_columns.emplace_back(interpolate_expression, expression_to_interpolate->result_type, column_name);
            }

            for (const auto & [interpolate_expression, target_type, target_name] : interpolated_columns)
            {
                const auto * interpolated_column = interpolate_expression;
                if (!interpolated_column->result_type->equals(*target_type))
                    interpolated_column = &interpolate_actions_dag.addCast(*interpolated_column, target_type, {}, planner_context->getQueryContext());

                interpolate_actions_dag.getOutputs().push_back(&interpolate_actions_dag.addAlias(*interpolated_column, target_name));
            }

            if (!rename_dag.getOutputs().empty())
                interpolate_actions_dag = ActionsDAG::merge(std::move(rename_dag), std::move(interpolate_actions_dag));

            interpolate_actions_dag.removeUnusedActions();
        }

        appendSetsFromActionsDAG(interpolate_actions_dag, useful_sets);

        Aliases empty_aliases;
        interpolate_description = std::make_shared<InterpolateDescription>(std::move(interpolate_actions_dag), empty_aliases);
    }

    const auto & query_context = planner_context->getQueryContext();
    const Settings & settings = query_context->getSettingsRef();
    auto filling_step = std::make_unique<FillingStep>(
        query_plan.getCurrentHeader(),
        query_analysis_result.sort_description,
        interpolate_description,
        settings[Setting::use_with_fill_by_sorting_prefix]);
    query_plan.addStep(std::move(filling_step));
}

void addLimitByStep(
    QueryPlan & query_plan,
    const LimitByAnalysisResult & limit_by_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const QueryNode & query_node,
    bool do_not_skip_offset)
{
    const Settings & settings = planner_context->getQueryContext()->getSettingsRef();
    const bool always_read_till_end = limitByAlwaysReadsTillEnd(query_analysis_result, settings, query_node);

    /// Constness of LIMIT BY limit is validated during query analysis stage
    UInt64 limit_by_length = query_analysis_result.limit_by_length;
    UInt64 limit_by_offset = query_analysis_result.limit_by_offset;
    bool is_limit_negative = query_analysis_result.is_limit_by_length_negative;
    bool is_offset_negative = query_analysis_result.is_limit_by_offset_negative;

    if (do_not_skip_offset)
    {
        if (limit_by_offset > 0)
        {
            if (limit_by_length > std::numeric_limits<UInt64>::max() - limit_by_offset)
                return;

            limit_by_length += limit_by_offset;
            limit_by_offset = 0;
            is_offset_negative = false;
        }
    }

    const auto & column_names = limit_by_analysis_result.limit_by_column_names;

    if (!is_limit_negative && !is_offset_negative) [[likely]]
    {
        /// LIMIT N [OFFSET M] BY cols - standard positive case
        auto step = std::make_unique<LimitByStep>(
            query_plan.getCurrentHeader(), limit_by_length, limit_by_offset, column_names, always_read_till_end);
        query_plan.addStep(std::move(step));
    }
    else if (is_limit_negative && is_offset_negative)
    {
        /// LIMIT -N OFFSET -M BY cols -> NegativeLimitByStep(limit=N, offset=M)
        auto step = std::make_unique<NegativeLimitByStep>(query_plan.getCurrentHeader(), limit_by_length, limit_by_offset, column_names);
        query_plan.addStep(std::move(step));
    }
    else if (is_limit_negative && !is_offset_negative)
    {
        /// LIMIT -N [OFFSET M] BY cols -> up to two steps:
        ///   1. LimitByStep(limit=MAX, offset=M) - skip first M per group (only if M > 0)
        ///   2. NegativeLimitByStep(limit=N, offset=0) - take last N from remainder
        if (limit_by_offset > 0)
        {
            auto step1 = std::make_unique<LimitByStep>(
                query_plan.getCurrentHeader(), std::numeric_limits<UInt64>::max(), limit_by_offset, column_names, always_read_till_end);
            query_plan.addStep(std::move(step1));
        }
        auto step2 = std::make_unique<NegativeLimitByStep>(query_plan.getCurrentHeader(), limit_by_length, 0, column_names);
        query_plan.addStep(std::move(step2));
    }
    else // !is_limit_negative && is_offset_negative
    {
        /// LIMIT N OFFSET -M BY cols -> two steps:
        ///   1. NegativeLimitByStep(limit=MAX, offset=M) - strip last M per group
        ///   2. LimitByStep(limit=N, offset=0) - take first N from remainder
        ///      (N==0 is not a no-op here: it means "keep 0 per group" -> empty result)
        auto step1 = std::make_unique<NegativeLimitByStep>(
            query_plan.getCurrentHeader(), std::numeric_limits<UInt64>::max(), limit_by_offset, column_names);
        query_plan.addStep(std::move(step1));
        auto step2 = std::make_unique<LimitByStep>(
            query_plan.getCurrentHeader(), limit_by_length, 0, column_names, always_read_till_end);
        query_plan.addStep(std::move(step2));
    }
}

void addLimitRangeStep(
    QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const QueryNode & query_node,
    const NameToNameMap & interpolate_target_column_names,
    UsefulSets & useful_sets)
{
    if (query_node.isLimitWithTies())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "LIMIT WITH TIES is not supported with LIMIT AFTER/UNTIL");

    if (query_node.hasOffset())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "OFFSET is not supported together with LIMIT AFTER/UNTIL");

    if (query_analysis_result.is_limit_length_negative
        || query_analysis_result.is_limit_offset_negative
        || query_analysis_result.fractional_limit > 0
        || query_analysis_result.fractional_offset > 0)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Fractional and negative LIMIT/OFFSET are not supported with LIMIT AFTER/UNTIL");
    }

    std::optional<UInt64> limit_length;
    if (query_node.hasLimit())
        limit_length = query_analysis_result.limit_length;

    auto conditions = buildLimitRangeConditions(query_plan.getCurrentHeader(), query_node, planner_context, interpolate_target_column_names);
    appendSetsFromActionsDAG(conditions.actions, useful_sets);

    const auto & query_context = planner_context->getQueryContext();
    const Settings & settings = query_context->getSettingsRef();
    bool always_read_till_end = settings[Setting::exact_rows_before_limit];
    if (query_node.isGroupByWithTotals() && !query_node.hasOrderBy() && !query_node.isShuffle())
        always_read_till_end = true;
    if (!query_node.isGroupByWithTotals() && query_analysis_result.query_has_with_totals_in_any_subquery_in_join_tree)
        always_read_till_end = true;

    auto limit_range_step = std::make_unique<LimitRangeStep>(
        query_plan.getCurrentHeader(),
        std::move(conditions.actions),
        std::move(conditions.start_column_name),
        std::move(conditions.end_column_name),
        query_node.isLimitAfterAll(),
        limit_length,
        always_read_till_end);
    limit_range_step->setStepDescription("LIMIT range (AFTER/UNTIL)");
    query_plan.addStep(std::move(limit_range_step));
}

bool addPreliminaryLimitOptimizationStepIfNeeded(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const PlannerQueryProcessingInfo & query_processing_info,
    const QueryTreeNodePtr & query_tree,
    const SelectQueryOptions & select_query_options)
{
    const auto & query_node = query_tree->as<QueryNode &>();
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();
    const auto & sort_description = query_analysis_result.sort_description;

    bool has_withfill = false;

    for (const auto & desc : sort_description)
    {
        if (desc.with_fill)
        {
            has_withfill = true;
            break;
        }
    }

    /// Note: Preliminary Limits mustn't be added if there is a fractional limit/offset
    /// because in order to correctly calculate the number of rows to be produced
    /// based on the given fraction the final limit/offset processor must count the entire dataset.
    /// For example, LIMIT 0.1 and 30 rows in the sources - we must read all 30 rows to calculate that rows_cnt * 0.1 = 3.

    bool apply_limit = query_processing_info.getToStage() != QueryProcessingStage::WithMergeableStateAfterAggregation;
    bool apply_prelimit = apply_limit && query_node.hasLimit() && !query_node.isLimitWithTies() && !query_node.isGroupByWithTotals()
        && !query_node.hasLimitAfter() && !query_node.hasLimitUntil()
        && !query_analysis_result.query_has_with_totals_in_any_subquery_in_join_tree
        && !query_analysis_result.query_has_array_join_in_join_tree
        && query_analysis_result.fractional_limit == 0
        && query_analysis_result.fractional_offset == 0
        && !query_node.isDistinct() && !query_node.hasLimitBy()
        && !settings[Setting::extremes] && !has_withfill;

    /// `isToAggregationState` covers both `WithMergeableStateAfterAggregation` (stage 3) and
    /// `WithMergeableStateAfterAggregationAndLimit` (stage 4). OFFSET must not be applied at
    /// either stage because OFFSET means skipping rows from the entire query result, not from each
    /// shard individually.
    bool apply_offset = !query_processing_info.isToAggregationState();
    if (apply_prelimit)
    {
        addPreliminaryLimitStep(
            query_plan,
            query_analysis_result,
            planner_context,
            /* do_not_skip_offset= */ !apply_offset,
            select_query_options.is_local_shard_plan);
        return true;
    }

    return false;
}

/** For distributed query processing, add preliminary sort or distinct or limit
  * for first stage of query processing on shard, if there is no GROUP BY, HAVING,
  * WINDOW functions.
  */
void addPreliminarySortOrDistinctOrLimitStepsIfNeeded(
    QueryPlan & query_plan,
    PlannerExpressionsAnalysisResult & expressions_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const PlannerQueryProcessingInfo & query_processing_info,
    const QueryTreeNodePtr & query_tree,
    const SelectQueryOptions & select_query_options,
    UsefulSets & useful_sets)
{
    const auto & query_node = query_tree->as<QueryNode &>();

    if (query_processing_info.isSecondStage() ||
        expressions_analysis_result.hasAggregation() ||
        expressions_analysis_result.hasHaving() ||
        expressions_analysis_result.hasWindow())
        return;

    if (expressions_analysis_result.hasSort())
        addSortingStep(query_plan, query_analysis_result, planner_context);
    else if (query_node.isShuffle() && query_processing_info.isIntermediateStage())
        /// With nested `Distributed` tables, the incoming streams are already shuffled by the shards below
        /// and sorted by the internal key column. Merging them by this key keeps the result uniformly shuffled
        /// and lets the initiator merge the streams of this server in the same way.
        addMergeSortingStep(query_plan, query_analysis_result, planner_context, "Merge shuffled streams for SHUFFLE");
    else if (query_node.isShuffle())
        addShuffleStep(query_plan, query_analysis_result, planner_context, query_processing_info);

    /// Keep every candidate for the global `DISTINCT` followed by a fresh `SHUFFLE` on the initiator.
    if (query_node.isShuffle() && query_node.isDistinct())
        return;

    /** For DISTINCT step, pre_distinct = false, because if we have limit and distinct,
      * we need to merge streams to one and calculate overall distinct.
      * Otherwise we can take several equal values from different streams
      * according to limit and skip some distinct values.
      */
    if (query_node.hasLimit() && query_node.isDistinct())
    {
        addDistinctStep(query_plan,
            query_analysis_result,
            planner_context,
            expressions_analysis_result.getProjection().projection_column_names,
            query_node,
            false /*before_order*/,
            false /*pre_distinct*/);
    }

    if (expressions_analysis_result.hasLimitBy())
    {
        auto & limit_by_analysis_result = expressions_analysis_result.getLimitBy();
        preserveShuffleKeyColumn(limit_by_analysis_result.before_limit_by_actions->dag, query_plan);
        addExpressionStep(
            planner_context,
            query_plan,
            limit_by_analysis_result.before_limit_by_actions,
            {},
            select_query_options,
            "Before LIMIT BY",
            useful_sets);
        /// We don't apply LIMIT BY on remote nodes at all in the old infrastructure.
        /// https://github.com/ClickHouse/ClickHouse/blob/67c1e89d90ef576e62f8b1c68269742a3c6f9b1e/src/Interpreters/InterpreterSelectQuery.cpp#L1697-L1705
        /// Let's be optimistic and only don't skip offset (it will be skipped on the initiator).
        addLimitByStep(
            query_plan,
            limit_by_analysis_result,
            query_analysis_result,
            planner_context,
            query_node,
            true /*do_not_skip_offset*/);
    }

    /// Do not apply PreLimit at first stage for LIMIT BY when the full input is required,
    /// as it may break `rows_before_limit_at_least` during the second stage or drop totals
    /// from a subquery.
    const Settings & settings = planner_context->getQueryContext()->getSettingsRef();

    if (query_node.hasLimitBy()
        && (settings[Setting::exact_rows_before_limit]
            || query_analysis_result.query_has_with_totals_in_any_subquery_in_join_tree))
    {
        return;
    }


    /// Note: Preliminary Limits mustn't be added if there is a fractional limit/offset
    /// because in order to correctly calculate the number of rows to be produced
    /// based on the given fraction the final limit/offset processor must count the entire dataset.
    /// For example, LIMIT 0.1 and 30 rows in the sources - we must read all 30 rows to calculate that rows_cnt * 0.1 = 3.

    /// WITH TIES simply not supported properly for preliminary steps, so let's disable it.
    if (query_node.hasLimit() && !query_node.hasLimitByOffset() && !query_node.isLimitWithTies()
        && !query_node.hasLimitAfter() && !query_node.hasLimitUntil()
        && query_analysis_result.fractional_limit == 0 && query_analysis_result.fractional_offset == 0)
    {
        addPreliminaryLimitStep(
            query_plan,
            query_analysis_result,
            planner_context,
            /* do_not_skip_offset= */ true,
            select_query_options.is_local_shard_plan);
    }
}

void addWindowSteps(QueryPlan & query_plan,
    const PlannerContextPtr & planner_context,
    WindowAnalysisResult & window_analysis_result,
    size_t max_step_description_length)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    auto & window_descriptions = window_analysis_result.window_descriptions;
    sortWindowDescriptions(window_descriptions);

    size_t window_descriptions_size = window_descriptions.size();

    for (size_t i = 0; i < window_descriptions_size; ++i)
    {
        const auto & window_description = window_descriptions[i];

        /** We don't need to sort again if the input from previous window already
          * has suitable sorting. Also don't create sort steps when there are no
          * columns to sort by, because the sort nodes are confused by this. It
          * happens in case of `over ()`.
          * Even if full_sort_description of both windows match, in case of different
          * partitioning we need to add a SortingStep to reshuffle data in the streams.
          */

        bool need_sort = !window_description.full_sort_description.empty();
        if (need_sort && i != 0)
        {
            const size_t effective_max_threads = getEffectiveMaxThreads(*query_context);
            need_sort = !sortDescriptionIsPrefix(window_description.full_sort_description, window_descriptions[i - 1].full_sort_description)
                || (effective_max_threads != 1 && window_description.partition_by.size() != window_descriptions[i - 1].partition_by.size());
        }
        SortingStep::Settings sort_settings(query_context->getSettingsRef());

        /// Window functions require fully sorted input. Applying sort_overflow_mode = 'break'
        /// would produce incomplete data and cause the pipeline to get stuck.
        sort_settings.size_limits.overflow_mode = OverflowMode::THROW;

        if (need_sort)
        {
            /// The sort scatters rows across threads by the hash of the partition columns it is given,
            /// but `WindowTransform` finds partition boundaries with `compareAt`. For key types where
            /// the two disagree the scatter would split one logical partition across threads, so such
            /// windows sort in a single merged stream instead.
            SortDescription scatter_partition_by = window_description.partition_by;
            const auto & sort_input_header = query_plan.getCurrentHeader();
            for (const auto & partition_column : window_description.partition_by)
            {
                if (QueryPlanOptimizations::keyTypeBreaksHashSharding(
                        *sort_input_header->getByName(partition_column.column_name).type))
                {
                    scatter_partition_by.clear();
                    break;
                }
            }

            auto sorting_step = std::make_unique<SortingStep>(
                query_plan.getCurrentHeader(),
                window_description.full_sort_description,
                scatter_partition_by,
                0 /*limit*/,
                sort_settings);
            sorting_step->setStepDescription("Sorting for window '" + window_description.window_name + "'", max_step_description_length);
            sorting_step->setWindowSorting();
            query_plan.addStep(std::move(sorting_step));
        }

        // Fan out streams only for the last window to preserve the ordering between windows,
        // and WindowTransform works on single stream anyway.
        const bool streams_fan_out
            = settings[Setting::query_plan_enable_multithreading_after_window_functions] && ((i + 1) == window_descriptions_size);

        auto window_step
            = std::make_unique<WindowStep>(query_plan.getCurrentHeader(), window_description, window_description.window_functions, streams_fan_out, sort_settings);
        window_step->setStepDescription("Window step for window '" + window_description.window_name + "'", max_step_description_length);
        query_plan.addStep(std::move(window_step));
    }
}

void addLimitStep(
    QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const QueryNode & query_node)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();
    /** Special cases handled by the predicate:
      *
      * 1. If there is WITH TOTALS and there is no ORDER BY, then read the data to the end,
      *  otherwise TOTALS is counted according to incomplete data.
      *
      * 2. If there is no WITH TOTALS and there is a subquery in FROM, and there is WITH TOTALS on one of the levels,
      *  then when using LIMIT, you should read the data to the end, rather than cancel the query earlier,
      *  because if you cancel the query, we will not get `totals` data from the remote server.
      */
    bool always_read_till_end = limitAlwaysReadsTillEnd(query_analysis_result, settings, query_node);
    bool limit_with_ties = query_node.isLimitWithTies();

    SortDescription limit_with_ties_sort_description;

    if (query_node.isLimitWithTies())
    {
        /// Validated during parser stage
        if (!query_node.hasOrderBy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "LIMIT WITH TIES without ORDER BY");

        limit_with_ties_sort_description = query_analysis_result.sort_description;
    }

    UInt64 limit_length = query_analysis_result.limit_length;
    UInt64 limit_offset = query_analysis_result.limit_offset;
    Float64 fractional_limit = query_analysis_result.fractional_limit;
    Float64 fractional_offset = query_analysis_result.fractional_offset;
    bool is_limit_length_negative = query_analysis_result.is_limit_length_negative;
    bool is_limit_offset_negative = query_analysis_result.is_limit_offset_negative;

    if (is_limit_length_negative && fractional_offset > 0)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Negative LIMIT is not supported with fractional OFFSET yet");

    if (is_limit_offset_negative && fractional_limit > 0)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Fractional LIMIT is not supported with negative OFFSET yet");

    /// only one of limit_length or fractional_limit should have a value not both
    if (limit_length && fractional_limit > 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LIMIT can't have both absolute and fractional values non-zero");
    /// only one of limit_offset or fractional_offset should have a value not both
    if (limit_offset && fractional_offset > 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "OFFSET can't have both absolute and fractional values non-zero");

    /// if fractional_limit has value is_limit_length_negative should be always false
    if (fractional_limit > 0 && is_limit_length_negative)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LIMIT can't have both negative and fractional values non-zero");
    /// if fractional_offset has value is_limit_offset_negative should be always false
    if (fractional_offset > 0 && is_limit_offset_negative)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "OFFSET can't have both negative and fractional values non-zero");

    if (!is_limit_length_negative && !is_limit_offset_negative
            && fractional_offset == 0 && fractional_limit == 0) [[likely]]
    {
        auto limit = std::make_unique<LimitStep>(
            query_plan.getCurrentHeader(),
            limit_length,
            limit_offset,
            always_read_till_end,
            limit_with_ties,
            limit_with_ties_sort_description);

        if (limit_with_ties)
            limit->setStepDescription("LIMIT WITH TIES");

        query_plan.addStep(std::move(limit));
    }
    else if (is_limit_length_negative && is_limit_offset_negative)
    {
        auto limit = std::make_unique<NegativeLimitStep>(
            query_plan.getCurrentHeader(), limit_length, limit_offset, limit_with_ties, limit_with_ties_sort_description);

        if (limit_with_ties)
            limit->setStepDescription("NEGATIVE LIMIT WITH TIES");

        query_plan.addStep(std::move(limit));
    }
    else if (is_limit_length_negative && !is_limit_offset_negative)
    {
        auto offset = std::make_unique<OffsetStep>(query_plan.getCurrentHeader(), limit_offset);

        query_plan.addStep(std::move(offset));

        auto limit = std::make_unique<NegativeLimitStep>(
            query_plan.getCurrentHeader(), limit_length, 0, limit_with_ties, limit_with_ties_sort_description);

        if (limit_with_ties)
            limit->setStepDescription("NEGATIVE LIMIT WITH TIES");

        query_plan.addStep(std::move(limit));
    }
    else if (!is_limit_length_negative && is_limit_offset_negative)
    {
        auto offset = std::make_unique<NegativeOffsetStep>(query_plan.getCurrentHeader(), limit_offset);

        query_plan.addStep(std::move(offset));

        auto limit = std::make_unique<LimitStep>(
            query_plan.getCurrentHeader(), limit_length, 0, always_read_till_end, limit_with_ties, limit_with_ties_sort_description);
        if (limit_with_ties)
            limit->setStepDescription("LIMIT WITH TIES");
        query_plan.addStep(std::move(limit));
    }
    else if (limit_length && fractional_offset > 0)
    {
        auto fractional_offset_step
            = std::make_unique<FractionalOffsetStep>(query_plan.getCurrentHeader(), query_analysis_result.fractional_offset);
        query_plan.addStep(std::move(fractional_offset_step));

        auto limit = std::make_unique<LimitStep>(
            query_plan.getCurrentHeader(),
            limit_length,
            0,
            always_read_till_end,
            limit_with_ties,
            limit_with_ties_sort_description);
        query_plan.addStep(std::move(limit));
    }
    else
    {
        // Else its fractional limit + fractional offset or normal offset
        auto fractional_limit_step = std::make_unique<FractionalLimitStep>(
            query_plan.getCurrentHeader(),
            fractional_limit,
            fractional_offset,
            limit_offset,
            limit_with_ties,
            limit_with_ties_sort_description);
        query_plan.addStep(std::move(fractional_limit_step));
    }
}

void addExtremesStepIfNeeded(QueryPlan & query_plan, const PlannerContextPtr & planner_context)
{
    const auto & query_context = planner_context->getQueryContext();
    if (!query_context->getSettingsRef()[Setting::extremes])
        return;

    auto extremes_step = std::make_unique<ExtremesStep>(query_plan.getCurrentHeader());
    query_plan.addStep(std::move(extremes_step));
}

void addOffsetStep(QueryPlan & query_plan, const QueryAnalysisResult & query_analysis_result)
{
    /// If there is not a LIMIT or but an OFFSET
    if ((!query_analysis_result.limit_length && query_analysis_result.fractional_limit == 0)
        && (query_analysis_result.limit_offset || query_analysis_result.fractional_offset > 0))
    {
        // only one of limit_offset or fractional_offset should have a value not both
        if (query_analysis_result.limit_offset && query_analysis_result.fractional_offset > 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "OFFSET can't have both absolute and fractional values non-zero");

        // negative fractional offset is not supported yet
        if (query_analysis_result.is_limit_offset_negative && query_analysis_result.fractional_offset > 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "OFFSET can't have both negative and fractional values non-zero");

        if (query_analysis_result.is_limit_offset_negative) [[unlikely]]
        {
            auto offset_step = std::make_unique<NegativeOffsetStep>(query_plan.getCurrentHeader(), query_analysis_result.limit_offset);
            query_plan.addStep(std::move(offset_step));
        }
        else if (query_analysis_result.limit_offset) [[likely]]
        {
            auto offset_step = std::make_unique<OffsetStep>(query_plan.getCurrentHeader(), query_analysis_result.limit_offset);
            query_plan.addStep(std::move(offset_step));
        }
        else [[unlikely]]
        {
            auto fractional_offset_step
                = std::make_unique<FractionalOffsetStep>(query_plan.getCurrentHeader(), query_analysis_result.fractional_offset);
            query_plan.addStep(std::move(fractional_offset_step));
        }
    }
}

void addBuildSubqueriesForSetsStepIfNeeded(
    QueryPlan & query_plan,
    const SelectQueryOptions & select_query_options,
    const PlannerContextPtr & planner_context,
    const UsefulSets & useful_sets)
{
    auto subqueries = planner_context->getPreparedSets().getSubqueries();

    auto predicate = [&useful_sets](const auto & set) { return !useful_sets.contains(set); };
    auto it = std::remove_if(subqueries.begin(), subqueries.end(), std::move(predicate));
    subqueries.erase(it, subqueries.end());

    for (auto & subquery : subqueries)
    {
        auto query_tree = subquery->detachQueryTree();
        auto subquery_options = select_query_options.subquery();
        /// Sets may use Materialized CTEs, so we need to materialize them in order to correctly build set from subquery.
        /// Normally CTEs are materialized before building sets and running the main query, but in case when
        /// set is built for primary key analysis, CTEs are not materialized yet.
        ///
        /// To build the set correctly, we need to add a DelayedMaterializingCTEsStep for CTEs in the set subquery plan.
        /// This is done by forceMaterializeCTE() method, which would lead collectMaterializedCTEs() to return non-empty result.
        ///
        /// As a result:
        /// 1. If set is built during primary key analysis FutureSetFromSubquery::buildSetInplace(), we will build plans for used CTEs materialization.
        ///    Later, when the main query plan is optimized, DelayedMaterializingCTEsStep for these CTEs will not add materialization plans again.
        /// 2. If set is built during main query execution, we will build plans for used CTEs materialization to be run before the set is built
        ///    and before the main query is executed.
        subquery_options.forceMaterializeCTE();
        /// I don't know if this is a good decision,
        /// but for now it is done in the same way as in old analyzer.
        /// This would not ignore limits for subqueries (affects mutations only).
        /// See test_build_sets_from_multiple_threads-analyzer.
        subquery_options.ignore_limits = false;
        Planner subquery_planner(
            query_tree,
            subquery_options,
            std::make_shared<GlobalPlannerContext>(
                nullptr, nullptr, nullptr, collectFiltersForAnalysis(query_tree, subquery_options, nullptr)));
        subquery_planner.buildQueryPlanIfNeeded();

        auto subquery_plan = std::move(subquery_planner).extractQueryPlan();
        /// Contexts should be copied into the root query plan, because some functions may
        /// be created using them while this subquery plan will be destroyed after
        /// FutureSetFromSubquery::buildSetInplace(). Otherwise, function execution may fail
        /// with a "Context has expired" exception. The set source is not united into this plan,
        /// so its decision contexts are copied the same way.
        query_plan.takeContextsFrom(subquery_plan);
        subquery->setQueryPlan(std::make_unique<QueryPlan>(std::move(subquery_plan)));
    }

    if (!subqueries.empty())
    {
        const auto & settings = planner_context->getQueryContext()->getSettingsRef();
        SizeLimits network_transfer_limits(settings[Setting::max_rows_to_transfer], settings[Setting::max_bytes_to_transfer], settings[Setting::transfer_overflow_mode]);
        auto prepared_sets_cache = planner_context->getQueryContext()->getPreparedSetsCache();

        auto step = std::make_unique<DelayedCreatingSetsStep>(
            query_plan.getCurrentHeader(),
            std::move(subqueries),
            network_transfer_limits,
            prepared_sets_cache);
        step->setStepDescription("DelayedCreatingSetsStep");
        query_plan.addStep(std::move(step));
    }
}

/// Support for `additional_result_filter` setting
void addAdditionalFilterStepIfNeeded(QueryPlan & query_plan,
    const QueryNode & query_node,
    const SelectQueryOptions & select_query_options,
    PlannerContextPtr & planner_context,
    UsefulSets & useful_sets
)
{
    if (select_query_options.subquery_depth != 0)
        return;

    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    auto additional_result_filter_ast = parseAdditionalResultFilter(settings);
    if (!additional_result_filter_ast)
        return;

    ColumnsDescription fake_column_descriptions;
    NameSet fake_name_set;
    for (const auto & column : query_node.getProjectionColumns())
    {
        fake_column_descriptions.add(ColumnDescription(column.name, column.type));
        fake_name_set.emplace(column.name);
    }

    auto storage = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, fake_column_descriptions);
    auto fake_table_expression = std::make_shared<TableNode>(std::move(storage), query_context);

    /// Each call to collectSourceColumns will register column identifiers in the shared GlobalPlannerContext.
    /// When multiple UNION branches share the same GlobalPlannerContext and each applies additional_result_filter,
    /// the bare column names (e.g. "a") would collide. Give the fake table expression a unique alias
    /// so that identifiers become unique (e.g. "_additional_result_filter_0.a").
    /// Loop until we find an alias that does not collide with any existing identifiers,
    /// so that even a user alias like "_additional_result_filter_0" cannot cause a conflict.
    auto & global_context = planner_context->getGlobalPlannerContext();
    std::string unique_alias;
    while (true)
    {
        unique_alias = "_additional_result_filter_" + std::to_string(global_context->nextUniqueId());
        bool has_collision = false;
        for (const auto & column : query_node.getProjectionColumns())
        {
            if (global_context->hasColumnIdentifier(unique_alias + "." + column.name))
            {
                has_collision = true;
                break;
            }
        }
        if (!has_collision)
            break;
    }
    fake_table_expression->setAlias(unique_alias);

    auto filter_info = buildFilterInfo(additional_result_filter_ast, fake_table_expression, planner_context, std::move(fake_name_set));
    if (!query_plan.isInitialized())
        return;

    auto filter_step = std::make_unique<FilterStep>(query_plan.getCurrentHeader(),
        std::move(filter_info.actions),
        filter_info.column_name,
        filter_info.do_remove_column);
    filter_step->setStepDescription("additional result filter");
    appendSetsFromActionsDAG(filter_step->getExpression(), useful_sets);

    /// The extremes are computed before `LIMIT`, and the result filter, which comes after it, must not filter them either.
    const bool bypass_extremes = settings[Setting::extremes];
    if (bypass_extremes)
        query_plan.addStep(std::make_unique<DetachExtremesStep>(query_plan.getCurrentHeader()));

    query_plan.addStep(std::move(filter_step));

    if (bypass_extremes)
        query_plan.addStep(std::make_unique<AttachExtremesStep>(query_plan.getCurrentHeader()));
}

/// Replace a header that holds nothing but row-count-only columns (or no column at all) with one
/// canonical materialized marker, so that the row count has a column to live in.
void addRowCountMarkerStepIfNeeded(QueryPlan & query_plan, const PlannerContextPtr & planner_context)
{
    ColumnIdentifierSet row_count_only_identifiers;
    for (const auto & [_, table_expression_data] : planner_context->getTableExpressionNodeToData())
    {
        if (const auto & column_identifier = table_expression_data.getRowCountOnlyColumnIdentifier())
            row_count_only_identifiers.insert(*column_identifier);
    }

    if (row_count_only_identifiers.empty())
        return;

    const auto & header = query_plan.getCurrentHeader();
    for (const auto & column : *header)
    {
        if (!row_count_only_identifiers.contains(column.name))
            return;
    }

    ActionsDAG marker_dag(header->getNamesAndTypesList());
    auto marker_type = std::make_shared<DataTypeUInt8>();
    marker_dag.getOutputs()
        = {&marker_dag.materializeNode(marker_dag.addColumn(marker_type->createColumnConst(0, 0u), marker_type, "__row_count_marker"))};

    auto marker_step = std::make_unique<ExpressionStep>(header, std::move(marker_dag));
    marker_step->setStepDescription("Row count marker for zero-column mergeable state");
    query_plan.addStep(std::move(marker_step));
}

}
