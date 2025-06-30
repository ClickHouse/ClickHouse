#include <Planner/Planner.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Core/Names.h>
#include <Core/ProtocolDefines.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Processors/QueryPlan/BlocksMarshallingStep.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <DataTypes/DataTypeString.h>

#include <Functions/FunctionFactory.h>
#include <Functions/CastOverloadResolver.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/ReadFromRecursiveCTEStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/Context.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/StorageID.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageMerge.h>
#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/Utils.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/InterpolateNode.h>
#include <Analyzer/WindowNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/WindowFunctionsUtils.h>

#include <Planner/CollectColumnIdentifiers.h>
#include <Planner/CollectSets.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/findQueryForParallelReplicas.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerAggregation.h>
#include <Planner/PlannerContext.h>
#include <Planner/PlannerCorrelatedSubqueries.h>
#include <Planner/PlannerExpressionAnalysis.h>
#include <Planner/PlannerJoins.h>
#include <Planner/PlannerJoinTree.h>
#include <Planner/PlannerQueryProcessingInfo.h>
#include <Planner/PlannerSorting.h>
#include <Planner/PlannerWindowFunctions.h>
#include <Planner/Utils.h>


namespace ProfileEvents
{
    extern const Event SelectQueriesWithSubqueries;
    extern const Event QueriesWithSubqueries;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 aggregation_in_order_max_block_bytes;
    extern const SettingsUInt64 aggregation_memory_efficient_merge_threads;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool collect_hash_table_stats_during_aggregation;
    extern const SettingsOverflowMode distinct_overflow_mode;
    extern const SettingsBool distributed_aggregation_memory_efficient;
    extern const SettingsBool enable_memory_bound_merging_of_aggregation_results;
    extern const SettingsBool empty_result_for_aggregation_by_constant_keys_on_empty_set;
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
    extern const SettingsBool exact_rows_before_limit;
    extern const SettingsBool extremes;
    extern const SettingsBool force_aggregation_in_order;
    extern const SettingsUInt64 group_by_two_level_threshold;
    extern const SettingsUInt64 group_by_two_level_threshold_bytes;
    extern const SettingsBool group_by_use_nulls;
    extern const SettingsUInt64 max_bytes_in_distinct;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 max_size_to_preallocate_for_aggregation;
    extern const SettingsUInt64 max_subquery_depth;
    extern const SettingsUInt64 max_rows_in_distinct;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsBool parallel_replicas_allow_in_with_subquery;
    extern const SettingsString parallel_replicas_custom_key;
    extern const SettingsUInt64 parallel_replicas_min_number_of_rows_per_replica;
    extern const SettingsBool query_plan_enable_multithreading_after_window_functions;
    extern const SettingsBool throw_on_unsupported_query_inside_transaction;
    extern const SettingsFloat totals_auto_threshold;
    extern const SettingsTotalsMode totals_mode;
    extern const SettingsBool use_with_fill_by_sorting_prefix;
    extern const SettingsFloat min_hit_rate_to_use_consecutive_keys_optimization;
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const SettingsUInt64 max_bytes_before_external_group_by;
    extern const SettingsDouble max_bytes_ratio_before_external_group_by;
    extern const SettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const SettingsBool compile_aggregate_expressions;
    extern const SettingsUInt64 min_count_to_compile_aggregate_expression;
    extern const SettingsBool enable_software_prefetch_in_aggregation;
    extern const SettingsBool optimize_group_by_constant_keys;
    extern const SettingsUInt64 max_bytes_to_transfer;
    extern const SettingsUInt64 max_rows_to_transfer;
    extern const SettingsOverflowMode transfer_overflow_mode;
    extern const SettingsBool enable_parallel_blocks_marshalling;
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
    extern const int TOO_DEEP_SUBQUERIES;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
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

FiltersForTableExpressionMap collectFiltersForAnalysis(const QueryTreeNodePtr & query_tree, const QueryTreeNodes & table_nodes, const ContextPtr & query_context)
{
    bool collect_filters = false;
    const auto & settings = query_context->getSettingsRef();

    bool parallel_replicas_estimation_enabled
        = query_context->canUseParallelReplicasOnInitiator() && settings[Setting::parallel_replicas_min_number_of_rows_per_replica] > 0;

    for (const auto & table_expression : table_nodes)
    {
        auto * table_node = table_expression->as<TableNode>();
        auto * table_function_node = table_expression->as<TableFunctionNode>();
        if (!table_node && !table_function_node)
            continue;

        const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
        if (typeid_cast<const StorageDistributed *>(storage.get())
            || (parallel_replicas_estimation_enabled && std::dynamic_pointer_cast<MergeTreeData>(storage)))
        {
            collect_filters = true;
            break;
        }
        if (typeid_cast<const StorageObjectStorageCluster *>(storage.get()))
        {
            collect_filters = true;
            break;
        }
    }

    if (!collect_filters)
        return {};

    ResultReplacementMap replacement_map;

    auto updated_query_tree = replaceTableExpressionsWithDummyTables(query_tree, table_nodes, query_context, &replacement_map);

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

    QueryPlanOptimizationSettings optimization_settings(query_context);
    optimization_settings.build_sets = false; // no need to build sets to collect filters
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
            res[table_node] = FiltersForTableExpression{std::move(filter_actions), read_from_dummy->getPrewhereInfo()};
        }
    }

    return res;
}

FiltersForTableExpressionMap collectFiltersForAnalysis(const QueryTreeNodePtr & query_tree_node, const SelectQueryOptions & select_query_options)
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
        = extractTableExpressions(query_tree_node, false /* add_array_join */, true /* recursive */);

    return collectFiltersForAnalysis(query_tree_node, table_expressions_nodes, context);
}

/// Extend lifetime of query context, storages, and table locks
void extendQueryContextAndStoragesLifetime(QueryPlan & query_plan, const PlannerContextPtr & planner_context)
{
    query_plan.addInterpreterContext(planner_context->getQueryContext());

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

class QueryAnalysisResult
{
public:
    QueryAnalysisResult(const QueryTreeNodePtr & query_tree,
        const PlannerQueryProcessingInfo & query_processing_info,
        const PlannerContextPtr & planner_context)
    {
        const auto & query_node = query_tree->as<QueryNode &>();
        const auto & query_context = planner_context->getQueryContext();
        const auto & settings = query_context->getSettingsRef();

        aggregate_overflow_row = query_node.isGroupByWithTotals() && settings[Setting::max_rows_to_group_by]
            && settings[Setting::group_by_overflow_mode] == OverflowMode::ANY && settings[Setting::totals_mode] != TotalsMode::AFTER_HAVING_EXCLUSIVE;
        aggregate_final = query_processing_info.getToStage() > QueryProcessingStage::WithMergeableState
            && !query_node.isGroupByWithTotals() && !query_node.isGroupByWithRollup() && !query_node.isGroupByWithCube();
        aggregation_with_rollup_or_cube_or_grouping_sets = query_node.isGroupByWithRollup() || query_node.isGroupByWithCube() ||
            query_node.isGroupByWithGroupingSets();
        aggregation_should_produce_results_in_order_of_bucket_number
            = query_processing_info.getToStage() == QueryProcessingStage::WithMergeableState
            && (settings[Setting::distributed_aggregation_memory_efficient] || settings[Setting::enable_memory_bound_merging_of_aggregation_results]);

        query_has_array_join_in_join_tree = queryHasArrayJoinInJoinTree(query_tree);
        query_has_with_totals_in_any_subquery_in_join_tree = queryHasWithTotalsInAnySubqueryInJoinTree(query_tree);

        sort_description = extractSortDescription(query_node.getOrderByNode(), *planner_context);

        if (query_node.hasLimit())
        {
            /// Constness of limit is validated during query analysis stage
            limit_length = query_node.getLimit()->as<ConstantNode &>().getValue().safeGet<UInt64>();

            if (query_node.hasOffset() && limit_length)
            {
                /// Constness of offset is validated during query analysis stage
                limit_offset = query_node.getOffset()->as<ConstantNode &>().getValue().safeGet<UInt64>();
            }
        }
        else if (query_node.hasOffset())
        {
            /// Constness of offset is validated during query analysis stage
            limit_offset = query_node.getOffset()->as<ConstantNode &>().getValue().safeGet<UInt64>();
        }

        /// Partial sort can be done if there is LIMIT, but no DISTINCT, LIMIT WITH TIES, LIMIT BY, ARRAY JOIN
        if (limit_length != 0 &&
            !query_node.isDistinct() &&
            !query_node.isLimitWithTies() &&
            !query_node.hasLimitBy() &&
            !query_has_array_join_in_join_tree &&
            limit_length <= std::numeric_limits<UInt64>::max() - limit_offset)
        {
            partial_sorting_limit = limit_length + limit_offset;
        }
    }

    bool aggregate_overflow_row = false;
    bool aggregate_final = false;
    bool aggregation_with_rollup_or_cube_or_grouping_sets = false;
    bool aggregation_should_produce_results_in_order_of_bucket_number = false;
    bool query_has_array_join_in_join_tree = false;
    bool query_has_with_totals_in_any_subquery_in_join_tree = false;
    SortDescription sort_description;
    UInt64 limit_length = 0;
    UInt64 limit_offset = 0;
    UInt64 partial_sorting_limit = 0;
};

void addExpressionStep(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    ActionsAndProjectInputsFlagPtr & expression_actions,
    const CorrelatedSubtrees & correlated_subtrees,
    const SelectQueryOptions & select_query_options,
    const std::string & step_description,
    UsefulSets & useful_sets)
{
    NameSet input_columns_set;
    for (const auto & column : query_plan.getCurrentHeader().getColumnsWithTypeAndName())
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
                    query_plan.getCurrentHeader().dumpNames());
        }
        buildQueryPlanForCorrelatedSubquery(planner_context, query_plan, correlated_subquery, select_query_options);
    }

    auto actions = std::move(expression_actions->dag);
    if (expression_actions->project_input)
        actions.appendInputsForUnusedColumns(query_plan.getCurrentHeader());

    auto expression_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(actions));
    appendSetsFromActionsDAG(expression_step->getExpression(), useful_sets);
    expression_step->setStepDescription(step_description);
    query_plan.addStep(std::move(expression_step));
}

void addFilterStep(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    FilterAnalysisResult & filter_analysis_result,
    const SelectQueryOptions & select_query_options,
    const std::string & step_description,
    UsefulSets & useful_sets)
{
    for (const auto & correlated_subquery : filter_analysis_result.correlated_subtrees.subqueries)
    {
        buildQueryPlanForCorrelatedSubquery(planner_context, query_plan, correlated_subquery, select_query_options);
    }

    auto actions = std::move(filter_analysis_result.filter_actions->dag);
    if (filter_analysis_result.filter_actions->project_input)
        actions.appendInputsForUnusedColumns(query_plan.getCurrentHeader());

    auto where_step = std::make_unique<FilterStep>(query_plan.getCurrentHeader(),
        std::move(actions),
        filter_analysis_result.filter_column_name,
        filter_analysis_result.remove_filter_column);
    appendSetsFromActionsDAG(where_step->getExpression(), useful_sets);
    where_step->setStepDescription(step_description);
    query_plan.addStep(std::move(where_step));
}

Aggregator::Params getAggregatorParams(const PlannerContextPtr & planner_context,
    const AggregationAnalysisResult & aggregation_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const SelectQueryInfo & select_query_info,
    bool aggregate_descriptions_remove_arguments = false)
{
    const auto & query_context = planner_context->getQueryContext();
    const Settings & settings = query_context->getSettingsRef();

    const auto stats_collecting_params = StatsCollectingParams(
        calculateCacheKey(select_query_info.query),
        settings[Setting::collect_hash_table_stats_during_aggregation],
        query_context->getServerSettings()[ServerSetting::max_entries_for_hash_table_stats],
        settings[Setting::max_size_to_preallocate_for_aggregation]);

    auto aggregate_descriptions = aggregation_analysis_result.aggregate_descriptions;
    if (aggregate_descriptions_remove_arguments)
    {
        for (auto & aggregate_description : aggregate_descriptions)
            aggregate_description.argument_names.clear();
    }

    Aggregator::Params aggregator_params = Aggregator::Params(
        aggregation_analysis_result.aggregation_keys,
        aggregate_descriptions,
        query_analysis_result.aggregate_overflow_row,
        settings[Setting::max_rows_to_group_by],
        settings[Setting::group_by_overflow_mode],
        settings[Setting::group_by_two_level_threshold],
        settings[Setting::group_by_two_level_threshold_bytes],
        Aggregator::Params::getMaxBytesBeforeExternalGroupBy(settings[Setting::max_bytes_before_external_group_by], settings[Setting::max_bytes_ratio_before_external_group_by]),
        settings[Setting::empty_result_for_aggregation_by_empty_set]
            || (settings[Setting::empty_result_for_aggregation_by_constant_keys_on_empty_set] && aggregation_analysis_result.aggregation_keys.empty()
                && aggregation_analysis_result.group_by_with_constant_keys),
        query_context->getTempDataOnDisk(),
        settings[Setting::max_threads],
        settings[Setting::min_free_disk_space_for_temporary_data],
        settings[Setting::compile_aggregate_expressions],
        settings[Setting::min_count_to_compile_aggregate_expression],
        settings[Setting::max_block_size],
        settings[Setting::enable_software_prefetch_in_aggregation],
        /* only_merge */ false,
        settings[Setting::optimize_group_by_constant_keys],
        settings[Setting::min_hit_rate_to_use_consecutive_keys_optimization],
        stats_collecting_params);

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

void addAggregationStep(QueryPlan & query_plan,
    const AggregationAnalysisResult & aggregation_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const SelectQueryInfo & select_query_info)
{
    const Settings & settings = planner_context->getQueryContext()->getSettingsRef();
    auto aggregator_params = getAggregatorParams(planner_context, aggregation_analysis_result, query_analysis_result, select_query_info);

    SortDescription sort_description_for_merging;
    SortDescription group_by_sort_description;

    if (settings[Setting::force_aggregation_in_order])
    {
        group_by_sort_description = getSortDescriptionFromNames(aggregation_analysis_result.aggregation_keys);
        sort_description_for_merging = group_by_sort_description;
    }

    auto merge_threads = settings[Setting::max_threads];
    auto temporary_data_merge_threads = settings[Setting::aggregation_memory_efficient_merge_threads]
        ? static_cast<size_t>(settings[Setting::aggregation_memory_efficient_merge_threads])
        : static_cast<size_t>(settings[Setting::max_threads]);

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
        settings[Setting::force_aggregation_in_order]);
    query_plan.addStep(std::move(aggregating_step));
}

void addMergingAggregatedStep(QueryPlan & query_plan,
    const AggregationAnalysisResult & aggregation_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context)
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

    const auto & keys = aggregation_analysis_result.aggregation_keys;

    /// For count() without parameters try to use just one thread
    /// Typically this will either be a trivial count or a really small number of states
    size_t max_threads = settings[Setting::max_threads];
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
        settings[Setting::min_hit_rate_to_use_consecutive_keys_optimization]);

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
}

void addTotalsHavingStep(QueryPlan & query_plan,
    PlannerExpressionsAnalysisResult & expression_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const QueryNode & query_node,
    UsefulSets & useful_sets)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    auto & aggregation_analysis_result = expression_analysis_result.getAggregation();
    auto & having_analysis_result = expression_analysis_result.getHaving();
    bool need_finalize = !query_node.isGroupByWithRollup() && !query_node.isGroupByWithCube();

    std::optional<ActionsDAG> actions;
    if (having_analysis_result.filter_actions)
    {
        actions = std::move(having_analysis_result.filter_actions->dag);
        if (having_analysis_result.filter_actions->project_input)
            actions->appendInputsForUnusedColumns(query_plan.getCurrentHeader());
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
    const SelectQueryInfo & select_query_info,
    const QueryNode & query_node)
{
    if (!query_node.isGroupByWithCube() && !query_node.isGroupByWithRollup())
        return;

    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    auto aggregator_params = getAggregatorParams(planner_context,
        aggregation_analysis_result,
        query_analysis_result,
        select_query_info,
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
      * Then you can get no more than limit_length + limit_offset of different rows.
      */
    if ((!query_node.hasOrderBy() || !before_order) && !query_node.hasLimitBy())
    {
        if (limit_length <= std::numeric_limits<UInt64>::max() - limit_offset)
            limit_hint_for_distinct = limit_length + limit_offset;
    }

    SizeLimits limits(settings[Setting::max_rows_in_distinct], settings[Setting::max_bytes_in_distinct], settings[Setting::distinct_overflow_mode]);

    auto distinct_step = std::make_unique<DistinctStep>(
        query_plan.getCurrentHeader(),
        limits,
        limit_hint_for_distinct,
        column_names,
        pre_distinct);

    distinct_step->setStepDescription(pre_distinct ? "Preliminary DISTINCT" : "DISTINCT");
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

void addMergeSortingStep(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const std::string & description)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    const auto & sort_description = query_analysis_result.sort_description;

    auto merging_sorted = std::make_unique<SortingStep>(
        query_plan.getCurrentHeader(),
        sort_description,
        settings[Setting::max_block_size],
        query_analysis_result.partial_sorting_limit,
        settings[Setting::exact_rows_before_limit]);
    merging_sorted->setStepDescription("Merge sorted streams " + description);
    query_plan.addStep(std::move(merging_sorted));
}

void addWithFillStepIfNeeded(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const QueryNode & query_node)
{
    NameSet column_names_with_fill;
    SortDescription fill_description;

    const auto & header = query_plan.getCurrentHeader();

    for (const auto & description : query_analysis_result.sort_description)
    {
        if (description.with_fill)
        {
            if (!header.findByName(description.column_name))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Filling column {} is not present in the block {}", description.column_name, header.dumpNames());
            fill_description.push_back(description);
            column_names_with_fill.insert(description.column_name);
        }
    }

    if (fill_description.empty())
        return;

    InterpolateDescriptionPtr interpolate_description;

    if (query_node.hasInterpolate())
    {
        ActionsDAG interpolate_actions_dag;
        auto query_plan_columns = header.getColumnsWithTypeAndName();
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
                if (column_names_with_fill.contains(input_node->result_name))
                    continue;

                interpolate_actions_dag.getOutputs().push_back(input_node);
            }
        }
        else
        {
            ActionsDAG rename_dag;

            for (auto & interpolate_node : interpolate_list_nodes)
            {
                auto & interpolate_node_typed = interpolate_node->as<InterpolateNode &>();

                ColumnNodePtrWithHashSet empty_correlated_columns_set;
                PlannerActionsVisitor planner_actions_visitor(planner_context, empty_correlated_columns_set);
                auto [expression_to_interpolate_expression_nodes, expression_to_interpolate_correlated_subtrees] = planner_actions_visitor.visit(interpolate_actions_dag,
                    interpolate_node_typed.getExpression());
                expression_to_interpolate_correlated_subtrees.assertEmpty("in expression to interpolate");
                if (expression_to_interpolate_expression_nodes.size() != 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expression to interpolate expected to have single action node");

                auto [interpolate_expression_nodes, interpolate_correlated_subtrees] = planner_actions_visitor.visit(interpolate_actions_dag,
                    interpolate_node_typed.getInterpolateExpression());
                interpolate_correlated_subtrees.assertEmpty("in interpolate expression");
                if (interpolate_expression_nodes.size() != 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Interpolate expression expected to have single action node");

                const auto * expression_to_interpolate = expression_to_interpolate_expression_nodes[0];
                const auto & expression_to_interpolate_name = expression_to_interpolate->result_name;

                const auto * interpolate_expression = interpolate_expression_nodes[0];
                if (!interpolate_expression->result_type->equals(*expression_to_interpolate->result_type))
                {
                    interpolate_expression = &interpolate_actions_dag.addCast(*interpolate_expression,
                        expression_to_interpolate->result_type,
                        interpolate_expression->result_name);
                }

                const auto * alias_node = &interpolate_actions_dag.addAlias(*interpolate_expression, expression_to_interpolate_name);
                interpolate_actions_dag.getOutputs().push_back(alias_node);

                /// Here we fix INTERPOLATE by constant expression.
                /// Example from 02336_sort_optimization_with_fill:
                ///
                /// SELECT 5 AS x, 'Hello' AS s ORDER BY x WITH FILL FROM 1 TO 10 INTERPOLATE (s AS s||'A')
                ///
                /// For this query, INTERPOLATE_EXPRESSION would be : s AS concat(s, 'A'),
                /// so that interpolate_actions_dag would have INPUT `s`.
                ///
                /// However, INPUT `s` does not exist. Instead, we have a constant with execution name 'Hello'_String.
                /// To fix this, we prepend a rename : 'Hello'_String -> s
                if (const auto * /*constant_node*/ _ = interpolate_node_typed.getExpression()->as<const ConstantNode>())
                {
                    const auto & name = interpolate_node_typed.getExpressionName();
                    const auto * node = &rename_dag.addInput(alias_node->result_name, alias_node->result_type);
                    node = &rename_dag.addAlias(*node, name);
                    rename_dag.getOutputs().push_back(node);

                    /// Interpolate DAG should contain INPUT with same name to ensure a proper merging
                    const auto & inputs = interpolate_actions_dag.getInputs();
                    if (std::ranges::find_if(inputs, [&name](const auto & input){ return input->result_name == name; }) == inputs.end())
                        interpolate_actions_dag.addInput(name, interpolate_node_typed.getExpression()->getResultType());
                }
            }

            if (!rename_dag.getOutputs().empty())
                interpolate_actions_dag = ActionsDAG::merge(std::move(rename_dag), std::move(interpolate_actions_dag));

            interpolate_actions_dag.removeUnusedActions();
        }

        Aliases empty_aliases;
        interpolate_description = std::make_shared<InterpolateDescription>(std::move(interpolate_actions_dag), empty_aliases);
    }

    const auto & query_context = planner_context->getQueryContext();
    const Settings & settings = query_context->getSettingsRef();
    auto filling_step = std::make_unique<FillingStep>(
        header,
        query_analysis_result.sort_description,
        std::move(fill_description),
        interpolate_description,
        settings[Setting::use_with_fill_by_sorting_prefix]);
    query_plan.addStep(std::move(filling_step));
}

void addLimitByStep(
    QueryPlan & query_plan, const LimitByAnalysisResult & limit_by_analysis_result, const QueryNode & query_node, bool do_not_skip_offset)
{
    /// Constness of LIMIT BY limit is validated during query analysis stage
    UInt64 limit_by_limit = query_node.getLimitByLimit()->as<ConstantNode &>().getValue().safeGet<UInt64>();
    UInt64 limit_by_offset = 0;

    if (query_node.hasLimitByOffset())
    {
        /// Constness of LIMIT BY offset is validated during query analysis stage
        limit_by_offset = query_node.getLimitByOffset()->as<ConstantNode &>().getValue().safeGet<UInt64>();
    }

    if (do_not_skip_offset)
    {
        if (limit_by_limit > std::numeric_limits<UInt64>::max() - limit_by_offset)
            return;

        limit_by_limit += limit_by_offset;
        limit_by_offset = 0;
    }

    auto limit_by_step = std::make_unique<LimitByStep>(query_plan.getCurrentHeader(),
        limit_by_limit,
        limit_by_offset,
        limit_by_analysis_result.limit_by_column_names);
    query_plan.addStep(std::move(limit_by_step));
}

void addPreliminaryLimitStep(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    bool do_not_skip_offset)
{
    UInt64 limit_offset = query_analysis_result.limit_offset;
    UInt64 limit_length = query_analysis_result.limit_length;

    if (do_not_skip_offset)
    {
        if (limit_length > std::numeric_limits<UInt64>::max() - limit_offset)
            return;

        limit_length += limit_offset;
        limit_offset = 0;
    }

    const auto & query_context = planner_context->getQueryContext();
    const Settings & settings = query_context->getSettingsRef();

    auto limit
        = std::make_unique<LimitStep>(query_plan.getCurrentHeader(), limit_length, limit_offset, settings[Setting::exact_rows_before_limit]);
    limit->setStepDescription(do_not_skip_offset ? "preliminary LIMIT (with OFFSET)" : "preliminary LIMIT (without OFFSET)");
    query_plan.addStep(std::move(limit));
}

bool addPreliminaryLimitOptimizationStepIfNeeded(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr planner_context,
    const PlannerQueryProcessingInfo & query_processing_info,
    const QueryTreeNodePtr & query_tree)
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

    bool apply_limit = query_processing_info.getToStage() != QueryProcessingStage::WithMergeableStateAfterAggregation;
    bool apply_prelimit = apply_limit && query_node.hasLimit() && !query_node.isLimitWithTies() && !query_node.isGroupByWithTotals()
        && !query_analysis_result.query_has_with_totals_in_any_subquery_in_join_tree
        && !query_analysis_result.query_has_array_join_in_join_tree && !query_node.isDistinct() && !query_node.hasLimitBy()
        && !settings[Setting::extremes] && !has_withfill;
    bool apply_offset = query_processing_info.getToStage() != QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;
    if (apply_prelimit)
    {
        addPreliminaryLimitStep(query_plan, query_analysis_result, planner_context, /* do_not_skip_offset= */!apply_offset);
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
        addLimitByStep(query_plan, limit_by_analysis_result, query_node, true /*do_not_skip_offset*/);
    }

    /// Do not apply PreLimit at first stage for LIMIT BY and `exact_rows_before_limit`,
    /// as it may break `rows_before_limit_at_least` value during the second stage in
    /// case it also contains LIMIT BY
    const Settings & settings = planner_context->getQueryContext()->getSettingsRef();

    if (query_node.hasLimitBy() && settings[Setting::exact_rows_before_limit])
    {
        return;
    }

    /// WITH TIES simply not supported properly for preliminary steps, so let's disable it.
    if (query_node.hasLimit() && !query_node.hasLimitByOffset() && !query_node.isLimitWithTies())
        addPreliminaryLimitStep(query_plan, query_analysis_result, planner_context, true /*do_not_skip_offset*/);
}

void addWindowSteps(QueryPlan & query_plan,
    const PlannerContextPtr & planner_context,
    WindowAnalysisResult & window_analysis_result)
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
            need_sort = !sortDescriptionIsPrefix(window_description.full_sort_description, window_descriptions[i - 1].full_sort_description)
                || (settings[Setting::max_threads] != 1 && window_description.partition_by.size() != window_descriptions[i - 1].partition_by.size());
        }
        if (need_sort)
        {
            SortingStep::Settings sort_settings(query_context->getSettingsRef());

            auto sorting_step = std::make_unique<SortingStep>(
                query_plan.getCurrentHeader(),
                window_description.full_sort_description,
                window_description.partition_by,
                0 /*limit*/,
                sort_settings);
            sorting_step->setStepDescription("Sorting for window '" + window_description.window_name + "'");
            query_plan.addStep(std::move(sorting_step));
        }

        // Fan out streams only for the last window to preserve the ordering between windows,
        // and WindowTransform works on single stream anyway.
        const bool streams_fan_out
            = settings[Setting::query_plan_enable_multithreading_after_window_functions] && ((i + 1) == window_descriptions_size);

        auto window_step
            = std::make_unique<WindowStep>(query_plan.getCurrentHeader(), window_description, window_description.window_functions, streams_fan_out);
        window_step->setStepDescription("Window step for window '" + window_description.window_name + "'");
        query_plan.addStep(std::move(window_step));
    }
}

void addLimitStep(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const QueryNode & query_node)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();
    bool always_read_till_end = settings[Setting::exact_rows_before_limit];
    bool limit_with_ties = query_node.isLimitWithTies();

    /** Special cases:
      *
      * 1. If there is WITH TOTALS and there is no ORDER BY, then read the data to the end,
      *  otherwise TOTALS is counted according to incomplete data.
      *
      * 2. If there is no WITH TOTALS and there is a subquery in FROM, and there is WITH TOTALS on one of the levels,
      *  then when using LIMIT, you should read the data to the end, rather than cancel the query earlier,
      *  because if you cancel the query, we will not get `totals` data from the remote server.
      */
    if (query_node.isGroupByWithTotals() && !query_node.hasOrderBy())
        always_read_till_end = true;

    if (!query_node.isGroupByWithTotals() && query_analysis_result.query_has_with_totals_in_any_subquery_in_join_tree)
        always_read_till_end = true;

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
    /// If there is not a LIMIT but an offset
    if (!query_analysis_result.limit_length && query_analysis_result.limit_offset)
    {
        auto offsets_step = std::make_unique<OffsetStep>(query_plan.getCurrentHeader(), query_analysis_result.limit_offset);
        query_plan.addStep(std::move(offsets_step));
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
        /// I don't know if this is a good decision,
        /// but for now it is done in the same way as in old analyzer.
        /// This would not ignore limits for subqueries (affects mutations only).
        /// See test_build_sets_from_multiple_threads-analyzer.
        subquery_options.ignore_limits = false;
        Planner subquery_planner(
            query_tree,
            subquery_options,
            std::make_shared<GlobalPlannerContext>(nullptr, nullptr, FiltersForTableExpressionMap{}));
        subquery_planner.buildQueryPlanIfNeeded();

        subquery->setQueryPlan(std::make_unique<QueryPlan>(std::move(subquery_planner).extractQueryPlan()));
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
    PlannerContextPtr & planner_context
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

    auto filter_info = buildFilterInfo(additional_result_filter_ast, fake_table_expression, planner_context, std::move(fake_name_set));
    if (!query_plan.isInitialized())
        return;

    auto filter_step = std::make_unique<FilterStep>(query_plan.getCurrentHeader(),
        std::move(filter_info.actions),
        filter_info.column_name,
        filter_info.do_remove_column);
    filter_step->setStepDescription("additional result filter");
    query_plan.addStep(std::move(filter_step));
}

}

PlannerContextPtr buildPlannerContext(const QueryTreeNodePtr & query_tree_node,
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
    auto min_major = static_cast<UInt64>(DBMS_MIN_MAJOR_VERSION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD);
    auto min_minor = static_cast<UInt64>(DBMS_MIN_MINOR_VERSION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD);

    bool need_to_disable_two_level_aggregation = client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY &&
        client_info.connection_client_version_major < min_major &&
        client_info.connection_client_version_minor < min_minor;

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
    SelectQueryOptions & select_query_options_)
    : query_tree(query_tree_)
    , select_query_options(select_query_options_)
    , planner_context(buildPlannerContext(query_tree, select_query_options,
        std::make_shared<GlobalPlannerContext>(
            findQueryForParallelReplicas(query_tree, select_query_options),
            findTableForParallelReplicas(query_tree, select_query_options),
            collectFiltersForAnalysis(query_tree, select_query_options))))
{
}

Planner::Planner(const QueryTreeNodePtr & query_tree_,
    SelectQueryOptions & select_query_options_,
    GlobalPlannerContextPtr global_planner_context_)
    : query_tree(query_tree_)
    , select_query_options(select_query_options_)
    , planner_context(buildPlannerContext(query_tree_, select_query_options, std::move(global_planner_context_)))
{
}

Planner::Planner(const QueryTreeNodePtr & query_tree_,
    SelectQueryOptions & select_query_options_,
    PlannerContextPtr planner_context_)
    : query_tree(query_tree_)
    , select_query_options(select_query_options_)
    , planner_context(std::move(planner_context_))
{
}

void Planner::buildQueryPlanIfNeeded()
{
    if (query_plan.isInitialized())
        return;

    LOG_TRACE(
        log,
        "Query to stage {}{}",
        QueryProcessingStage::toString(select_query_options.to_stage),
        select_query_options.only_analyze ? " only analyze" : "");

    if (query_tree->getNodeType() == QueryTreeNodeType::UNION)
        buildPlanForUnionNode();
    else
        buildPlanForQueryNode();
    extendQueryContextAndStoragesLifetime(query_plan, planner_context);
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

        auto read_from_recursive_cte_step = std::make_unique<ReadFromRecursiveCTEStep>(Block(std::move(recursive_cte_columns)), query_tree);
        read_from_recursive_cte_step->setStepDescription(query_tree->toAST()->formatForErrorMessage());
        query_plan.addStep(std::move(read_from_recursive_cte_step));
        return;
    }

    const auto & union_queries_nodes = union_node.getQueries().getNodes();
    size_t queries_size = union_queries_nodes.size();

    std::vector<std::unique_ptr<QueryPlan>> query_plans;
    query_plans.reserve(queries_size);

    Blocks query_plans_headers;
    query_plans_headers.reserve(queries_size);

    for (const auto & query_node : union_queries_nodes)
    {
        Planner query_planner(query_node, select_query_options, planner_context->getGlobalPlannerContext());

        query_planner.buildQueryPlanIfNeeded();
        for (const auto & row_policy : query_planner.getUsedRowPolicies())
            used_row_policies.insert(row_policy);
        const auto & mapping = query_planner.getQueryNodeToPlanStepMapping();
        query_node_to_plan_step_mapping.insert(mapping.begin(), mapping.end());
        auto query_node_plan = std::make_unique<QueryPlan>(std::move(query_planner).extractQueryPlan());
        query_plans_headers.push_back(query_node_plan->getCurrentHeader());
        query_plans.push_back(std::move(query_node_plan));
    }

    Block union_common_header = buildCommonHeaderForUnion(query_plans_headers, union_mode);
    addConvertingToCommonHeaderActionsIfNeeded(query_plans, union_common_header, query_plans_headers);

    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();
    auto max_threads = settings[Setting::max_threads];

    bool is_distinct = union_mode == SelectUnionMode::UNION_DISTINCT || union_mode == SelectUnionMode::INTERSECT_DISTINCT
        || union_mode == SelectUnionMode::EXCEPT_DISTINCT;

    if (union_mode == SelectUnionMode::UNION_ALL || union_mode == SelectUnionMode::UNION_DISTINCT)
    {
        auto union_step = std::make_unique<UnionStep>(std::move(query_plans_headers), max_threads);
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
        SizeLimits limits(settings[Setting::max_rows_in_distinct], settings[Setting::max_bytes_in_distinct], settings[Setting::distinct_overflow_mode]);

        auto distinct_step = std::make_unique<DistinctStep>(
            query_plan.getCurrentHeader(),
            limits,
            0 /*limit hint*/,
            query_plan.getCurrentHeader().getNames(),
            false /*pre distinct*/);
        query_plan.addStep(std::move(distinct_step));
    }
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

    StorageLimitsList current_storage_limits = storage_limits;
    select_query_info.local_storage_limits = buildStorageLimits(*query_context, select_query_options);
    current_storage_limits.push_back(select_query_info.local_storage_limits);
    select_query_info.storage_limits = std::make_shared<StorageLimitsList>(current_storage_limits);
    select_query_info.has_order_by = query_node.hasOrderBy();
    select_query_info.has_window = hasWindowFunctionNodes(query_tree);
    select_query_info.has_aggregates = hasAggregateFunctionNodes(query_tree);
    select_query_info.need_aggregate = query_node.hasGroupBy() || select_query_info.has_aggregates;
    select_query_info.merge_tree_enable_remove_parts_from_snapshot_optimization = select_query_options.merge_tree_enable_remove_parts_from_snapshot_optimization;

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

    collectSets(query_tree, *planner_context);

    const auto & settings = query_context->getSettingsRef();
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
        const auto & table_expression_nodes = planner_context->getTableExpressionNodeToData();
        for (const auto & it : table_expression_nodes)
        {
            auto * table_node = it.first->as<TableNode>();
            if (!table_node)
                continue;

            const auto & modifiers = table_node->getTableExpressionModifiers();
            if (modifiers.has_value() && modifiers->hasFinal())
            {
                if (settings[Setting::allow_experimental_parallel_reading_from_replicas] >= 2)
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "FINAL modifier is not supported with parallel replicas");

                LOG_DEBUG(log, "FINAL modifier is not supported with parallel replicas. Query will be executed without using them.");
                auto & mutable_context = planner_context->getMutableQueryContext();
                mutable_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
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
    if (planner_context->getMutableQueryContext()->canUseTaskBasedParallelReplicas()
        && planner_context->getGlobalPlannerContext()->parallel_replicas_node == &query_node)
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

    auto from_stage = join_tree_query_plan.from_stage;
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

    if (select_query_options.to_stage == QueryProcessingStage::FetchColumns)
        return;

    PlannerQueryProcessingInfo query_processing_info(from_stage, select_query_options.to_stage);
    QueryAnalysisResult query_analysis_result(query_tree, query_processing_info, planner_context);
    auto expression_analysis_result = buildExpressionAnalysisResult(query_tree,
        query_plan.getCurrentHeader().getColumnsWithTypeAndName(),
        planner_context,
        query_processing_info);

    auto useful_sets = std::move(join_tree_query_plan.useful_sets);

    for (auto & [_, table_expression_data] : planner_context->getTableExpressionNodeToData())
    {
        if (table_expression_data.getPrewhereFilterActions())
            appendSetsFromActionsDAG(*table_expression_data.getPrewhereFilterActions(), useful_sets);

        if (table_expression_data.getRowLevelFilterActions())
            appendSetsFromActionsDAG(*table_expression_data.getRowLevelFilterActions(), useful_sets);
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
            addMergingAggregatedStep(query_plan, aggregation_analysis_result, query_analysis_result, planner_context);
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

            addAggregationStep(query_plan, aggregation_analysis_result, query_analysis_result, planner_context, select_query_info);
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
                addMergingAggregatedStep(query_plan, aggregation_analysis_result, query_analysis_result, planner_context);
            }

            bool having_executed = false;

            if (query_node.isGroupByWithTotals())
            {
                addTotalsHavingStep(query_plan, expression_analysis_result, query_analysis_result, planner_context, query_node, useful_sets);
                having_executed = true;
            }

            addCubeOrRollupStepIfNeeded(query_plan, aggregation_analysis_result, query_analysis_result, planner_context, select_query_info, query_node);

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

                addWindowSteps(query_plan, planner_context, window_analysis_result);
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

        if (expression_analysis_result.hasSort())
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
                addMergeSortingStep(query_plan, query_analysis_result, planner_context, "after aggregation stage for ORDER BY");
            else if (!query_processing_info.isFirstStage() &&
                !expression_analysis_result.hasAggregation() &&
                !expression_analysis_result.hasWindow() &&
                !(query_node.isGroupByWithTotals() && !query_analysis_result.aggregate_final))
                addMergeSortingStep(query_plan, query_analysis_result, planner_context, "for ORDER BY, without aggregation");
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
            query_tree);

        //// If there was more than one stream, then DISTINCT needs to be performed once again after merging all streams.
        if (!query_processing_info.isFromAggregationState() && query_node.isDistinct())
        {
            addDistinctStep(query_plan,
                query_analysis_result,
                planner_context,
                expression_analysis_result.getProjection().projection_column_names,
                query_node,
                false /*before_order*/,
                false /*pre_distinct*/);
        }

        if (!query_processing_info.isFromAggregationState() && expression_analysis_result.hasLimitBy())
        {
            auto & limit_by_analysis_result = expression_analysis_result.getLimitBy();
            addExpressionStep(
                planner_context,
                query_plan,
                limit_by_analysis_result.before_limit_by_actions,
                /*correlated_subtrees=*/{},
                select_query_options,
                "Before LIMIT BY",
                useful_sets);
            addLimitByStep(query_plan, limit_by_analysis_result, query_node, false /*do_not_skip_offset*/);
        }

        if (query_node.hasOrderBy())
            addWithFillStepIfNeeded(query_plan, query_analysis_result, planner_context, query_node);

        const bool apply_limit = query_processing_info.getToStage() != QueryProcessingStage::WithMergeableStateAfterAggregation;
        const bool apply_offset = query_processing_info.getToStage() != QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;
        if (query_node.hasLimit() && query_node.isLimitWithTies() && apply_limit && apply_offset)
            addLimitStep(query_plan, query_analysis_result, planner_context, query_node);

        addExtremesStepIfNeeded(query_plan, planner_context);

        bool limit_applied = applied_prelimit || (query_node.isLimitWithTies() && apply_offset);

        /** Limit is no longer needed if there is prelimit.
          *
          * That LIMIT cannot be applied if OFFSET should not be applied, since LIMIT will apply OFFSET too.
          * This is the case for various optimizations for distributed queries,
          * and when LIMIT cannot be applied it will be applied on the initiator anyway.
          */
        if (query_node.hasLimit() && apply_limit && !limit_applied && apply_offset)
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
        addAdditionalFilterStepIfNeeded(query_plan, query_node, select_query_options, planner_context);
    }

    // Not all cases are supported here yet. E.g. for this query:
    // select * from remote('127.0.0.{1,2}', numbers_mt(1e6)) group by number
    // we will have `BlocksMarshallingStep` added to the query plan, but not for
    // select * from remote('127.0.0.{1,2}', numbers_mt(1e6))
    // because `to_stage` for it will be `QueryProcessingStage::Complete`.
    if (query_context->getSettingsRef()[Setting::enable_parallel_blocks_marshalling]
        && query_context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY
        && select_query_options.to_stage != QueryProcessingStage::Complete // Don't do it for INSERT SELECT, for example
        && query_context->getClientInfo().distributed_depth <= 1 // Makes sense for higher depths too, just not supported
    )
        query_plan.addStep(std::make_unique<BlocksMarshallingStep>(query_plan.getCurrentHeader()));

    if (!select_query_options.only_analyze)
        addBuildSubqueriesForSetsStepIfNeeded(query_plan, select_query_options, planner_context, useful_sets);

    query_node_to_plan_step_mapping[&query_node] = query_plan.getRootNode();
}

SelectQueryInfo Planner::buildSelectQueryInfo() const
{
    return ::DB::buildSelectQueryInfo(query_tree, planner_context);
}

void Planner::addStorageLimits(const StorageLimitsList & limits)
{
    for (const auto & limit : limits)
        storage_limits.push_back(limit);
}

}
