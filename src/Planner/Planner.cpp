#include <Planner/Planner.h>

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
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/Context.h>

#include <Storages/SelectQueryInfo.h>
#include <Storages/IStorage.h>

#include <Analyzer/Utils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/InterpolateNode.h>
#include <Analyzer/WindowNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/CollectAggregateFunctionNodes.h>
#include <Analyzer/CollectWindowFunctionNodes.h>

#include <Planner/Utils.h>
#include <Planner/PlannerContext.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerJoins.h>
#include <Planner/PlannerAggregation.h>
#include <Planner/PlannerSorting.h>
#include <Planner/PlannerWindowFunctions.h>
#include <Planner/ActionsChain.h>
#include <Planner/CollectSets.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/PlannerJoinTree.h>
#include <Planner/PlannerExpressionAnalysis.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

/** ClickHouse query planner.
  *
  * TODO: Support JOIN with JOIN engine.
  * TODO: Support VIEWs.
  * TODO: JOIN drop unnecessary columns after ON, USING section
  * TODO: Support RBAC. Support RBAC for ALIAS columns
  * TODO: Support distributed query processing
  * TODO: Support PREWHERE
  * TODO: Support DISTINCT
  * TODO: Support trivial count optimization
  * TODO: Support projections
  * TODO: Support read in order optimization
  * TODO: UNION storage limits
  * TODO: Support max streams
  * TODO: Support ORDER BY read in order optimization
  * TODO: Support GROUP BY read in order optimization
  * TODO: Support Key Condition. Support indexes for IN function.
  */

namespace
{

void addBuildSubqueriesForSetsStepIfNeeded(QueryPlan & query_plan, const SelectQueryOptions & select_query_options, const PlannerContextPtr & planner_context)
{
    if (select_query_options.is_subquery)
        return;

    PreparedSets::SubqueriesForSets subqueries_for_sets;
    const auto & subquery_node_to_sets = planner_context->getGlobalPlannerContext()->getSubqueryNodesForSets();

    for (auto [key, subquery_node_for_set] : subquery_node_to_sets)
    {
        auto subquery_context = buildSubqueryContext(planner_context->getQueryContext());
        auto subquery_options = select_query_options.subquery();
        Planner subquery_planner(
            subquery_node_for_set.subquery_node,
            subquery_options,
            std::move(subquery_context),
            planner_context->getGlobalPlannerContext());
        subquery_planner.buildQueryPlanIfNeeded();

        SubqueryForSet subquery_for_set;
        subquery_for_set.set = subquery_node_for_set.set;
        subquery_for_set.source = std::make_unique<QueryPlan>(std::move(subquery_planner).extractQueryPlan());

        subqueries_for_sets.emplace(key, std::move(subquery_for_set));
    }

    addCreatingSetsStep(query_plan, std::move(subqueries_for_sets), planner_context->getQueryContext());
}

}

Planner::Planner(const QueryTreeNodePtr & query_tree_,
    const SelectQueryOptions & select_query_options_,
    ContextPtr context_)
    : query_tree(query_tree_)
    , select_query_options(select_query_options_)
    , planner_context(std::make_shared<PlannerContext>(context_, std::make_shared<GlobalPlannerContext>()))
{
    if (query_tree->getNodeType() != QueryTreeNodeType::QUERY &&
        query_tree->getNodeType() != QueryTreeNodeType::UNION)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected QUERY or UNION node. Actual {}",
            query_tree->formatASTForErrorMessage());
}

/// Initialize interpreter with query tree after query analysis phase and global planner context
Planner::Planner(const QueryTreeNodePtr & query_tree_,
    const SelectQueryOptions & select_query_options_,
    ContextPtr context_,
    GlobalPlannerContextPtr global_planner_context_)
    : query_tree(query_tree_)
    , select_query_options(select_query_options_)
    , planner_context(std::make_shared<PlannerContext>(context_, std::move(global_planner_context_)))
{
    if (query_tree->getNodeType() != QueryTreeNodeType::QUERY &&
        query_tree->getNodeType() != QueryTreeNodeType::UNION)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected QUERY or UNION node. Actual {}",
            query_tree->formatASTForErrorMessage());
}

void Planner::buildQueryPlanIfNeeded()
{
    if (query_plan.isInitialized())
        return;

    auto query_context = planner_context->getQueryContext();

    if (auto * union_query_tree = query_tree->as<UnionNode>())
    {
        auto union_mode = union_query_tree->getUnionMode();
        if (union_mode == SelectUnionMode::UNION_DEFAULT ||
            union_mode == SelectUnionMode::EXCEPT_DEFAULT ||
            union_mode == SelectUnionMode::INTERSECT_DEFAULT)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UNION mode must be initialized");

        size_t queries_size = union_query_tree->getQueries().getNodes().size();

        std::vector<std::unique_ptr<QueryPlan>> query_plans;
        query_plans.reserve(queries_size);

        Blocks query_plans_headers;
        query_plans_headers.reserve(queries_size);

        for (auto & query_node : union_query_tree->getQueries().getNodes())
        {
            Planner query_planner(query_node, select_query_options, query_context);
            query_planner.buildQueryPlanIfNeeded();
            auto query_node_plan = std::make_unique<QueryPlan>(std::move(query_planner).extractQueryPlan());
            query_plans_headers.push_back(query_node_plan->getCurrentDataStream().header);
            query_plans.push_back(std::move(query_node_plan));
        }

        Block union_common_header = buildCommonHeaderForUnion(query_plans_headers);
        DataStreams query_plans_streams;
        query_plans_streams.reserve(query_plans.size());

        for (auto & query_node_plan : query_plans)
        {
            if (blocksHaveEqualStructure(query_node_plan->getCurrentDataStream().header, union_common_header))
            {
                query_plans_streams.push_back(query_node_plan->getCurrentDataStream());
                continue;
            }

            auto actions_dag = ActionsDAG::makeConvertingActions(
                    query_node_plan->getCurrentDataStream().header.getColumnsWithTypeAndName(),
                    union_common_header.getColumnsWithTypeAndName(),
                    ActionsDAG::MatchColumnsMode::Position);
            auto converting_step = std::make_unique<ExpressionStep>(query_node_plan->getCurrentDataStream(), std::move(actions_dag));
            converting_step->setStepDescription("Conversion before UNION");
            query_node_plan->addStep(std::move(converting_step));

            query_plans_streams.push_back(query_node_plan->getCurrentDataStream());
        }

        const auto & settings = query_context->getSettingsRef();
        auto max_threads = settings.max_threads;

        bool is_distinct = union_mode == SelectUnionMode::UNION_DISTINCT || union_mode == SelectUnionMode::INTERSECT_DISTINCT ||
            union_mode == SelectUnionMode::EXCEPT_DISTINCT;

        if (union_mode == SelectUnionMode::UNION_ALL || union_mode == SelectUnionMode::UNION_DISTINCT)
        {
            auto union_step = std::make_unique<UnionStep>(std::move(query_plans_streams), max_threads);
            query_plan.unitePlans(std::move(union_step), std::move(query_plans));
        }
        else if (union_mode == SelectUnionMode::INTERSECT_ALL || union_mode == SelectUnionMode::INTERSECT_DISTINCT ||
            union_mode == SelectUnionMode::EXCEPT_ALL || union_mode == SelectUnionMode::EXCEPT_DISTINCT)
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

            auto union_step = std::make_unique<IntersectOrExceptStep>(std::move(query_plans_streams), intersect_or_except_operator, max_threads);
            query_plan.unitePlans(std::move(union_step), std::move(query_plans));
        }

        if (is_distinct)
        {
            /// Add distinct transform
            SizeLimits limits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode);

            auto distinct_step = std::make_unique<DistinctStep>(
                query_plan.getCurrentDataStream(),
                limits,
                0 /*limit hint*/,
                query_plan.getCurrentDataStream().header.getNames(),
                false /*pre distinct*/,
                settings.optimize_distinct_in_order);

            query_plan.addStep(std::move(distinct_step));
        }

        return;
    }

    auto & query_node = query_tree->as<QueryNode &>();

    if (query_node.hasPrewhere())
    {
        if (query_node.hasWhere())
        {
            auto function_node = std::make_shared<FunctionNode>("and");
            auto and_function = FunctionFactory::instance().get("and", query_context);
            function_node->resolveAsFunction(std::move(and_function), std::make_shared<DataTypeUInt8>());
            function_node->getArguments().getNodes() = {query_node.getPrewhere(), query_node.getWhere()};
            query_node.getWhere() = std::move(function_node);
            query_node.getPrewhere() = {};
        }
        else
        {
            query_node.getWhere() = query_node.getPrewhere();
        }
    }

    SelectQueryInfo select_query_info;
    select_query_info.original_query = queryNodeToSelectQuery(query_tree);
    select_query_info.query = select_query_info.original_query;
    select_query_info.planner_context = planner_context;

    StorageLimitsList storage_limits;
    storage_limits.push_back(buildStorageLimits(*query_context, select_query_options));
    select_query_info.storage_limits = std::make_shared<StorageLimitsList>(storage_limits);

    collectTableExpressionData(query_tree, *planner_context);
    collectSets(query_tree, *planner_context);

    query_plan = buildQueryPlanForJoinTreeNode(query_node.getJoinTree(), select_query_info, select_query_options, planner_context);
    auto expression_analysis_result = buildExpressionAnalysisResult(query_tree, query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName(), planner_context);

    if (expression_analysis_result.hasWhere())
    {
        const auto & where_analysis_result = expression_analysis_result.getWhere();
        auto where_step = std::make_unique<FilterStep>(query_plan.getCurrentDataStream(),
            where_analysis_result.filter_actions,
            where_analysis_result.filter_column_name,
            where_analysis_result.remove_filter_column);
        where_step->setStepDescription("WHERE");
        query_plan.addStep(std::move(where_step));
    }

    bool having_executed = false;

    if (expression_analysis_result.hasAggregation())
    {
        const auto & aggregation_analysis_result = expression_analysis_result.getAggregation();

        if (aggregation_analysis_result.before_aggregation_actions)
        {
            auto expression_before_aggregation = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), aggregation_analysis_result.before_aggregation_actions);
            expression_before_aggregation->setStepDescription("Before GROUP BY");
            query_plan.addStep(std::move(expression_before_aggregation));
        }

        const Settings & settings = planner_context->getQueryContext()->getSettingsRef();

        const auto stats_collecting_params = Aggregator::Params::StatsCollectingParams(
            select_query_info.query,
            settings.collect_hash_table_stats_during_aggregation,
            settings.max_entries_for_hash_table_stats,
            settings.max_size_to_preallocate_for_aggregation);

        bool aggregate_overflow_row =
            query_node.isGroupByWithTotals() &&
            settings.max_rows_to_group_by &&
            settings.group_by_overflow_mode == OverflowMode::ANY &&
            settings.totals_mode != TotalsMode::AFTER_HAVING_EXCLUSIVE;

        Aggregator::Params aggregator_params = Aggregator::Params(
            aggregation_analysis_result.aggregation_keys,
            aggregation_analysis_result.aggregate_descriptions,
            aggregate_overflow_row,
            settings.max_rows_to_group_by,
            settings.group_by_overflow_mode,
            settings.group_by_two_level_threshold,
            settings.group_by_two_level_threshold_bytes,
            settings.max_bytes_before_external_group_by,
            settings.empty_result_for_aggregation_by_empty_set
                || (settings.empty_result_for_aggregation_by_constant_keys_on_empty_set && aggregation_analysis_result.aggregation_keys.empty()
                    && aggregation_analysis_result.group_by_with_constant_keys),
            planner_context->getQueryContext()->getTempDataOnDisk(),
            settings.max_threads,
            settings.min_free_disk_space_for_temporary_data,
            settings.compile_aggregate_expressions,
            settings.min_count_to_compile_aggregate_expression,
            settings.max_block_size,
            settings.enable_software_prefetch_in_aggregation,
            /* only_merge */ false,
            stats_collecting_params
        );

        SortDescription group_by_sort_description;

        auto merge_threads = settings.max_threads;
        auto temporary_data_merge_threads = settings.aggregation_memory_efficient_merge_threads
            ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
            : static_cast<size_t>(settings.max_threads);

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

        const bool should_produce_results_in_order_of_bucket_number
            = select_query_options.to_stage == QueryProcessingStage::WithMergeableState && settings.distributed_aggregation_memory_efficient;

        InputOrderInfoPtr input_order_info;
        bool aggregate_final =
            select_query_options.to_stage > QueryProcessingStage::WithMergeableState &&
            !query_node.isGroupByWithTotals() && !query_node.isGroupByWithRollup() && !query_node.isGroupByWithCube();

        auto aggregating_step = std::make_unique<AggregatingStep>(
            query_plan.getCurrentDataStream(),
            aggregator_params,
            aggregation_analysis_result.grouping_sets_parameters_list,
            aggregate_final,
            settings.max_block_size,
            settings.aggregation_in_order_max_block_bytes,
            merge_threads,
            temporary_data_merge_threads,
            storage_has_evenly_distributed_read,
            settings.group_by_use_nulls,
            std::move(input_order_info),
            std::move(group_by_sort_description),
            should_produce_results_in_order_of_bucket_number);
        query_plan.addStep(std::move(aggregating_step));

        if (query_node.isGroupByWithRollup())
        {
            auto rollup_step = std::make_unique<RollupStep>(query_plan.getCurrentDataStream(), std::move(aggregator_params), true /*final*/, settings.group_by_use_nulls);
            query_plan.addStep(std::move(rollup_step));
        }
        else if (query_node.isGroupByWithCube())
        {
            auto cube_step = std::make_unique<CubeStep>(query_plan.getCurrentDataStream(), std::move(aggregator_params), true /*final*/, settings.group_by_use_nulls);
            query_plan.addStep(std::move(cube_step));
        }

        if (query_node.isGroupByWithTotals())
        {
            const auto & having_analysis_result = expression_analysis_result.getHaving();
            bool final = !query_node.isGroupByWithRollup() && !query_node.isGroupByWithCube();

            auto totals_having_step = std::make_unique<TotalsHavingStep>(
                query_plan.getCurrentDataStream(),
                aggregation_analysis_result.aggregate_descriptions,
                aggregate_overflow_row,
                having_analysis_result.filter_actions,
                having_analysis_result.filter_column_name,
                having_analysis_result.remove_filter_column,
                settings.totals_mode,
                settings.totals_auto_threshold,
                final);

            query_plan.addStep(std::move(totals_having_step));
        }
    }

    if (!having_executed && expression_analysis_result.hasHaving())
    {
        const auto & having_analysis_result = expression_analysis_result.getHaving();

        auto having_step = std::make_unique<FilterStep>(query_plan.getCurrentDataStream(),
            having_analysis_result.filter_actions,
            having_analysis_result.filter_column_name,
            having_analysis_result.remove_filter_column);
        having_step->setStepDescription("HAVING");
        query_plan.addStep(std::move(having_step));
    }

    if (expression_analysis_result.hasWindow())
    {
        const auto & window_analysis_result = expression_analysis_result.getWindow();

        if (window_analysis_result.before_window_actions)
        {
            auto expression_step_before_window = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), window_analysis_result.before_window_actions);
            expression_step_before_window->setStepDescription("Before WINDOW");
            query_plan.addStep(std::move(expression_step_before_window));
        }

        auto window_descriptions = window_analysis_result.window_descriptions;
        sortWindowDescriptions(window_descriptions);

        size_t window_descriptions_size = window_descriptions.size();

        const auto & settings = query_context->getSettingsRef();
        for (size_t i = 0; i < window_descriptions_size; ++i)
        {
            const auto & window_description = window_descriptions[i];

            /** We don't need to sort again if the input from previous window already
              * has suitable sorting. Also don't create sort steps when there are no
              * columns to sort by, because the sort nodes are confused by this. It
              * happens in case of `over ()`.
              */
            if (!window_description.full_sort_description.empty() &&
                (i == 0 || !sortDescriptionIsPrefix(window_description.full_sort_description, window_descriptions[i - 1].full_sort_description)))
            {
                auto sorting_step = std::make_unique<SortingStep>(
                    query_plan.getCurrentDataStream(),
                    window_description.full_sort_description,
                    settings.max_block_size,
                    0 /* LIMIT */,
                    SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode),
                    settings.max_bytes_before_remerge_sort,
                    settings.remerge_sort_lowered_memory_bytes_ratio,
                    settings.max_bytes_before_external_sort,
                    query_context->getTempDataOnDisk(),
                    settings.min_free_disk_space_for_temporary_data,
                    settings.optimize_sorting_by_input_stream_properties);

                sorting_step->setStepDescription("Sorting for window '" + window_description.window_name + "'");
                query_plan.addStep(std::move(sorting_step));
            }

            auto window_step = std::make_unique<WindowStep>(query_plan.getCurrentDataStream(), window_description, window_description.window_functions);
            window_step->setStepDescription("Window step for window '" + window_description.window_name + "'");
            query_plan.addStep(std::move(window_step));
        }
    }

    const auto & projection_analysis_result = expression_analysis_result.getProjection();
    auto expression_step_projection = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), projection_analysis_result.projection_actions);
    expression_step_projection->setStepDescription("Projection");
    query_plan.addStep(std::move(expression_step_projection));

    if (query_node.isDistinct())
    {
        const Settings & settings = planner_context->getQueryContext()->getSettingsRef();
        UInt64 limit_hint_for_distinct = 0;
        bool pre_distinct = true;

        SizeLimits limits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode);

        auto distinct_step = std::make_unique<DistinctStep>(
            query_plan.getCurrentDataStream(),
            limits,
            limit_hint_for_distinct,
            projection_analysis_result.projection_column_names,
            pre_distinct,
            settings.optimize_distinct_in_order);

        if (pre_distinct)
            distinct_step->setStepDescription("Preliminary DISTINCT");
        else
            distinct_step->setStepDescription("DISTINCT");

        query_plan.addStep(std::move(distinct_step));
    }

    if (expression_analysis_result.hasSort())
    {
        const auto & sort_analysis_result = expression_analysis_result.getSort();
        auto expression_step_before_order_by = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), sort_analysis_result.before_order_by_actions);
        expression_step_before_order_by->setStepDescription("Before ORDER BY");
        query_plan.addStep(std::move(expression_step_before_order_by));
    }

    QueryPlanStepPtr filling_step;
    SortDescription sort_description;

    if (query_node.hasOrderBy())
    {
        sort_description = extractSortDescription(query_node.getOrderByNode(), *planner_context);

        UInt64 limit = 0;

        const Settings & settings = planner_context->getQueryContext()->getSettingsRef();

        /// Merge the sorted blocks.
        auto sorting_step = std::make_unique<SortingStep>(
            query_plan.getCurrentDataStream(),
            sort_description,
            settings.max_block_size,
            limit,
            SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode),
            settings.max_bytes_before_remerge_sort,
            settings.remerge_sort_lowered_memory_bytes_ratio,
            settings.max_bytes_before_external_sort,
            planner_context->getQueryContext()->getTempDataOnDisk(),
            settings.min_free_disk_space_for_temporary_data,
            settings.optimize_sorting_by_input_stream_properties);

        sorting_step->setStepDescription("Sorting for ORDER BY");
        query_plan.addStep(std::move(sorting_step));

        NameSet column_names_with_fill;
        SortDescription fill_description;
        for (auto & description : sort_description)
        {
            if (description.with_fill)
            {
                fill_description.push_back(description);
                column_names_with_fill.insert(description.column_name);
            }
        }

        if (!fill_description.empty())
        {
            InterpolateDescriptionPtr interpolate_description;

            if (query_node.hasInterpolate())
            {
                auto interpolate_actions_dag = std::make_shared<ActionsDAG>();

                auto & interpolate_list_node = query_node.getInterpolate()->as<ListNode &>();
                auto & interpolate_list_nodes = interpolate_list_node.getNodes();

                if (interpolate_list_nodes.empty())
                {
                    auto query_plan_columns = query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();
                    for (auto & query_plan_column : query_plan_columns)
                    {
                        if (column_names_with_fill.contains(query_plan_column.name))
                            continue;

                        const auto * input_action_node = &interpolate_actions_dag->addInput(query_plan_column);
                        interpolate_actions_dag->getOutputs().push_back(input_action_node);
                    }
                }
                else
                {
                    for (auto & interpolate_node : interpolate_list_nodes)
                    {
                        auto & interpolate_node_typed = interpolate_node->as<InterpolateNode &>();

                        PlannerActionsVisitor planner_actions_visitor(planner_context);
                        auto expression_to_interpolate_expression_nodes = planner_actions_visitor.visit(interpolate_actions_dag, interpolate_node_typed.getExpression());
                        auto interpolate_expression_nodes = planner_actions_visitor.visit(interpolate_actions_dag, interpolate_node_typed.getInterpolateExpression());

                        if (expression_to_interpolate_expression_nodes.size() != 1)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expression to interpolate expected to have single action node");

                        if (interpolate_expression_nodes.size() != 1)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Interpolate expression expected to have single action node");

                        const auto * expression_to_interpolate = expression_to_interpolate_expression_nodes[0];
                        const auto & expression_to_interpolate_name = expression_to_interpolate->result_name;

                        const auto * interpolate_expression = interpolate_expression_nodes[0];
                        if (!interpolate_expression->result_type->equals(*expression_to_interpolate->result_type))
                        {
                            auto cast_type_name = expression_to_interpolate->result_type->getName();
                            Field cast_type_constant_value(cast_type_name);

                            ColumnWithTypeAndName column;
                            column.name = calculateConstantActionNodeName(cast_type_name);
                            column.column = DataTypeString().createColumnConst(0, cast_type_constant_value);
                            column.type = std::make_shared<DataTypeString>();

                            const auto * cast_type_constant_node = &interpolate_actions_dag->addColumn(std::move(column));

                            FunctionCastBase::Diagnostic diagnostic = {interpolate_expression->result_name, interpolate_expression->result_name};
                            FunctionOverloadResolverPtr func_builder_cast
                                = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl(std::move(diagnostic));

                            ActionsDAG::NodeRawConstPtrs children = {interpolate_expression, cast_type_constant_node};
                            interpolate_expression = &interpolate_actions_dag->addFunction(func_builder_cast, std::move(children), interpolate_expression->result_name);
                        }

                        const auto * alias_node = &interpolate_actions_dag->addAlias(*interpolate_expression, expression_to_interpolate_name);
                        interpolate_actions_dag->getOutputs().push_back(alias_node);
                    }

                    interpolate_actions_dag->removeUnusedActions();
                }

                Aliases empty_aliases;
                interpolate_description = std::make_shared<InterpolateDescription>(std::move(interpolate_actions_dag), empty_aliases);
            }

            filling_step = std::make_unique<FillingStep>(query_plan.getCurrentDataStream(), std::move(fill_description), interpolate_description);
        }
    }

    if (expression_analysis_result.hasLimitBy())
    {
        const auto & limit_by_analysis_result = expression_analysis_result.getLimitBy();
        auto expression_step_before_limit_by = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), limit_by_analysis_result.before_limit_by_actions);
        expression_step_before_limit_by->setStepDescription("Before LIMIT BY");
        query_plan.addStep(std::move(expression_step_before_limit_by));

        /// Constness of LIMIT BY limit is validated during query analysis stage
        UInt64 limit_by_limit = query_node.getLimitByLimit()->getConstantValue().getValue().safeGet<UInt64>();
        UInt64 limit_by_offset = 0;

        if (query_node.hasLimitByOffset())
        {
            /// Constness of LIMIT BY offset is validated during query analysis stage
            limit_by_offset = query_node.getLimitByOffset()->getConstantValue().getValue().safeGet<UInt64>();
        }

        auto limit_by_step = std::make_unique<LimitByStep>(query_plan.getCurrentDataStream(),
            limit_by_limit,
            limit_by_offset,
            limit_by_analysis_result.limit_by_column_names);
        query_plan.addStep(std::move(limit_by_step));
    }

    if (filling_step)
        query_plan.addStep(std::move(filling_step));

    if (query_context->getSettingsRef().extremes)
    {
        auto extremes_step = std::make_unique<ExtremesStep>(query_plan.getCurrentDataStream());
        query_plan.addStep(std::move(extremes_step));
    }

    UInt64 limit_offset = 0;
    if (query_node.hasOffset())
    {
        /// Constness of offset is validated during query analysis stage
        limit_offset = query_node.getOffset()->getConstantValue().getValue().safeGet<UInt64>();
    }

    if (query_node.hasLimit())
    {
        const Settings & settings = planner_context->getQueryContext()->getSettingsRef();
        bool always_read_till_end = settings.exact_rows_before_limit;
        bool limit_with_ties = query_node.isLimitWithTies();

        /// Constness of limit is validated during query analysis stage
        UInt64 limit_length = query_node.getLimit()->getConstantValue().getValue().safeGet<UInt64>();

        SortDescription limit_with_ties_sort_description;

        if (query_node.isLimitWithTies())
        {
            /// Validated during parser stage
            if (!query_node.hasOrderBy())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "LIMIT WITH TIES without ORDER BY");

            limit_with_ties_sort_description = sort_description;
        }

        auto limit = std::make_unique<LimitStep>(query_plan.getCurrentDataStream(),
            limit_length,
            limit_offset,
            always_read_till_end,
            limit_with_ties,
            limit_with_ties_sort_description);

        if (limit_with_ties)
            limit->setStepDescription("LIMIT WITH TIES");

        query_plan.addStep(std::move(limit));
    }
    else if (query_node.hasOffset())
    {
        auto offsets_step = std::make_unique<OffsetStep>(query_plan.getCurrentDataStream(), limit_offset);
        query_plan.addStep(std::move(offsets_step));
    }

    auto projection_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), projection_analysis_result.project_names_actions);
    projection_step->setStepDescription("Project names");
    query_plan.addStep(std::move(projection_step));

    addBuildSubqueriesForSetsStepIfNeeded(query_plan, select_query_options, planner_context);

    /// Extend lifetime of context, table locks, storages
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

}
