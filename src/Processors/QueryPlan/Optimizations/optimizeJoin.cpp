#include <Common/logger_useful.h>
#include <Common/safe_cast.h>

#include <Core/Joins.h>
#include <Core/Settings.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/TableJoin.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/SortingStep.h>

#include <Processors/QueryPlan/Optimizations/joinOrder.h>

#include <Storages/StorageMemory.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <ranges>
#include <base/types.h>

namespace ProfileEvents
{
    extern const Event JoinOptimizeMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool use_statistics;
    extern const SettingsBool use_hash_table_stats_for_join_reordering;
}

RelationStats getDummyStats(ContextPtr context, const String & table_name);
RelationStats getDummyStats(const String & dummy_stats_str, const String & table_name);

namespace QueryPlanOptimizations
{

const size_t MAX_ROWS = std::numeric_limits<size_t>::max();
static String dumpStatsForLogs(const RelationStats & stats);

static size_t functionDoesNotChangeNumberOfValues(std::string_view function_name, size_t num_args)
{
    if (function_name == "materialize" || function_name == "_CAST" || function_name == "CAST" || function_name == "toNullable")
        return 1;
    if (function_name == "firstNonDefault")
        return num_args;
    return 0;
}

NameSet backTrackColumnsInDag(const String & input_name, const ActionsDAG & actions)
{
    NameSet output_names;

    std::unordered_set<const ActionsDAG::Node *> input_nodes;
    for (const auto * node : actions.getInputs())
    {
        if (input_name == node->result_name)
            input_nodes.insert(node);
    }

    std::unordered_set<const ActionsDAG::Node *> visited_nodes;
    for (const auto * out_node : actions.getOutputs())
    {

        std::stack<const ActionsDAG::Node *> nodes_to_process;
        nodes_to_process.push(out_node);

        while (!nodes_to_process.empty())
        {
            const auto * node = nodes_to_process.top();
            nodes_to_process.pop();

            auto [_, inserted] = visited_nodes.insert(node);
            if (!inserted)
            {
                /// Node was already visited, check if it was an input or if it was already remapped to and input
                if (input_nodes.contains(node))
                    output_names.insert(out_node->result_name);
                break;
            }

            if (input_nodes.contains(node))
            {
                /// We reached an input node so add the current output node name to list of remapped
                output_names.insert(out_node->result_name);
                /// Also add this output node to the list of inputs to handle more aliases pointing to it
                input_nodes.insert(out_node);
                break;
            }

            if (node->type == ActionsDAG::ActionType::ALIAS && node->children.size() == 1)
            {
                nodes_to_process.push(node->children[0]);
            }
            else if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base)
            {
                auto number_of_args = functionDoesNotChangeNumberOfValues(node->function_base->getName(), node->children.size());
                for (const auto * child : node->children)
                {
                    if (number_of_args == 0)
                        break;
                    number_of_args -= 1;
                    nodes_to_process.push(child);
                }
            }
        }
    }
    return output_names;
}

/// If we have stats for column names for storage we need to find corresponding internal column names
void remapColumnStats(std::unordered_map<String, ColumnStats> & mapped, const ActionsDAG & actions)
{
    std::unordered_map<String, ColumnStats> original = std::move(mapped);
    mapped = {};
    for (const auto & [name, value] : original)
    {
        for (const auto & remapped : backTrackColumnsInDag(name, actions))
            mapped[remapped] = value;
    }
}

struct RuntimeHashStatisticsContext
{
    std::unordered_map<const QueryPlan::Node *, UInt64> cache_keys;
    StatsCollectingParams params;

    RuntimeHashStatisticsContext(const QueryPlanOptimizationSettings & optimization_settings, const QueryPlan::Node & root_node)
        : params{
            /*key_=*/0,
            /*enable=*/ optimization_settings.collect_hash_table_stats_during_joins,
            optimization_settings.max_entries_for_hash_table_stats,
            optimization_settings.max_size_to_preallocate_for_joins}
    {
        if (optimization_settings.collect_hash_table_stats_during_joins)
        {
            cache_keys = calculateHashTableCacheKeys(root_node);
        }
    }

    UInt64 getCachedKey(const QueryPlan::Node * node)
    {
        if (auto it = cache_keys.find(node); it != cache_keys.end())
            return it->second;
        return 0;
    }

    std::optional<size_t> getCachedHint(const QueryPlan::Node * node)
    {
        if (auto cache_key = getCachedKey(node))
        {
            auto & hash_table_stats = getHashTablesStatistics<HashJoinEntry>();
            if (auto hint = hash_table_stats.getSizeHint(params.setKey(cache_key)))
                return hint->source_rows;
        }
        return {};
    }
};

RelationStats estimateAggregatingStepStats(const AggregatingStep & aggregating_step, const RelationStats & input_stats)
{
    const auto & aggregator_params = aggregating_step.getAggregatorParameters();
    std::optional<Float64> total_number_of_distinct_values = 1;
    RelationStats aggregation_stats;
    for (const auto & key : aggregator_params.keys)
    {
        auto key_stats = input_stats.column_stats.find(key);
        if (key_stats == input_stats.column_stats.end())
        {
            /// Cannot calculate total number of groups if we don't know NDV of any of the aggregation columns
            total_number_of_distinct_values.reset();
            continue;
        }

        UInt64 key_number_of_distinct_values = key_stats->second.num_distinct_values;

        if (input_stats.estimated_rows)
            key_number_of_distinct_values = std::min(key_number_of_distinct_values, *input_stats.estimated_rows);

        aggregation_stats.column_stats[key].num_distinct_values = key_number_of_distinct_values;

        /// For now assume that aggregation columns are independent, so multiply their NDVs
        if (total_number_of_distinct_values)
            *total_number_of_distinct_values *= static_cast<Float64>(key_number_of_distinct_values);
    }

    if (total_number_of_distinct_values && input_stats.estimated_rows)
        total_number_of_distinct_values = std::min(*total_number_of_distinct_values, Float64(*input_stats.estimated_rows));
    else
        total_number_of_distinct_values = input_stats.estimated_rows;

    aggregation_stats.estimated_rows = total_number_of_distinct_values;

    return aggregation_stats;
}

RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter = nullptr)
{
    IQueryPlanStep * step = node.step.get();
    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(step))
    {
        String table_display_name = reading->getStorageID().getTableName();

        if (reading->getContext()->getSettingsRef()[Setting::use_statistics])
        {
            if (auto estimator = reading->getConditionSelectivityEstimator(reading->getAllColumnNames()))
            {
                auto prewhere_info = reading->getPrewhereInfo();
                const ActionsDAG::Node * prewhere_node = prewhere_info
                    ? static_cast<const ActionsDAG::Node *>(prewhere_info->prewhere_actions.tryFindInOutputs(prewhere_info->prewhere_column_name))
                    : nullptr;
                auto relation_profile = estimator->estimateRelationProfile(reading->getStorageMetadata(), filter, prewhere_node);
                RelationStats stats {.estimated_rows = relation_profile.rows, .column_stats = relation_profile.column_stats, .table_name = table_display_name};
                LOG_TRACE(getLogger("optimizeJoin"), "estimate statistics {}", dumpStatsForLogs(stats));
                return stats;
            }
        }
        if (auto dummy_stats = getDummyStats(reading->getContext(), table_display_name); !dummy_stats.table_name.empty())
            return dummy_stats;

        ReadFromMergeTree::AnalysisResultPtr analyzed_result = nullptr;
        analyzed_result = analyzed_result ? analyzed_result : reading->getAnalyzedResult();
        analyzed_result = analyzed_result ? analyzed_result : reading->selectRangesToRead();
        if (!analyzed_result)
            return RelationStats{.estimated_rows = {}, .table_name = table_display_name};

        bool is_filtered_by_index = false;
        UInt64 total_parts = 0;
        UInt64 total_granules = 0;
        for (const auto & idx_stat : analyzed_result->index_stats)
        {
            /// We expect the first element to be an index with None type, which is used to estimate the total amount of data in the table.
            /// Further index_stats are used to estimate amount of filtered data after applying the index.
            if (ReadFromMergeTree::IndexType::None == idx_stat.type)
            {
                total_parts = idx_stat.num_parts_after;
                total_granules = idx_stat.num_granules_after;
                continue;
            }

            is_filtered_by_index = is_filtered_by_index
                || (total_parts && idx_stat.num_parts_after < total_parts)
                || (total_granules && idx_stat.num_granules_after < total_granules);

            if (is_filtered_by_index)
                break;
        }
        bool has_filter = filter || reading->getPrewhereInfo();

        /// If any conditions are pushed down to storage but not used in the index,
        /// we cannot precisely estimate the row count
        if (has_filter && !is_filtered_by_index)
            return RelationStats{.estimated_rows = {}, .table_name = table_display_name};

        return RelationStats{.estimated_rows = analyzed_result->selected_rows, .table_name = table_display_name};
    }

    if (const auto * reading = typeid_cast<const ReadFromMemoryStorageStep *>(step))
    {
        UInt64 estimated_rows = reading->getStorage()->totalRows({}).value_or(0);
        String table_display_name = reading->getStorage()->getName();
        return RelationStats{.estimated_rows = estimated_rows, .table_name = table_display_name};
    }

    if (const auto * reading = typeid_cast<const CommonSubplanReferenceStep *>(step))
    {
        return estimateReadRowsCount(*reading->getSubplanReferenceRoot(), filter);
    }

    if (const auto * join_step = typeid_cast<const JoinStepLogical *>(step); join_step && join_step->isOptimized())
    {
        return RelationStats{
            .estimated_rows = join_step->getResultRowsEstimation(),
            .column_stats = {},
            .table_name = join_step->getReadableRelationName()};
    }

    if (node.children.size() != 1)
        return {};

    if (const auto * limit_step = typeid_cast<const LimitStep *>(step))
    {
        auto estimated = estimateReadRowsCount(*node.children.front(), filter);
        auto limit = limit_step->getLimit();
        if (!estimated.estimated_rows || estimated.estimated_rows > limit)
            estimated.estimated_rows = limit;
        return estimated;
    }

    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(step))
    {
        auto stats = estimateReadRowsCount(*node.children.front(), filter);
        remapColumnStats(stats.column_stats, expression_step->getExpression());
        return stats;
    }

    if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
    {
        const auto & dag = filter_step->getExpression();
        const auto * predicate = static_cast<const ActionsDAG::Node *>(dag.tryFindInOutputs(filter_step->getFilterColumnName()));
        auto stats = estimateReadRowsCount(*node.children.front(), predicate);
        remapColumnStats(stats.column_stats, filter_step->getExpression());
        return stats;
    }

    if (const auto * aggregating_step = typeid_cast<const AggregatingStep *>(step))
    {
        auto stats = estimateReadRowsCount(*node.children.front(), filter);
        auto aggregation_stats = estimateAggregatingStepStats(*aggregating_step, stats);
        return aggregation_stats;
    }

    if (const auto * sorting_step = typeid_cast<const SortingStep *>(step))
    {
        auto stats = estimateReadRowsCount(*node.children.front(), filter);
        if (sorting_step->getLimit())
        {
            if (!stats.estimated_rows || stats.estimated_rows > sorting_step->getLimit())
                stats.estimated_rows = sorting_step->getLimit();
        }
        return stats;
    }

    return {};
}


bool optimizeJoinLegacy(QueryPlan::Node & node, QueryPlan::Nodes & /*nodes*/, const QueryPlanOptimizationSettings &)
{
    auto * join_step = typeid_cast<JoinStep *>(node.step.get());
    if (!join_step || node.children.size() != 2 || join_step->isOptimized())
        return false;

    const auto & join = join_step->getJoin();
    if (join->pipelineType() != JoinPipelineType::FillRightFirst || !join->isCloneSupported())
        return true;

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::JoinOptimizeMicroseconds);

    const auto & table_join = join->getTableJoin();

    /// Algorithms other than HashJoin may not support all JOIN kinds, so changing from LEFT to RIGHT is not always possible
    bool allow_outer_join = typeid_cast<const HashJoin *>(join.get());
    if (table_join.kind() != JoinKind::Inner && !allow_outer_join)
        return true;

    /// fixme: USING clause handled specially in join algorithm, so swap breaks it
    /// fixme: Swapping for SEMI and ANTI joins should be alright, need to try to enable it and test
    if (table_join.hasUsing() || table_join.strictness() != JoinStrictness::All)
        return true;

    bool need_swap = false;
    if (!join_step->swap_join_tables.has_value())
    {
        auto lhs_extimation = estimateReadRowsCount(*node.children[0]).estimated_rows;
        auto rhs_extimation = estimateReadRowsCount(*node.children[1]).estimated_rows;
        LOG_TRACE(getLogger("optimizeJoinLegacy"), "Left table estimation: {}, right table estimation: {}",
            lhs_extimation ? toString(lhs_extimation.value()) : "unknown",
            rhs_extimation ? toString(rhs_extimation.value()) : "unknown");

        if (lhs_extimation && rhs_extimation && lhs_extimation < rhs_extimation)
            need_swap = true;
    }
    else if (join_step->swap_join_tables.value())
    {
        need_swap = true;
    }

    if (!need_swap)
        return true;

    const auto & headers = join_step->getInputHeaders();
    if (headers.size() != 2)
        return true;

    auto left_stream_input_header = headers.front();
    auto right_stream_input_header = headers.back();

    auto updated_table_join = std::make_shared<TableJoin>(table_join);
    updated_table_join->swapSides();
    auto updated_join = join->clone(updated_table_join, right_stream_input_header, left_stream_input_header);

    /// After swapping, the join output may lose columns because TableJoin::swapSides
    /// swaps result_columns_from_left_table with columns_added_by_join, and the join
    /// algorithm may filter different columns from the (now swapped) left input.
    /// If any column required by downstream steps would be missing, skip the swap.
    auto original_output = join_step->getOutputHeader();
    auto swapped_algorithm_header = JoiningTransform::transformHeader(*right_stream_input_header, updated_join);
    for (const auto & col : *original_output)
    {
        if (!swapped_algorithm_header.has(col.name))
            return true;
    }

    join_step->setJoin(std::move(updated_join), /* swap_streams= */ true);

    return true;
}

bool convertLogicalJoinToPhysical(
    QueryPlan::Node & node,
    QueryPlan::Nodes & nodes,
    const QueryPlanOptimizationSettings & optimization_settings)
{
    bool keep_logical = optimization_settings.keep_logical_steps;
    if (keep_logical)
        return false;
    if (!typeid_cast<JoinStepLogical *>(node.step.get()))
        return false;
    if (node.children.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have exactly 2 children, but has {}", node.children.size());

    JoinStepLogical::buildPhysicalJoin(node, optimization_settings, nodes);

    return true;
}

struct QueryGraphBuilder
{
    JoinExpressionActions expression_actions;

    std::vector<RelationStats> relation_stats;
    std::vector<QueryPlan::Node *> inputs;

    std::vector<JoinActionRef> join_edges;

    /// Outer joined relation should be joined after all other relations involved in its join expressions.
    /// It is joined with specified join kind.
    /// The `join_kinds` maps (join relation index) -> (set of relations it depends on, join kind)
    std::unordered_map<size_t, std::pair<BitSet, JoinKind>> join_kinds;
    std::unordered_map<size_t, ActionsDAG::NodeRawConstPtrs> type_changes;
    std::unordered_map<JoinActionRef, size_t> pinned;

    struct BuilderContext
    {
        const QueryPlanOptimizationSettings & optimization_settings;
        RuntimeHashStatisticsContext statistics_context;
        JoinSettings join_settings;
        SortingStep::Settings sorting_settings;
        String dummy_stats;

        BuilderContext(
            const QueryPlanOptimizationSettings & optimization_settings_,
            const QueryPlan::Node & root_node,
            const JoinSettings & join_settings_,
            const SortingStep::Settings & sorting_settings_)
            : optimization_settings(optimization_settings_)
            , statistics_context(optimization_settings_, root_node)
            , join_settings(join_settings_)
            , sorting_settings(sorting_settings_)
        {}
    };

    std::shared_ptr<BuilderContext> context;

    explicit QueryGraphBuilder(std::shared_ptr<BuilderContext> context_)
        : context(std::move(context_)) {}

    QueryGraphBuilder(const QueryPlanOptimizationSettings & optimization_settings_, const QueryPlan::Node & root_node,
                      const JoinSettings & join_settings_, const SortingStep::Settings & sorting_settings_)
        : context(std::make_shared<BuilderContext>(optimization_settings_, root_node, join_settings_, sorting_settings_))
    {}

    bool hasCompatibleSettings(const JoinStepLogical & join_step) const
    {
        return context && join_step.getJoinSettings() == context->join_settings; // && join_step.getSortingSettings() == context->sorting_settings;
    }
};

void uniteGraphs(QueryGraphBuilder & lhs, QueryGraphBuilder rhs)
{
    size_t shift = lhs.relation_stats.size();

    auto rhs_edges_raw = std::ranges::to<std::vector>(rhs.join_edges | std::views::transform([](const auto & e) { return e.getNode(); }));
    auto rhs_pinned_raw = std::ranges::to<std::unordered_map>(rhs.pinned | std::views::transform([](const auto & e) { return std::make_pair(e.first.getNode(), e.second); }));

    auto [rhs_actions_dag, rhs_expression_sources] = rhs.expression_actions.detachActionsDAG();

    lhs.expression_actions.getActionsDAG()->unite(std::move(rhs_actions_dag));
    for (auto & [node, sources] : rhs_expression_sources)
        sources.shift(shift);
    lhs.expression_actions.setNodeSources(rhs_expression_sources);

    lhs.relation_stats.append_range(std::move(rhs.relation_stats));
    lhs.inputs.append_range(std::move(rhs.inputs));

    lhs.join_edges.append_range(rhs_edges_raw | std::views::transform([&](auto p) { return JoinActionRef(p, lhs.expression_actions); }));

    for (auto [id, restriction] : rhs.join_kinds)
    {
        restriction.first.shift(shift);
        lhs.join_kinds[id + shift] = restriction;
    }

    for (auto && [sources, nodes] : rhs.type_changes)
        lhs.type_changes[sources + shift] = std::move(nodes);

    for (auto [action, pin] : rhs_pinned_raw)
        lhs.pinned[JoinActionRef(action, lhs.expression_actions)] = pin + shift;
}

void buildQueryGraph(QueryGraphBuilder & query_graph, QueryPlan::Node & node, QueryPlan::Nodes & nodes, int join_steps_limit);

static String dumpStatsForLogs(const RelationStats & stats)
{
    return fmt::format("{}: {} rows, columns: [{}]",
        stats.table_name.empty() ? "<unknown>" : stats.table_name,
        stats.estimated_rows ? toString(stats.estimated_rows.value()) : "unknown",
        fmt::join(stats.column_stats | std::views::transform(
            [](const auto & p)
            {
                return fmt::format("{}: {}", p.first, p.second.num_distinct_values);
            }), ", "));
}


static bool isTrivialStep(const QueryPlan::Node * node)
{
    if (node->children.size() != 1)
        return false;

    auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get());
    if (!expression_step)
        return false;

    return isPassthroughActions(expression_step->getExpression());
}

void optimizeJoinLogicalImpl(JoinStepLogical * join_step, QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings);

constexpr bool isInnerOrCross(JoinKind kind)
{
    return kind == JoinKind::Inner || kind == JoinKind::Cross || kind == JoinKind::Comma;
}

size_t addChildQueryGraph(QueryGraphBuilder & graph, QueryPlan::Node * node, QueryPlan::Nodes & nodes, const String & label, int join_steps_limit)
{
    if (isTrivialStep(node))
        node = node->children[0];

    auto * child_join_step = typeid_cast<JoinStepLogical *>(node->step.get());
    if (child_join_step && !child_join_step->isOptimized())
    {
        auto child_join_kind = child_join_step->getJoinOperator().kind;
        bool allow_child_join_kind = isInnerOrCross(child_join_kind) || isLeft(child_join_kind) || isRight(child_join_kind);
        allow_child_join_kind = allow_child_join_kind && child_join_step->getJoinOperator().strictness == JoinStrictness::All;
        if (graph.hasCompatibleSettings(*child_join_step) && join_steps_limit > 1 && allow_child_join_kind)
        {
            QueryGraphBuilder child_graph(graph.context);
            buildQueryGraph(child_graph, *node, nodes, join_steps_limit);
            size_t count = child_graph.inputs.size();
            uniteGraphs(graph, std::move(child_graph));
            return count;
        }
        /// Optimize child subplan before continuing to get size estimation
        optimizeJoinLogicalImpl(child_join_step, *node, nodes, graph.context->optimization_settings);
    }

    graph.inputs.push_back(node);
    RelationStats stats = estimateReadRowsCount(*node);

    std::optional<size_t> num_rows_from_cache = graph.context->statistics_context.getCachedHint(node);
    if (graph.context->join_settings.use_hash_table_stats_for_join_reordering && num_rows_from_cache)
        stats.estimated_rows = std::min<UInt64>(stats.estimated_rows.value_or(MAX_ROWS), num_rows_from_cache.value());

    if (!label.empty())
        stats.table_name = label;

    LOG_TRACE(getLogger("optimizeJoin"), "Estimated statistics{} for {} {}",
        num_rows_from_cache.has_value() ? " (from cache)" : "",
        node->step->getName(), dumpStatsForLogs(stats));
    graph.relation_stats.push_back(stats);
    return 1;
}

void buildQueryGraph(QueryGraphBuilder & query_graph, QueryPlan::Node & node, QueryPlan::Nodes & nodes, int join_steps_limit)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical expected");
    if (node.children.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have exactly 2 children, but has {}", node.children.size());

    QueryPlan::Node * lhs_plan = node.children[0];
    QueryPlan::Node * rhs_plan = node.children[1];
    auto [lhs_label, rhs_label] = join_step->getInputLabels();
    auto join_kind = join_step->getJoinOperator().kind;

    auto type_changing_sides = join_step->typeChangingSides();
    bool allow_left_subgraph = !type_changing_sides.contains(JoinTableSide::Left) && (isInnerOrCross(join_kind) || isLeft(join_kind));
    size_t lhs_count = addChildQueryGraph(query_graph, lhs_plan, nodes, lhs_label, allow_left_subgraph ? join_steps_limit - 1 : 0);
    bool allow_right_subgraph = !type_changing_sides.contains(JoinTableSide::Right) && (isInnerOrCross(join_kind) || isRight(join_kind));
    size_t rhs_count = addChildQueryGraph(query_graph, rhs_plan, nodes, rhs_label, allow_right_subgraph ? static_cast<int>(join_steps_limit - lhs_count) : 0);

    size_t total_inputs = query_graph.inputs.size();
    if (join_kind == JoinKind::Cross || join_kind == JoinKind::Comma)
        query_graph.join_kinds[0] = std::make_pair(BitSet{}, JoinKind::Cross);

    chassert(lhs_count && rhs_count && lhs_count + rhs_count == total_inputs && query_graph.relation_stats.size() == total_inputs);

    auto [expression_actions, join_operator] = join_step->detachExpressions();

    auto get_raw_nodes = std::views::transform([](const auto & ref) { return ref.getNode(); });
    auto join_expression = std::ranges::to<std::vector>(join_operator.expression | get_raw_nodes);
    auto residual_filter = std::ranges::to<std::vector>(join_operator.residual_filter | get_raw_nodes);

    auto [expression_actions_dag, expression_actions_sources] = expression_actions.detachActionsDAG();

    auto existing_outputs = std::ranges::to<std::unordered_set>(query_graph.expression_actions.getActionsDAG()->getOutputs());

    ActionsDAG::NodeMapping node_mapping;
    query_graph.expression_actions.getActionsDAG()->mergeInplace(std::move(expression_actions_dag), node_mapping, true);

    ActionsDAG::NodeRawConstPtrs join_outputs = query_graph.expression_actions.getActionsDAG()->getOutputs();

    JoinExpressionActions::NodeToSourceMapping new_sources;
    for (const auto & [old_node, sources] : expression_actions_sources)
    {
        const auto & new_node_entry = node_mapping.try_emplace(old_node, old_node);
        const auto * new_node = new_node_entry.first->second;
        if (BitSet(sources).set(0, false).set(1, false))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expression node {} is not a binary join: {}", old_node->result_name, toString(sources));
        if (lhs_count == 1 && sources.count() == 1 && sources.test(0))
            new_sources[new_node] = BitSet().set(0);
        if (rhs_count == 1 && sources.count() == 1 && sources.test(1))
            new_sources[new_node] = BitSet().set(total_inputs - 1);
    }
    query_graph.expression_actions.setNodeSources(new_sources);

    BitSet left_mask = BitSet::allSet(lhs_count);
    BitSet right_mask = BitSet::allSet(rhs_count);
    right_mask.shift(lhs_count);

    ActionsDAG::NodeRawConstPtrs left_changes_types;
    ActionsDAG::NodeRawConstPtrs right_changes_types;
    for (const auto * out_node : join_outputs)
    {
        if (out_node->type == ActionsDAG::ActionType::INPUT ||
            out_node->type == ActionsDAG::ActionType::COLUMN ||
            existing_outputs.contains(out_node))
            continue;

        auto source = JoinActionRef(out_node, query_graph.expression_actions).getSourceRelations();
        auto rel_id = source.getSingleBit();
        if (!rel_id.has_value())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot determine source relations for node {}", out_node->result_name);

        if (rel_id == 0)
            left_changes_types.push_back(out_node);
        else
            right_changes_types.push_back(out_node);
    }

    if (!left_changes_types.empty())
        query_graph.type_changes[0] = std::move(left_changes_types);
    if (!right_changes_types.empty())
        query_graph.type_changes[total_inputs - 1] = std::move(right_changes_types);

    /// During-join predicates cannot be pushed past preserved-row tables;
    /// After-join predicates (those in WHERE) cannot be pushed past null-supplying tables.
    BitSet join_expression_sources;
    for (const auto * old_node : join_expression)
    {
        const auto & new_node_entry = node_mapping.try_emplace(old_node, old_node);
        const auto * new_node = new_node_entry.first->second;
        auto & edge = query_graph.join_edges.emplace_back(new_node, query_graph.expression_actions);

        /// Collect all sources from join expressions
        join_expression_sources |= edge.getSourceRelations();

        if (isRightOrFull(join_kind))
        {
            query_graph.pinned[edge] = 0;
        }
        else if (isLeftOrFull(join_kind))
        {
            query_graph.pinned[edge] = total_inputs - 1;
        }
        else
        {
            auto sources = edge.getSourceRelations();
            for (auto rel_id : sources)
            {
                auto it = query_graph.join_kinds.find(rel_id);
                if (it != query_graph.join_kinds.end())
                {
                    query_graph.pinned[edge] = total_inputs - 1;
                    break;
                }
            }
        }
    }

    if (isRightOrFull(join_kind))
    {
        if (lhs_count != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical with RIGHT or FULL join must have exactly one left input, but has {}", lhs_count);
        join_expression_sources.set(0, false);
        query_graph.join_kinds[0] = std::make_pair(join_expression_sources, join_kind);
    }
    if (isLeftOrFull(join_kind))
    {
        if (rhs_count != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical with LEFT or FULL join must have exactly one right input, but has {}", rhs_count);
        join_expression_sources.set(total_inputs - 1, false);
        query_graph.join_kinds[total_inputs - 1] = std::make_pair(join_expression_sources, join_kind);
    }

    if (!residual_filter.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Residual filter is not supported in join reorder");
}

static std::vector<DPJoinEntry *> getJoinTreePostOrderSequence(DPJoinEntryPtr root)
{
    std::vector<DPJoinEntry *> result;
    result.reserve(root->relations.count() * 2);

    std::vector<DPJoinEntry *> stack;
    stack.push_back(root.get());

    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        if (!node->isLeaf())
        {
            if (!node->left || !node->right)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Join node should have both left and right children");

            /// In post-order, we want LEFT -> RIGHT -> NODE
            /// Since we're using a stack (LIFO), we push RIGHT then LEFT
            stack.push_back(node->right.get());
            stack.push_back(node->left.get());
        }

        result.push_back(node);
    }

    /// Reverse the order to get post-order traversal
    std::reverse(result.begin(), result.end());

    return result;
}

/// Find single input node that is parent of the node
/// Only aliases and cast/toNullable functions are allowed in the path, otherwise an logical error is thrown
static const ActionsDAG::Node * trackInputColumn(const ActionsDAG::Node * node)
{
    while (!node->children.empty())
    {

        if (node->type == ActionsDAG::ActionType::FUNCTION)
        {
            const auto & function_name = node->function_base->getName();
            if (function_name != "toNullable" && function_name != "_CAST" && function_name != "CAST")
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} is a function '{}', expected toNullable or CAST",
                    node->result_name, function_name);
        }

        size_t non_const_children = 0;
        for (const auto * child_node : node->children)
        {
            if (child_node->type == ActionsDAG::ActionType::COLUMN)
                continue;
            node = child_node;
            non_const_children++;
        }

        if (non_const_children != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} has {} non const children, expected 1", node->result_name, non_const_children);
    }
    return node;
}

constexpr bool isSwapOnlyJoinKind(JoinKind kind)
{
    return kind == JoinKind::Full;
}

constexpr bool isSwapOnlyJoinStrictness(JoinStrictness strictness)
{
    return strictness == JoinStrictness::Any || strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti;
}

QueryPlan::Node chooseJoinOrder(QueryGraphBuilder query_graph_builder, QueryPlan::Nodes & nodes, JoinStrictness join_strictness)
{
    QueryGraph query_graph;
    query_graph.relation_stats = std::move(query_graph_builder.relation_stats);
    query_graph.edges = std::move(query_graph_builder.join_edges);
    query_graph.join_kinds = std::move(query_graph_builder.join_kinds);
    query_graph.pinned = std::move(query_graph_builder.pinned);

    LOG_DEBUG(&Poco::Logger::get("QueryPlanOptimizations"), "Optimizing join order for query graph with {} relations", query_graph.relation_stats.size());

    std::unordered_map<BitSet, String> relation_names;
    for (size_t i = 0; i < query_graph.relation_stats.size(); ++i)
    {
        const auto & table_name = query_graph.relation_stats[i].table_name;
        auto estimated_count = query_graph.relation_stats[i].estimated_rows;
        String estimation = estimated_count ? fmt::format("[{}]", estimated_count.value()) : "";
        if (!table_name.empty())
            relation_names[BitSet().set(i)] = fmt::format("{}{}", table_name, estimation);
        else
            relation_names[BitSet().set(i)] = fmt::format("R{}{}", i, estimation);
    }

    auto global_expression_actions = std::move(query_graph_builder.expression_actions);
    auto global_actions_dag = global_expression_actions.getActionsDAG();
    if (!global_actions_dag)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global expression actions DAG is not set");

    const auto & optimization_settings = query_graph_builder.context->optimization_settings;

    auto optimized = optimizeJoinOrder(std::move(query_graph), optimization_settings);
    auto sequence = getJoinTreePostOrderSequence(optimized);

    if (sequence.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join tree is empty");

    /// Mapping from node to maximal step position where it is used
    /// It's used to drop unused expressions
    std::unordered_map<const ActionsDAG::Node *, size_t> usage_level_map;
    {
        std::deque<std::pair<const ActionsDAG::Node *, size_t>> stack;

        /// Join expressions used by i-th join step
        for (size_t i = 0; i < sequence.size(); ++i)
        {
            const auto & entry = sequence[i];
            for (const auto & action : entry->join_operator.expression)
                stack.emplace_back(action.getNode(), i);
            for (const auto & action : entry->join_operator.residual_filter)
                stack.emplace_back(action.getNode(), i);
        }
        /// Outputs at the end
        for (const auto & out : global_actions_dag->getOutputs())
            stack.emplace_back(out, sequence.size());

        while (!stack.empty())
        {
            auto [node, level] = stack.back();
            stack.pop_back();

            /// Stack is sorted with respect to level
            /// If we processed node it means that it and its children used by higher level
            auto [_, inserted] = usage_level_map.try_emplace(node, level);
            if (!inserted)
                continue;

            for (const auto * child : node->children)
                stack.push_back(std::make_pair(child, level));
        }
    }

    std::stack<QueryPlan::Node *> nodeStack;
    auto & input_nodes = query_graph_builder.inputs;

    if (!query_graph_builder.context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryGraphBuilder context is not set");

    auto join_settings = std::move(query_graph_builder.context->join_settings);
    auto sorting_settings = std::move(query_graph_builder.context->sorting_settings);

    /// After applying OUTER joins some columns may change their types (in case of join_use_nulls or JOIN USING)
    /// We need to track this change, this map tracks input columns and how they are transformed
    /// Each next step uses mapped columns as inputs

    /// Input in global dag -> it's position
    std::unordered_map<const ActionsDAG::Node *, size_t> input_node_map;

    /// input_position -> (relation no, input)
    std::vector<std::pair<size_t, const ActionsDAG::Node *>> current_input_nodes;

    const auto & global_inputs = global_actions_dag->getInputs();
    for (size_t input_idx = 0; input_idx < global_inputs.size(); ++input_idx)
    {
        const auto * input = global_inputs[input_idx];
        auto src_rels = JoinActionRef(input, global_expression_actions).getSourceRelations();
        auto rel_idx = src_rels.getSingleBit();
        if (!rel_idx.has_value())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Input node {} has {} source relations, expected 1", input->result_name, toString(src_rels));

        current_input_nodes.emplace_back(rel_idx.value(), input);
        input_node_map[input] = input_idx;
    }

    for (size_t entry_idx = 0; entry_idx < sequence.size(); ++entry_idx)
    {
        auto * entry = sequence[entry_idx];
        if (entry->isLeaf())
        {
            /// Base relation, use this step as input node
            size_t relation_id = safe_cast<size_t>(entry->relation_id);
            if (relation_id >= input_nodes.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid relation id: {}, input nodes size: {}", relation_id, input_nodes.size());
            nodeStack.push(input_nodes[relation_id]);
        }
        else
        {
            /// Combine two nodes from the stack into a single join operation
            auto * left_child_node = nodeStack.top();
            nodeStack.pop();
            auto * right_child_node = nodeStack.top();
            nodeStack.pop();

            auto join_operator = std::move(entry->join_operator);
            join_operator.strictness = join_strictness;
            auto left_rels = entry->left->relations;
            auto right_rels = entry->right->relations;

            bool has_prepared_storage_at_right = bool(typeid_cast<const JoinStepLogicalLookup *>(right_child_node->step.get()));
            bool has_prepared_storage_at_left = bool(typeid_cast<const JoinStepLogicalLookup *>(left_child_node->step.get()));

            auto lhs_estimation = entry->left->estimated_rows;
            auto rhs_estimation = entry->right->estimated_rows;

            bool swap_on_sizes = optimization_settings.join_swap_table.has_value()
                ? optimization_settings.join_swap_table.value()
                : entry->join_method == JoinMethod::Hash && lhs_estimation && rhs_estimation
                    && lhs_estimation.value() < rhs_estimation.value();

            bool flip_join = has_prepared_storage_at_left || (!has_prepared_storage_at_right && swap_on_sizes);

            /// fixme: USING clause handled specially in join algorithm, so swap breaks it
            /// At the time of writing, we're not able to swap inputs for ANY or SEMI partial merge join, because it only supports inner or left joins, but not right
            /// ANTI partial merge join is not supported for any join kind
            const bool partial_merge_join_can_be_selected = std::ranges::any_of(
                join_settings.join_algorithms,
                [](JoinAlgorithm alg)
                { return alg == JoinAlgorithm::PARTIAL_MERGE || alg == JoinAlgorithm::PREFER_PARTIAL_MERGE || alg == JoinAlgorithm::AUTO; });
            const bool should_worry_about_partial_merge_join = partial_merge_join_can_be_selected
                && (!MergeJoin::isSupported(join_operator.kind, join_operator.strictness)
                    || !MergeJoin::isSupported(reverseJoinKind(join_operator.kind), join_operator.strictness));
            const bool suitable_swap_only_join = isSwapOnlyJoinStrictness(join_operator.strictness) && !should_worry_about_partial_merge_join;
            if (join_operator.strictness != JoinStrictness::All && !suitable_swap_only_join)
                flip_join = false;

            if (flip_join)
            {
                /// For hash joins, we want to keep the smaller side on the right
                std::swap(left_rels, right_rels);
                std::swap(left_child_node, right_child_node);
                std::swap(lhs_estimation, rhs_estimation);
                join_operator.kind = reverseJoinKind(join_operator.kind);
            }

            auto left_header_ptr = left_child_node->step->getOutputHeader();
            auto right_header_ptr = right_child_node->step->getOutputHeader();

            const auto & left_header = *left_header_ptr;
            const auto & right_header = *right_header_ptr;

            ActionsDAG::NodeRawConstPtrs required_output_nodes;

            /// input pos -> new input node
            std::unordered_map<size_t, const ActionsDAG::Node *> current_step_type_changes;

            for (auto rel_id : {left_rels.getSingleBit(), right_rels.getSingleBit()})
            {
                if (!rel_id.has_value())
                    continue;
                const auto & new_inputs = query_graph_builder.type_changes[rel_id.value()];
                for (const auto * new_input : new_inputs)
                {
                    const auto * input_node = trackInputColumn(new_input);
                    auto it = input_node_map.find(input_node);
                    if (it == input_node_map.end())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} not found in inputs of dag {}", input_node->result_name, global_actions_dag->dumpDAG());
                    current_step_type_changes[it->second] = new_input;
                }
            }

            auto joined_mask = entry->relations;
            ActionsDAG::NodeMapping current_inputs;
            for (size_t input_pos = 0; input_pos < current_input_nodes.size(); ++input_pos)
            {
                auto [rel_idx, input_node] = current_input_nodes[input_pos];
                if (!joined_mask.test(rel_idx) || !input_node)
                    continue;

                current_inputs[input_node] = input_node;

                const auto * out_node = input_node;
                if (auto it2 = current_step_type_changes.find(input_pos); it2 != current_step_type_changes.end())
                    out_node = it2->second;
                /// add input (possibly which changed type) to required output
                required_output_nodes.push_back(out_node);
            }

            for (const auto & action : join_operator.expression)
                required_output_nodes.push_back(action.getNode());
            for (const auto & action : join_operator.residual_filter)
                required_output_nodes.push_back(action.getNode());

            if (entry_idx == sequence.size() - 1)
            {
                for (const auto * output_node : global_actions_dag->getOutputs())
                    required_output_nodes.push_back(output_node);
            }

            JoinExpressionActions current_expression_actions(left_header, right_header,
                ActionsDAG::foldActionsByProjection(current_inputs, required_output_nodes));

            auto current_dag = current_expression_actions.getActionsDAG();
            auto & dag_outputs = current_dag->getOutputs();
            if (required_output_nodes.size() != dag_outputs.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Required output nodes size {} does not match current output nodes size in dag {}", required_output_nodes.size(), current_dag->dumpDAG());

            auto join_expression_map = std::ranges::to<ActionsDAG::NodeMapping>(std::views::zip(required_output_nodes, dag_outputs));
            for (auto & action : join_operator.expression)
            {
                const auto * mapped_node = join_expression_map[action.getNode()];
                if (!mapped_node)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} not found in current dag {}", action.getNode()->result_name, current_dag->dumpDAG());
                action = JoinActionRef(mapped_node, current_expression_actions);
            }

            /// Setup outputs after join
            dag_outputs.clear();
            for (auto [input_pos, new_input] : current_step_type_changes)
            {
                current_input_nodes.at(input_pos).second = new_input;
            }

            const ActionsDAG::Node * first_dropped_node = nullptr;
            size_t first_dropped_node_pos = 0;
            /// Columns returned from JOIN is input with possibly corrected type
            for (size_t input_pos = 0; input_pos < current_input_nodes.size(); ++input_pos)
            {
                auto & [rel_idx, input_node] = current_input_nodes[input_pos];
                if (!joined_mask.test(rel_idx) || !input_node)
                    continue;

                if (usage_level_map[input_node] <= entry_idx)
                {
                    if (!first_dropped_node)
                    {
                        auto mapped_it = join_expression_map.find(input_node);
                        if (mapped_it == join_expression_map.end())
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} not found in join expression map", input_node->result_name);
                        first_dropped_node = mapped_it->second;
                        first_dropped_node_pos = input_pos;
                    }
                    input_node = nullptr;
                    continue;
                }

                auto mapped_it = join_expression_map.find(input_node);
                if (mapped_it == join_expression_map.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} not found in join expression map", input_node->result_name);
                dag_outputs.push_back(mapped_it->second);
            }

            /// Last step, output should correspond to the global actions DAG
            if (entry_idx == sequence.size() - 1)
            {
                dag_outputs.clear();
                for (const auto * output_node : global_actions_dag->getOutputs())
                {
                    auto mapped_it = join_expression_map.find(output_node);
                    if (mapped_it == join_expression_map.end())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} not found in join expression map", output_node->result_name);
                    dag_outputs.push_back(mapped_it->second);
                }
            }

            if (dag_outputs.empty())
            {
                if (!first_dropped_node)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "No columns returned from join: {}", current_dag->dumpDAG());
                dag_outputs.push_back(first_dropped_node);
                current_input_nodes.at(first_dropped_node_pos).second = first_dropped_node;
            }

            if (!join_operator.residual_filter.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Residual filter is not supported in join reorder");

            auto join_step = std::make_unique<JoinStepLogical>(
                left_header_ptr,
                right_header_ptr,
                std::move(join_operator),
                std::move(current_expression_actions),
                dag_outputs,
                join_settings,
                sorting_settings);

            auto left_label = relation_names[left_rels];
            auto right_label = relation_names[right_rels];

            if (!right_label.empty() && right_rels.count() > 1)
                right_label = fmt::format("({})", right_label);

            join_step->setInputLabels(std::move(left_label), std::move(right_label));
            relation_names[entry->relations] = join_step->getReadableRelationName();

            join_step->setOptimized(entry->estimated_rows, lhs_estimation, rhs_estimation);

            auto right_table_key = query_graph_builder.context->statistics_context.getCachedKey(right_child_node);
            if (right_table_key)
                join_step->setRightHashTableCacheKey(right_table_key);

            auto & new_node = nodes.emplace_back();
            new_node.step = std::move(join_step);
            new_node.children = {left_child_node, right_child_node};
            nodeStack.push(&new_node);
        }
    }

    if (nodeStack.size() != 1 || nodeStack.top() != &nodes.back())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal join sequence produced: [{}]",
            fmt::join(sequence | std::views::transform([](const auto * e) { return e ? e->dump() : "null"; }), ", "));

    /// Return the current node by value and remove it from the nodes list
    /// Caller may put the node back into the list if needed or replace existing node
    auto result = std::move(nodes.back());
    nodes.pop_back();
    return result;
}

void optimizeJoinLogical(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step || join_step->isOptimized())
        return;

    if (node.children.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have exactly 2 children, but has {}", node.children.size());

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::JoinOptimizeMicroseconds);
    optimizeJoinLogicalImpl(join_step, node, nodes, optimization_settings);
}

void optimizeJoinLogicalImpl(JoinStepLogical * join_step, QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    for (auto * child : node.children)
    {
        if (auto * lookup_step = typeid_cast<JoinStepLogicalLookup *>(child->step.get()))
            lookup_step->optimize(optimization_settings);
    }

    const auto & join_operator = join_step->getJoinOperator();
    auto strictness = join_operator.strictness;
    auto kind = join_operator.kind;
    auto locality = join_operator.locality;
    if (!optimization_settings.query_plan_optimize_join_order_limit
        || (strictness != JoinStrictness::All && !isSwapOnlyJoinStrictness(strictness))
        || locality != JoinLocality::Unspecified
        || kind == JoinKind::Paste
        || !join_operator.residual_filter.empty()
    )
    {
        join_step->setOptimized();
        return;
    }

    QueryGraphBuilder query_graph_builder(optimization_settings, node, join_step->getJoinSettings(), join_step->getSortingSettings());
    query_graph_builder.context->dummy_stats = join_step->getDummyStats();

    int query_graph_size_limit = safe_cast<int>(optimization_settings.query_plan_optimize_join_order_limit);
    if ((isSwapOnlyJoinStrictness(strictness) || isSwapOnlyJoinKind(kind)) && query_graph_size_limit > 2)
        /// Do not reorder joins, only allow swap
        query_graph_size_limit = 2;

    buildQueryGraph(query_graph_builder, node, nodes, query_graph_size_limit);
    node = chooseJoinOrder(std::move(query_graph_builder), nodes, strictness);
}

}

}
