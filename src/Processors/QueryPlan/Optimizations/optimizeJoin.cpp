#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/StorageMemory.h>
#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/ReadFromSystemNumbersStep.h>
#include <Core/Settings.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>

#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Interpreters/FullSortingMergeJoin.h>

#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>

#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <ranges>
#include <Core/Joins.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Common/logger_useful.h>
#include <Common/safe_cast.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/QueryPlan.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 min_joined_block_size_bytes;
}

RelationStats getDummyStats(ContextPtr context, const String & table_name);

namespace QueryPlanOptimizations
{

static size_t functionDoesNotChangeNumberOfValues(std::string_view function_name, size_t num_args)
{
    if (function_name == "materialize" || function_name == "_CAST" || function_name == "CAST" || function_name == "toNullable")
        return 1;
    if (function_name == "firstTruthy")
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
                break;

            if (input_nodes.contains(node))
            {
                output_names.insert(out_node->result_name);
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

void remapColumnStats(std::unordered_map<String, ColumnStats> & mapped, const ActionsDAG & actions)
{
    std::unordered_map<String, ColumnStats> original = std::move(mapped);
    for (const auto & [name, value] : original)
    {
        for (const auto & remapped : backTrackColumnsInDag(name, actions))
            mapped[remapped] = std::move(value);
    }
}

struct StatisticsContext
{
    std::unordered_map<const QueryPlan::Node *, UInt64> cache_keys;
    StatsCollectingParams params;

    StatisticsContext(const QueryPlanOptimizationSettings & optimization_settings, const QueryPlan::Node & root_node)
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

    size_t getCachedHint(const QueryPlan::Node * node)
    {
        if (auto cache_key = getCachedKey(node))
        {
            auto & hash_table_stats = getHashTablesStatistics<HashJoinEntry>();
            if (auto hint = hash_table_stats.getSizeHint(params.setKey(cache_key)))
                return hint->source_rows;
        }
        return std::numeric_limits<size_t>::max();
    }
};

static RelationStats estimateReadRowsCount(QueryPlan::Node & node, bool has_filter = false)
{
    IQueryPlanStep * step = node.step.get();
    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(step))
    {
        String table_diplay_name = reading->getStorageID().getTableName();

        if (auto dummy_stats = getDummyStats(reading->getContext(), table_diplay_name); !dummy_stats.table_name.empty())
            return dummy_stats;

        ReadFromMergeTree::AnalysisResultPtr analyzed_result = nullptr;
        analyzed_result = analyzed_result ? analyzed_result : reading->getAnalyzedResult();
        analyzed_result = analyzed_result ? analyzed_result : reading->selectRangesToRead();
        if (!analyzed_result)
            return RelationStats{.estimated_rows = 0, .table_name = table_diplay_name};

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
        has_filter = has_filter || reading->getPrewhereInfo();

        /// If any conditions are pushed down to storage but not used in the index,
        /// we cannot precisely estimate the row count
        if (has_filter && !is_filtered_by_index)
            return RelationStats{.estimated_rows = 0, .table_name = table_diplay_name};

        return RelationStats{.estimated_rows = analyzed_result->selected_rows, .table_name = table_diplay_name};
    }

    if (const auto * reading = typeid_cast<const ReadFromMemoryStorageStep *>(step))
    {
        UInt64 estimated_rows = reading->getStorage()->totalRows({}).value_or(0);
        String table_diplay_name = reading->getStorage()->getName();
        return RelationStats{.estimated_rows = estimated_rows, .table_name = table_diplay_name};
    }

    if (const auto * reading = typeid_cast<const ReadFromSystemNumbersStep *>(step))
    {
        RelationStats relation_stats;
        relation_stats.estimated_rows = reading->getNumberOfRows();

        auto column_name = reading->getColumnName();
        relation_stats.column_stats[column_name].num_distinct_values = relation_stats.estimated_rows;
        relation_stats.table_name = reading->getStorageID().getTableName();

        return relation_stats;
    }

    if (node.children.size() != 1)
        return {};

    if (const auto * limit_step = typeid_cast<const LimitStep *>(step))
    {
        auto estimated = estimateReadRowsCount(*node.children.front(), has_filter);
        auto limit = limit_step->getLimit();
        if (estimated.estimated_rows == 0 || estimated.estimated_rows > limit)
            estimated.estimated_rows = limit;
        return estimated;
    }

    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(step))
    {
        auto stats = estimateReadRowsCount(*node.children.front(), has_filter);
        remapColumnStats(stats.column_stats, expression_step->getExpression());
        return stats;
    }

    if (const auto * expression_step = typeid_cast<const FilterStep *>(step))
    {
        auto stats = estimateReadRowsCount(*node.children.front(), true);
        remapColumnStats(stats.column_stats, expression_step->getExpression());
        return stats;
    }

    if (const auto * join_step = typeid_cast<const JoinStepLogical *>(step); join_step && join_step->isOptimized())
    {
        return RelationStats{
            .estimated_rows = join_step->getResultRowsEstimation(),
            .column_stats = {},
            .table_name = join_step->getReadableRelationName()};
    }

    return {};
}


bool optimizeJoinLegacy(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &)
{
    auto * join_step = typeid_cast<JoinStep *>(node.step.get());
    if (!join_step || node.children.size() != 2 || join_step->isOptimized())
        return false;

    const auto & join = join_step->getJoin();
    if (join->pipelineType() != JoinPipelineType::FillRightFirst || !join->isCloneSupported())
        return true;

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
        LOG_TRACE(getLogger("optimizeJoinLegacy"), "Left table estimation: {}, right table estimation: {}", lhs_extimation, rhs_extimation);

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
    join_step->setJoin(std::move(updated_join), /* swap_streams= */ true);
    return true;
}

void addSortingForMergeJoin(
    const FullSortingMergeJoin * join_ptr,
    QueryPlan::Node *& left_node,
    QueryPlan::Node *& right_node,
    QueryPlan::Nodes & nodes,
    const SortingStep::Settings & sort_settings,
    const JoinSettings & join_settings,
    const JoinOperator & join_operator)
{
    auto join_kind = join_operator.kind;
    auto join_strictness = join_operator.strictness;
    auto add_sorting = [&] (QueryPlan::Node *& node, const Names & key_names, JoinTableSide join_table_side)
    {
        SortDescription sort_description;
        sort_description.reserve(key_names.size());
        for (const auto & key_name : key_names)
            sort_description.emplace_back(key_name);

        auto sorting_step = std::make_unique<SortingStep>(
            node->step->getOutputHeader(), std::move(sort_description), 0 /*limit*/, sort_settings, true /*is_sorting_for_merge_join*/);
        sorting_step->setStepDescription(fmt::format("Sort {} before JOIN", join_table_side));
        node = &nodes.emplace_back(QueryPlan::Node{std::move(sorting_step), {node}});
    };

    auto crosswise_connection = CreateSetAndFilterOnTheFlyStep::createCrossConnection();
    auto add_create_set = [&](QueryPlan::Node *& node, const Names & key_names, JoinTableSide join_table_side)
    {
        auto creating_set_step = std::make_unique<CreateSetAndFilterOnTheFlyStep>(
            node->step->getOutputHeader(), key_names, join_settings.max_rows_in_set_to_optimize_join, crosswise_connection, join_table_side);
        creating_set_step->setStepDescription(fmt::format("Create set and filter {} joined stream", join_table_side));

        auto * step_raw_ptr = creating_set_step.get();
        node = &nodes.emplace_back(QueryPlan::Node{std::move(creating_set_step), {node}});
        return step_raw_ptr;
    };

    const auto & join_clause = join_ptr->getTableJoin().getOnlyClause();

    bool join_type_allows_filtering = (join_strictness == JoinStrictness::All || join_strictness == JoinStrictness::Any)
                                    && (isInner(join_kind) || isLeft(join_kind) || isRight(join_kind));


    auto has_non_const = [](const Block & block, const auto & keys)
    {
        for (const auto & key : keys)
        {
            const auto & column = block.getByName(key).column;
            if (column && !isColumnConst(*column))
                return true;
        }
        return false;
    };

    /// This optimization relies on the sorting that should buffer data from both streams before emitting any rows.
    /// Sorting on a stream with const keys can start returning rows immediately and pipeline may stuck.
    /// Note: it's also doesn't work with the read-in-order optimization.
    /// No checks here because read in order is not applied if we have `CreateSetAndFilterOnTheFlyStep` in the pipeline between the reading and sorting steps.
    bool has_non_const_keys = has_non_const(*left_node->step->getOutputHeader(), join_clause.key_names_left)
        && has_non_const(*right_node->step->getOutputHeader() , join_clause.key_names_right);

    if (join_settings.max_rows_in_set_to_optimize_join > 0 && join_type_allows_filtering && has_non_const_keys)
    {
        auto * left_set = add_create_set(left_node, join_clause.key_names_left, JoinTableSide::Left);
        auto * right_set = add_create_set(right_node, join_clause.key_names_right, JoinTableSide::Right);

        if (isInnerOrLeft(join_kind))
            right_set->setFiltering(left_set->getSet());

        if (isInnerOrRight(join_kind))
            left_set->setFiltering(right_set->getSet());
    }

    add_sorting(left_node, join_clause.key_names_left, JoinTableSide::Left);
    add_sorting(right_node, join_clause.key_names_right, JoinTableSide::Right);
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

    std::vector<std::tuple<BitSet, BitSet, JoinKind>> dependencies;
    std::vector<std::pair<BitSet, ActionsDAG::NodeRawConstPtrs>> type_changes;
    std::unordered_map<JoinActionRef, BitSet> pinned;

    struct BuilderContext
    {
        const QueryPlanOptimizationSettings & optimization_settings;
        StatisticsContext statistics_context;
        JoinSettings join_settings;
        SortingStep::Settings sorting_settings;

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
    for (auto && dep : rhs.dependencies)
    {
        auto [left_mask, right_mask, join_kind] = dep;
        left_mask.shift(shift);
        right_mask.shift(shift);
        lhs.dependencies.push_back(std::make_tuple(std::move(left_mask), std::move(right_mask), join_kind));
    }

    for (auto && [sources, nodes] : rhs.type_changes)
    {
        sources.shift(shift);
        lhs.type_changes.emplace_back(sources, std::move(nodes));
    }

    for (auto & [action, pin] : rhs_pinned_raw)
    {
        pin.shift(shift);
        lhs.pinned[JoinActionRef(action, lhs.expression_actions)] = pin;
    }
}

void buildQueryGraph(QueryGraphBuilder & query_graph, QueryPlan::Node & node, QueryPlan::Nodes & nodes);

static String dumpStatsForLogs(const RelationStats & stats)
{
    return fmt::format("{}: {} rows, columns: [{}]",
        stats.table_name.empty() ? "<unknown>" : stats.table_name,
        stats.estimated_rows,
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

size_t addChildQueryGraph(QueryGraphBuilder & graph, QueryPlan::Node * node, QueryPlan::Nodes & nodes, std::string_view label = {})
{
    if (isTrivialStep(node))
        node = node->children[0];

    auto * child_join_step = typeid_cast<JoinStepLogical *>(node->step.get());
    if (child_join_step && graph.hasCompatibleSettings(*child_join_step))
    {
        QueryGraphBuilder child_graph(graph.context);
        buildQueryGraph(child_graph, *node, nodes);
        size_t count = child_graph.inputs.size();
        uniteGraphs(graph, std::move(child_graph));
        return count;
    }

    graph.inputs.push_back(node);
    RelationStats stats = estimateReadRowsCount(*node);
    size_t num_rows_from_cache = graph.context->statistics_context.getCachedHint(node);
    stats.estimated_rows = std::min(stats.estimated_rows, num_rows_from_cache);
    if (!label.empty())
        stats.table_name = label;

    LOG_TRACE(getLogger("optimizeJoin"), "Estimated statistics for {} {}", node->step->getName(), dumpStatsForLogs(stats));
    graph.relation_stats.push_back(stats);
    return 1;
}

void buildQueryGraph(QueryGraphBuilder & query_graph, QueryPlan::Node & node, QueryPlan::Nodes & nodes)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical expected");
    if (node.children.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have exactly 2 children, but has {}", node.children.size());

    QueryPlan::Node * lhs_plan = node.children[0];
    QueryPlan::Node * rhs_plan = node.children[1];
    auto [lhs_label, rhs_label] = join_step->getInputLabels();
    size_t lhs_count = addChildQueryGraph(query_graph, lhs_plan, nodes, lhs_label);
    size_t rhs_count = addChildQueryGraph(query_graph, rhs_plan, nodes, rhs_label);
    size_t total_inputs = query_graph.inputs.size();

    chassert(lhs_count && rhs_count && lhs_count + rhs_count == total_inputs && query_graph.relation_stats.size() == total_inputs);

    auto [expression_actions, join_operator] = join_step->detachExpressions();

    auto get_raw_nodes = std::views::transform([](const auto & ref) { return ref.getNode(); });
    auto join_expression = std::ranges::to<std::vector>(join_operator.expression | get_raw_nodes);
    auto residual_filter = std::ranges::to<std::vector>(join_operator.residual_filter | get_raw_nodes);

    auto [expression_actions_dag, expression_actions_sources] = expression_actions.detachActionsDAG();

    ActionsDAG::NodeMapping node_mapping;
    query_graph.expression_actions.getActionsDAG()->mergeInplace(std::move(expression_actions_dag), node_mapping, true);

    ActionsDAG::NodeRawConstPtrs join_outputs = query_graph.expression_actions.getActionsDAG()->getOutputs();

    // for (auto & output : join_outputs)
    // {
    //     if (auto it = node_mapping.find(output); it != node_mapping.end())
    //         output = it->second;
    // }

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
        if (out_node->type == ActionsDAG::ActionType::INPUT || out_node->type == ActionsDAG::ActionType::COLUMN)
            continue;
        auto source = JoinActionRef(out_node, query_graph.expression_actions).getSourceRelations();
        if (source.count() == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot determine source relations for node {}", out_node->result_name);
        else if (isSubsetOf(source, left_mask))
            left_changes_types.push_back(out_node);
        else if (isSubsetOf(source, right_mask))
            right_changes_types.push_back(out_node);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot determine source relations for node {}", out_node->result_name);
    }

    if (!left_changes_types.empty())
        query_graph.type_changes.emplace_back(left_mask, std::move(left_changes_types));
    if (!right_changes_types.empty())
        query_graph.type_changes.emplace_back(right_mask, std::move(right_changes_types));

    for (const auto * old_node : join_expression)
    {
        const auto & new_node_entry = node_mapping.try_emplace(old_node, old_node);
        const auto * new_node = new_node_entry.first->second;
        auto & edge = query_graph.join_edges.emplace_back(new_node, query_graph.expression_actions);
        auto sources = edge.getSourceRelations();
        for (auto & [null_side, _, join_kind] : query_graph.dependencies)
        {
            if (sources & null_side)
            {
                auto & pinned = query_graph.pinned[edge];
                pinned = pinned | null_side;
            }
        }
    }


    /// Non-reorderable joins
    if (isLeftOrFull(join_operator.kind))
        query_graph.dependencies.emplace_back(right_mask, left_mask, join_operator.kind);
    if (isRightOrFull(join_operator.kind))
        query_graph.dependencies.emplace_back(left_mask, right_mask, reverseJoinKind(join_operator.kind));

    UNUSED(residual_filter);
}

static std::vector<const DPJoinEntry *> getJoinTreePostOrderSequence(DPJoinEntryPtr root)
{
    std::vector<const DPJoinEntry *> result;
    result.reserve(root->relations.count() * 2);

    std::vector<const DPJoinEntry *> stack;
    stack.push_back(root.get());

    while (!stack.empty())
    {
        const auto * node = stack.back();
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

QueryPlan::Node chooseJoinOrder(QueryGraphBuilder query_graph_builder, QueryPlan::Nodes & nodes)
{
    QueryGraph query_graph;
    query_graph.relation_stats = std::move(query_graph_builder.relation_stats);
    query_graph.edges = std::move(query_graph_builder.join_edges);
    query_graph.dependencies = std::move(query_graph_builder.dependencies);
    query_graph.pinned = std::move(query_graph_builder.pinned);

    std::unordered_map<BitSet, String> relation_names;
    for (size_t i = 0; i < query_graph.relation_stats.size(); ++i)
    {
        const auto & table_name = query_graph.relation_stats[i].table_name;
        auto estimated_count = query_graph.relation_stats[i].estimated_rows;
        String estimation = estimated_count > 0 ? fmt::format("[{}]", estimated_count) : "";
        if (!table_name.empty())
            relation_names[BitSet().set(i)] = fmt::format("{}{}", table_name, estimation);
        else
            relation_names[BitSet().set(i)] = fmt::format("R{}{}", i, estimation);
    }

    auto global_expression_actions = std::move(query_graph_builder.expression_actions);
    auto global_actions_dag = global_expression_actions.getActionsDAG();
    if (!global_actions_dag)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global expression actions DAG is not set");

    auto optimized = optimizeJoinOrder(std::move(query_graph));
    auto sequence = getJoinTreePostOrderSequence(optimized);
    std::stack<QueryPlan::Node *> nodeStack;
    auto & input_nodes = query_graph_builder.inputs;

    if (!query_graph_builder.context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryGraphBuilder context is not set");

    auto join_settings = std::move(query_graph_builder.context->join_settings);
    auto sorting_settings = std::move(query_graph_builder.context->sorting_settings);

    std::unordered_map<std::string_view, const ActionsDAG::Node *> input_node_map;
    for (const auto * input : global_actions_dag->getInputs())
        input_node_map[input->result_name] = input;

    const auto & optimization_settings = query_graph_builder.context->optimization_settings;
    for (auto * entry : sequence)
    {
        if (entry->isLeaf())
        {
            size_t relation_id = safe_cast<size_t>(entry->relation_id);
            if (relation_id >= input_nodes.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid relation id: {}, input nodes size: {}", relation_id, input_nodes.size());
            nodeStack.push(input_nodes[relation_id]);
        }
        else
        {
            auto * left_child_node = nodeStack.top();
            nodeStack.pop();
            auto * right_child_node = nodeStack.top();
            nodeStack.pop();

            auto join_operator = std::move(entry->join_operator);
            auto left_rels = entry->left->relations;
            auto right_rels = entry->right->relations;

            bool has_prepared_storage = bool(typeid_cast<const JoinStepLogicalLookup *>(right_child_node->step.get()));

            bool flip_join = !has_prepared_storage && optimization_settings.join_swap_table.has_value()
                ? optimization_settings.join_swap_table.value()
                : entry->join_method == JoinMethod::Hash && entry->left->estimated_rows < entry->right->estimated_rows;

            if (flip_join)
            {
                /// For hash joins, we want to keep the smaller side on the right
                std::swap(left_rels, right_rels);
                std::swap(left_child_node, right_child_node);
                join_operator.kind = reverseJoinKind(join_operator.kind);
            }

            auto left_header_ptr = left_child_node->step->getOutputHeader();
            auto right_header_ptr = right_child_node->step->getOutputHeader();

            const auto & left_header = *left_header_ptr;
            const auto & right_header = *right_header_ptr;

            ActionsDAG::NodeRawConstPtrs required_output_nodes;
            for (const auto & action : join_operator.expression)
                required_output_nodes.push_back(action.getNode());
            for (const auto & action : join_operator.residual_filter)
                required_output_nodes.push_back(action.getNode());

            ActionsDAG::NodeMapping current_step_type_changes;
            auto joined_mask = entry->relations;
            for (const auto & [sources, new_inputs] : query_graph_builder.type_changes)
            {
                if (isSubsetOf(sources, joined_mask))
                {
                    for (const auto * new_input : new_inputs)
                    {
                        auto it = input_node_map.find(new_input->result_name);
                        if (it == input_node_map.end())
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} not found in inputs of dag {}", new_input->result_name, global_actions_dag->dumpDAG());
                        current_step_type_changes[it->second] = new_input;
                    }
                }
            }

            ActionsDAG::NodeMapping current_inputs;

            auto process_input_column = [&](const auto & column)
            {
                auto it = input_node_map.find(column.name);
                if (it == input_node_map.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} not found in inputs of dag {}", column.name, global_actions_dag->dumpDAG());
                if (!it->second->result_type->equals(*column.type))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} expected to habe type {}", column.dumpStructure(), it->second->result_type->getName());
                current_inputs[it->second] = it->second;

                auto out_node = it->second;
                if (auto it2 = current_step_type_changes.find(out_node); it2 != current_step_type_changes.end())
                    out_node = it2->second;
                required_output_nodes.push_back(out_node);
            };

            for (const auto & column : left_header)
                process_input_column(column);
            for (const auto & column : right_header)
                process_input_column(column);

            JoinExpressionActions current_expression_actions(left_header, right_header,
                ActionsDAG::foldActionsByProjection(current_inputs, required_output_nodes));

            auto current_dag = current_expression_actions.getActionsDAG();
            auto & dag_outputs = current_dag->getOutputs();
            if (required_output_nodes.size() != dag_outputs.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Required output nodes size {} does not match current output nodes size in dag {}", required_output_nodes.size(), current_dag->dumpDAG());

            auto join_expression_map = std::ranges::to<ActionsDAG::NodeMapping>(std::views::zip(required_output_nodes, dag_outputs));
            for (auto & action : join_operator.expression)
            {
                auto mapped_node = join_expression_map[action.getNode()];
                if (!mapped_node)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} not found in current dag {}", action.getNode()->result_name, current_dag->dumpDAG());
                action = JoinActionRef(mapped_node, current_expression_actions);
            }

            /// Setup outputs after join
            dag_outputs.clear();
            /// Set current inputs to nodes after current join
            for (auto & e : input_node_map)
            {
                auto it = current_step_type_changes.find(e.second);
                if (it != current_step_type_changes.end())
                    e.second = it->second;
            }

            /// Columns returned from JOIN is input with possibly corrected type
            for (const auto & column : left_header)
            {
                auto it = input_node_map.find(column.name);
                if (it == input_node_map.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} not found in inputs of dag {}", column.name, global_actions_dag->dumpDAG());
                auto mapped_it = join_expression_map.find(it->second);
                if (mapped_it == join_expression_map.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} not found in join expression map", column.name);
                dag_outputs.push_back(mapped_it->second);
            }
            for (const auto & column : right_header)
            {
                auto it = input_node_map.find(column.name);
                if (it == input_node_map.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} not found in inputs of dag {}", column.name, global_actions_dag->dumpDAG());
                auto mapped_it = join_expression_map.find(it->second);
                if (mapped_it == join_expression_map.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} not found in join expression map", column.name);
                dag_outputs.push_back(mapped_it->second);
            }

            /// FIXME:
            join_operator.residual_filter.clear();

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
            if (!left_label.empty() && !right_label.empty())
                relation_names[entry->relations] = fmt::format("{} {} {}", left_label, joinTypePretty(join_operator.kind, join_operator.strictness), right_label);

            join_step->setInputLabels(std::move(left_label), std::move(right_label));
            join_step->setOptimized(entry->estimated_rows);
            auto & new_node = nodes.emplace_back();
            new_node.step = std::move(join_step);
            new_node.children = {left_child_node, right_child_node};
            nodeStack.push(&new_node);
        }
    }

    if (nodeStack.size() != 1 || nodeStack.top() != &nodes.back())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal join sequence produced: [{}]",
            fmt::join(sequence | std::views::transform([](const auto * dpe) { return dpe ? dpe->dump() : "null"; }), ", "));

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

    for (auto * child : node.children)
    {
        if (auto * lookup_step = typeid_cast<JoinStepLogicalLookup *>(child->step.get()))
            lookup_step->optimize(optimization_settings);
    }

    if (!optimization_settings.optimize_join_order)
    {
        /// TODO: we still can try swap tables if query_plan_join_swap_table is enabled
        join_step->setOptimized();
        return;
    }

    QueryGraphBuilder query_graph_builder(optimization_settings, node, join_step->getJoinSettings(), join_step->getSortingSettings());
    buildQueryGraph(query_graph_builder, node, nodes);
    node = chooseJoinOrder(std::move(query_graph_builder), nodes);
}

}

}
