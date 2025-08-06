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

#include <Interpreters/Context.h>
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
    extern const SettingsBool allow_statistics_optimize;
}

RelationStats getDummyStats(ContextPtr context, const String & table_name);

namespace QueryPlanOptimizations
{

const size_t MAX_ROWS = std::numeric_limits<size_t>::max();

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

static RelationStats estimateReadRowsCount(QueryPlan::Node & node, bool has_filter = false)
{
    IQueryPlanStep * step = node.step.get();
    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(step))
    {
        String table_diplay_name = reading->getStorageID().getTableName();

        if (reading->getContext()->getSettingsRef()[Setting::allow_statistics_optimize])
        {
            auto estimator_ = reading->getConditionSelectivityEstimator();
            if (estimator_ != nullptr)
            {
                RelationStats stats{.estimated_rows = {}, .table_name = table_diplay_name, .estimator = estimator_};
                if (!has_filter)
                {
                    stats.materialize();
                }
                return stats;
            }
        }
        if (auto dummy_stats = getDummyStats(reading->getContext(), table_diplay_name); !dummy_stats.table_name.empty())
            return dummy_stats;

        ReadFromMergeTree::AnalysisResultPtr analyzed_result = nullptr;
        analyzed_result = analyzed_result ? analyzed_result : reading->getAnalyzedResult();
        analyzed_result = analyzed_result ? analyzed_result : reading->selectRangesToRead();
        if (!analyzed_result)
            return RelationStats{.estimated_rows = {}, .table_name = table_diplay_name};

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
            return RelationStats{.estimated_rows = {}, .table_name = table_diplay_name};

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
        UInt64 estimated_rows = reading->getNumberOfRows();
        relation_stats.estimated_rows = estimated_rows;

        auto column_name = reading->getColumnName();
        relation_stats.column_stats[column_name].num_distinct_values = estimated_rows;
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

    if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
    {
        auto stats = estimateReadRowsCount(*node.children.front(), true);
        if (stats.estimator != nullptr)
        {
            auto & dag = filter_step->getExpression();
            auto * predicate = const_cast<ActionsDAG::Node *>(dag.tryFindInOutputs(filter_step->getFilterColumnName()));
            stats.materialize(predicate);
        }
        remapColumnStats(stats.column_stats, filter_step->getExpression());
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

    std::vector<std::tuple<BitSet, BitSet, JoinKind>> dependencies;
    std::vector<std::pair<BitSet, ActionsDAG::NodeRawConstPtrs>> type_changes;
    std::unordered_map<JoinActionRef, BitSet> pinned;

    struct BuilderContext
    {
        const QueryPlanOptimizationSettings & optimization_settings;
        RuntimeHashStatisticsContext statistics_context;
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

size_t addChildQueryGraph(QueryGraphBuilder & graph, QueryPlan::Node * node, QueryPlan::Nodes & nodes, std::string_view label = {})
{
    if (isTrivialStep(node))
        node = node->children[0];

    auto * child_join_step = typeid_cast<JoinStepLogical *>(node->step.get());
    if (child_join_step && !child_join_step->isOptimized() && graph.hasCompatibleSettings(*child_join_step))
    {
        QueryGraphBuilder child_graph(graph.context);
        buildQueryGraph(child_graph, *node, nodes);
        size_t count = child_graph.inputs.size();
        uniteGraphs(graph, std::move(child_graph));
        return count;
    }

    graph.inputs.push_back(node);
    RelationStats stats = estimateReadRowsCount(*node);
    std::optional<size_t> num_rows_from_cache = graph.context->statistics_context.getCachedHint(node);
    if (num_rows_from_cache)
        stats.estimated_rows = std::min(stats.estimated_rows.value_or(MAX_ROWS), num_rows_from_cache.value());

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
        if (out_node->type == ActionsDAG::ActionType::INPUT ||
            out_node->type == ActionsDAG::ActionType::COLUMN)
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

    bool allow_reorder = query_graph.context->optimization_settings.optimize_join_order;
    /// Non-reorderable joins
    if (isLeftOrFull(join_operator.kind) || !allow_reorder)
        query_graph.dependencies.emplace_back(right_mask, left_mask, join_operator.kind);
    if (isRightOrFull(join_operator.kind) || !allow_reorder)
        query_graph.dependencies.emplace_back(left_mask, right_mask, reverseJoinKind(join_operator.kind));

    UNUSED(residual_filter);
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

static const ActionsDAG::Node * trackInputColumn(const ActionsDAG::Node * node)
{
    for (; !node->children.empty(); node = node->children[0])
    {
        if (node->children.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} has {} children, expected 1", node->result_name, node->children.size());
    }
    return node;
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

    auto optimized = optimizeJoinOrder(std::move(query_graph));
    auto sequence = getJoinTreePostOrderSequence(optimized);

    if (sequence.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join tree is empty");

    std::unordered_map<const ActionsDAG::Node *, size_t> usage_level_map;

    for (size_t i = 0; i < sequence.size(); ++i)
    {
        const auto & entry = sequence[i];
        for (const auto & action : entry->join_operator.expression)
            usage_level_map[action.getNode()] = i;
        for (const auto & action : entry->join_operator.residual_filter)
            usage_level_map[action.getNode()] = i;
    }

    for (const auto & out : global_actions_dag->getOutputs())
        usage_level_map[out] = sequence.size();

    {
        std::stack<std::pair<const ActionsDAG::Node *, size_t>> stack;
        for (const auto & entry : usage_level_map)
            stack.push(std::make_pair(entry.first, entry.second));

        while (!stack.empty())
        {
            auto [node, level] = stack.top();
            stack.pop();
            for (const auto * child : node->children)
                stack.push(std::make_pair(child, level));
            auto & map_level = usage_level_map[node];
            map_level = std::max(map_level, level);
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
    std::unordered_map<const ActionsDAG::Node *, size_t> input_node_map;
    std::vector<std::pair<size_t, const ActionsDAG::Node *>> current_input_nodes;

    const auto & global_inputs = global_actions_dag->getInputs();
    for (size_t input_idx = 0; input_idx < global_inputs.size(); ++input_idx)
    {
        const auto * input = global_inputs[input_idx];
        auto src_rels = JoinActionRef(input, global_expression_actions).getSourceRelations();
        if (src_rels.count() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Input node {} has {} source relations, expected 1", input->result_name, toString(src_rels));
        auto rel_idx = src_rels.findFirstSet();
        current_input_nodes.emplace_back(safe_cast<size_t>(rel_idx), input);
        input_node_map[input] = input_idx;
    }

    const auto & optimization_settings = query_graph_builder.context->optimization_settings;
    for (size_t entry_idx = 0; entry_idx < sequence.size(); ++entry_idx)
    {
        auto * entry = sequence[entry_idx];
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

            bool has_prepared_storage_at_right = bool(typeid_cast<const JoinStepLogicalLookup *>(right_child_node->step.get()));
            bool has_prepared_storage_at_left = bool(typeid_cast<const JoinStepLogicalLookup *>(left_child_node->step.get()));

            auto lhs_estimation = entry->left->estimated_rows;
            auto rhs_estimation = entry->right->estimated_rows;

            bool swap_on_sizes = optimization_settings.join_swap_table.has_value()
                ? optimization_settings.join_swap_table.value()
                : entry->join_method == JoinMethod::Hash && lhs_estimation && rhs_estimation
                    && lhs_estimation.value() < rhs_estimation.value();

            bool flip_join = has_prepared_storage_at_left || (!has_prepared_storage_at_right && swap_on_sizes);

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
            for (const auto & action : join_operator.expression)
                required_output_nodes.push_back(action.getNode());
            for (const auto & action : join_operator.residual_filter)
                required_output_nodes.push_back(action.getNode());

            std::unordered_map<size_t, const ActionsDAG::Node *> current_step_type_changes;
            auto joined_mask = entry->relations;
            for (const auto & [sources, new_inputs] : query_graph_builder.type_changes)
            {
                if (isSubsetOf(sources, left_rels) || isSubsetOf(sources, right_rels))
                {
                    for (const auto * new_input : new_inputs)
                    {
                        const auto * input_node = trackInputColumn(new_input);
                        auto it = input_node_map.find(input_node);
                        if (it == input_node_map.end())
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} not found in inputs of dag {}", input_node->result_name, global_actions_dag->dumpDAG());
                        current_step_type_changes[it->second] = new_input;
                    }
                }
            }

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
                required_output_nodes.push_back(out_node);
            }

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
                relation_names[entry->relations] = join_step->getReadableRelationName();

            join_step->setInputLabels(std::move(left_label), std::move(right_label));
            join_step->setOptimized(entry->estimated_rows, lhs_estimation, rhs_estimation);

            auto & new_node = nodes.emplace_back();
            new_node.step = std::move(join_step);
            new_node.children = {left_child_node, right_child_node};
            nodeStack.push(&new_node);
        }
    }

    if (nodeStack.size() != 1 || nodeStack.top() != &nodes.back())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal join sequence produced: [{}]",
            fmt::join(sequence | std::views::transform([](const auto * e) { return e ? e->dump() : "null"; }), ", "));

    auto result = std::move(nodes.back());
    nodes.pop_back();

    // {
    //     ActionsDAG::NodeMapping current_inputs;
    //     auto all_outputs = global_actions_dag->getOutputs();
    //     size_t outputs_num = all_outputs.size();
    //     for (size_t input_pos = 0; input_pos < current_input_nodes.size(); ++input_pos)
    //     {
    //         auto [rel_idx, input_node] = current_input_nodes[input_pos];
    //         current_inputs[input_node] = input_node;
    //         /// Add all inputs to outputs to make sure all of them are used in this dag
    //         all_outputs.push_back(input_node);
    //     }
    //     auto final_dag = ActionsDAG::foldActionsByProjection(current_inputs, all_outputs);
    //     /// Keep only original outputs
    //     final_dag.getOutputs().resize(outputs_num);
    //     makeExpressionNodeOnTopOf(result, std::move(final_dag), nodes, "Actions After Join Reorder");
    // }

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

    if (!optimization_settings.optimize_joins ||
        join_step->getJoinOperator().strictness != JoinStrictness::All ||
        join_step->getJoinOperator().kind == JoinKind::Paste)
    {
        join_step->setOptimized();
        return;
    }

    QueryGraphBuilder query_graph_builder(optimization_settings, node, join_step->getJoinSettings(), join_step->getSortingSettings());
    buildQueryGraph(query_graph_builder, node, nodes);
    node = chooseJoinOrder(std::move(query_graph_builder), nodes);
}

}

}
