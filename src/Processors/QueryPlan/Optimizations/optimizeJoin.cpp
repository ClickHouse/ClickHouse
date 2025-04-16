#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/StorageMemory.h>
#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>
#include <Core/Settings.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>

#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Interpreters/FullSortingMergeJoin.h>

#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>

#include <Common/logger_useful.h>
#include <Core/Joins.h>
#include <ranges>
#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 max_block_size;
    extern const SettingsUInt64 min_joined_block_size_bytes;
}

namespace QueryPlanOptimizations
{

static std::optional<UInt64> estimateReadRowsCount(QueryPlan::Node & node, bool has_filter = false)
{
    IQueryPlanStep * step = node.step.get();
    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(step))
    {
        ReadFromMergeTree::AnalysisResultPtr analyzed_result = nullptr;
        analyzed_result = analyzed_result ? analyzed_result : reading->getAnalyzedResult();
        analyzed_result = analyzed_result ? analyzed_result : reading->selectRangesToRead();
        if (!analyzed_result)
            return {};

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
            return {};

        return analyzed_result->selected_rows;
    }

    if (const auto * reading = typeid_cast<const ReadFromMemoryStorageStep *>(step))
        return reading->getStorage()->totalRows(Settings{});

    if (node.children.size() != 1)
        return {};

    if (typeid_cast<ExpressionStep *>(step))
        return estimateReadRowsCount(*node.children.front(), has_filter);
    if (typeid_cast<FilterStep *>(step))
        return estimateReadRowsCount(*node.children.front(), true);

    return {};
}


bool optimizeJoinLegacy(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &)
{
    auto * join_step = typeid_cast<JoinStep *>(node.step.get());
    if (!join_step || node.children.size() != 2)
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
        auto lhs_extimation = estimateReadRowsCount(*node.children[0]);
        auto rhs_extimation = estimateReadRowsCount(*node.children[1]);
        LOG_TRACE(getLogger("optimizeJoinLegacy"), "Left table estimation: {}, right table estimation: {}",
            lhs_extimation.transform(toString<UInt64>).value_or("unknown"),
            rhs_extimation.transform(toString<UInt64>).value_or("unknown"));

        if (lhs_extimation && rhs_extimation && *lhs_extimation < *rhs_extimation)
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

    const auto & left_stream_input_header = headers.front();
    const auto & right_stream_input_header = headers.back();

    auto updated_table_join = std::make_shared<TableJoin>(table_join);
    updated_table_join->swapSides();
    auto updated_join = join->clone(updated_table_join, right_stream_input_header, left_stream_input_header);
    join_step->setJoin(std::move(updated_join), /* swap_streams= */ true);
    return true;
}

QueryPlan::Node * makeExpressionNodeOnTopOf(QueryPlan::Node * node, ActionsDAG actions_dag, const String & filter_column_name, QueryPlan::Nodes & nodes)
{
    const auto & header = node->step->getOutputHeader();
    if (!header)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create ExpressionStep on top of node without header");

    QueryPlanStepPtr step;

    if (filter_column_name.empty())
        step = std::make_unique<ExpressionStep>(header, std::move(actions_dag));
    else
        step = std::make_unique<FilterStep>(header, std::move(actions_dag), filter_column_name, false);

    return &nodes.emplace_back(QueryPlan::Node{std::move(step), {node}});
}

void addSortingForMergeJoin(
    const FullSortingMergeJoin * join_ptr,
    QueryPlan::Node *& left_node,
    QueryPlan::Node *& right_node,
    QueryPlan::Nodes & nodes,
    const SortingStep::Settings & sort_settings,
    const JoinSettings & join_settings,
    const JoinInfo & join_info)
{
    auto join_kind = join_info.kind;
    auto join_strictness = join_info.strictness;
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
    bool has_non_const_keys = has_non_const(left_node->step->getOutputHeader(), join_clause.key_names_left)
        && has_non_const(right_node->step->getOutputHeader() , join_clause.key_names_right);

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

bool convertLogicalJoinToPhysical(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    bool keep_logical = optimization_settings.keep_logical_steps;
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
        return false;
    if (node.children.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have exactly 2 children, but has {}", node.children.size());

    JoinActionRef left_filter(nullptr);
    JoinActionRef right_filter(nullptr);
    JoinActionRef post_filter(nullptr);
    auto join_ptr = join_step->convertToPhysical(left_filter, right_filter, post_filter, keep_logical);

    if (join_ptr->isFilled())
    {
        node.children.pop_back();
    }

    if (keep_logical)
        return true;

    Header output_header = join_step->getOutputHeader();

    auto & join_expression_actions = join_step->getExpressionActions();

    QueryPlan::Node * new_left_node = makeExpressionNodeOnTopOf(node.children.at(0), std::move(join_expression_actions.left_pre_join_actions), left_filter.column_name, nodes);
    QueryPlan::Node * new_right_node = nullptr;
    if (node.children.size() >= 2)
        new_right_node = makeExpressionNodeOnTopOf(node.children.at(1), std::move(join_expression_actions.right_pre_join_actions), right_filter.column_name, nodes);

    if (join_step->areInputsSwapped() && new_right_node)
        std::swap(new_left_node, new_right_node);

    const auto & settings = join_step->getContext()->getSettingsRef();

    auto & new_join_node = nodes.emplace_back();

    if (!join_ptr->isFilled())
    {
        chassert(new_right_node);
        if (const auto * fsmjoin = dynamic_cast<const FullSortingMergeJoin *>(join_ptr.get()))
            addSortingForMergeJoin(fsmjoin, new_left_node, new_right_node, nodes,
                join_step->getSortingSettings(), join_step->getJoinSettings(), join_step->getJoinInfo());

        auto required_output_from_join = join_expression_actions.post_join_actions.getRequiredColumnsNames();
        new_join_node.step = std::make_unique<JoinStep>(
            new_left_node->step->getOutputHeader(),
            new_right_node->step->getOutputHeader(),
            join_ptr,
            settings[Setting::max_block_size],
            settings[Setting::min_joined_block_size_bytes],
            settings[Setting::max_threads],
            NameSet(required_output_from_join.begin(), required_output_from_join.end()),
            false /*optimize_read_in_order*/,
            true /*use_new_analyzer*/);
        new_join_node.children = {new_left_node, new_right_node};
    }
    else
    {
        new_join_node.step = std::make_unique<FilledJoinStep>(
            new_left_node->step->getOutputHeader(),
            join_ptr,
            settings[Setting::max_block_size]);
        new_join_node.children = {new_left_node};
    }

    if (!post_filter)
        node.step = std::make_unique<ExpressionStep>(new_join_node.step->getOutputHeader(), std::move(join_expression_actions.post_join_actions));
    else
    {
        bool remove_filter = !output_header.has(post_filter.column_name);
        node.step = std::make_unique<FilterStep>(new_join_node.step->getOutputHeader(), std::move(join_expression_actions.post_join_actions), post_filter.column_name, remove_filter);
    }
    node.children = {&new_join_node};
    return true;
}

bool optimizeJoinLogical(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings & optimization_settings)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
        return false;

    if (join_step->hasPreparedJoinStorage())
        return false;

    if (node.children.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have exactly 2 children, but has {}", node.children.size());

    bool need_swap = false;
    if (!optimization_settings.join_swap_table.has_value())
    {
        auto lhs_extimation = estimateReadRowsCount(*node.children[0]);
        auto rhs_extimation = estimateReadRowsCount(*node.children[1]);
        LOG_TRACE(getLogger("optimizeJoin"), "Left table estimation: {}, right table estimation: {}",
            lhs_extimation.transform(toString<UInt64>).value_or("unknown"),
            rhs_extimation.transform(toString<UInt64>).value_or("unknown"));

        if (lhs_extimation && rhs_extimation && *lhs_extimation < *rhs_extimation)
            need_swap = true;
    }
    else if (optimization_settings.join_swap_table.value())
    {
        need_swap = true;
    }

    if (!need_swap)
        return false;

    /// fixme: USING clause handled specially in join algorithm, so swap breaks it
    /// fixme: Swapping for SEMI and ANTI joins should be alright, need to try to enable it and test
    const auto & join_info = join_step->getJoinInfo();
    if (join_info.expression.is_using || join_info.strictness != JoinStrictness::All)
        return true;

    join_step->setSwapInputs();

    return true;
}

}

}
