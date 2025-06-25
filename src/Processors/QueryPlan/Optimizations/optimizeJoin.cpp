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
#include <Core/Joins.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Common/logger_useful.h>


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

namespace QueryPlanOptimizations
{

static std::optional<UInt64> estimateReadRowsCount(QueryPlan::Node & node, bool has_filter = false)
{
    IQueryPlanStep * step = node.step.get();
    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(step))
    {
        ReadFromMergeTree::AnalysisResultPtr analyzed_result = reading->getAnalyzedResult();
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
        return reading->getStorage()->totalRows({});

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

bool convertLogicalJoinToPhysical(
    QueryPlan::Node & node,
    QueryPlan::Nodes & nodes,
    const QueryPlanOptimizationSettings & optimization_settings,
    std::optional<UInt64> rhs_size_estimation)
{
    bool keep_logical = optimization_settings.keep_logical_steps;
    UNUSED(keep_logical);
    if (!typeid_cast<JoinStepLogical *>(node.step.get()))
        return false;
    if (node.children.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have exactly 2 children, but has {}", node.children.size());

    std::vector<RelationStats> relation_stats;
    relation_stats.emplace_back();
    relation_stats.emplace_back();
    if (rhs_size_estimation)
        relation_stats[1].estimated_rows = *rhs_size_estimation;

    JoinStepLogical::buildPhysicalJoin(node, relation_stats, optimization_settings, nodes);

    return true;
}

std::optional<UInt64>
optimizeJoinLogical(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings & optimization_settings)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
        return {};

    if (auto * lookup_step = typeid_cast<JoinStepLogicalLookup *>(node.children.back()->step.get()))
    {
        return lookup_step->optimize(optimization_settings);
    }

    if (node.children.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have exactly 2 children, but has {}", node.children.size());

    bool need_swap = false;
    auto lhs_estimation = estimateReadRowsCount(*node.children[0]);
    auto rhs_estimation = estimateReadRowsCount(*node.children[1]);

    /// Consider estimations from hash table sizes cache too
    if (const auto & hash_table_key_hashes = join_step->getHashTableKeyHashes();
        hash_table_key_hashes && optimization_settings.collect_hash_table_stats_during_joins)
    {
        StatsCollectingParams params{
            /*key_=*/0,
            /*enable=*/true,
            optimization_settings.max_entries_for_hash_table_stats,
            optimization_settings.max_size_to_preallocate_for_joins};
        if (auto hint = getHashTablesStatistics<HashJoinEntry>().getSizeHint(params.setKey(hash_table_key_hashes->key_hash_left)))
            lhs_estimation = std::min<size_t>(lhs_estimation.value_or(std::numeric_limits<size_t>::max()), hint->source_rows);
        if (auto hint = getHashTablesStatistics<HashJoinEntry>().getSizeHint(params.setKey(hash_table_key_hashes->key_hash_right)))
            rhs_estimation = std::min<size_t>(rhs_estimation.value_or(std::numeric_limits<size_t>::max()), hint->source_rows);
    }

    LOG_TRACE(
        getLogger("optimizeJoin"),
        "Left table estimation: {}, right table estimation: {}",
        lhs_estimation.transform(toString<UInt64>).value_or("unknown"),
        rhs_estimation.transform(toString<UInt64>).value_or("unknown"));

    if (!optimization_settings.join_swap_table.has_value())
    {
        if (lhs_estimation && rhs_estimation && *lhs_estimation < *rhs_estimation)
            need_swap = true;
    }
    else if (optimization_settings.join_swap_table.value())
    {
        need_swap = true;
    }

    if (!need_swap)
        return rhs_estimation;
    return lhs_estimation;
}

}

}
