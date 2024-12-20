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

#include <Interpreters/TableJoin.h>

#include <Common/logger_useful.h>
#include <Core/Joins.h>
#include <ranges>

namespace DB::QueryPlanOptimizations
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

void optimizeJoin(QueryPlan::Node & node, QueryPlan::Nodes &)
{
    auto * join_step = typeid_cast<JoinStep *>(node.step.get());
    if (!join_step || node.children.size() != 2)
        return;

    const auto & join = join_step->getJoin();
    if (join->pipelineType() != JoinPipelineType::FillRightFirst || !join->isCloneSupported())
        return;

    const auto & table_join = join->getTableJoin();

    /// Algorithms other than HashJoin may not support all JOIN kinds, so changing from LEFT to RIGHT is not always possible
    bool allow_outer_join = typeid_cast<const HashJoin *>(join.get());
    if (table_join.kind() != JoinKind::Inner && !allow_outer_join)
        return;

    /// fixme: USING clause handled specially in join algorithm, so swap breaks it
    /// fixme: Swapping for SEMI and ANTI joins should be alright, need to try to enable it and test
    if (table_join.hasUsing() || table_join.strictness() != JoinStrictness::All)
        return;

    bool need_swap = false;
    if (!join_step->swap_join_tables.has_value())
    {
        auto lhs_extimation = estimateReadRowsCount(*node.children[0]);
        auto rhs_extimation = estimateReadRowsCount(*node.children[1]);
        LOG_TRACE(getLogger("optimizeJoin"), "Left table estimation: {}, right table estimation: {}",
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
        return;

    const auto & headers = join_step->getInputHeaders();
    if (headers.size() != 2)
        return;

    const auto & left_stream_input_header = headers.front();
    const auto & right_stream_input_header = headers.back();

    auto updated_table_join = std::make_shared<TableJoin>(table_join);
    updated_table_join->swapSides();
    auto updated_join = join->clone(updated_table_join, right_stream_input_header, left_stream_input_header);
    join_step->setJoin(std::move(updated_join), /* swap_streams= */ true);
}

}
