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

static std::optional<UInt64> estimateReadRowsCount(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(step))
    {
        if (auto analyzed_result = reading->getAnalyzedResult())
            return analyzed_result->selected_rows;
        if (auto analyzed_result = reading->selectRangesToRead())
            return analyzed_result->selected_rows;
        return {};
    }

    if (const auto * reading = typeid_cast<const ReadFromMemoryStorageStep *>(step))
        return reading->getStorage()->totalRows(Settings{});

    if (node.children.size() != 1)
        return {};

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step))
        return estimateReadRowsCount(*node.children.front());

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

    /// Algorithms other than HashJoin may not support OUTER JOINs
    if (table_join.kind() != JoinKind::Inner && !typeid_cast<const HashJoin *>(join.get()))
        return;

    /// fixme: USING clause handled specially in join algorithm, so swap breaks it
    /// fixme: Swapping for SEMI and ANTI joins should be alright, need to try to enable it and test
    if (table_join.hasUsing() || table_join.strictness() != JoinStrictness::All)
        return;

    bool need_swap = false;
    if (join_step->inner_table_selection_mode == JoinInnerTableSelectionMode::Auto)
    {
        auto lhs_extimation = estimateReadRowsCount(*node.children[0]);
        auto rhs_extimation = estimateReadRowsCount(*node.children[1]);
        LOG_TRACE(getLogger("optimizeJoin"), "Left table estimation: {}, right table estimation: {}",
            lhs_extimation.transform(toString<UInt64>).value_or("unknown"),
            rhs_extimation.transform(toString<UInt64>).value_or("unknown"));

        if (lhs_extimation && rhs_extimation && *lhs_extimation < *rhs_extimation)
            need_swap = true;
    }
    else if (join_step->inner_table_selection_mode == JoinInnerTableSelectionMode::Left)
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
