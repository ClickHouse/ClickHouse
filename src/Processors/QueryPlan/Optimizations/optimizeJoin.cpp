#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Storages/StorageMemory.h>
#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>
#include <Processors/QueryPlan/ReadFromSystemNumbersStep.h>
#include <Core/Settings.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>

#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>

#include <Interpreters/TableJoin.h>

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

static RelationStats estimateReadRowsCount(QueryPlan::Node & node, bool has_filter = false)
{
    IQueryPlanStep * step = node.step.get();
    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(step))
    {
        String table_diplay_name = reading->getStorageID().getNameForLogs();
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
        UInt64 estimated_rows = reading->getNumberOfRows();
        return RelationStats{.estimated_rows = estimated_rows, .table_name = ""};
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

    if (typeid_cast<const ExpressionStep *>(step))
        return estimateReadRowsCount(*node.children.front(), has_filter);

    if (typeid_cast<const FilterStep *>(step))
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
        auto lhs_extimation = estimateReadRowsCount(*node.children[0]).estimated_rows;
        auto rhs_extimation = estimateReadRowsCount(*node.children[1]).estimated_rows;
        LOG_TRACE(getLogger("optimizeJoinLegacy"), "Left table estimation: {}, right table estimation: {}",
            lhs_extimation, rhs_extimation);

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

    const auto & left_stream_input_header = headers.front();
    const auto & right_stream_input_header = headers.back();

    auto updated_table_join = std::make_shared<TableJoin>(table_join);
    updated_table_join->swapSides();
    auto updated_join = join->clone(updated_table_join, right_stream_input_header, left_stream_input_header);
    join_step->setJoin(std::move(updated_join), /* swap_streams= */ true);
    return true;
}

bool convertLogicalJoinToPhysical(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
        return false;

    if (node.children.size() < 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have at least 2 children, but has {}", node.children.size());

    auto * optimized_node = join_step->optimizeToPhysicalPlan(node.children, nodes, optimization_settings);

    if (!optimization_settings.keep_logical_steps)
    {
        chassert(optimized_node);
        node = std::move(*optimized_node);
    }

    return true;
}

bool optimizeJoinLogical(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings & optimization_settings)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
        return false;

    if (join_step->hasPreparedJoinStorage())
        return false;

    for (size_t i = 0; i < node.children.size(); ++i)
        join_step->setRelationStats(estimateReadRowsCount(*node.children[i]), i);

    if (node.children.size() != 2)
        /// TODO(@vdimir): optimize flattened join
        return false;

    bool need_swap = false;
    if (!optimization_settings.join_swap_table.has_value())
    {
        auto lhs_extimation = estimateReadRowsCount(*node.children[0]).estimated_rows;
        auto rhs_extimation = estimateReadRowsCount(*node.children[1]).estimated_rows;
        LOG_TRACE(getLogger("optimizeJoin"), "Left table estimation: {}, right table estimation: {}",
            lhs_extimation, rhs_extimation);

        if (lhs_extimation && rhs_extimation && lhs_extimation < rhs_extimation)
            need_swap = true;
    }
    else if (optimization_settings.join_swap_table.value())
    {
        need_swap = true;
    }

    if (!need_swap)
        return false;

    if (join_step->getNumberOfTables() != 2)
        return false;

    /// fixme: USING clause handled specially in join algorithm, so swap breaks it
    /// fixme: Swapping for SEMI and ANTI joins should be alright, need to try to enable it and test
    const auto & join_info = join_step->getJoinOperator();
    if (join_info.expression.is_using || join_info.strictness != JoinStrictness::All)
        return true;

    return true;
}

}

}
