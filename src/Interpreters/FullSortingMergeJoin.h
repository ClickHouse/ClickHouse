#pragma once

#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/logger_useful.h>
#include <Poco/Logger.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TYPE_MISMATCH;
}

/// Dummy class, actual joining is done by MergeTransform
class FullSortingMergeJoin : public IJoin
{
public:
    explicit FullSortingMergeJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader & right_sample_block_,
                                  int null_direction_ = 1, JoinAlgorithm selected_algorithm_ = JoinAlgorithm::FULL_SORTING_MERGE)
        : table_join(table_join_)
        , right_sample_block(right_sample_block_)
        , null_direction(null_direction_)
        , selected_algorithm(selected_algorithm_)
    {
        LOG_TRACE(getLogger("FullSortingMergeJoin"), "Will use full sorting merge join");
    }

    std::string getName() const override { return "FullSortingMergeJoin"; }

    const TableJoin & getTableJoin() const override { return *table_join; }

    bool isCloneSupported() const override
    {
        return getTotals().empty();
    }

    std::shared_ptr<IJoin> clone(const std::shared_ptr<TableJoin> & table_join_,
        SharedHeader,
        SharedHeader right_sample_block_) const override
    {
        return std::make_shared<FullSortingMergeJoin>(table_join_, right_sample_block_, null_direction, selected_algorithm);
    }

    int getNullDirection() const { return null_direction; }

    /// The algorithm actually selected from the `join_algorithm` priority list: `full_sorting_merge`,
    /// `parallel_full_sorting_merge`, `sorted_merge` or `parallel_sorted_merge`. All four build this same
    /// object, so this cannot be recovered from list membership alone (`full_sorting_merge,
    /// parallel_full_sorting_merge` selects the former and never reaches the latter). The optimizer passes
    /// gate their rewrites on this, so listing a variant as a fallback does not silently change behavior.
    JoinAlgorithm getSelectedAlgorithm() const { return selected_algorithm; }

    /// True when `parallel_full_sorting_merge` was the algorithm selected: only then may
    /// `optimizeParallelFullSortingMergeJoin` shard the join by the hash of the join keys. The
    /// `parallel_sorted_merge` algorithm is parallelized differently - by primary-key ranges
    /// (`optimizeJoinByShards`), which keeps the in-order reads intact.
    bool isParallel() const { return selected_algorithm == JoinAlgorithm::PARALLEL_FULL_SORTING_MERGE; }

    /// True when the join was selected as `sorted_merge` or `parallel_sorted_merge`: the algorithms that are
    /// available only when both inputs can be efficiently read in the order of the join keys.
    bool isSortedMerge() const
    {
        return selected_algorithm == JoinAlgorithm::SORTED_MERGE || selected_algorithm == JoinAlgorithm::PARALLEL_SORTED_MERGE;
    }

    bool addBlockToJoin(const Block & /* block */, bool /* check_limits */) override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FullSortingMergeJoin::addBlockToJoin should not be called");
    }

    /// Strictness/kind combinations that MergeJoinAlgorithm (the engine behind this join) implements.
    /// Mirrors the checks in MergeJoinAlgorithm's constructor (MergeJoinTransform.cpp).
    static bool isMergeAlgorithmStrictnessAndKindSupported(JoinKind kind, JoinStrictness strictness)
    {
        if (strictness != JoinStrictness::Any && strictness != JoinStrictness::All && strictness != JoinStrictness::Asof)
            return false;

        if (strictness == JoinStrictness::Asof)
            return isInner(kind) || isLeft(kind);

        return isInner(kind) || isLeft(kind) || isRight(kind) || isFull(kind);
    }

    static bool isSupported(const std::shared_ptr<TableJoin> & table_join)
    {
        if (!table_join->oneDisjunct())
            return false;

        /// The actual joining is done by MergeJoinAlgorithm, which only implements
        /// Any/All/Asof strictness and Inner/Left/Right/Full kind (Asof restricted to Left/Inner).
        /// Decline anything else here so chooseJoinAlgorithm falls back to another algorithm
        /// instead of building a pipeline that raises a query exception later.
        if (!isMergeAlgorithmStrictnessAndKindSupported(table_join->kind(), table_join->strictness()))
            return false;

        /// `MergeJoinAlgorithm` never evaluates a mixed (cross-side non-equi) `ON` condition, so
        /// accepting one here would silently drop it.
        if (table_join->getMixedJoinExpression())
            return false;

        bool support_storage = !table_join->isSpecialStorage();

        const auto & on_expr = table_join->getOnlyClause();
        bool support_conditions = !on_expr.on_filter_condition_left && !on_expr.on_filter_condition_right;

        if (!on_expr.analyzer_left_filter_condition_column_name.empty() ||
            !on_expr.analyzer_right_filter_condition_column_name.empty())
            support_conditions = false;

        /// Key column can change nullability and it's not handled on type conversion stage, so algorithm should be aware of it
        bool support_using_and_nulls = !table_join->hasUsing() || !table_join->joinUseNulls();

        return support_conditions && support_using_and_nulls && support_storage;
    }

    void checkTypesOfKeys(const Block & left_block) const override
    {
        if (!isSupported(table_join))
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "FullSortingMergeJoin doesn't support specified query");

        const auto & onexpr = table_join->getOnlyClause();
        for (size_t i = 0; i < onexpr.key_names_left.size(); ++i)
        {
            DataTypePtr left_type = left_block.getByName(onexpr.key_names_left[i]).type;
            DataTypePtr right_type = right_sample_block->getByName(onexpr.key_names_right[i]).type;

            bool type_equals
                = table_join->hasUsing() ? left_type->equals(*right_type) : removeNullable(left_type)->equals(*removeNullable(right_type));

            /// Even slightly different types should be converted on previous pipeline steps.
            /// If we still have some differences, we can't join, because the algorithm expects strict type equality.
            if (!type_equals)
            {
                throw DB::Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "Type mismatch of columns to JOIN by: {} :: {} at left, {} :: {} at right",
                    onexpr.key_names_left[i], left_type->getName(),
                    onexpr.key_names_right[i], right_type->getName());
            }
        }
    }

    /// Used just to get result header
    JoinResultPtr joinBlock(Block block) override
    {
        for (const auto & col : *right_sample_block)
            block.insert(col);
        block = materializeBlock(block).cloneEmpty();
        return IJoinResult::createFromBlock(std::move(block));
    }

    void setTotals(const Block & block) override { totals = block; }
    const Block & getTotals() const override { return totals; }

    size_t getTotalRowCount() const override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FullSortingMergeJoin::getTotalRowCount should not be called");
    }

    size_t getTotalByteCount() const override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FullSortingMergeJoin::getTotalByteCount should not be called");
    }

    bool alwaysReturnsEmptySet() const override { return false; }

    StepAnalysisReport getAnalysisReport() const override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FullSortingMergeJoin::getAnalysisReport should not be called");
    }

    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & /* left_sample_block */, const Block & /* result_sample_block */, UInt64 /* max_block_size */) const override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FullSortingMergeJoin::getNonJoinedBlocks should not be called");
    }

    /// Left and right streams have the same priority and are processed simultaneously
    JoinPipelineType pipelineType() const override { return JoinPipelineType::YShaped; }

private:
    std::shared_ptr<TableJoin> table_join;
    SharedHeader right_sample_block;
    Block totals;
    int null_direction;
    JoinAlgorithm selected_algorithm;
};

}
