#pragma once

#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
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
    explicit FullSortingMergeJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_)
        : table_join(table_join_)
        , right_sample_block(right_sample_block_)
    {
        LOG_TRACE(&Poco::Logger::get("FullSortingMergeJoin"), "Will use full sorting merge join");
    }

    const TableJoin & getTableJoin() const override { return *table_join; }

    bool addJoinedBlock(const Block & /* block */, bool /* check_limits */) override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FullSortingMergeJoin::addJoinedBlock should not be called");
    }

    static bool isSupported(const std::shared_ptr<TableJoin> & table_join)
    {
        if (!table_join->oneDisjunct())
            return false;

        bool support_storage = !table_join->isSpecialStorage();

        const auto & on_expr = table_join->getOnlyClause();
        bool support_conditions = !on_expr.on_filter_condition_left && !on_expr.on_filter_condition_right;

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
            DataTypePtr right_type = right_sample_block.getByName(onexpr.key_names_right[i]).type;

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
    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & /* not_processed */) override
    {
        for (const auto & col : right_sample_block)
            block.insert(col);
        block = materializeBlock(block).cloneEmpty();
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

    std::shared_ptr<NotJoinedBlocks>
    getNonJoinedBlocks(const Block & /* left_sample_block */, const Block & /* result_sample_block */, UInt64 /* max_block_size */) const override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FullSortingMergeJoin::getNonJoinedBlocks should not be called");
    }

    /// Left and right streams have the same priority and are processed simultaneously
    JoinPipelineType pipelineType() const override { return JoinPipelineType::YShaped; }

private:
    std::shared_ptr<TableJoin> table_join;
    Block right_sample_block;
    Block totals;
};

}
