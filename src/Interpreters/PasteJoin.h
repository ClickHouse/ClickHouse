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
}

/// Dummy class, actual joining is done by MergeTransform
class PasteJoin : public IJoin
{
public:
    explicit PasteJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_)
        : table_join(table_join_)
        , right_sample_block(right_sample_block_)
    {
        LOG_TRACE(getLogger("PasteJoin"), "Will use paste join");
    }

    std::string getName() const override { return "PasteJoin"; }
    const TableJoin & getTableJoin() const override { return *table_join; }

    bool addBlockToJoin(const Block & /* block */, bool /* check_limits */) override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PasteJoin::addBlockToJoin should not be called");
    }

    static bool isSupported(const std::shared_ptr<TableJoin> & table_join)
    {
        bool support_storage = !table_join->isSpecialStorage();

        /// Key column can change nullability and it's not handled on type conversion stage, so algorithm should be aware of it
        bool support_using = !table_join->hasUsing();

        bool check_strictness = table_join->strictness() == JoinStrictness::All;

        bool if_has_keys = table_join->getClauses().empty();

        return support_using && support_storage && check_strictness && if_has_keys;
    }

    void checkTypesOfKeys(const Block & /*left_block*/) const override
    {
        if (!isSupported(table_join))
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "PasteJoin doesn't support specified query");
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PasteJoin::getTotalRowCount should not be called");
    }

    size_t getTotalByteCount() const override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PasteJoin::getTotalByteCount should not be called");
    }

    bool alwaysReturnsEmptySet() const override { return false; }

    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & /* left_sample_block */, const Block & /* result_sample_block */, UInt64 /* max_block_size */) const override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PasteJoin::getNonJoinedBlocks should not be called");
    }

    /// Left and right streams have the same priority and are processed simultaneously
    JoinPipelineType pipelineType() const override { return JoinPipelineType::YShaped; }

private:
    std::shared_ptr<TableJoin> table_join;
    Block right_sample_block;
    Block totals;
};

}
