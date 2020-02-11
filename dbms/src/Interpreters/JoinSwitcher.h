#pragma once

#include <Interpreters/IJoin.h>
#include <Interpreters/Join.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/AnalyzedJoin.h>

namespace DB
{

class JoinSwitcher : public IJoin
{
public:
    JoinSwitcher(std::shared_ptr<AnalyzedJoin> table_join, const Block & right_sample_block)
    {
        if (table_join->allowMergeJoin())
            join = std::make_shared<MergeJoin>(table_join, right_sample_block);
        else
            join = std::make_shared<Join>(table_join, right_sample_block);
    }

    bool addJoinedBlock(const Block & block) override
    {
        /// TODO: switch Join -> MergeJoin
        return join->addJoinedBlock(block);
    }

    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed) override
    {
        join->joinBlock(block, not_processed);
    }

    bool hasTotals() const override
    {
        return join->hasTotals();
    }

    void setTotals(const Block & block) override
    {
        join->setTotals(block);
    }

    void joinTotals(Block & block) const override
    {
        join->joinTotals(block);
    }

    size_t getTotalRowCount() const override
    {
        return join->getTotalRowCount();
    }

    bool alwaysReturnsEmptySet() const override
    {
        return join->alwaysReturnsEmptySet();
    }

    BlockInputStreamPtr createStreamWithNonJoinedRows(const Block & block, UInt64 max_block_size) const override
    {
        return join->createStreamWithNonJoinedRows(block, max_block_size);
    }

    bool hasStreamWithNonJoinedRows() const override
    {
        return join->hasStreamWithNonJoinedRows();
    }

private:
    JoinPtr join;
};

}
