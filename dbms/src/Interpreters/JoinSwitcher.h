#pragma once

#include <mutex>

#include <Core/Block.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/AnalyzedJoin.h>

namespace DB
{

class JoinSwitcher : public IJoin
{
public:
    JoinSwitcher(std::shared_ptr<AnalyzedJoin> table_join_, const Block & right_sample_block_);

    bool addJoinedBlock(const Block & block, bool check_limits = true) override;

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

    size_t getTotalByteCount() const override
    {
        return join->getTotalByteCount();
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
    bool switched;
    mutable std::mutex switch_mutex;
    std::shared_ptr<AnalyzedJoin> table_join;
    Block right_sample_block;

    void switchJoin();
};

}
