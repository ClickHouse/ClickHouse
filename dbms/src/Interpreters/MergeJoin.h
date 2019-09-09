#pragma once

#include <memory>

#include <Core/Block.h>
#include <Interpreters/IJoin.h>


namespace DB
{

class AnalyzedJoin;

class MergeJoin : public IJoin
{
public:
    MergeJoin(const AnalyzedJoin & table_join_, const Block & right_sample_block);

    bool addJoinedBlock(const Block &) override { return false; }
    void joinBlock(Block &) override {}
    void joinTotals(Block &) const override {}
    void setTotals(const Block &) override {}

    void joinBlocks(const Block & src_block, Block & dst_block, size_t & src_row);
    size_t rightBlocksCount() const { return right_blocks.size(); }
    void addRightBlock(const Block & block) { right_blocks.push_back(block); }

private:
    const AnalyzedJoin & table_join;
    Block sample_block_with_columns_to_add;
    BlocksList right_blocks;

    void join(const Block & left_block, const Block & right_block, Block & dst_block, size_t & src_row);
};

}
