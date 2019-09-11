#pragma once

#include <memory>
#include <shared_mutex>

#include <Core/Block.h>
#include <Interpreters/IJoin.h>


namespace DB
{

class AnalyzedJoin;

class MergeJoin : public IJoin
{
public:
    MergeJoin(const AnalyzedJoin & table_join_, const Block & right_sample_block);

    bool addJoinedBlock(const Block & block) override;
    void joinBlock(Block &) override;
    void joinTotals(Block &) const override {}
    void setTotals(const Block &) override {}
    size_t getTotalRowCount() const override { return right_blocks_row_count; }

private:
    mutable std::shared_mutex rwlock;
    const AnalyzedJoin & table_join;
    const NameSet required_right_keys;
    Block right_table_keys;
    Block sample_block_with_columns_to_add;
    BlocksList right_blocks;
    size_t right_blocks_row_count = 0;
    size_t right_blocks_bytes = 0;

    void addRightColumns(Block & block);
    void mergeJoin(Block & block, const Block & right_block);
};

}
