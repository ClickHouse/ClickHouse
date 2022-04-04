#pragma once

#include <Interpreters/IJoin.h>

namespace DB
{

class ParallelMergeJoin : public IJoin
{
public:
    ParallelMergeJoin(std::shared_ptr<TableJoin> table_join_, const Block & /*right_sample_block*/)
        : table_join(table_join_)
    {  }

    const TableJoin & getTableJoin() const override { return *table_join; }
    bool addJoinedBlock(const Block &, bool /*check_limits*/) override { return true; }
    void checkTypesOfKeys(const Block &) const override {}

    void joinBlock(Block &, ExtraBlockPtr & /*not_processed*/) override {}

    void setTotals(const Block &) override {}
    const Block & getTotals() const override { return totals; }

    size_t getTotalRowCount() const override { return 0; }
    size_t getTotalByteCount() const override { return 0; }
    /// Has to be called only after setTotals()/mergeRightBlocks()
    bool alwaysReturnsEmptySet() const override { return false; }
    bool isParallel() const override { return true; }

    virtual std::shared_ptr<NotJoinedBlocks>
    getNonJoinedBlocks(const Block & /*left_sample_block*/, const Block & /*result_sample_block*/, UInt64 /*max_block_size*/) const override { return nullptr; }
private:
    Block totals;
    std::shared_ptr<TableJoin> table_join;
};

}
