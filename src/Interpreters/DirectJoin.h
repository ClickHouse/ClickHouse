#pragma once


#include <Core/Block.h>

#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>

#include <QueryPipeline/SizeLimits.h>

#include <Interpreters/IKeyValueEntity.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class NotJoinedBlocks;

class DirectKeyValueJoin : public IJoin
{
public:
    DirectKeyValueJoin(
        std::shared_ptr<TableJoin> table_join_,
        const Block & right_sample_block_,
        std::shared_ptr<const IKeyValueEntity> storage_);

    DirectKeyValueJoin(
        std::shared_ptr<TableJoin> table_join_,
        const Block & right_sample_block_,
        std::shared_ptr<const IKeyValueEntity> storage_,
        const Block & right_sample_block_with_storage_column_names_);

    std::string getName() const override { return "DirectKeyValueJoin"; }
    const TableJoin & getTableJoin() const override { return *table_join; }

    bool addBlockToJoin(const Block &, bool) override;
    void checkTypesOfKeys(const Block &) const override;

    /// Join the block with data from left hand of JOIN to the right hand data (that was previously built by calls to addBlockToJoin).
    /// Could be called from different threads in parallel.
    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> &) override;

    size_t getTotalRowCount() const override { return 0; }

    size_t getTotalByteCount() const override { return 0; }

    bool alwaysReturnsEmptySet() const override { return false; }

    bool isFilled() const override { return true; }

    IBlocksStreamPtr getNonJoinedBlocks(const Block &, const Block &, UInt64) const override { return nullptr; }

private:
    std::shared_ptr<TableJoin> table_join;
    std::shared_ptr<const IKeyValueEntity> storage;
    Block right_sample_block;
    Block right_sample_block_with_storage_column_names;
    Block sample_block_with_columns_to_add;
    LoggerPtr log;

};

}
