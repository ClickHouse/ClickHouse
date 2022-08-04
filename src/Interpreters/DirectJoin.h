#pragma once

#include <Common/logger_useful.h>

#include <Core/Block.h>

#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>

#include <QueryPipeline/SizeLimits.h>

#include <Storages/IKeyValueStorage.h>
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
        std::shared_ptr<IKeyValueStorage> storage_);

    virtual const TableJoin & getTableJoin() const override { return *table_join; }

    virtual bool addJoinedBlock(const Block &, bool) override;
    virtual void checkTypesOfKeys(const Block &) const override;

    /// Join the block with data from left hand of JOIN to the right hand data (that was previously built by calls to addJoinedBlock).
    /// Could be called from different threads in parallel.
    virtual void joinBlock(Block & block, std::shared_ptr<ExtraBlock> &) override;

    virtual size_t getTotalRowCount() const override { return 0; }

    virtual size_t getTotalByteCount() const override { return 0; }

    virtual bool alwaysReturnsEmptySet() const override { return false; }

    virtual bool isFilled() const override { return true; }

    virtual std::shared_ptr<NotJoinedBlocks>
    getNonJoinedBlocks(const Block &, const Block &, UInt64) const override
    {
        return nullptr;
    }

private:
    std::shared_ptr<TableJoin> table_join;
    std::shared_ptr<IKeyValueStorage> storage;
    Block right_sample_block;
    Block sample_block_with_columns_to_add;
    Poco::Logger * log;

};

}
