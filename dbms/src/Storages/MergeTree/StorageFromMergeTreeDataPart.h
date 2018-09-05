#pragma once

#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Core/Defines.h>

#include <ext/shared_ptr_helper.h>


namespace DB
{

/// A Storage that allows reading from a single MergeTree data part.
class StorageFromMergeTreeDataPart : public ext::shared_ptr_helper<StorageFromMergeTreeDataPart>, public IStorage
{
public:
    String getName() const override { return "FromMergeTreeDataPart"; }
    String getTableName() const override { return part->storage.getTableName() + " (part " + part->name + ")"; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override
    {
        return MergeTreeDataSelectExecutor(part->storage).readFromParts(
            {part}, column_names, query_info, context, processed_stage, max_block_size, num_streams, 0);
    }

protected:
    StorageFromMergeTreeDataPart(const MergeTreeData::DataPartPtr & part_)
        : IStorage(part_->storage.getColumns()), part(part_)
    {}

private:
    MergeTreeData::DataPartPtr part;
};

}
