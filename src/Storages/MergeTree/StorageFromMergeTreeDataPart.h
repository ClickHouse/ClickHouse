#pragma once

#include <Storages/IStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Core/Defines.h>

#include <ext/shared_ptr_helper.h>


namespace DB
{

/// A Storage that allows reading from a single MergeTree data part.
class StorageFromMergeTreeDataPart final : public ext::shared_ptr_helper<StorageFromMergeTreeDataPart>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageFromMergeTreeDataPart>;
public:
    String getName() const override { return "FromMergeTreeDataPart"; }

    Pipes read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        unsigned num_streams) override
    {
        return MergeTreeDataSelectExecutor(part->storage)
            .readFromParts({part}, column_names, metadata_snapshot, query_info, context, max_block_size, num_streams);
    }


    bool supportsIndexForIn() const override { return true; }

    bool mayBenefitFromIndexForIn(
        const ASTPtr & left_in_operand, const Context & query_context, const StorageMetadataPtr & metadata_snapshot) const override
    {
        return part->storage.mayBenefitFromIndexForIn(left_in_operand, query_context, metadata_snapshot);
    }

    NamesAndTypesList getVirtuals() const override
    {
        return part->storage.getVirtuals();
    }

protected:
    StorageFromMergeTreeDataPart(const MergeTreeData::DataPartPtr & part_)
        : IStorage(getIDFromPart(part_))
        , part(part_)
    {
        setInMemoryMetadata(part_->storage.getInMemoryMetadata());
    }

private:
    MergeTreeData::DataPartPtr part;

    static StorageID getIDFromPart(const MergeTreeData::DataPartPtr & part_)
    {
        auto table_id = part_->storage.getStorageID();
        return StorageID(table_id.database_name, table_id.table_name + " (part " + part_->name + ")");
    }
};

}
