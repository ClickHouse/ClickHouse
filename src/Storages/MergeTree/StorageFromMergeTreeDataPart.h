#pragma once

#include <Storages/IStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Core/Defines.h>

#include <ext/shared_ptr_helper.h>
#include <Processors/Executors/TreeExecutorBlockInputStream.h>


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
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        unsigned num_streams) override
    {
        return MergeTreeDataSelectExecutor(part->storage).readFromParts(
                {part}, column_names, query_info, context, max_block_size, num_streams);
    }


    bool supportsIndexForIn() const override { return true; }

    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context & query_context) const override
    {
        return part->storage.mayBenefitFromIndexForIn(left_in_operand, query_context);
    }

    //bool hasAnyTTL() const override { return part->storage.hasAnyTTL(); }

    ColumnDependencies getColumnDependencies(const NameSet & updated_columns) const override
    {
        return part->storage.getColumnDependencies(updated_columns);
    }

    StorageInMemoryMetadata getInMemoryMetadata() const override
    {
        return part->storage.getInMemoryMetadata();
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
        setColumns(part_->storage.getColumns());
        setIndices(part_->storage.getIndices());
        setSortingKey(part_->storage.getSortingKey());
        setColumnTTLs(part->storage.getColumnTTLs());
        setTableTTLs(part->storage.getTableTTLs());
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
