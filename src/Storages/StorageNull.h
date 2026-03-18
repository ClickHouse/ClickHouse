#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>


namespace DB
{

/** When writing, does nothing.
  * When reading, returns nothing.
  */
class StorageNull final : public IStorage
{
public:
    StorageNull(
        const StorageID & table_id_, ColumnsDescription columns_description_, ConstraintsDescription constraints_, const String & comment)
        : IStorage(table_id_)
    {
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(columns_description_);
        storage_metadata.setConstraints(constraints_);
        storage_metadata.setComment(comment);
        setInMemoryMetadata(storage_metadata);
    }

    std::string getName() const override { return "Null"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo &,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processing_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override
    {
        return Pipe(
            std::make_shared<NullSource>(storage_snapshot->getSampleBlockForColumns(column_names)));
    }

    bool parallelizeOutputAfterReading(ContextPtr) const override { return false; }

    bool supportsParallelInsert() const override { return true; }

    bool supportsSubcolumns() const override { return true; }

    bool supportsDynamicSubcolumns() const override { return true; }

    SinkToStoragePtr write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, bool) override
    {
        return std::make_shared<NullSinkToStorage>(metadata_snapshot->getSampleBlock());
    }

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & table_lock_holder) override;

    std::optional<UInt64> totalRows(const Settings &) const override
    {
        return {0};
    }
    std::optional<UInt64> totalBytes(const Settings &) const override
    {
        return {0};
    }

};

}
