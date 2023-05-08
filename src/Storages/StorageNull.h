#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>

#include <mutex>
#include <condition_variable>
namespace DB
{

/** When writing, does nothing.
  * When reading, returns nothing.
  */
class StorageNull final : public IStorage
{
friend class NullSource;
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

        blocks_ptr = std::make_shared<BlocksPtr>();
    }
    ~StorageNull() override;
    void drop() override;
    void shutdown() override;
    bool getNewBlocks();
    void refresh(bool grab_lock = true);

    std::string getName() const override { return "Null"; }

    Pipe read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams) override;


    bool supportsParallelInsert() const override { return true; }

    SinkToStoragePtr write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr) override;

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
private:
    std::atomic<bool> shutdown_called = false;
    std::mutex mutex;
    std::condition_variable condition;
    bool is_stream_{false};
    std::shared_ptr<BlocksPtr> blocks_ptr;

};

}
