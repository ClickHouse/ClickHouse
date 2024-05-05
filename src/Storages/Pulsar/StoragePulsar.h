#pragma once

#include <Storages/IStorage.h>

#include <atomic>

namespace DB
{

class ReadFromStoragePulsar;

class StoragePulsar final : public IStorage, WithContext
{
    friend class ReadFromStoragePulsar;
    
public:
    StoragePulsar(const StorageID & table_id_, ContextPtr context_, std::unique_ptr<PulsarSettings> pulsar_settings_);

    ~StoragePulsar() override;

    std::string getName() const override { return "Pulsar"; }

    bool noPushingToViews() const override { return true; }

    void startup() override;
    void shutdown(bool is_drop) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr
    write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    /// We want to control the number of rows in a chunk inserted into Kafka
    bool prefersLargeBlocks() const override { return false; }

private:
    size_t num_consumers;
    std::atomic<bool> shutdown_called{false};
};

}
