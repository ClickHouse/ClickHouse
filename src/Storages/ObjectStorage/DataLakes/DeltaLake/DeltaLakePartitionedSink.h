#pragma once
#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Columns/IColumn.h>
#include <Common/HashTable/HashMap.h>
#include <Common/Arena.h>
#include <Common/PODArray.h>
#include <absl/container/flat_hash_map.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Storages/IPartitionStrategy.h>


namespace DeltaLake
{
class WriteTransaction;
using WriteTransactionPtr = std::shared_ptr<WriteTransaction>;
}

namespace DB
{
class DeltaLakeMetadataDeltaKernel;
class StorageObjectStorageConfiguration;
using StorageObjectStorageConfigurationPtr = std::shared_ptr<StorageObjectStorageConfiguration>;

/**
 * Sink to write partitioned data to DeltaLake.
 * Writes a N data files, a file per partition key, and commits them to DeltaLake metadata.
 */
class DeltaLakePartitionedSink : public SinkToStorage, private WithContext
{
public:
    DeltaLakePartitionedSink(
        DeltaLake::WriteTransactionPtr delta_transaction_,
        StorageObjectStorageConfigurationPtr configuration_,
        const Names & partition_columns_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_,
        SharedHeader sample_block_,
        const std::optional<FormatSettings> & format_settings_);

    ~DeltaLakePartitionedSink() override = default;

    String getName() const override { return "DeltaLakePartitionedSink"; }

    void consume(Chunk & chunk) override;

    void onFinish() override;

private:
    using StorageSinkPtr = std::unique_ptr<StorageObjectStorageSink>;

    struct DataFileInfo
    {
        explicit DataFileInfo(StorageSinkPtr sink_) : sink(std::move(sink_)) {}

        StorageSinkPtr sink;
        size_t written_bytes = 0;
        size_t written_rows = 0;
    };
    struct PartitionInfo
    {
        explicit PartitionInfo(StringRef partition_key_) : partition_key(partition_key_) {}

        const StringRef partition_key;
        std::vector<DataFileInfo> data_files;
    };
    using PartitionInfoPtr = std::shared_ptr<PartitionInfo>;

    const LoggerPtr log;
    const Names partition_columns;
    const ObjectStoragePtr object_storage;
    const std::optional<FormatSettings> format_settings;
    const StorageObjectStorageConfigurationPtr configuration;
    const size_t data_file_max_rows;
    const size_t data_file_max_bytes;
    const std::unique_ptr<IPartitionStrategy> partition_strategy;
    const DeltaLake::WriteTransactionPtr delta_transaction;

    absl::flat_hash_map<StringRef, PartitionInfoPtr> partitions_data;
    size_t total_data_files_count = 0;
    IColumn::Selector chunk_row_index_to_partition_index;
    Arena partition_keys_arena;

    StorageSinkPtr createSinkForPartition(StringRef partition_key);
    PartitionInfoPtr getPartitionDataForPartitionKey(StringRef partition_key);
};

}

#endif
