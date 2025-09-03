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
    struct PartitionData
    {
        SinkPtr sink;
        std::string path;
        size_t size = 0;
    };
    using PartitionDataPtr = std::shared_ptr<PartitionData>;
    PartitionDataPtr getPartitionDataForPartitionKey(StringRef partition_key);

    const LoggerPtr log;
    const Names partition_columns;
    const ObjectStoragePtr object_storage;
    const std::optional<FormatSettings> format_settings;
    const StorageObjectStorageConfigurationPtr configuration;
    const std::unique_ptr<IPartitionStrategy> partition_strategy;
    const DeltaLake::WriteTransactionPtr delta_transaction;

    absl::flat_hash_map<StringRef, PartitionDataPtr> partition_id_to_sink;
    IColumn::Selector chunk_row_index_to_partition_index;
    Arena partition_keys_arena;
};

}

#endif
