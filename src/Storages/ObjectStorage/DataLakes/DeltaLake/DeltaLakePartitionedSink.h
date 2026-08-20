#pragma once
#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Columns/IColumn.h>
#include <Common/HashTable/HashMap.h>
#include <Common/Arena.h>
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

/**
 * Sink to write partitioned data to DeltaLake.
 * Writes a N data files, a file per partition key, and commits them to DeltaLake metadata.
 */
class DeltaLakePartitionedSink final : public SinkToStorage, private WithContext
{
public:
    DeltaLakePartitionedSink(
        DeltaLake::WriteTransactionPtr delta_transaction_,
        const Names & partition_columns_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_,
        SharedHeader sample_block_,
        const std::optional<FormatSettings> & format_settings_,
        const String & write_format_,
        const String & write_compression_method_);

    ~DeltaLakePartitionedSink() override;

    String getName() const override { return "DeltaLakePartitionedSink"; }

    void consume(Chunk & chunk) override;

    void onException(std::exception_ptr exception) override;

    void onFinish() override;

    /// A single partition column's logical value for one partition.
    /// `is_null` covers the Delta null-equivalent forms (SQL NULL and empty string);
    /// `value` is the `toString`-serialized value and is meaningless when `is_null`.
    struct PartitionValue
    {
        String name;
        String value;
        bool is_null = false;
    };
    using PartitionValues = std::vector<PartitionValue>;

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
        explicit PartitionInfo(std::string_view partition_key_, PartitionValues partition_values_)
            : partition_key(partition_key_), partition_values(std::move(partition_values_)) {}

        /// Null-tagged grouping key that uniquely identifies this partition (not a path).
        const std::string_view partition_key;
        /// The true logical partition values; the physical directory and `partitionValues` are
        /// built from these, never parsed back out of a path.
        const PartitionValues partition_values;
        std::vector<DataFileInfo> data_files;
    };
    using PartitionInfoPtr = std::shared_ptr<PartitionInfo>;

    const LoggerPtr log;
    const Names partition_columns;
    const ObjectStoragePtr object_storage;
    const std::optional<FormatSettings> format_settings;
    const size_t data_file_max_rows;
    const size_t data_file_max_bytes;
    const std::unique_ptr<IPartitionStrategy> partition_strategy;
    const DeltaLake::WriteTransactionPtr delta_transaction;
    const String write_format;
    const String write_compression_method;

    absl::flat_hash_map<std::string_view, PartitionInfoPtr> partitions_data;
    size_t total_data_files_count = 0;
    IColumn::Selector chunk_row_index_to_partition_index;
    Arena partition_keys_arena;

    /// Per-partition-column expressions that serialize each value with `toString`,
    /// preserving nulls (result columns are `Nullable(String)`). Built once, in order.
    std::vector<IPartitionStrategy::PartitionExpressionActionsAndColumnName> partition_value_actions;

    /// Whether each partition column (in `partition_columns` order) is nullable in the table
    /// schema. A null-equivalent value for a non-nullable column is rejected up front, because
    /// Delta stores it as a JSON null, which cannot be read back into a non-nullable column.
    std::vector<UInt8> partition_column_nullable;

    /// Serialize the partition columns of `chunk` to `Nullable(String)` (one column per
    /// partition column, in `partition_columns` order).
    Columns computePartitionValueColumns(const Chunk & chunk) const;

    StorageSinkPtr createSinkForPartition(std::string_view partition_key);
    PartitionInfoPtr getPartitionDataForPartitionKey(std::string_view partition_key, const PartitionValues & partition_values);

    /// Cancel every inner sink so its WriteBuffer is not left unfinalized on failure.
    void cancelBuffers();
};

}

#endif
