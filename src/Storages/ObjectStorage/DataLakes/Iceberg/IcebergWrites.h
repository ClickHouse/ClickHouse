#pragma once

#include <unordered_map>
#include <unordered_set>
#include <Core/Range.h>
#include <Core/SortDescription.h>
#include <Databases/DataLake/ICatalog.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Functions/IFunction.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBuffer.h>
#include <Processors/Chunk.h>
#include <Storages/KeyDescription.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/UUIDGenerator.h>
#include <Common/Config/ConfigProcessor.h>

#if USE_AVRO

#include <Interpreters/Context_fwd.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/PartitionedSink.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ChunkPartitioner.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/DataFileStatistics.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MultipleFileWriter.h>

#include <Common/randomSeed.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Compiler.hh>
#include <Encoder.hh>
#include <Generic.hh>
#include <Stream.hh>
#include <ValidSchema.hh>


namespace DB
{

String removeEscapedSlashes(const String & json_str);

String stringifyJSON(const Poco::Dynamic::Var & json, unsigned indent = 0);

/// Per-file column statistics carried over verbatim from a source manifest entry during a manifest-only rewrite.
struct DataFileColumnStatistics
{
    std::vector<std::pair<Int32, Int64>> column_sizes;
    std::vector<std::pair<Int32, Int64>> value_counts;
    std::vector<std::pair<Int32, Int64>> null_value_counts;
    std::vector<std::pair<Int32, String>> lower_bounds;
    std::vector<std::pair<Int32, String>> upper_bounds;
};

/// Per-file manifest-entry lineage (`added_snapshot_id`, data `sequence_number` and `file_sequence_number`) carried over for a manifest-only rewrite.
struct DataFileEntryLineage
{
    std::optional<Int64> added_snapshot_id;
    std::optional<Int64> sequence_number;
    std::optional<Int64> file_sequence_number;
};

void generateManifestFile(
    Poco::JSON::Object::Ptr metadata,
    const std::vector<String> & partition_columns,
    const std::vector<Field> & partition_values,
    const DataTypes & partition_types,
    const std::vector<Iceberg::IcebergPathFromMetadata> & data_file_names,
    const std::vector<UInt64> & data_file_row_counts,
    const std::vector<UInt64> & data_file_byte_counts,
    const std::optional<DataFileStatistics> & data_file_statistics,
    SharedHeader sample_block,
    Poco::JSON::Object::Ptr new_snapshot,
    const String & format,
    Poco::JSON::Object::Ptr partition_spec,
    Int64 partition_spec_id,
    WriteBuffer & buf,
    Iceberg::FileContentType content_type,
    std::optional<Int64> user_defined_sequence_number = std::nullopt,
    /// Optional per-file formats parallel to `data_file_names`; when non-empty each entry's original `file_format` is preserved, else `format` is used.
    const std::vector<String> & data_file_formats = {},
    /// Optional per-file column statistics parallel to `data_file_names`; when non-empty each entry's stats come from the matching element, else `data_file_statistics` is used.
    const std::vector<DataFileColumnStatistics> & per_file_statistics = {},
    /// Optional per-file `sort_order_id` parallel to `data_file_names`; when set it is written back to preserve sortedness, else the field is left null.
    const std::vector<std::optional<Int32>> & data_file_sort_order_ids = {},
    /// Optional per-file manifest-entry lineage parallel to `data_file_names`; when non-empty entries are written as EXISTING keeping their original snapshot-id and sequence number, else as ADDED by the new snapshot.
    const std::vector<DataFileEntryLineage> & per_file_entry_lineage = {},
    /// Optional schema to serialize into the manifest's Avro `schema` header; when null the table's current schema is used.
    Poco::JSON::Object::Ptr schema_to_serialize = nullptr);

/// Per manifest-list entry existing-file/existing-row counts for a manifest-only rewrite, where every referenced data file already existed.
struct ManifestListEntryExistingCounts
{
    Int64 existing_files_count = 0;
    Int64 existing_rows_count = 0;
    /// Minimum data sequence number across the entries in this manifest, used as the manifest-list `min_sequence_number`.
    Int64 min_sequence_number = 0;
};

void generateManifestList(
    const Iceberg::IcebergPathResolver & path_resolver,
    Poco::JSON::Object::Ptr metadata,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    const std::vector<Iceberg::IcebergPathFromMetadata> & manifest_entry_names,
    Poco::JSON::Object::Ptr new_snapshot,
    const std::vector<Int64> & manifest_entry_sizes,
    WriteBuffer & buf,
    Iceberg::FileContentType content_type,
    bool use_previous_snapshots = true,
    const std::vector<Iceberg::FileContentType> & per_entry_content_types = {},
    const std::vector<ManifestListEntryExistingCounts> & existing_entry_counts = {},
    const std::unordered_set<String> & carry_forward_manifest_paths = {},
    const std::vector<Int64> & entry_partition_spec_ids = {},
    const std::vector<std::vector<std::pair<Field, DataTypePtr>>> & entry_partition_summaries = {});

class IcebergStorageSink final : public SinkToStorage
{
public:
    IcebergStorageSink(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        const std::optional<FormatSettings> & format_settings_,
        SharedHeader sample_block_,
        ContextPtr context_,
        std::shared_ptr<DataLake::ICatalog> catalog_,
        const Iceberg::PersistentTableComponents & persistent_table_components_,
        const StorageID & table_id_);

    ~IcebergStorageSink() override;


    String getName() const override { return "IcebergStorageSink"; }

    void consume(Chunk & chunk) override;

    void onFinish() override;
    void onException(std::exception_ptr exception) override;

private:
    LoggerPtr log = getLogger("IcebergStorageSink");
    SharedHeader sample_block;
    std::unordered_map<ChunkPartitioner::PartitionKey, MultipleFileWriter, ChunkPartitioner::PartitionKeyHasher> writer_per_partition_key;
    std::unordered_map<ChunkPartitioner::PartitionKey, std::vector<Field>, ChunkPartitioner::PartitionKeyHasher> last_fields_of_last_chunks;
    std::unordered_map<String, size_t> column_name_to_column_index;
    ObjectStoragePtr object_storage;
    Poco::JSON::Object::Ptr metadata;
    Int64 current_schema_id;
    Poco::JSON::Object::Ptr current_schema;
    ContextPtr context;
    std::optional<FormatSettings> format_settings;
    KeyDescription sort_description;
    Int64 total_rows = 0;
    Int64 total_chunks_size = 0;

    void finalizeBuffers();
    void releaseBuffers();
    void cancelBuffers();
    bool initializeMetadata();

    FileNamesGenerator filename_generator;
    std::optional<ChunkPartitioner> partitioner;
    Poco::JSON::Object::Ptr partititon_spec;
    Int64 partition_spec_id;

    std::shared_ptr<DataLake::ICatalog> catalog;
    StorageID table_id;
    CompressionMethod metadata_compression_method;
    Iceberg::PersistentTableComponents persistent_table_components;
    const DataLakeStorageSettings & data_lake_settings;
    const String write_format;

};

}

#endif
