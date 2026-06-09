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

/// Per-file column statistics carried over verbatim from a source manifest entry during a
/// manifest-only rewrite. Each vector maps an Iceberg field-id to its value. Bounds hold the
/// raw serialized bytes exactly as stored in the source manifest (no type-aware
/// re-serialization), so they round-trip losslessly.
struct DataFileColumnStatistics
{
    std::vector<std::pair<Int32, Int64>> column_sizes;
    std::vector<std::pair<Int32, Int64>> value_counts;
    std::vector<std::pair<Int32, Int64>> null_value_counts;
    std::vector<std::pair<Int32, String>> lower_bounds;
    std::vector<std::pair<Int32, String>> upper_bounds;
};

void generateManifestFile(
    Poco::JSON::Object::Ptr metadata,
    const std::vector<String> & partition_columns,
    const std::vector<Field> & partition_values,
    const std::vector<DataTypePtr> & partition_types,
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
    /// Optional per-file formats parallel to data_file_names. When non-empty, each entry's
    /// original file_format is preserved (used by manifest-only compaction, which may rewrite
    /// entries for files written by external writers in ORC/AVRO or a different format than the
    /// table's own write format). When empty, every entry is written with `format`.
    const std::vector<String> & data_file_formats = {},
    /// Optional per-file column statistics parallel to data_file_names. When non-empty, each
    /// entry's stats are written from the matching element (used by manifest-only compaction to
    /// preserve the source files' column stats). When empty, `data_file_statistics` is used.
    const std::vector<DataFileColumnStatistics> & per_file_statistics = {});

/// Per manifest-list entry counts for a manifest-only rewrite (a `replace` operation), where
/// every data file referenced by the new manifest already existed in the table. When supplied to
/// generateManifestList, the entry at the matching index is written with these existing-file and
/// existing-row counts and zero added/deleted counts, instead of the append-style defaults that
/// assume the manifest holds only newly added files.
struct ManifestListEntryExistingCounts
{
    Int64 existing_files_count = 0;
    Int64 existing_rows_count = 0;
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
    const std::vector<ManifestListEntryExistingCounts> & existing_entry_counts = {},
    /// Manifest-list entry paths (as stored in the parent snapshot's manifest list) to copy
    /// verbatim from the parent into the new manifest list. Used by manifest-only compaction to
    /// carry delete-file manifests forward unchanged, since it rewrites only the data manifests.
    /// Copied in addition to manifest_entry_names and independent of use_previous_snapshots.
    const std::unordered_set<String> & carry_forward_manifest_paths = {});

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
