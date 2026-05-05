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
    std::optional<Int64> user_defined_sequence_number = std::nullopt);

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
    bool use_previous_snapshots = true);

/// Writes a manifest file whose entries are pre-existing data files carried over
/// from a previous snapshot with status=EXISTING. Used by DROP PARTITION when a
/// matched manifest contains both files that should be dropped and files that
/// should survive: the dropped files are simply omitted from the new manifest,
/// and the survivors are re-emitted here so the new manifest list replaces the
/// old one. Per Iceberg v2 spec, surviving entries preserve their original
/// snapshot_id and sequence_number rather than inheriting the new snapshot's.
void generateExistingManifestFile(
    Poco::JSON::Object::Ptr metadata,
    Poco::JSON::Object::Ptr partition_spec,
    Int64 partition_spec_id,
    const std::vector<String> & partition_columns,
    const std::vector<DataTypePtr> & partition_types,
    const std::vector<Iceberg::ProcessedManifestFileEntryPtr> & entries,
    WriteBuffer & buf);

/// Writes a manifest list for a snapshot that only removes files (DROP PARTITION).
/// Entries can be a mix of:
///   - newly written EXISTING-status manifests (rewrites of partially-matched manifests)
///   - untouched manifests carried over from the parent snapshot by path
/// Untouched manifests are copied verbatim from the parent's manifest list (so their
/// counts, partition summaries, and sequence numbers are preserved); paths in
/// `skip_manifest_paths` from the parent are dropped entirely.
struct ManifestListEntryForDelete
{
    Iceberg::IcebergPathFromMetadata manifest_path;
    Int64 manifest_length = 0;
    /// Smallest sequence number among entries inside this manifest. For a survivor
    /// rewrite that holds only EXISTING entries, this is the inherited sequence
    /// number of those entries — not the new DROP snapshot's. The manifest list's
    /// own `added_snapshot_id` and `sequence_number` (which identify the manifest
    /// *file*, not its contents) are filled by the writer from the new snapshot.
    Int64 min_sequence_number = 0;
    Int32 added_files_count = 0;
    Int32 existing_files_count = 0;
    Int32 deleted_files_count = 0;
    Int32 added_rows_count = 0;
    Int32 existing_rows_count = 0;
    Int32 deleted_rows_count = 0;
    Iceberg::FileContentType content_type = Iceberg::FileContentType::DATA;
};

void generateManifestListForDelete(
    const Iceberg::IcebergPathResolver & path_resolver,
    Poco::JSON::Object::Ptr metadata,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    Poco::JSON::Object::Ptr new_snapshot,
    const std::vector<ManifestListEntryForDelete> & new_entries,
    Int64 partition_spec_id,
    const std::unordered_set<String> & skip_manifest_paths,
    WriteBuffer & buf);

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
