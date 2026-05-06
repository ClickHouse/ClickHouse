#pragma once

#include <unordered_map>
#include <Columns/IColumn.h>
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
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataFileEntry.h>
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

/// Read a data-file sidecar and return its contents in Iceberg wire format.
/// The returned struct carries the row count, byte size, and per-column statistics.
IcebergSerializedFileStats readDataFileSidecar(
    const String & sidecar_storage_path,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & context);

/// Write a sidecar Avro file alongside a data file.
/// All six fields are written; empty stat vectors are valid when statistics are unavailable.
void writeDataFileSidecar(
    const String & data_file_storage_path,
    const IcebergSerializedFileStats & stats,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & context);

/// Convert in-memory DataFileStatistics (ClickHouse-internal) to the Iceberg wire format.
/// Bounds are serialized to bytes using the same encoding used in the manifest file,
/// so the result can be stored in sidecar Avro files and used at commit time on any node.
IcebergSerializedFileStats serializeDataFileStats(
    const DataFileStatistics & stats,
    SharedHeader sample_block,
    Int64 record_count,
    Int64 file_size_in_bytes);

/// Generate an Iceberg manifest file for a set of data files.
///
/// \param data_file_statistics  Aggregate column statistics applied to every file (regular
///     INSERT and mutation paths).  Ignored when \p per_file_stats is non-empty.
/// \param per_file_stats  Per-file pre-serialized statistics (export-commit path).
///     When non-empty each entry overrides both the record count / file size AND the column
///     statistics for the corresponding file.  Leave empty to preserve the existing behaviour.
void generateManifestFile(
    Poco::JSON::Object::Ptr metadata,
    const std::vector<String> & partition_columns,
    const std::vector<Field> & partition_values,
    const std::vector<DataTypePtr> & partition_types,
    const std::vector<String> & data_file_names,
    const std::optional<DataFileStatistics> & data_file_statistics,
    SharedHeader sample_block,
    Poco::JSON::Object::Ptr new_snapshot,
    const String & format,
    Poco::JSON::Object::Ptr partition_spec,
    Int64 partition_spec_id,
    WriteBuffer & buf,
    Iceberg::FileContentType content_type,
    const std::vector<IcebergSerializedFileStats> & per_file_stats = {});

void generateManifestList(
    const FileNamesGenerator & filename_generator,
    Poco::JSON::Object::Ptr metadata,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    const Strings & manifest_entry_names,
    Poco::JSON::Object::Ptr new_snapshot,
    Int64 manifest_length,
    WriteBuffer & buf,
    Iceberg::FileContentType content_type,
    bool use_previous_snapshots = true);

std::string getIcebergExportPartSidecarStoragePath(const String & data_file_storage_path);

class IcebergStorageSink : public SinkToStorage
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
    const String blob_storage_type_name;
    const String blob_storage_namespace_name;

};

class IcebergImportSink : public SinkToStorage
{
public:
    IcebergImportSink(
        std::shared_ptr<DataLake::ICatalog> catalog_,
        const Iceberg::PersistentTableComponents & persistent_table_components_,
        Poco::JSON::Object::Ptr metadata_json_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_,
        std::optional<FormatSettings> format_settings_,
        const String & write_format_,
        SharedHeader sample_block_,
        const DataLakeStorageSettings & data_lake_settings_,
        std::function<void(const std::string &)> new_file_path_callback_ = {});

    ~IcebergImportSink() override;

    String getName() const override { return "IcebergImportSink"; }

    void consume(Chunk & chunk) override;

    void onFinish() override;
    void onException(std::exception_ptr exception) override;

private:
    void finalizeBuffers();
    void releaseBuffers();
    void cancelBuffers();

    std::shared_ptr<DataLake::ICatalog> catalog;
    const Iceberg::PersistentTableComponents & persistent_table_components;
    Poco::JSON::Object::Ptr metadata_json;
    Poco::JSON::Object::Ptr current_schema;
    FileNamesGenerator filename_generator;
    ObjectStoragePtr object_storage;
    ContextPtr context;
    std::optional<FormatSettings> format_settings;
    const String& write_format;
    SharedHeader sample_block;
    std::unique_ptr<MultipleFileWriter> writer;
    const DataLakeStorageSettings & data_lake_settings;
    std::function<void(const std::string &)> new_file_path_callback;
};

}

#endif
