#pragma once

#include <unordered_map>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Functions/IFunction.h>
#include <IO/WriteBuffer.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/UUIDGenerator.h>
#include <Common/Config/ConfigProcessor.h>
#include <Core/Range.h>
#include <Columns/IColumn.h>
#include <IO/CompressionMethod.h>
#include <Databases/DataLake/ICatalog.h>

#if USE_AVRO

#include <Interpreters/Context_fwd.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/PartitionedSink.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Common/randomSeed.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Compiler.hh>
#include <Encoder.hh>
#include <Generic.hh>
#include <Stream.hh>
#include <ValidSchema.hh>
#include <new>


namespace DB
{

String removeEscapedSlashes(const String & json_str);

class FileNamesGenerator
{
public:
    struct Result
    {
        /// Path recorded in the Iceberg metadata files.
        /// If `write_full_path_in_iceberg_metadata` is disabled, it will be a simple relative path (e.g., /a/b/c.avro).
        /// Otherwise, it will include a prefix indicating the file system type (e.g., s3://a/b/c.avro).
        String path_in_metadata;

        /// Actual path to the object in the storage (e.g., /a/b/c.avro).
        String path_in_storage;
    };

    FileNamesGenerator() = default;
    explicit FileNamesGenerator(
        const String & table_dir_,
        const String & storage_dir_,
        bool use_uuid_in_metadata_,
        CompressionMethod compression_method_,
        const String & format_name_);

    FileNamesGenerator(const FileNamesGenerator & other);
    FileNamesGenerator & operator=(const FileNamesGenerator & other);

    Result generateDataFileName();
    Result generateManifestEntryName();
    Result generateManifestListName(Int64 snapshot_id, Int32 format_version);
    Result generateMetadataName();
    Result generateVersionHint();
    Result generatePositionDeleteFile();

    String convertMetadataPathToStoragePath(const String & metadata_path) const;

    void setVersion(Int32 initial_version_) { initial_version = initial_version_; }
    void setCompressionMethod(CompressionMethod compression_method_) { compression_method = compression_method_; }

private:
    Poco::UUIDGenerator uuid_generator;
    String table_dir;
    String storage_dir;

    String data_dir;
    String metadata_dir;
    String storage_data_dir;
    String storage_metadata_dir;
    bool use_uuid_in_metadata;
    CompressionMethod compression_method;
    String format_name;

    Int32 initial_version = 0;
};

class DataFileStatistics
{
public:
    explicit DataFileStatistics(Poco::JSON::Array::Ptr schema_);

    void update(const Chunk & chunk);

    std::vector<std::pair<size_t, size_t>> getColumnSizes() const;
    std::vector<std::pair<size_t, size_t>> getNullCounts() const;
    std::vector<std::pair<size_t, Field>> getLowerBounds() const;
    std::vector<std::pair<size_t, Field>> getUpperBounds() const;

    const std::vector<Int64> & getFieldIds() const { return field_ids; }
private:
    static Range uniteRanges(const Range & left, const Range & right);

    std::vector<Int64> field_ids;
    std::vector<Int64> column_sizes;
    std::vector<Int64> null_counts;
    std::vector<Range> ranges;
};

class MultipleFileWriter
{
public:
    explicit MultipleFileWriter(
        UInt64 max_data_file_num_rows_,
        UInt64 max_data_file_num_bytes_,
        Poco::JSON::Array::Ptr schema,
        FileNamesGenerator & filename_generator_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_,
        const std::optional<FormatSettings> & format_settings_,
        StorageObjectStorageConfigurationPtr configuration_,
        SharedHeader sample_block_);

    void consume(const Chunk & chunk);
    void finalize();
    void release();
    void cancel();
    void clearAllDataFiles() const;

    UInt64 getResultBytes() const;

    const std::vector<String> & getDataFiles() const
    {
        return data_file_names;
    }

    const DataFileStatistics & getResultStatistics() const
    {
        return stats;
    }

private:
    UInt64 max_data_file_num_rows;
    UInt64 max_data_file_num_bytes;
    DataFileStatistics stats;
    std::optional<size_t> current_file_num_rows = std::nullopt;
    std::optional<size_t> current_file_num_bytes = std::nullopt;
    std::vector<String> data_file_names;
    std::unique_ptr<WriteBufferFromFileBase> buffer;
    OutputFormatPtr output_format;
    FileNamesGenerator & filename_generator;
    ObjectStoragePtr object_storage;
    ContextPtr context;
    std::optional<FormatSettings> format_settings;
    StorageObjectStorageConfigurationPtr configuration;
    SharedHeader sample_block;
    UInt64 total_bytes = 0;
};


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
    Iceberg::FileContentType content_type);

void generateManifestList(
    const FileNamesGenerator & filename_generator,
    Poco::JSON::Object::Ptr metadata,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    const Strings & manifest_entry_names,
    Poco::JSON::Object::Ptr new_snapshot,
    Int32 manifest_length,
    WriteBuffer & buf,
    Iceberg::FileContentType content_type,
    bool use_previous_snapshots = true);

class MetadataGenerator
{
public:
    explicit MetadataGenerator(Poco::JSON::Object::Ptr metadata_object_);

    struct NextMetadataResult
    {
        Poco::JSON::Object::Ptr snapshot = nullptr;
        String metadata_path;
        String storage_metadata_path;
    };

    NextMetadataResult generateNextMetadata(
        FileNamesGenerator & generator,
        const String & metadata_filename,
        Int64 parent_snapshot_id,
        Int32 added_files,
        Int32 added_records,
        Int32 added_files_size,
        Int32 num_partitions,
        Int32 added_delete_files,
        Int32 num_deleted_rows,
        std::optional<Int64> user_defined_snapshot_id = std::nullopt,
        std::optional<Int64> user_defined_timestamp = std::nullopt);

    void generateAddColumnMetadata(const String & column_name, DataTypePtr type);
    void generateDropColumnMetadata(const String & column_name);
    void generateModifyColumnMetadata(const String & column_name, DataTypePtr type);

private:
    Poco::JSON::Object::Ptr metadata_object;

    pcg64_fast gen;
    std::uniform_int_distribution<Int32> dis;

    Int64 getMaxSequenceNumber();
    Poco::JSON::Object::Ptr getParentSnapshot(Int64 parent_snapshot_id);
};

class ChunkPartitioner
{
public:
    explicit ChunkPartitioner(
        Poco::JSON::Array::Ptr partition_specification, Poco::JSON::Object::Ptr schema, ContextPtr context, SharedHeader sample_block_);

    using PartitionKey = Row;
    struct PartitionKeyHasher
    {
        size_t operator()(const PartitionKey & key) const;

        mutable std::hash<String> hasher;
    };

    std::vector<std::pair<PartitionKey, Chunk>> partitionChunk(const Chunk & chunk);

    const std::vector<String> & getColumns() const { return columns_to_apply; }

    const std::vector<DataTypePtr> & getResultTypes() const { return result_data_types; }

private:
    SharedHeader sample_block;

    std::vector<FunctionOverloadResolverPtr> functions;
    std::vector<std::optional<size_t>> function_params;
    std::vector<String> columns_to_apply;
    std::vector<DataTypePtr> result_data_types;
};

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
        const StorageID & table_id_);

    ~IcebergStorageSink() override = default;

    String getName() const override { return "IcebergStorageSink"; }

    void consume(Chunk & chunk) override;

    void onFinish() override;

private:
    LoggerPtr log = getLogger("IcebergStorageSink");
    SharedHeader sample_block;
    std::unordered_map<ChunkPartitioner::PartitionKey, MultipleFileWriter, ChunkPartitioner::PartitionKeyHasher> writer_per_partition_key;
    ObjectStoragePtr object_storage;
    Poco::JSON::Object::Ptr metadata;
    Int64 current_schema_id;
    Poco::JSON::Object::Ptr current_schema;
    ContextPtr context;
    StorageObjectStorageConfigurationPtr configuration;
    std::optional<FormatSettings> format_settings;
    Int32 total_rows = 0;
    Int32 total_chunks_size = 0;

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
};

}

#endif
