#pragma once

#include <unordered_map>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Functions/IFunction.h>
#include <IO/WriteBuffer.h>
#include <Poco/UUIDGenerator.h>
#include <Common/Config/ConfigProcessor.h>

#if USE_AVRO

#include <Interpreters/Context_fwd.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/PartitionedSink.h>
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

class FileNamesGenerator
{
public:
    explicit FileNamesGenerator(const String & table_dir);

    String generateDataFileName();
    String generateManifestEntryName();
    String generateManifestListName(Int64 snapshot_id, Int32 format_version);
    String generateMetadataName();

    void setVersion(Int32 initial_version_) { initial_version = initial_version_; }

private:
    Poco::UUIDGenerator uuid_generator;
    String data_dir;
    String metadata_dir;
    Int32 initial_version;
};

void generateManifestFile(
    Poco::JSON::Object::Ptr metadata,
    const std::vector<String> & partition_columns,
    const std::vector<Field> & partition_values,
    const String & data_file_name,
    Poco::JSON::Object::Ptr new_snapshot,
    const String & format,
    Poco::JSON::Object::Ptr partition_spec,
    Int64 partition_spec_id,
    WriteBuffer & buf);

void generateManifestList(
    Poco::JSON::Object::Ptr metadata,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    const Strings & manifest_entry_names,
    Poco::JSON::Object::Ptr new_snapshot,
    Int32 manifest_length,
    WriteBuffer & buf);

class MetadataGenerator
{
public:
    explicit MetadataGenerator(Poco::JSON::Object::Ptr metadata_object_);

    std::pair<Poco::JSON::Object::Ptr, String> generateNextMetadata(
        FileNamesGenerator & generator,
        const String & metadata_filename,
        Int64 parent_snapshot_id,
        Int32 added_files,
        Int32 added_records,
        Int32 added_files_size,
        Int32 num_partitions);

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

private:
    SharedHeader sample_block;

    std::vector<FunctionOverloadResolverPtr> functions;
    std::vector<std::optional<size_t>> function_params;
    std::vector<String> columns_to_apply;
};

class IcebergStorageSink : public SinkToStorage
{
public:
    IcebergStorageSink(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        const std::optional<FormatSettings> & format_settings_,
        SharedHeader sample_block_,
        ContextPtr context_);

    ~IcebergStorageSink() override = default;

    String getName() const override { return "IcebergStorageSink"; }

    void consume(Chunk & chunk) override;

    void onFinish() override;

private:
    SharedHeader sample_block;
    std::unordered_map<ChunkPartitioner::PartitionKey, std::unique_ptr<WriteBuffer>, ChunkPartitioner::PartitionKeyHasher> write_buffers;
    std::unordered_map<ChunkPartitioner::PartitionKey, OutputFormatPtr, ChunkPartitioner::PartitionKeyHasher> writers;
    std::unordered_map<ChunkPartitioner::PartitionKey, String, ChunkPartitioner::PartitionKeyHasher> data_filenames;
    ObjectStoragePtr object_storage;
    Poco::JSON::Object::Ptr metadata;
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
};

}

#endif
