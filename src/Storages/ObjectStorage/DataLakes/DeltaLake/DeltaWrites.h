#pragma once

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Functions/IFunction.h>
#include <IO/WriteBuffer.h>
#include <Poco/UUIDGenerator.h>
#include <Common/Config/ConfigProcessor.h>
#include <Databases/DataLake/ICatalog.h>

#include <Interpreters/Context_fwd.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/PartitionedSink.h>
#include <Common/randomSeed.h>
#include "Core/Range.h"

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

class DeltaFileNameGenerator
{
public:
    explicit DeltaFileNameGenerator(const String & path);

    String generateMetadataPath(Int64 version);

    /// {data_path, uuid_data}
    std::pair<String, String> generateDataPath(const Row & partition_values, const std::vector<String> & partition_columns);

private:
    Poco::UUIDGenerator uuid_generator;

    String data_dir;
    String metadata_dir;
};


class DeltaChunkPartitioner
{
public:
    explicit DeltaChunkPartitioner(
        const std::vector<String> & columns_to_apply_, SharedHeader sample_block_);

    using PartitionKey = Row;
    struct PartitionKeyHasher
    {
        size_t operator()(const PartitionKey & key) const;

        mutable std::hash<String> hasher;
    };

    std::vector<std::pair<PartitionKey, Chunk>> partitionChunk(const Chunk & chunk);

private:
    std::vector<String> columns_to_apply;
    SharedHeader sample_block;

    std::unordered_map<String, Int64> column_name_to_index;
};


class DeltaLakeStorageSink : public SinkToStorage
{
public:
    DeltaLakeStorageSink(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        const std::optional<FormatSettings> & format_settings_,
        SharedHeader sample_block_,
        ContextPtr context_);

    ~DeltaLakeStorageSink() override = default;

    String getName() const override { return "DeltaStorageSink"; }

    void consume(Chunk & chunk) override;

    void onFinish() override;

private:
    SharedHeader sample_block;
    ObjectStoragePtr object_storage;
    ContextPtr context;

    std::unordered_map<DeltaChunkPartitioner::PartitionKey, OutputFormatPtr, DeltaChunkPartitioner::PartitionKeyHasher> data_file_writer;
    std::unordered_map<DeltaChunkPartitioner::PartitionKey, std::unique_ptr<WriteBuffer>, DeltaChunkPartitioner::PartitionKeyHasher> data_file_buffer;
    std::unordered_map<DeltaChunkPartitioner::PartitionKey, OutputFormatPtr, DeltaChunkPartitioner::PartitionKeyHasher> metadata_file_writer;

    std::unordered_map<DeltaChunkPartitioner::PartitionKey, String, DeltaChunkPartitioner::PartitionKeyHasher> data_filename_in_storage;
    std::unordered_map<DeltaChunkPartitioner::PartitionKey, String, DeltaChunkPartitioner::PartitionKeyHasher> uuid_data_file;

    StorageObjectStorageConfigurationPtr configuration;
    std::optional<FormatSettings> format_settings;
    DeltaFileNameGenerator filename_generator;

    std::unordered_map<DeltaChunkPartitioner::PartitionKey, std::vector<Range>, DeltaChunkPartitioner::PartitionKeyHasher> column_stats;
    std::unordered_map<DeltaChunkPartitioner::PartitionKey, std::vector<Int64>, DeltaChunkPartitioner::PartitionKeyHasher> null_counts;

    std::optional<std::vector<String>> partition_columns;
    std::optional<DeltaChunkPartitioner> partitioner;

    std::unordered_map<DeltaChunkPartitioner::PartitionKey, Int32, DeltaChunkPartitioner::PartitionKeyHasher> total_rows;
    std::unordered_map<DeltaChunkPartitioner::PartitionKey, Int32, DeltaChunkPartitioner::PartitionKeyHasher> total_chunks_size;

    void finalizeBuffers();
    void releaseBuffers();
    void cancelBuffers();
    bool initializeMetadata();
};

}
