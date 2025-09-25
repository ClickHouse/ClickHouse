#pragma once
#include <Storages/PartitionedSink.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
class StorageObjectStorageSink : public SinkToStorage
{
public:
    StorageObjectStorageSink(
        const std::string & path_,
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationPtr configuration,
        const std::optional<FormatSettings> & format_settings_,
        SharedHeader sample_block_,
        ContextPtr context);

    /// For building a sink that receives chunks shaped like input_header_
    /// and serializes them with the schema of the format_header_. A name
    /// based position map (input --> format) is built using the input_header_.
    StorageObjectStorageSink(
        const std::string & path_,
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationPtr configuration,
        const std::optional<FormatSettings> & format_settings_,
        SharedHeader input_header_,
        SharedHeader format_header_,
        ContextPtr context);

    ~StorageObjectStorageSink() override;

    String getName() const override { return "StorageObjectStorageSink"; }

    void consume(Chunk & chunk) override;

    void onFinish() override;

    const String & getPath() const { return path; }

    size_t getFileSize() const;

private:
    /// For each column in format_header_, input_to_format_pos stores the
    /// position (index) of the column with the same name in input_header_.
    /// Used to project/reorder incoming columns before writing.
    std::vector<size_t> input_to_format_pos;

    const String path;
    /// writer or file header i.e. the format header
    SharedHeader sample_block;
    /// router or pipeline header (shape of the incoming chunks)
    SharedHeader input_header;
    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
    std::optional<size_t> result_file_size;

    void finalizeBuffers();
    void releaseBuffers();
    void cancelBuffers();
};

class PartitionedStorageObjectStorageSink : public PartitionedSink
{
public:
    PartitionedStorageObjectStorageSink(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        std::optional<FormatSettings> format_settings_,
        SharedHeader sample_block_,
        ContextPtr context_);

    SinkPtr createSinkForPartition(const String & partition_id) override;

private:
    ObjectStoragePtr object_storage;
    StorageObjectStorageConfigurationPtr configuration;

    const StorageObjectStorageQuerySettings query_settings;
    const std::optional<FormatSettings> format_settings;
    SharedHeader sample_block;
    const ContextPtr context;
};

}
