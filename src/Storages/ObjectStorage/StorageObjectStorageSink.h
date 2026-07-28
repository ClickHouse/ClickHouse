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

    ~StorageObjectStorageSink() override;

    String getName() const override { return "StorageObjectStorageSink"; }

    void consume(Chunk & chunk) override;

    void onFinish() override;

    const String & getPath() const { return path; }

    size_t getFileSize() const;

private:
    const String path;
    SharedHeader sample_block;
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
