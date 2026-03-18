#pragma once
#include <Storages/PartitionedSink.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{
class StorageObjectStorageSink : public SinkToStorage
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    StorageObjectStorageSink(
        ObjectStoragePtr object_storage,
        ConfigurationPtr configuration,
        const std::optional<FormatSettings> & format_settings_,
        const Block & sample_block_,
        ContextPtr context,
        const std::string & blob_path = "");

    ~StorageObjectStorageSink() override;

    String getName() const override { return "StorageObjectStorageSink"; }

    void consume(Chunk & chunk) override;

    void onFinish() override;

private:
    const Block sample_block;
    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;

    void finalizeBuffers();
    void releaseBuffers();
    void cancelBuffers();
};

class PartitionedStorageObjectStorageSink : public PartitionedSink
{
public:
    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    PartitionedStorageObjectStorageSink(
        ObjectStoragePtr object_storage_,
        ConfigurationPtr configuration_,
        std::optional<FormatSettings> format_settings_,
        const Block & sample_block_,
        ContextPtr context_,
        const ASTPtr & partition_by);

    SinkPtr createSinkForPartition(const String & partition_id) override;

private:
    void validateKey(const String & str);
    void validateNamespace(const String & str);

    ObjectStoragePtr object_storage;
    ConfigurationPtr configuration;

    const StorageObjectStorage::QuerySettings query_settings;
    const std::optional<FormatSettings> format_settings;
    const Block sample_block;
    const ContextPtr context;
};

}
