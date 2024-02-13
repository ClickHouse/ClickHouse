#pragma once
#include <Storages/PartitionedSink.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>

namespace DB
{
class StorageObjectStorageSink : public SinkToStorage
{
public:
    StorageObjectStorageSink(
        ObjectStoragePtr object_storage,
        StorageObjectStorageConfigurationPtr configuration,
        std::optional<FormatSettings> format_settings_,
        const Block & sample_block_,
        ContextPtr context,
        const std::string & blob_path = "");

    String getName() const override { return "StorageObjectStorageSink"; }

    void consume(Chunk chunk) override;

    void onCancel() override;

    void onException(std::exception_ptr exception) override;

    void onFinish() override;

private:
    const Block sample_block;
    const std::optional<FormatSettings> format_settings;

    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
    bool cancelled = false;
    std::mutex cancel_mutex;

    void finalize();
    void release();
};

class PartitionedStorageObjectStorageSink : public PartitionedSink
{
public:
    PartitionedStorageObjectStorageSink(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        std::optional<FormatSettings> format_settings_,
        const Block & sample_block_,
        ContextPtr context_,
        const ASTPtr & partition_by);

    SinkPtr createSinkForPartition(const String & partition_id) override;

private:
    ObjectStoragePtr object_storage;
    StorageObjectStorageConfigurationPtr configuration;
    const std::optional<FormatSettings> format_settings;
    const Block sample_block;
    const ContextPtr context;
};

}
