#pragma once
#include <Storages/PartitionedSink.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Interpreters/Context_fwd.h>

#include <functional>

namespace DB
{

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

class StorageObjectStorageSink final : public SinkToStorage
{
public:
    /// Called when the current object has reached the size limit configured by `*_split_on_write_by_size_bytes`.
    /// Returns the path of the next object to write the data into.
    using GetNextPathCallback = std::function<String()>;

    StorageObjectStorageSink(
        const std::string & path_,
        ObjectStoragePtr object_storage_,
        const std::optional<FormatSettings> & format_settings_,
        SharedHeader sample_block_,
        ContextPtr context_,
        const String & format_,
        const String & compression_method_,
        size_t split_on_write_by_size_bytes_ = 0,
        GetNextPathCallback get_next_path_ = {});

    ~StorageObjectStorageSink() override;

    String getName() const override { return "StorageObjectStorageSink"; }

    void consume(Chunk & chunk) override;

    void onFinish() override;

    /// The path of the object that is being written at the moment.
    const String & getPath() const { return path; }

    /// The size of the last written object. Makes sense only when the data is not split into multiple objects.
    size_t getFileSize() const;

private:
    String path;
    const ObjectStoragePtr object_storage;
    const std::optional<FormatSettings> format_settings;
    SharedHeader sample_block;
    const ContextPtr context;
    const String format;
    const String compression_method;
    const size_t split_on_write_by_size_bytes;
    const GetNextPathCallback get_next_path;

    std::unique_ptr<WriteBuffer> write_buf;
    /// Non-owning pointer to the buffer that writes to the object storage (`write_buf` may be a compressing wrapper around it).
    /// It is used to count the number of bytes written to the object.
    WriteBuffer * destination_buf = nullptr;
    OutputFormatPtr writer;
    std::optional<size_t> result_file_size;

    void initialize();
    void finalizeBuffers();
    void releaseBuffers();
    void cancelBuffers();
};

class PartitionedStorageObjectStorageSink final : public PartitionedSink
{
public:
    PartitionedStorageObjectStorageSink(
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        std::optional<FormatSettings> format_settings_,
        SharedHeader sample_block_,
        ContextPtr context_);

    SinkPtr createSinkForPartition(const String & partition_id) override;

    /// Returns the object path of the last object written by `createSinkForPartition`.
    /// This is the final resolved path (after any rewrite by `checkAndGetNewFileOnInsertIfNeeded`),
    /// not the partition id passed to `createSinkForPartition`.
    const String & getLastWrittenObjectPath() const { return last_written_object_path; }

private:
    ObjectStoragePtr object_storage;
    StorageObjectStorageConfigurationPtr configuration;

    const StorageObjectStorageQuerySettings query_settings;
    const std::optional<FormatSettings> format_settings;
    SharedHeader sample_block;
    const ContextPtr context;
    String last_written_object_path;
};

}
