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

    /// Called after the object returned by `GetNextPathCallback` has been written and committed. Only then
    /// the key is registered in the table, so that a concurrent `SELECT` never sees the key of an object
    /// that is still being written or that the insert could not create at all.
    using PublishPathCallback = std::function<void(const String &)>;

    StorageObjectStorageSink(
        const std::string & path_,
        ObjectStoragePtr object_storage_,
        const std::optional<FormatSettings> & format_settings_,
        SharedHeader sample_block_,
        ContextPtr context_,
        const String & format_,
        const String & compression_method_,
        size_t split_on_write_by_size_bytes_ = 0,
        GetNextPathCallback get_next_path_ = {},
        PublishPathCallback publish_path_ = {});

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
    const PublishPathCallback publish_path;
    /// The first object of the insert is already a part of the table; the next ones are registered
    /// in it only after they have been written.
    bool path_is_published = true;

    /// The buffer that writes into the object storage. It is also used to count the number of bytes
    /// written to the object. It is declared before `write_buf` so that it outlives the compressing
    /// wrapper referencing it.
    std::unique_ptr<WriteBuffer> destination_buf;
    /// The non-owning compressing wrapper around `destination_buf`; it is empty if the data is written uncompressed.
    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
    std::optional<size_t> result_file_size;

    /// The buffer the data is formatted into: the compressing wrapper if the data is compressed,
    /// the object storage buffer otherwise.
    WriteBuffer & getWriteBuffer();

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
