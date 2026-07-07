#pragma once
#include <Storages/PartitionedSink.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
class WriteBufferFromFileBase;

class StorageObjectStorageSink final : public SinkToStorage
{
public:
    StorageObjectStorageSink(
        const std::string & path_,
        ObjectStoragePtr object_storage,
        const std::optional<FormatSettings> & format_settings_,
        SharedHeader sample_block_,
        ContextPtr context,
        const String & format,
        const String & compression_method);

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
    /// Non-owning pointer to the underlying file buffer returned by writeObject(),
    /// captured before the compression wrap. Used to fdatasync the data file when
    /// `object_storage_fsync_after_insert` is enabled: compression wrappers inherit
    /// WriteBuffer::sync() which does not fdatasync, so we must sync the file buffer.
    WriteBufferFromFileBase * file_buf = nullptr;
    const bool fsync_after_insert;
    OutputFormatPtr writer;
    std::optional<size_t> result_file_size;

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
