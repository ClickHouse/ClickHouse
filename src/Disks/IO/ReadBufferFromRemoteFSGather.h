#pragma once

#include <Common/config.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <IO/AsynchronousReader.h>
#include <Disks/ObjectStorages/IObjectStorage.h>

#if USE_AZURE_BLOB_STORAGE
#include <azure/storage/blobs.hpp>
#endif

namespace Aws { namespace S3 { class S3Client; } }

namespace Poco { class Logger; }

namespace DB
{

/**
 * Remote disk might need to split one clickhouse file into multiple files in remote fs.
 * This class works like a proxy to allow transition from one file into multiple.
 */
class ReadBufferFromRemoteFSGather : public ReadBuffer
{
friend class ReadIndirectBufferFromRemoteFS;

public:
    ReadBufferFromRemoteFSGather(
        const StoredObjects & blobs_to_read_,
        const ReadSettings & settings_);

    ~ReadBufferFromRemoteFSGather() override;

    String getFileName() const;

    void reset();

    void setReadUntilPosition(size_t position) override;

    IAsynchronousReader::Result readInto(char * data, size_t size, size_t offset, size_t ignore) override;

    size_t getFileSize() const;

    size_t getFileOffsetOfBufferEnd() const;

    bool initialized() const { return current_buf != nullptr; }

    String getInfoForLog();

    size_t getImplementationBufferOffset() const;

protected:
    virtual SeekableReadBufferPtr createImplementationBufferImpl(const String & path, size_t file_size) = 0;

    StoredObjects blobs_to_read;

    ReadSettings settings;

    size_t read_until_position = 0;

    String current_file_path;
    size_t current_file_size = 0;

    bool with_cache;

    String query_id;

    Poco::Logger * log;

private:
    SeekableReadBufferPtr createImplementationBuffer(const String & path, size_t file_size);

    bool nextImpl() override;

    void initialize();

    bool readImpl();

    bool moveToNextBuffer();

    void appendFilesystemCacheLog();

    SeekableReadBufferPtr current_buf;

    size_t current_buf_idx = 0;

    size_t file_offset_of_buffer_end = 0;

    /**
     * File:                        |___________________|
     * Buffer:                            |~~~~~~~|
     * file_offset_of_buffer_end:                 ^
     */
    size_t bytes_to_ignore = 0;

    size_t total_bytes_read_from_current_file = 0;

    bool enable_cache_log = false;
};


#if USE_AWS_S3
/// Reads data from S3 using stored paths in metadata.
class ReadBufferFromS3Gather final : public ReadBufferFromRemoteFSGather
{
public:
    ReadBufferFromS3Gather(
        std::shared_ptr<const Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & version_id_,
        const StoredObjects & blobs_to_read_,
        size_t max_single_read_retries_,
        const ReadSettings & settings_)
        : ReadBufferFromRemoteFSGather(blobs_to_read_, settings_)
        , client_ptr(std::move(client_ptr_))
        , bucket(bucket_)
        , version_id(version_id_)
        , max_single_read_retries(max_single_read_retries_)
    {
    }

    SeekableReadBufferPtr createImplementationBufferImpl(const String & path, size_t file_size) override;

private:
    std::shared_ptr<const Aws::S3::S3Client> client_ptr;
    String bucket;
    String version_id;
    UInt64 max_single_read_retries;
};
#endif


#if USE_AZURE_BLOB_STORAGE
/// Reads data from AzureBlob Storage using paths stored in metadata.
class ReadBufferFromAzureBlobStorageGather final : public ReadBufferFromRemoteFSGather
{
public:
    ReadBufferFromAzureBlobStorageGather(
        std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
        const StoredObjects & blobs_to_read_,
        size_t max_single_read_retries_,
        size_t max_single_download_retries_,
        const ReadSettings & settings_)
        : ReadBufferFromRemoteFSGather(blobs_to_read_, settings_)
        , blob_container_client(blob_container_client_)
        , max_single_read_retries(max_single_read_retries_)
        , max_single_download_retries(max_single_download_retries_)
    {
    }

    SeekableReadBufferPtr createImplementationBufferImpl(const String & path, size_t file_size) override;

private:
    std::shared_ptr<const Azure::Storage::Blobs::BlobContainerClient> blob_container_client;
    size_t max_single_read_retries;
    size_t max_single_download_retries;
};
#endif


class ReadBufferFromWebServerGather final : public ReadBufferFromRemoteFSGather
{
public:
    ReadBufferFromWebServerGather(
            const String & uri_,
            const StoredObjects & blobs_to_read_,
            ContextPtr context_,
            const ReadSettings & settings_)
        : ReadBufferFromRemoteFSGather(blobs_to_read_, settings_)
        , uri(uri_)
        , context(context_)
    {
    }

    SeekableReadBufferPtr createImplementationBufferImpl(const String & path, size_t file_size) override;

private:
    String uri;
    ContextPtr context;
};


#if USE_HDFS
/// Reads data from HDFS using stored paths in metadata.
class ReadBufferFromHDFSGather final : public ReadBufferFromRemoteFSGather
{
public:
    ReadBufferFromHDFSGather(
            const Poco::Util::AbstractConfiguration & config_,
            const StoredObjects & blobs_to_read_,
            const ReadSettings & settings_)
        : ReadBufferFromRemoteFSGather(blobs_to_read_, settings_)
        , config(config_)
    {
    }

    SeekableReadBufferPtr createImplementationBufferImpl(const String & path, size_t file_size) override;

private:
    const Poco::Util::AbstractConfiguration & config;
};

#endif

}
