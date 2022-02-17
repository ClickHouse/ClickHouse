#pragma once

#include <Common/config.h>
#include <Disks/IDiskRemote.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadSettings.h>

#if USE_AZURE_BLOB_STORAGE
#include <azure/storage/blobs.hpp>
#endif

namespace Aws
{
namespace S3
{
class S3Client;
}
}

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
    explicit ReadBufferFromRemoteFSGather(const RemoteMetadata & metadata_, const String & path_);

    String getFileName() const;

    void reset();

    void setReadUntilPosition(size_t position) override;

    struct ReadResult
    {
        size_t size = 0;
        size_t offset = 0;
    };

    ReadResult readInto(char * data, size_t size, size_t offset, size_t ignore = 0);

    size_t getFileSize() const;

    size_t offset() const { return file_offset_of_buffer_end; }

    bool initialized() const { return current_buf != nullptr; }

protected:
    virtual SeekableReadBufferPtr createImplementationBuffer(const String & path, size_t read_until_position) const = 0;

    RemoteMetadata metadata;

private:
    bool nextImpl() override;

    void initialize();

    bool readImpl();

    SeekableReadBufferPtr current_buf;

    size_t current_buf_idx = 0;

    size_t file_offset_of_buffer_end = 0;

    /**
     * File:                        |___________________|
     * Buffer:                            |~~~~~~~|
     * file_offset_of_buffer_end:                 ^
     */
    size_t bytes_to_ignore = 0;

    size_t read_until_position = 0;

    String canonical_path;
};


#if USE_AWS_S3
/// Reads data from S3 using stored paths in metadata.
class ReadBufferFromS3Gather final : public ReadBufferFromRemoteFSGather
{
public:
    ReadBufferFromS3Gather(
        const String & path_,
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        IDiskRemote::Metadata metadata_,
        size_t max_single_read_retries_,
        const ReadSettings & settings_,
        bool threadpool_read_ = false)
        : ReadBufferFromRemoteFSGather(metadata_, path_)
        , client_ptr(std::move(client_ptr_))
        , bucket(bucket_)
        , max_single_read_retries(max_single_read_retries_)
        , settings(settings_)
        , threadpool_read(threadpool_read_)
    {
    }

    SeekableReadBufferPtr createImplementationBuffer(const String & path, size_t read_until_position) const override;

private:
    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    String bucket;
    UInt64 max_single_read_retries;
    ReadSettings settings;
    bool threadpool_read;
};
#endif


#if USE_AZURE_BLOB_STORAGE
/// Reads data from AzureBlob Storage using paths stored in metadata.
class ReadBufferFromAzureBlobStorageGather final : public ReadBufferFromRemoteFSGather
{
public:
    ReadBufferFromAzureBlobStorageGather(
        const String & path_,
        std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client_,
        IDiskRemote::Metadata metadata_,
        size_t max_single_read_retries_,
        size_t max_single_download_retries_,
        const ReadSettings & settings_,
        bool threadpool_read_ = false)
        : ReadBufferFromRemoteFSGather(metadata_, path_)
        , blob_container_client(blob_container_client_)
        , max_single_read_retries(max_single_read_retries_)
        , max_single_download_retries(max_single_download_retries_)
        , settings(settings_)
        , threadpool_read(threadpool_read_)
    {
    }

    SeekableReadBufferPtr createImplementationBuffer(const String & path, size_t read_until_position) const override;

private:
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> blob_container_client;
    size_t max_single_read_retries;
    size_t max_single_download_retries;
    ReadSettings settings;
    bool threadpool_read;
};
#endif


class ReadBufferFromWebServerGather final : public ReadBufferFromRemoteFSGather
{
public:
    ReadBufferFromWebServerGather(
            const String & path_,
            const String & uri_,
            RemoteMetadata metadata_,
            ContextPtr context_,
            size_t threadpool_read_,
            const ReadSettings & settings_)
        : ReadBufferFromRemoteFSGather(metadata_, path_)
        , uri(uri_)
        , context(context_)
        , threadpool_read(threadpool_read_)
        , settings(settings_)
    {
    }

    SeekableReadBufferPtr createImplementationBuffer(const String & path, size_t read_until_position) const override;

private:
    String uri;
    ContextPtr context;
    bool threadpool_read;
    ReadSettings settings;
};


#if USE_HDFS
/// Reads data from HDFS using stored paths in metadata.
class ReadBufferFromHDFSGather final : public ReadBufferFromRemoteFSGather
{
public:
    ReadBufferFromHDFSGather(
            const String & path_,
            const Poco::Util::AbstractConfiguration & config_,
            const String & hdfs_uri_,
            IDiskRemote::Metadata metadata_,
            size_t buf_size_)
        : ReadBufferFromRemoteFSGather(metadata_, path_)
        , config(config_)
        , buf_size(buf_size_)
    {
        const size_t begin_of_path = hdfs_uri_.find('/', hdfs_uri_.find("//") + 2);
        hdfs_directory = hdfs_uri_.substr(begin_of_path);
        hdfs_uri = hdfs_uri_.substr(0, begin_of_path);
    }

    SeekableReadBufferPtr createImplementationBuffer(const String & path, size_t read_until_position) const override;

private:
    const Poco::Util::AbstractConfiguration & config;
    String hdfs_uri;
    String hdfs_directory;
    size_t buf_size;
};
#endif

}
