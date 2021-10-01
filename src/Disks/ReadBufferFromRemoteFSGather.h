#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <Disks/IDiskRemote.h>
#include <IO/ReadBufferFromFile.h>

namespace Aws
{
namespace S3
{
class S3Client;
}
}

namespace DB
{

class ReadBufferFromRemoteFSGather : public ReadBuffer
{
friend class ThreadPoolRemoteFSReader;
friend class ReadIndirectBufferFromRemoteFS;

public:
    explicit ReadBufferFromRemoteFSGather(const RemoteMetadata & metadata_);

    String getFileName() const { return metadata.metadata_file_path; }

    void reset();

protected:
    size_t readInto(char * data, size_t size, size_t offset);

    virtual SeekableReadBufferPtr createImplementationBuffer(const String & path) const = 0;

    RemoteMetadata metadata;

private:
    bool nextImpl() override;

    SeekableReadBufferPtr initialize();

    bool readImpl();

    SeekableReadBufferPtr current_buf;

    size_t current_buf_idx = 0;

    size_t absolute_position = 0;
};


#if USE_AWS_S3
/// Reads data from S3 using stored paths in metadata.
class ReadBufferFromS3Gather final : public ReadBufferFromRemoteFSGather
{
public:
    ReadBufferFromS3Gather(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        IDiskRemote::Metadata metadata_,
        size_t max_single_read_retries_,
        size_t buf_size_,
        bool threadpool_read_ = false)
        : ReadBufferFromRemoteFSGather(metadata_)
        , client_ptr(std::move(client_ptr_))
        , bucket(bucket_)
        , max_single_read_retries(max_single_read_retries_)
        , buf_size(buf_size_)
        , threadpool_read(threadpool_read_)
    {
    }

    SeekableReadBufferPtr createImplementationBuffer(const String & path) const override;

private:
    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    String bucket;
    UInt64 max_single_read_retries;
    size_t buf_size;
    bool threadpool_read;
};
#endif


class ReadBufferFromWebServerGather final : public ReadBufferFromRemoteFSGather
{
public:
    ReadBufferFromWebServerGather(
            const String & uri_,
            RemoteMetadata metadata_,
            ContextPtr context_,
            size_t buf_size_,
            size_t backoff_threshold_,
            size_t max_tries_,
            size_t threadpool_read_)
        : ReadBufferFromRemoteFSGather(metadata_)
        , uri(uri_)
        , context(context_)
        , buf_size(buf_size_)
        , backoff_threshold(backoff_threshold_)
        , max_tries(max_tries_)
        , threadpool_read(threadpool_read_)
    {
    }

    SeekableReadBufferPtr createImplementationBuffer(const String & path) const override;

private:
    String uri;
    ContextPtr context;
    size_t buf_size;
    size_t backoff_threshold;
    size_t max_tries;
    bool threadpool_read;
};


#if USE_HDFS
/// Reads data from HDFS using stored paths in metadata.
class ReadBufferFromHDFSGather final : public ReadBufferFromRemoteFSGather
{
public:
    ReadBufferFromHDFSGather(
            const Poco::Util::AbstractConfiguration & config_,
            const String & hdfs_uri_,
            IDiskRemote::Metadata metadata_,
            size_t buf_size_)
        : ReadBufferFromRemoteFSGather(metadata_)
        , config(config_)
        , buf_size(buf_size_)
    {
        const size_t begin_of_path = hdfs_uri_.find('/', hdfs_uri_.find("//") + 2);
        hdfs_directory = hdfs_uri_.substr(begin_of_path);
        hdfs_uri = hdfs_uri_.substr(0, begin_of_path);
    }

    SeekableReadBufferPtr createImplementationBuffer(const String & path) const override;

private:
    const Poco::Util::AbstractConfiguration & config;
    String hdfs_uri;
    String hdfs_directory;
    size_t buf_size;
};
#endif

}
