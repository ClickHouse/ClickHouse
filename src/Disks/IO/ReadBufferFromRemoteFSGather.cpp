#include "ReadBufferFromRemoteFSGather.h"

#include <Disks/IDiskRemote.h>
#include <IO/SeekableReadBuffer.h>
#include <Disks/IO/ReadBufferFromWebServer.h>

#if USE_AWS_S3
#include <IO/ReadBufferFromS3.h>
#endif

#if USE_AZURE_BLOB_STORAGE
#include <IO/ReadBufferFromAzureBlobStorage.h>
#endif

#if USE_HDFS
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#endif

#include <Disks/IO/CachedReadBufferFromRemoteFS.h>
#include <base/logger_useful.h>
#include <filesystem>
#include <iostream>
#include <Common/hex.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

#if USE_AWS_S3
SeekableReadBufferPtr ReadBufferFromS3Gather::createImplementationBuffer(const String & path, size_t file_size)
{
    current_path = path;

    auto cache = settings.remote_fs_cache;
    bool with_cache = cache && settings.remote_fs_enable_cache && !IFileCache::shouldBypassCache();

    auto remote_file_reader_creator = [=, this]()
    {
        return std::make_unique<ReadBufferFromS3>(
            client_ptr, bucket, fs::path(metadata.remote_fs_root_path) / path, max_single_read_retries,
            settings, /* use_external_buffer */true, read_until_position, /* restricted_seek */true);
    };

    if (with_cache)
    {
        return std::make_shared<CachedReadBufferFromRemoteFS>(
            path, cache, remote_file_reader_creator, settings, read_until_position ? read_until_position : file_size);
    }

    return remote_file_reader_creator();
}
#endif


#if USE_AZURE_BLOB_STORAGE
SeekableReadBufferPtr ReadBufferFromAzureBlobStorageGather::createImplementationBuffer(const String & path, size_t /* file_size */)
{
    current_path = path;
    return std::make_unique<ReadBufferFromAzureBlobStorage>(blob_container_client, path, max_single_read_retries,
        max_single_download_retries, settings.remote_fs_buffer_size, /* use_external_buffer */true, read_until_position);
}
#endif


SeekableReadBufferPtr ReadBufferFromWebServerGather::createImplementationBuffer(const String & path, size_t /* file_size */)
{
    current_path = path;
    return std::make_unique<ReadBufferFromWebServer>(fs::path(uri) / path, context, settings, /* use_external_buffer */true, read_until_position);
}


#if USE_HDFS
SeekableReadBufferPtr ReadBufferFromHDFSGather::createImplementationBuffer(const String & path, size_t /* file_size */)
{
    return std::make_unique<ReadBufferFromHDFS>(hdfs_uri, fs::path(hdfs_directory) / path, config, settings.remote_fs_buffer_size);
}
#endif


ReadBufferFromRemoteFSGather::ReadBufferFromRemoteFSGather(const RemoteMetadata & metadata_, const ReadSettings & settings_, const String & path_)
    : ReadBuffer(nullptr, 0)
    , metadata(metadata_)
    , settings(settings_)
    , canonical_path(path_)
    , log(&Poco::Logger::get("ReadBufferFromRemoteFSGather"))
{
}


ReadBufferFromRemoteFSGather::ReadResult ReadBufferFromRemoteFSGather::readInto(char * data, size_t size, size_t offset, size_t ignore)
{
    /**
     * Set `data` to current working and internal buffers.
     * Internal buffer with size `size`. Working buffer with size 0.
     */
    set(data, size);

    file_offset_of_buffer_end = offset;
    bytes_to_ignore = ignore;

    assert(!bytes_to_ignore || initialized());

    auto result = nextImpl();

    if (result)
        return {working_buffer.size(), BufferBase::offset()};

    return {0, 0};
}


void ReadBufferFromRemoteFSGather::initialize()
{
    /// One clickhouse file can be split into multiple files in remote fs.
    auto current_buf_offset = file_offset_of_buffer_end;
    for (size_t i = 0; i < metadata.remote_fs_objects.size(); ++i)
    {
        const auto & [file_path, size] = metadata.remote_fs_objects[i];

        if (size > current_buf_offset)
        {
            /// Do not create a new buffer if we already have what we need.
            if (!current_buf || current_buf_idx != i)
            {
                current_buf_idx = i;
                current_buf = createImplementationBuffer(file_path, size);
            }

            current_buf->seek(current_buf_offset, SEEK_SET);
            return;
        }

        current_buf_offset -= size;
    }
    current_buf_idx = metadata.remote_fs_objects.size();
    current_buf = nullptr;
}


bool ReadBufferFromRemoteFSGather::nextImpl()
{
    /// Find first available buffer that fits to given offset.
    if (!current_buf)
        initialize();

    /// If current buffer has remaining data - use it.
    if (current_buf)
    {
        if (readImpl())
            return true;
    }
    else
        return false;

    if (!moveToNextBuffer())
        return false;

    return readImpl();
}


bool ReadBufferFromRemoteFSGather::moveToNextBuffer()
{
    /// If there is no available buffers - nothing to read.
    if (current_buf_idx + 1 >= metadata.remote_fs_objects.size())
        return false;

    ++current_buf_idx;

    const auto & [path, size] = metadata.remote_fs_objects[current_buf_idx];
    current_buf = createImplementationBuffer(path, size);

    return true;
}


bool ReadBufferFromRemoteFSGather::readImpl()
{
    swap(*current_buf);

    bool result = false;

    /**
     * Lazy seek is performed here.
     * In asynchronous buffer when seeking to offset in range [pos, pos + min_bytes_for_seek]
     * we save how many bytes need to be ignored (new_offset - position() bytes).
     */
    if (bytes_to_ignore)
    {
        current_buf->ignore(bytes_to_ignore);
        result = current_buf->hasPendingData();
        file_offset_of_buffer_end += bytes_to_ignore;
        bytes_to_ignore = 0;
    }

    if (!result)
        result = current_buf->next();

    if (metadata.remote_fs_objects.size() == 1)
    {
        file_offset_of_buffer_end = current_buf->getFileOffsetOfBufferEnd();
    }
    else
    {
        /// For log family engines there are multiple s3 files for the same clickhouse file
        file_offset_of_buffer_end += current_buf->available();
    }

    swap(*current_buf);

    /// Required for non-async reads.
    if (result)
    {
        assert(available());
        nextimpl_working_buffer_offset = offset();
    }

    return result;
}


size_t ReadBufferFromRemoteFSGather::getFileOffsetOfBufferEnd() const
{
    return file_offset_of_buffer_end;
}


void ReadBufferFromRemoteFSGather::setReadUntilPosition(size_t position)
{
    if (position != read_until_position)
    {
        read_until_position = position;
        reset();
    }
}


void ReadBufferFromRemoteFSGather::reset()
{
    current_buf.reset();
}

String ReadBufferFromRemoteFSGather::getFileName() const
{
    return current_path;
}


size_t ReadBufferFromRemoteFSGather::getFileSize() const
{
    size_t size = 0;
    for (const auto & object : metadata.remote_fs_objects)
        size += object.second;
    return size;
}

String ReadBufferFromRemoteFSGather::getInfoForLog()
{
    if (!current_buf)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get info: buffer not initialized");

    return current_buf->getInfoForLog();
}

size_t ReadBufferFromRemoteFSGather::getImplementationBufferOffset() const
{
    if (!current_buf)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Buffer not initialized");

    return current_buf->getFileOffsetOfBufferEnd();
}


}
