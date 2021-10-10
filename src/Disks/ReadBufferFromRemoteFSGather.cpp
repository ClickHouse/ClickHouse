#include "ReadBufferFromRemoteFSGather.h"

#include <Disks/IDiskRemote.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadBufferFromWebServer.h>

#if USE_AWS_S3
#include <IO/ReadBufferFromS3.h>
#endif

#if USE_HDFS
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#endif

#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
}


#if USE_AWS_S3
SeekableReadBufferPtr ReadBufferFromS3Gather::createImplementationBuffer(const String & path) const
{
    return std::make_unique<ReadBufferFromS3>(client_ptr, bucket, fs::path(metadata.remote_fs_root_path) / path, max_single_read_retries, buf_size, threadpool_read);
}
#endif


SeekableReadBufferPtr ReadBufferFromWebServerGather::createImplementationBuffer(const String & path) const
{
    return std::make_unique<ReadBufferFromWebServer>(fs::path(uri) / path, context, settings, threadpool_read);
}


#if USE_HDFS
SeekableReadBufferPtr ReadBufferFromHDFSGather::createImplementationBuffer(const String & path) const
{
    return std::make_unique<ReadBufferFromHDFS>(hdfs_uri, fs::path(hdfs_directory) / path, config, buf_size);
}
#endif


ReadBufferFromRemoteFSGather::ReadBufferFromRemoteFSGather(const RemoteMetadata & metadata_)
    : ReadBuffer(nullptr, 0)
    , metadata(metadata_)
{
}


size_t ReadBufferFromRemoteFSGather::readInto(char * data, size_t size, size_t offset, size_t ignore)
{
    /**
     * Set `data` to current working and internal buffers.
     * Internal buffer with size `size`. Working buffer with size 0.
     */
    set(data, size);

    absolute_position = offset;
    bytes_to_ignore = ignore;

    auto result = nextImpl();
    bytes_to_ignore = 0;

    if (result)
        return working_buffer.size();

    return 0;
}


void ReadBufferFromRemoteFSGather::initialize()
{
    /// One clickhouse file can be split into multiple files in remote fs.
    auto current_buf_offset = absolute_position;
    for (size_t i = 0; i < metadata.remote_fs_objects.size(); ++i)
    {
        current_buf_idx = i;
        const auto & [file_path, size] = metadata.remote_fs_objects[i];

        if (size > current_buf_offset)
        {
            /// Do not create a new buffer if we already have what we need.
            if (!current_buf || buf_idx != i)
            {
                current_buf = createImplementationBuffer(file_path);
                buf_idx = i;
            }

            current_buf->seek(current_buf_offset, SEEK_SET);
            return;
        }

        current_buf_offset -= size;
    }
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

    /// If there is no available buffers - nothing to read.
    if (current_buf_idx + 1 >= metadata.remote_fs_objects.size())
        return false;

    ++current_buf_idx;

    const auto & path = metadata.remote_fs_objects[current_buf_idx].first;
    current_buf = createImplementationBuffer(path);

    return readImpl();
}


bool ReadBufferFromRemoteFSGather::readImpl()
{
    swap(*current_buf);

    /**
     * Lazy seek is performed here.
     * In asynchronous buffer when seeking to offset in range [pos, pos + min_bytes_for_seek]
     * we save how many bytes need to be ignored (new_offset - position() bytes).
     */
    if (bytes_to_ignore)
        current_buf->ignore(bytes_to_ignore);

    auto result = current_buf->next();

    swap(*current_buf);

    if (result)
        absolute_position += working_buffer.size();

    return result;
}


void ReadBufferFromRemoteFSGather::seek(off_t offset)
{
    absolute_position = offset;
    initialize();
}


void ReadBufferFromRemoteFSGather::reset()
{
    current_buf.reset();
}

}
