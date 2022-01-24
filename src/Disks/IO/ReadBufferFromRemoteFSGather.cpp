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

#include <base/logger_useful.h>
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;

namespace DB
{

#if USE_AWS_S3
SeekableReadBufferPtr ReadBufferFromS3Gather::createImplementationBuffer(const String & path, size_t read_until_position_) const
{
    return std::make_unique<ReadBufferFromS3>(client_ptr, bucket,
        fs::path(metadata.remote_fs_root_path) / path, max_single_read_retries, settings, threadpool_read, read_until_position_);
}
#endif


#if USE_AZURE_BLOB_STORAGE
SeekableReadBufferPtr ReadBufferFromAzureBlobStorageGather::createImplementationBuffer(const String & path, size_t read_until_position_) const
{
    return std::make_unique<ReadBufferFromAzureBlobStorage>(blob_container_client, path, max_single_read_retries,
        max_single_download_retries, settings.remote_fs_buffer_size, threadpool_read, read_until_position_);
}
#endif


SeekableReadBufferPtr ReadBufferFromWebServerGather::createImplementationBuffer(const String & path, size_t read_until_position_) const
{
    return std::make_unique<ReadBufferFromWebServer>(fs::path(uri) / path, context, settings, threadpool_read, read_until_position_);
}


#if USE_HDFS
SeekableReadBufferPtr ReadBufferFromHDFSGather::createImplementationBuffer(const String & path, size_t read_until_position_) const
{
    return std::make_unique<ReadBufferFromHDFS>(hdfs_uri, fs::path(hdfs_directory) / path, config, buf_size, read_until_position_);
}
#endif


ReadBufferFromRemoteFSGather::ReadBufferFromRemoteFSGather(const RemoteMetadata & metadata_, const String & path_)
    : ReadBuffer(nullptr, 0)
    , metadata(metadata_)
    , canonical_path(path_)
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
    if (bytes_to_ignore)
        assert(initialized());

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
                current_buf = createImplementationBuffer(file_path, read_until_position);
                current_buf_idx = i;

                if (auto * in = dynamic_cast<SeekableReadBufferWithSize *>(current_buf.get()))
                    in->setReadType(SeekableReadBufferWithSize::ReadType::DISK_READ);
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

    /// If there is no available buffers - nothing to read.
    if (current_buf_idx + 1 >= metadata.remote_fs_objects.size())
        return false;

    ++current_buf_idx;

    const auto & current_path = metadata.remote_fs_objects[current_buf_idx].first;
    current_buf = createImplementationBuffer(current_path, read_until_position);

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
    {
        current_buf->ignore(bytes_to_ignore);
        bytes_to_ignore = 0;
    }

    bool result = current_buf->hasPendingData();
    if (result)
    {
        /// bytes_to_ignore already added.
        file_offset_of_buffer_end += current_buf->available();
    }
    else
    {
        result = current_buf->next();
        if (result)
            file_offset_of_buffer_end += current_buf->buffer().size();
    }

    swap(*current_buf);

    return result;
}


void ReadBufferFromRemoteFSGather::setReadUntilPosition(size_t position)
{
    read_until_position = position;
    reset();
}


void ReadBufferFromRemoteFSGather::reset()
{
    current_buf.reset();
}

String ReadBufferFromRemoteFSGather::getFileName() const
{
    return canonical_path;
}


size_t ReadBufferFromRemoteFSGather::getFileSize() const
{
    size_t size = 0;
    for (const auto & object : metadata.remote_fs_objects)
        size += object.second;
    return size;
}

}
