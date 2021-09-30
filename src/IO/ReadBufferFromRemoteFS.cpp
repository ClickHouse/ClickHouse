#include "ReadBufferFromRemoteFS.h"

#include <Disks/IDiskRemote.h>
#include <IO/SeekableReadBuffer.h>
#include <iostream>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
}


ReadBufferFromRemoteFS::ReadBufferFromRemoteFS(const RemoteMetadata & metadata_)
    : metadata(metadata_)
{
}


SeekableReadBufferPtr ReadBufferFromRemoteFS::initialize()
{
    /// One clickhouse file can be split into multiple files in remote fs.
    auto current_buf_offset = absolute_position;
    for (size_t i = 0; i < metadata.remote_fs_objects.size(); ++i)
    {
        current_buf_idx = i;
        const auto & [file_path, size] = metadata.remote_fs_objects[i];

        if (size > current_buf_offset)
        {
            auto buf = createReadBuffer(file_path);
            buf->seek(current_buf_offset, SEEK_SET);
            return buf;
        }

        current_buf_offset -= size;
    }
    return nullptr;
}


bool ReadBufferFromRemoteFS::nextImpl()
{
    /// Find first available buffer that fits to given offset.
    if (!current_buf)
        current_buf = initialize();

    /// If current buffer has remaining data - use it.
    if (current_buf)
    {
        if (read())
            return true;
    }
    else
        return false;

    /// If there is no available buffers - nothing to read.
    if (current_buf_idx + 1 >= metadata.remote_fs_objects.size())
        return false;

    ++current_buf_idx;

    const auto & path = metadata.remote_fs_objects[current_buf_idx].first;
    current_buf = createReadBuffer(path);

    return read();
}


bool ReadBufferFromRemoteFS::read()
{
    if (check)
    {
        assert(!internal_buffer.empty());
        assert(working_buffer.begin() != nullptr);
    }
    /// Transfer current position and working_buffer to actual ReadBuffer
    swap(*current_buf);
    /// Position and working_buffer will be updated in next() call
    if (check)
        assert(current_buf->buffer().begin() != nullptr);
    auto result = current_buf->next();
    /// Assign result to current buffer.
    swap(*current_buf);

    if (result)
        absolute_position += working_buffer.size();

    return result;
}


size_t ReadBufferFromRemoteFS::fetch(size_t offset)
{
    absolute_position = offset;
    auto result = nextImpl();
    if (result)
        return working_buffer.size();
    return 0;
}


off_t ReadBufferFromRemoteFS::seek(off_t offset, int whence)
{
    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET is allowed");

    absolute_position = offset;
    current_buf = initialize();
    return absolute_position;
}


void ReadBufferFromRemoteFS::reset(bool reset_inner_buf)
{
    if (reset_inner_buf)
        current_buf.reset();
    // BufferBase::set(nullptr, 0, 0);
}

}
