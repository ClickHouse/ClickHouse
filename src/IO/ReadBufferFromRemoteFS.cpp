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
    : metadata(std::move(metadata_))
{
}


SeekableReadBufferPtr ReadBufferFromRemoteFS::initialize()
{
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
        if (readImpl())
            return true;
    }

    /// If there is no available buffers - nothing to read.
    if (current_buf_idx + 1 >= metadata.remote_fs_objects.size())
        return false;

    ++current_buf_idx;

    const auto & path = metadata.remote_fs_objects[current_buf_idx].first;
    current_buf = createReadBuffer(path);

    return readImpl();
}


bool ReadBufferFromRemoteFS::readImpl()
{
    /// Transfer current position and working_buffer to actual ReadBuffer
    swap(*current_buf);
    /// Position and working_buffer will be updated in next() call
    auto result = current_buf->next();
    /// Assign result to current buffer.
    swap(*current_buf);
    /// Absolute position is updated by *IndirectBufferFromRemoteFS only.

    return result;
}


off_t ReadBufferFromRemoteFS::seek(off_t offset_, int whence)
{
    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET is allowed");
    /// We already made a seek and adjusted position in ReadIndirectBufferFromRemoteFS.
    assert(offset_ == static_cast<off_t>(absolute_position));

    current_buf = initialize();
    return absolute_position;
}


void ReadBufferFromRemoteFS::reset()
{
    set(nullptr, 0);
}

}
