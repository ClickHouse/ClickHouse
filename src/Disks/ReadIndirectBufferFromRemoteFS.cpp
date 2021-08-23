#include "ReadIndirectBufferFromRemoteFS.h"

#if USE_AWS_S3 || USE_HDFS
#include <IO/ReadBufferFromS3.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
}


template<typename T>
ReadIndirectBufferFromRemoteFS<T>::ReadIndirectBufferFromRemoteFS(
    IDiskRemote::Metadata metadata_)
    : metadata(std::move(metadata_))
{
}


template<typename T>
off_t ReadIndirectBufferFromRemoteFS<T>::seek(off_t offset_, int whence)
{
    if (whence == SEEK_CUR)
    {
        /// If position within current working buffer - shift pos.
        if (!working_buffer.empty() && size_t(getPosition() + offset_) < absolute_position)
        {
            pos += offset_;
            return getPosition();
        }
        else
        {
            absolute_position += offset_;
        }
    }
    else if (whence == SEEK_SET)
    {
        /// If position within current working buffer - shift pos.
        if (!working_buffer.empty() && size_t(offset_) >= absolute_position - working_buffer.size()
            && size_t(offset_) < absolute_position)
        {
            pos = working_buffer.end() - (absolute_position - offset_);
            return getPosition();
        }
        else
        {
            absolute_position = offset_;
        }
    }
    else
        throw Exception("Only SEEK_SET or SEEK_CUR modes are allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    current_buf = initialize();
    pos = working_buffer.end();

    return absolute_position;
}


template<typename T>
std::unique_ptr<T> ReadIndirectBufferFromRemoteFS<T>::initialize()
{
    size_t offset = absolute_position;
    for (size_t i = 0; i < metadata.remote_fs_objects.size(); ++i)
    {
        current_buf_idx = i;
        const auto & [file_path, size] = metadata.remote_fs_objects[i];
        if (size > offset)
        {
            auto buf = createReadBuffer(file_path);
            buf->seek(offset, SEEK_SET);
            return buf;
        }
        offset -= size;
    }
    return nullptr;
}


template<typename T>
bool ReadIndirectBufferFromRemoteFS<T>::nextImpl()
{
    /// Find first available buffer that fits to given offset.
    if (!current_buf)
        current_buf = initialize();

    /// If current buffer has remaining data - use it.
    if (current_buf)
    {
        bool result = nextAndShiftPosition();
        if (result)
            return true;
    }

    /// If there is no available buffers - nothing to read.
    if (current_buf_idx + 1 >= metadata.remote_fs_objects.size())
        return false;

    ++current_buf_idx;
    const auto & path = metadata.remote_fs_objects[current_buf_idx].first;

    current_buf = createReadBuffer(path);

    return nextAndShiftPosition();
}

template <typename T>
bool ReadIndirectBufferFromRemoteFS<T>::nextAndShiftPosition()
{
    /// Transfer current position and working_buffer to actual ReadBuffer
    swap(*current_buf);
    /// Position and working_buffer will be updated in next() call
    auto result = current_buf->next();
    /// and assigned to current buffer.
    swap(*current_buf);

    /// absolute position is shifted by a data size that was read in next() call above.
    if (result)
        absolute_position += working_buffer.size();

    return result;
}


#if USE_AWS_S3
template
class ReadIndirectBufferFromRemoteFS<ReadBufferFromS3>;
#endif

#if USE_HDFS
template
class ReadIndirectBufferFromRemoteFS<ReadBufferFromHDFS>;
#endif

}

#endif
