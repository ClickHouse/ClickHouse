#include "ReadIndirectBufferFromRemoteFS.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
}


ReadIndirectBufferFromRemoteFS::ReadIndirectBufferFromRemoteFS(
    std::shared_ptr<ReadBufferFromRemoteFS> impl_) : impl(std::move(impl_))
{
}


off_t ReadIndirectBufferFromRemoteFS::seek(off_t offset_, int whence)
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
        if (!working_buffer.empty()
            && size_t(offset_) >= absolute_position - working_buffer.size()
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

    impl->seek(absolute_position, SEEK_SET);
    pos = working_buffer.end();

    return absolute_position;
}


bool ReadIndirectBufferFromRemoteFS::nextImpl()
{
    /// Transfer current position and working_buffer to actual ReadBuffer
    swap(*impl);
    /// Position and working_buffer will be updated in next() call
    auto result = impl->next();
    /// and assigned to current buffer.
    swap(*impl);

    if (result)
        absolute_position += working_buffer.size();

    return result;
}

}
