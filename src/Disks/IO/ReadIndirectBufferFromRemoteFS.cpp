#include "ReadIndirectBufferFromRemoteFS.h"

#include <Disks/IO/ReadBufferFromRemoteFSGather.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
}


ReadIndirectBufferFromRemoteFS::ReadIndirectBufferFromRemoteFS(
    std::shared_ptr<ReadBufferFromRemoteFSGather> impl_)
    : ReadBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0)
    , impl(impl_)
{
}

size_t ReadIndirectBufferFromRemoteFS::getFileSize()
{
    return impl->getFileSize();
}

off_t ReadIndirectBufferFromRemoteFS::getPosition()
{
    return impl->file_offset_of_buffer_end - available();
}


String ReadIndirectBufferFromRemoteFS::getFileName() const
{
    return impl->getFileName();
}


void ReadIndirectBufferFromRemoteFS::setReadUntilPosition(size_t position)
{
    impl->setReadUntilPosition(position);
}


void ReadIndirectBufferFromRemoteFS::setReadUntilEnd()
{
    impl->setReadUntilPosition(impl->getFileSize());
}


off_t ReadIndirectBufferFromRemoteFS::seek(off_t offset_, int whence)
{
    if (whence == SEEK_CUR)
    {
        /// If position within current working buffer - shift pos.
        if (!working_buffer.empty() && size_t(getPosition() + offset_) < impl->file_offset_of_buffer_end)
        {
            pos += offset_;
            return getPosition();
        }
        else
        {
            impl->file_offset_of_buffer_end += offset_;
        }
    }
    else if (whence == SEEK_SET)
    {
        /// If position within current working buffer - shift pos.
        if (!working_buffer.empty()
            && size_t(offset_) >= impl->file_offset_of_buffer_end - working_buffer.size()
            && size_t(offset_) < impl->file_offset_of_buffer_end)
        {
            pos = working_buffer.end() - (impl->file_offset_of_buffer_end - offset_);
            return getPosition();
        }
        else
        {
            impl->file_offset_of_buffer_end = offset_;
        }
    }
    else
        throw Exception("Only SEEK_SET or SEEK_CUR modes are allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    impl->reset();
    resetWorkingBuffer();

    file_offset_of_buffer_end = impl->file_offset_of_buffer_end;
    return impl->file_offset_of_buffer_end;
}


bool ReadIndirectBufferFromRemoteFS::nextImpl()
{
    /// Transfer current position and working_buffer to actual ReadBuffer
    swap(*impl);

    assert(!impl->hasPendingData());
    /// Position and working_buffer will be updated in next() call
    auto result = impl->next();
    /// and assigned to current buffer.
    swap(*impl);

    if (result)
    {
        file_offset_of_buffer_end += available();
        BufferBase::set(working_buffer.begin() + offset(), available(), 0);
    }

    assert(file_offset_of_buffer_end == impl->file_offset_of_buffer_end);

    return result;
}

}
