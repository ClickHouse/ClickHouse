#include "ReadIndirectBufferFromRemoteFS.h"

#include <Disks/IO/ReadBufferFromRemoteFSGather.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
}


ReadIndirectBufferFromRemoteFS::ReadIndirectBufferFromRemoteFS(
    std::shared_ptr<ReadBufferFromRemoteFSGather> impl_, const ReadSettings & settings)
    : ReadBufferFromFileBase(settings.remote_fs_buffer_size, nullptr, 0)
    , impl(impl_)
    , read_settings(settings)
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
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET or SEEK_CUR modes are allowed.");

    impl->seek(impl->file_offset_of_buffer_end, SEEK_SET);
    resetWorkingBuffer();

    file_offset_of_buffer_end = impl->file_offset_of_buffer_end;
    return impl->file_offset_of_buffer_end;
}


bool ReadIndirectBufferFromRemoteFS::nextImpl()
{
    chassert(internal_buffer.size() == read_settings.remote_fs_buffer_size);
    chassert(file_offset_of_buffer_end <= impl->getFileSize());

    auto [size, offset, _] = impl->readInto(internal_buffer.begin(), internal_buffer.size(), file_offset_of_buffer_end, /* ignore */0);

    chassert(offset <= size);
    chassert(size <= internal_buffer.size());

    size_t bytes_read = size - offset;
    if (bytes_read)
        working_buffer = Buffer(internal_buffer.begin() + offset, internal_buffer.begin() + size);

    file_offset_of_buffer_end = impl->getFileOffsetOfBufferEnd();

    /// In case of multiple files for the same file in clickhouse (i.e. log family)
    /// file_offset_of_buffer_end will not match getImplementationBufferOffset()
    /// so we use [impl->getImplementationBufferOffset(), impl->getFileSize()]
    chassert(file_offset_of_buffer_end >= impl->getImplementationBufferOffset());
    chassert(file_offset_of_buffer_end <= impl->getFileSize());

    return bytes_read;
}

}
