#include <unistd.h>

#include <Disks/IO/ReadBufferFromRemoteDisk.h>

namespace DB
{

ReadBufferFromRemoteDisk::ReadBufferFromRemoteDisk(
    RemoteFSConnectionPool::Entry & conn_,
    const String & file_name_,
    const ReadSettings & settings_)
    : ReadBufferFromFileBase(settings_.remote_fs_buffer_size, nullptr, 0)
    , log(&Poco::Logger::get("ReadBufferFromRemoteDisk"))
    , buf_size(settings_.remote_fs_buffer_size)
    , file_name(file_name_)
    , conn(conn_)
{
}

bool ReadBufferFromRemoteDisk::nextImpl()
{
    size_t bytes_read = conn->readFile(file_name, offset_of_buffer_end, buf_size, internal_buffer.begin());
    offset_of_buffer_end += bytes_read;

    if (!bytes_read)
        return false;
    
    working_buffer = internal_buffer;
    working_buffer.resize(bytes_read);
    return true;
}

off_t ReadBufferFromRemoteDisk::seek(off_t offset, int whence)
{
    size_t new_pos;
    if (whence == SEEK_SET)
    {
        assert(offset >= 0);
        new_pos = offset;
    }
    else if (whence == SEEK_CUR)
    {
        new_pos = offset_of_buffer_end - (working_buffer.end() - pos) + offset;
    }
    else
    {
        // TODO maybe implement
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "ReadBufferFromRemoteDisk::seek expects SEEK_SET or SEEK_CUR as whence");
    }

    /// Position is unchanged.
    if (new_pos + (working_buffer.end() - pos) == offset_of_buffer_end)
        return new_pos;
    
    if (offset_of_buffer_end - working_buffer.size() <= new_pos
        && new_pos <= offset_of_buffer_end)
    {
        /// Position is still inside the buffer.
        /// Probably it is at the end of the buffer - then we will load data on the following 'next' call.

        pos = working_buffer.end() - offset_of_buffer_end + new_pos;
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());
    }
    else
    {
        resetWorkingBuffer();
        offset_of_buffer_end = new_pos;
    }
    return new_pos;
}

off_t ReadBufferFromRemoteDisk::getPosition()
{
    return offset_of_buffer_end - available();
}

}
