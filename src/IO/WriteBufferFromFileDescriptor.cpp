#include <unistd.h>
#include <errno.h>
#include <cassert>
#include <sys/types.h>
#include <sys/stat.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>

#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>


namespace ProfileEvents
{
    extern const Event WriteBufferFromFileDescriptorWrite;
    extern const Event WriteBufferFromFileDescriptorWriteFailed;
    extern const Event WriteBufferFromFileDescriptorWriteBytes;
    extern const Event DiskWriteElapsedMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric Write;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int CANNOT_FSYNC;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int CANNOT_FSTAT;
}


void WriteBufferFromFileDescriptor::nextImpl()
{
    if (!offset())
        return;

    Stopwatch watch;

    size_t bytes_written = 0;
    while (bytes_written != offset())
    {
        ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWrite);

        ssize_t res = 0;
        {
            CurrentMetrics::Increment metric_increment{CurrentMetrics::Write};
            res = ::write(fd, working_buffer.begin() + bytes_written, offset() - bytes_written);
        }

        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteFailed);
            throwFromErrnoWithPath("Cannot write to file " + getFileName(), getFileName(),
                                   ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
        }

        if (res > 0)
            bytes_written += res;
    }

    ProfileEvents::increment(ProfileEvents::DiskWriteElapsedMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteBytes, bytes_written);
}


/// Name or some description of file.
std::string WriteBufferFromFileDescriptor::getFileName() const
{
    return "(fd = " + toString(fd) + ")";
}


WriteBufferFromFileDescriptor::WriteBufferFromFileDescriptor(
    int fd_,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
    : WriteBufferFromFileBase(buf_size, existing_memory, alignment), fd(fd_) {}


WriteBufferFromFileDescriptor::~WriteBufferFromFileDescriptor()
{
    try
    {
        if (fd >= 0)
            next();
        else
            assert(!offset() && "attempt to write after close");
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void WriteBufferFromFileDescriptor::sync()
{
    /// If buffer has pending data - write it.
    next();

    /// Request OS to sync data with storage medium.
    int res = fsync(fd);
    if (-1 == res)
        throwFromErrnoWithPath("Cannot fsync " + getFileName(), getFileName(), ErrorCodes::CANNOT_FSYNC);
}


off_t WriteBufferFromFileDescriptor::seek(off_t offset, int whence)
{
    off_t res = lseek(fd, offset, whence);
    if (-1 == res)
        throwFromErrnoWithPath("Cannot seek through file " + getFileName(), getFileName(),
                               ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
    return res;
}


void WriteBufferFromFileDescriptor::truncate(off_t length)
{
    int res = ftruncate(fd, length);
    if (-1 == res)
        throwFromErrnoWithPath("Cannot truncate file " + getFileName(), getFileName(), ErrorCodes::CANNOT_TRUNCATE_FILE);
}


off_t WriteBufferFromFileDescriptor::size()
{
    struct stat buf;
    int res = fstat(fd, &buf);
    if (-1 == res)
        throwFromErrnoWithPath("Cannot execute fstat " + getFileName(), getFileName(), ErrorCodes::CANNOT_FSTAT);
    return buf.st_size;
}

}
