#include <unistd.h>
#include <cerrno>
#include <cassert>
#include <sys/stat.h>

#include <Common/Throttler.h>
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
    extern const Event FileSync;
    extern const Event FileSyncElapsedMicroseconds;
    extern const Event LocalWriteThrottlerBytes;
    extern const Event LocalWriteThrottlerSleepMicroseconds;
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

            /// Don't use getFileName() here because this method can be called from destructor
            String error_file_name = file_name;
            if (error_file_name.empty())
                error_file_name = "(fd = " + toString(fd) + ")";
            throwFromErrnoWithPath("Cannot write to file " + error_file_name, error_file_name,
                                   ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
        }

        if (res > 0)
        {
            bytes_written += res;
            if (throttler)
                throttler->add(res, ProfileEvents::LocalWriteThrottlerBytes, ProfileEvents::LocalWriteThrottlerSleepMicroseconds);
        }
    }

    ProfileEvents::increment(ProfileEvents::DiskWriteElapsedMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteBytes, bytes_written);
}

/// NOTE: This class can be used as a very low-level building block, for example
/// in trace collector. In such places allocations of memory can be dangerous,
/// so don't allocate anything in this constructor.
WriteBufferFromFileDescriptor::WriteBufferFromFileDescriptor(
    int fd_,
    size_t buf_size,
    char * existing_memory,
    ThrottlerPtr throttler_,
    size_t alignment,
    std::string file_name_)
    : WriteBufferFromFileBase(buf_size, existing_memory, alignment)
    , fd(fd_)
    , throttler(throttler_)
    , file_name(std::move(file_name_))
{
}


WriteBufferFromFileDescriptor::~WriteBufferFromFileDescriptor()
{
    finalize();
}

void WriteBufferFromFileDescriptor::finalizeImpl()
{
    if (fd < 0)
    {
        assert(!offset() && "attempt to write after close");
        return;
    }

    next();
}

void WriteBufferFromFileDescriptor::sync()
{
    /// If buffer has pending data - write it.
    next();

    ProfileEvents::increment(ProfileEvents::FileSync);

    Stopwatch watch;

    /// Request OS to sync data with storage medium.
#if defined(OS_DARWIN)
    int res = ::fsync(fd);
#else
    int res = ::fdatasync(fd);
#endif
    ProfileEvents::increment(ProfileEvents::FileSyncElapsedMicroseconds, watch.elapsedMicroseconds());

    if (-1 == res)
        throwFromErrnoWithPath("Cannot fsync " + getFileName(), getFileName(), ErrorCodes::CANNOT_FSYNC);
}


off_t WriteBufferFromFileDescriptor::seek(off_t offset, int whence) // NOLINT
{
    off_t res = lseek(fd, offset, whence);
    if (-1 == res)
        throwFromErrnoWithPath("Cannot seek through file " + getFileName(), getFileName(),
                               ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
    return res;
}

void WriteBufferFromFileDescriptor::truncate(off_t length) // NOLINT
{
    int res = ftruncate(fd, length);
    if (-1 == res)
        throwFromErrnoWithPath("Cannot truncate file " + getFileName(), getFileName(), ErrorCodes::CANNOT_TRUNCATE_FILE);
}


off_t WriteBufferFromFileDescriptor::size() const
{
    struct stat buf;
    int res = fstat(fd, &buf);
    if (-1 == res)
        throwFromErrnoWithPath("Cannot execute fstat " + getFileName(), getFileName(), ErrorCodes::CANNOT_FSTAT);
    return buf.st_size;
}

std::string WriteBufferFromFileDescriptor::getFileName() const
{
    if (file_name.empty())
        return "(fd = " + toString(fd) + ")";

    return file_name;
}


}
