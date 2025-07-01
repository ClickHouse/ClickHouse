#include <cerrno>
#include <ctime>
#include <optional>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <Common/Throttler.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <Common/filesystemHelpers.h>
#include <sys/stat.h>
#include <Interpreters/Context.h>


#pragma clang diagnostic ignored "-Wreserved-identifier"

namespace ProfileEvents
{
    extern const Event ReadBufferFromFileDescriptorRead;
    extern const Event ReadBufferFromFileDescriptorReadFailed;
    extern const Event ReadBufferFromFileDescriptorReadBytes;
    extern const Event DiskReadElapsedMicroseconds;
    extern const Event Seek;
    extern const Event LocalReadThrottlerBytes;
    extern const Event LocalReadThrottlerSleepMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric Read;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_ADVISE;
}


std::string ReadBufferFromFileDescriptor::getFileName() const
{
    return "(fd = " + toString(fd) + ")";
}


size_t ReadBufferFromFileDescriptor::readImpl(char * to, size_t min_bytes, size_t max_bytes, size_t offset) const
{
    chassert(min_bytes <= max_bytes);

    /// This is a workaround of a read past EOF bug in linux kernel with pread()
    if (file_size.has_value() && offset >= *file_size)
         return 0;

    size_t bytes_read = 0;
    while (bytes_read < min_bytes)
    {
        ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorRead);

        Stopwatch watch(profile_callback ? clock_type : CLOCK_MONOTONIC);

        ssize_t res = 0;
        size_t to_read = max_bytes - bytes_read;
        {
            CurrentMetrics::Increment metric_increment{CurrentMetrics::Read};

            if (use_pread)
                res = ::pread(fd, to + bytes_read, to_read, offset + bytes_read);
            else
                res = ::read(fd, to + bytes_read, to_read);
        }
        if (!res)
            break;

        if (-1 == res && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
            ErrnoException::throwFromPath(
                ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, getFileName(), "Cannot read from file {}", getFileName());
        }

        if (res > 0)
        {
            bytes_read += res;
            if (throttler)
                throttler->add(res, ProfileEvents::LocalReadThrottlerBytes, ProfileEvents::LocalReadThrottlerSleepMicroseconds);
        }


        /// It reports real time spent including the time spent while thread was preempted doing nothing.
        /// And it is Ok for the purpose of this watch (it is used to lower the number of threads to read from tables).
        /// Sometimes it is better to use taskstats::blkio_delay_total, but it is quite expensive to get it
        /// (NetlinkMetricsProvider has about 500K RPS).
        watch.stop();
        ProfileEvents::increment(ProfileEvents::DiskReadElapsedMicroseconds, watch.elapsedMicroseconds());

        if (profile_callback)
        {
            ProfileInfo info;
            info.bytes_requested = to_read;
            info.bytes_read = res;
            info.nanoseconds = watch.elapsed();
            profile_callback(info);
        }
    }

    if (bytes_read)
        ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadBytes, bytes_read);

    return bytes_read;
}


bool ReadBufferFromFileDescriptor::nextImpl()
{
    /// If internal_buffer size is empty, then read() cannot be distinguished from EOF
    assert(!internal_buffer.empty());

    size_t bytes_read = readImpl(internal_buffer.begin(), 1, internal_buffer.size(), file_offset_of_buffer_end);

    file_offset_of_buffer_end += bytes_read;

    if (bytes_read)
    {
        working_buffer = internal_buffer;
        working_buffer.resize(bytes_read);
    }
    else
        return false;

    return true;
}


void ReadBufferFromFileDescriptor::prefetch(Priority)
{
#if defined(POSIX_FADV_WILLNEED)
    /// For direct IO, loading data into page cache is pointless.
    if (required_alignment)
        return;

    /// Ask OS to prefetch data into page cache.
    if (0 != posix_fadvise(fd, file_offset_of_buffer_end, internal_buffer.size(), POSIX_FADV_WILLNEED))
        throw ErrnoException(ErrorCodes::CANNOT_ADVISE, "Cannot posix_fadvise");
#endif
}


/// If 'offset' is small enough to stay in buffer after seek, then true seek in file does not happen.
off_t ReadBufferFromFileDescriptor::seek(off_t offset, int whence)
{
    size_t new_pos;
    if (whence == SEEK_SET)
    {
        assert(offset >= 0);
        new_pos = offset;
    }
    else if (whence == SEEK_CUR)
    {
        new_pos = file_offset_of_buffer_end - (working_buffer.end() - pos) + offset;
    }
    else
    {
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "ReadBufferFromFileDescriptor::seek expects SEEK_SET or SEEK_CUR as whence");
    }

    /// Position is unchanged.
    if (new_pos + (working_buffer.end() - pos) == file_offset_of_buffer_end)
        return new_pos;

    /// NOLINTBEGIN(readability-else-after-return)
    if (file_offset_of_buffer_end - working_buffer.size() <= new_pos
        && new_pos <= file_offset_of_buffer_end)
    {
        /// Position is still inside the buffer.
        /// Probably it is at the end of the buffer - then we will load data on the following 'next' call.

        pos = working_buffer.end() - file_offset_of_buffer_end + new_pos;
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());

        return new_pos;
    }
    else
    {
        /// Position is out of the buffer, we need to do real seek.
        off_t seek_pos = required_alignment > 1
            ? new_pos / required_alignment * required_alignment
            : new_pos;

        off_t offset_after_seek_pos = new_pos - seek_pos;

        /// First reset the buffer so the next read will fetch new data to the buffer.
        resetWorkingBuffer();

        /// In case of using 'pread' we just update the info about the next position in file.
        /// In case of using 'read' we call 'lseek'.

        /// We account both cases as seek event as it leads to non-contiguous reads from file.
        ProfileEvents::increment(ProfileEvents::Seek);

        if (!use_pread)
        {
            Stopwatch watch(profile_callback ? clock_type : CLOCK_MONOTONIC);

            off_t res = ::lseek(fd, seek_pos, SEEK_SET);
            if (-1 == res)
                ErrnoException::throwFromPath(
                    ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
                    getFileName(),
                    "Cannot seek through file {} at offset {}",
                    getFileName(),
                    seek_pos);

            /// Also note that seeking past the file size is not allowed.
            if (res != seek_pos)
                throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE,
                    "The 'lseek' syscall returned value ({}) that is not expected ({})", res, seek_pos);

            watch.stop();
            ProfileEvents::increment(ProfileEvents::DiskReadElapsedMicroseconds, watch.elapsedMicroseconds());
        }

        file_offset_of_buffer_end = seek_pos;

        if (offset_after_seek_pos > 0)
            ignore(offset_after_seek_pos);

        return seek_pos;
    }
    /// NOLINTEND(readability-else-after-return)
}


void ReadBufferFromFileDescriptor::rewind()
{
    if (!use_pread)
    {
        ProfileEvents::increment(ProfileEvents::Seek);
        off_t res = ::lseek(fd, 0, SEEK_SET);
        if (-1 == res)
            ErrnoException::throwFromPath(
                ErrorCodes::CANNOT_SEEK_THROUGH_FILE, getFileName(), "Cannot seek through file {}", getFileName());
    }
    /// In case of pread, the ProfileEvents::Seek is not accounted, but it's Ok.

    /// Clearing the buffer with existing data. New data will be read on subsequent call to 'next'.
    working_buffer.resize(0);
    pos = working_buffer.begin();
    file_offset_of_buffer_end = 0;
}

std::optional<size_t> ReadBufferFromFileDescriptor::tryGetFileSize()
{
    return getSizeFromFileDescriptor(fd, getFileName());
}

bool ReadBufferFromFileDescriptor::checkIfActuallySeekable()
{
    struct stat stat;
    auto res = ::fstat(fd, &stat);
    return res == 0 && S_ISREG(stat.st_mode);
}

size_t ReadBufferFromFileDescriptor::readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t)> &) const
{
    chassert(use_pread);
    return readImpl(to, n, n, offset);
}

}
