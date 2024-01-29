#include <IO/SynchronousReader.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <base/errnoToString.h>
#include <unordered_map>
#include <mutex>
#include <unistd.h>
#include <fcntl.h>


namespace ProfileEvents
{
    extern const Event ReadBufferFromFileDescriptorRead;
    extern const Event ReadBufferFromFileDescriptorReadFailed;
    extern const Event ReadBufferFromFileDescriptorReadBytes;
    extern const Event DiskReadElapsedMicroseconds;
    extern const Event AsynchronousReaderIgnoredBytes;
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
    extern const int CANNOT_ADVISE;
}


std::future<IAsynchronousReader::Result> SynchronousReader::submit(Request request)
{
    /// If size is zero, then read() cannot be distinguished from EOF
    assert(request.size);

#if defined(POSIX_FADV_WILLNEED)
    int fd = assert_cast<const LocalFileDescriptor &>(*request.descriptor).fd;
    if (0 != posix_fadvise(fd, request.offset, request.size, POSIX_FADV_WILLNEED))
        throw ErrnoException(ErrorCodes::CANNOT_ADVISE, "Cannot posix_fadvise");
#endif

    return std::async(std::launch::deferred, [request, this]
    {
        return execute(request);
    });
}

IAsynchronousReader::Result SynchronousReader::execute(Request request)
{
    ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorRead);
    Stopwatch watch(CLOCK_MONOTONIC);

    int fd = assert_cast<const LocalFileDescriptor &>(*request.descriptor).fd;
    size_t bytes_read = 0;
    while (!bytes_read)
    {
        ssize_t res = 0;

        {
            CurrentMetrics::Increment metric_increment{CurrentMetrics::Read};
            res = ::pread(fd, request.buf, request.size, request.offset);
        }
        if (!res)
            break;

        if (-1 == res && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
            throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read from file {}", fd);
        }

        if (res > 0)
            bytes_read += res;
    }

    ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadBytes, bytes_read);

    /// It reports real time spent including the time spent while thread was preempted doing nothing.
    /// And it is Ok for the purpose of this watch (it is used to lower the number of threads to read from tables).
    /// Sometimes it is better to use taskstats::blkio_delay_total, but it is quite expensive to get it
    /// (NetlinkMetricsProvider has about 500K RPS).
    watch.stop();
    ProfileEvents::increment(ProfileEvents::DiskReadElapsedMicroseconds, watch.elapsedMicroseconds());

    ProfileEvents::increment(ProfileEvents::AsynchronousReaderIgnoredBytes, request.ignore);
    return Result{ .size = bytes_read, .offset = request.ignore };
}

}
