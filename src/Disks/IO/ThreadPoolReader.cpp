#include <Disks/IO/ThreadPoolReader.h>
#include <future>
#include <fcntl.h>
#include <unistd.h>
#include <IO/preadNoWait.h>
#include <Poco/Event.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/assert_cast.h>
#include <Common/setThreadName.h>


namespace ProfileEvents
{
    extern const Event ThreadPoolReaderPageCacheHit;
    extern const Event ThreadPoolReaderPageCacheHitBytes;
    extern const Event ThreadPoolReaderPageCacheHitElapsedMicroseconds;
    extern const Event ThreadPoolReaderPageCacheMiss;
    extern const Event ThreadPoolReaderPageCacheMissBytes;
    extern const Event ThreadPoolReaderPageCacheMissElapsedMicroseconds;
    extern const Event AsynchronousReaderIgnoredBytes;

    extern const Event ReadBufferFromFileDescriptorReadFailed;
    extern const Event ReadBufferFromFileDescriptorReadBytes;
    extern const Event DiskReadElapsedMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric Read;
    extern const Metric ThreadPoolFSReaderThreads;
    extern const Metric ThreadPoolFSReaderThreadsActive;
    extern const Metric ThreadPoolFSReaderThreadsScheduled;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int NOT_IMPLEMENTED;
}

ThreadPoolReader::ThreadPoolReader(size_t pool_size, size_t queue_size_)
    : pool(std::make_unique<ThreadPool>(CurrentMetrics::ThreadPoolFSReaderThreads, CurrentMetrics::ThreadPoolFSReaderThreadsActive, CurrentMetrics::ThreadPoolFSReaderThreadsScheduled, pool_size, pool_size, queue_size_))
{
}

std::future<IAsynchronousReader::Result> ThreadPoolReader::submit(Request request)
{
    /// If size is zero, then read() cannot be distinguished from EOF
    chassert(request.size);

    int fd = assert_cast<const LocalFileDescriptor &>(*request.descriptor).fd;

#if defined(OS_LINUX)
    /// Check if data is already in page cache with preadv2 syscall.
    /// It is not usable on every system - see `preadNoWaitUnavailableReason`. Then every read is
    /// handed off to the thread pool, which is why `applySettingsQuirks` switches the default
    /// `local_filesystem_read_method` from 'pread_threadpool' to 'pread' on such a system.
    ///
    /// RWF_NOWAIT is ignored for O_DIRECT (mostly, it may return EAGAIN if it cannot lock the inode in case of ext4, see [1])
    ///   [1]: https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=548feebec7e93e58b647dba70b3303dcb569c914
    /// The O_DIRECT check comes first: the support check runs a raw `preadv2` probe on the first
    /// call, and a kill-on-deny `seccomp` profile must not see the probe for a read that never
    /// looks at the page cache.
    if (!request.direct_io && preadNoWaitUnavailableReason().empty())
    {
        /// It reports real time spent including the time spent while thread was preempted doing nothing.
        /// And it is Ok for the purpose of this watch (it is used to lower the number of threads to read from tables).
        /// Sometimes it is better to use taskstats::blkio_delay_total, but it is quite expensive to get it.
        Stopwatch watch(CLOCK_MONOTONIC);

        SCOPE_EXIT({
            watch.stop();

            ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheHitElapsedMicroseconds, watch.elapsedMicroseconds());
            ProfileEvents::increment(ProfileEvents::DiskReadElapsedMicroseconds, watch.elapsedMicroseconds());
        });

        std::promise<Result> promise;
        std::future<Result> future = promise.get_future();

        size_t bytes_read = 0;
        while (!bytes_read)
        {
            ssize_t res = 0;

            {
                CurrentMetrics::Increment metric_increment{CurrentMetrics::Read};
                res = preadNoWait(fd, request.buf, request.size, request.offset);
            }

            if (!res)
            {
                /// The file has ended.
                promise.set_value({ .buf = nullptr, .size = 0, .offset = 0, .file_offset_of_buffer_end = request.offset });
                return future;
            }

            if (-1 == res)
            {
                if (isPreadNoWaitUnavailable(errno))
                {
                    /// No support for the syscall or the flag in the Linux kernel, or it is rejected
                    /// by a `seccomp` profile. It shouldn't happen, because the system call is probed
                    /// beforehand, but a particular filesystem can still reject the flag
                    /// (`tmpfs` answers `EOPNOTSUPP`, for example).
                    /// Hand the read off to the thread pool, which reads it with `pread`.
                    break;
                }
                if (errno == EAGAIN)
                {
                    /// Data is not available in page cache. Will hand off to thread pool.
                    break;
                }
                if (errno == EINTR)
                {
                    /// Interrupted by a signal.
                    continue;
                }

                ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
                promise.set_exception(
                    std::make_exception_ptr(ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read from file {}", fd)));
                return future;
            }

            bytes_read += res;
        }

        if (bytes_read)
        {
            /// Read successfully from page cache.
            ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheHit);
            ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheHitBytes, bytes_read);
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadBytes, bytes_read);
            ProfileEvents::increment(ProfileEvents::AsynchronousReaderIgnoredBytes, request.ignore);

            promise.set_value({ .buf = request.buf, .size = bytes_read, .offset = request.ignore, .file_offset_of_buffer_end = request.offset + bytes_read });
            return future;
        }
    }
#endif

    ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheMiss);

    auto schedule = threadPoolCallbackRunnerUnsafe<Result>(*pool, ThreadName::READ_THREAD_POOL);

    return schedule([request, fd]() -> Result
    {
        Stopwatch watch(CLOCK_MONOTONIC);
        SCOPE_EXIT({
            watch.stop();

            ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheMissElapsedMicroseconds, watch.elapsedMicroseconds());
            ProfileEvents::increment(ProfileEvents::DiskReadElapsedMicroseconds, watch.elapsedMicroseconds());
        });

        size_t bytes_read = 0;
        while (!bytes_read)
        {
            ssize_t res = 0;

            {
                CurrentMetrics::Increment metric_increment{CurrentMetrics::Read};
                res = ::pread(fd, request.buf, request.size, request.offset);
            }

            /// File has ended.
            if (!res)
                break;

            if (-1 == res && errno != EINTR)
            {
                ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
                throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read from file {}", fd);
            }

            bytes_read += res;
        }

        watch.stop();

        ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheMissBytes, bytes_read);
        ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadBytes, bytes_read);
        ProfileEvents::increment(ProfileEvents::AsynchronousReaderIgnoredBytes, request.ignore);

        return Result{ .buf = request.buf, .size = bytes_read, .offset = request.ignore, .file_offset_of_buffer_end = request.offset + bytes_read };
    }, request.priority);
}

IAsynchronousReader::Result ThreadPoolReader::execute(Request /* request */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method `execute` not implemented for ThreadpoolReader");
}

void ThreadPoolReader::wait()
{
    pool->wait();
}

}
