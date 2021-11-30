#include <IO/ThreadPoolReader.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <Common/MemorySanitizer.h>
#include <base/errnoToString.h>
#include <Poco/Event.h>
#include <future>
#include <unistd.h>
#include <fcntl.h>

#if defined(__linux__)

#include <sys/syscall.h>
#include <sys/uio.h>

/// We don't want to depend on specific glibc version.

#if !defined(RWF_NOWAIT)
    #define RWF_NOWAIT 8
#endif

#if !defined(SYS_preadv2)
    #if defined(__x86_64__)
        #define SYS_preadv2 327
    #elif defined(__aarch64__)
        #define SYS_preadv2 286
    #elif defined(__ppc64__)
        #define SYS_preadv2 380
    #else
        #error "Unsupported architecture"
    #endif
#endif

#endif


namespace ProfileEvents
{
    extern const Event ThreadPoolReaderPageCacheHit;
    extern const Event ThreadPoolReaderPageCacheHitBytes;
    extern const Event ThreadPoolReaderPageCacheHitElapsedMicroseconds;
    extern const Event ThreadPoolReaderPageCacheMiss;
    extern const Event ThreadPoolReaderPageCacheMissBytes;
    extern const Event ThreadPoolReaderPageCacheMissElapsedMicroseconds;

    extern const Event ReadBufferFromFileDescriptorRead;
    extern const Event ReadBufferFromFileDescriptorReadFailed;
    extern const Event ReadBufferFromFileDescriptorReadBytes;
    extern const Event DiskReadElapsedMicroseconds;
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

}


ThreadPoolReader::ThreadPoolReader(size_t pool_size, size_t queue_size_)
    : pool(pool_size, pool_size, queue_size_)
{
}

std::future<IAsynchronousReader::Result> ThreadPoolReader::submit(Request request)
{
    int fd = assert_cast<const LocalFileDescriptor &>(*request.descriptor).fd;

#if defined(__linux__)
    /// Check if data is already in page cache with preadv2 syscall.

    /// We don't want to depend on new Linux kernel.
    static std::atomic<bool> has_pread_nowait_support{true};

    if (has_pread_nowait_support.load(std::memory_order_relaxed))
    {
        Stopwatch watch(CLOCK_MONOTONIC);

        std::promise<Result> promise;
        std::future<Result> future = promise.get_future();

        size_t bytes_read = 0;
        while (!bytes_read)
        {
            ssize_t res = 0;

            {
                CurrentMetrics::Increment metric_increment{CurrentMetrics::Read};

                struct iovec io_vec{ .iov_base = request.buf, .iov_len = request.size };
                res = syscall(
                    SYS_preadv2, fd,
                    &io_vec, 1,
                    /// This is kind of weird calling convention for syscall.
                    static_cast<int64_t>(request.offset), static_cast<int64_t>(request.offset >> 32),
                    /// This flag forces read from page cache or returning EAGAIN.
                    RWF_NOWAIT);
            }

            if (!res)
            {
                /// The file has ended.
                promise.set_value(0);

                watch.stop();
                ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheHitElapsedMicroseconds, watch.elapsedMicroseconds());
                ProfileEvents::increment(ProfileEvents::DiskReadElapsedMicroseconds, watch.elapsedMicroseconds());

                return future;
            }

            if (-1 == res)
            {
                if (errno == ENOSYS || errno == EOPNOTSUPP)
                {
                    /// No support for the syscall or the flag in the Linux kernel.
                    has_pread_nowait_support.store(false, std::memory_order_relaxed);
                    break;
                }
                else if (errno == EAGAIN)
                {
                    /// Data is not available in page cache. Will hand off to thread pool.
                    break;
                }
                else if (errno == EINTR)
                {
                    /// Interrupted by a signal.
                    continue;
                }
                else
                {
                    ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
                    promise.set_exception(std::make_exception_ptr(ErrnoException(
                        fmt::format("Cannot read from file {}, {}", fd,
                            errnoToString(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno)),
                        ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno)));
                    return future;
                }
            }
            else
            {
                bytes_read += res;
                __msan_unpoison(request.buf, res);
            }
        }

        if (bytes_read)
        {
            /// It reports real time spent including the time spent while thread was preempted doing nothing.
            /// And it is Ok for the purpose of this watch (it is used to lower the number of threads to read from tables).
            /// Sometimes it is better to use taskstats::blkio_delay_total, but it is quite expensive to get it
            /// (TaskStatsInfoGetter has about 500K RPS).
            watch.stop();

            /// Read successfully from page cache.
            ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheHit);
            ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheHitBytes, bytes_read);
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadBytes, bytes_read);
            ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheHitElapsedMicroseconds, watch.elapsedMicroseconds());
            ProfileEvents::increment(ProfileEvents::DiskReadElapsedMicroseconds, watch.elapsedMicroseconds());

            promise.set_value(bytes_read);
            return future;
        }
    }
#endif

    ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheMiss);

    auto task = std::make_shared<std::packaged_task<Result()>>([request, fd]
    {
        setThreadName("ThreadPoolRead");
        Stopwatch watch(CLOCK_MONOTONIC);

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
                throwFromErrno(fmt::format("Cannot read from file {}", fd), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
            }

            bytes_read += res;
        }

        watch.stop();

        ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheMissBytes, bytes_read);
        ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadBytes, bytes_read);
        ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheMissElapsedMicroseconds, watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::DiskReadElapsedMicroseconds, watch.elapsedMicroseconds());

        return bytes_read;
    });

    auto future = task->get_future();

    /// ThreadPool is using "bigger is higher priority" instead of "smaller is more priority".
    pool.scheduleOrThrow([task]{ (*task)(); }, -request.priority);

    return future;
}

}
