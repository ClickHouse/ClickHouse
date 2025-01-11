#include "ThreadPoolReader.h"
#include <Common/VersionNumber.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <Common/MemorySanitizer.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadPool.h>
#include <Poco/Environment.h>
#include <base/errnoToString.h>
#include <Poco/Event.h>
#include <future>
#include <unistd.h>
#include <fcntl.h>

#if defined(OS_LINUX)

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
    #elif defined(__powerpc64__)
        #define SYS_preadv2 380
    #elif defined(__riscv)
        #define SYS_preadv2 286
    #elif defined(__loongarch64)
        #define SYS_preadv2 286
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
    extern const Event AsynchronousReaderIgnoredBytes;

    extern const Event ReadBufferFromFileDescriptorRead;
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

}

#if defined(OS_LINUX)
/// According to man, Linux 5.9 and 5.10 have a bug in preadv2() with the RWF_NOWAIT.
/// https://manpages.debian.org/testing/manpages-dev/preadv2.2.en.html#BUGS
/// We also disable it for older Linux kernels, because according to user's reports, RedHat-patched kernels might be also affected.
static bool hasBugInPreadV2()
{
    VersionNumber linux_version(Poco::Environment::osVersion());
    return linux_version < VersionNumber{5, 11, 0};
}
#endif

ThreadPoolReader::ThreadPoolReader(size_t pool_size, size_t queue_size_)
    : pool(std::make_unique<ThreadPool>(CurrentMetrics::ThreadPoolFSReaderThreads, CurrentMetrics::ThreadPoolFSReaderThreadsActive, CurrentMetrics::ThreadPoolFSReaderThreadsScheduled, pool_size, pool_size, queue_size_))
{
}

std::future<IAsynchronousReader::Result> ThreadPoolReader::submit(Request request)
{
    /// If size is zero, then read() cannot be distinguished from EOF
    assert(request.size);

    int fd = assert_cast<const LocalFileDescriptor &>(*request.descriptor).fd;

#if defined(OS_LINUX)
    /// Check if data is already in page cache with preadv2 syscall.

    /// We don't want to depend on new Linux kernel.
    /// But kernels 5.9 and 5.10 have a bug where preadv2() with the
    /// RWF_NOWAIT flag may return 0 even when not at end of file.
    /// It can't be distinguished from the real eof, so we have to
    /// disable pread with nowait.
    static const bool has_pread_nowait_support = !hasBugInPreadV2();

    if (has_pread_nowait_support)
    {
        /// It reports real time spent including the time spent while thread was preempted doing nothing.
        /// And it is Ok for the purpose of this watch (it is used to lower the number of threads to read from tables).
        /// Sometimes it is better to use taskstats::blkio_delay_total, but it is quite expensive to get it
        /// (NetlinkMetricsProvider has about 500K RPS).
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
                promise.set_value({0, 0, nullptr});
                return future;
            }

            if (-1 == res)
            {
                if (errno == ENOSYS || errno == EOPNOTSUPP)
                {
                    /// No support for the syscall or the flag in the Linux kernel.
                    /// It shouldn't happen because we check the kernel version but let's
                    /// fallback to the thread pool.
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
            __msan_unpoison(request.buf, res);
        }

        if (bytes_read)
        {
            /// Read successfully from page cache.
            ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheHit);
            ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheHitBytes, bytes_read);
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadBytes, bytes_read);
            ProfileEvents::increment(ProfileEvents::AsynchronousReaderIgnoredBytes, request.ignore);

            promise.set_value({bytes_read, request.ignore, nullptr});
            return future;
        }
    }
#endif

    ProfileEvents::increment(ProfileEvents::ThreadPoolReaderPageCacheMiss);

    auto schedule = threadPoolCallbackRunnerUnsafe<Result>(*pool, "ThreadPoolRead");

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

        return Result{ .size = bytes_read, .offset = request.ignore };
    }, request.priority);
}

void ThreadPoolReader::wait()
{
    pool->wait();
}

}
