#include <IO/ThreadPoolReader.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <common/errnoToString.h>
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

    /// TODO ProfileEvents for page cache hits and misses.

    /// We don't want to depend on new Linux kernel.
    static std::atomic<bool> has_pread_nowait_support{true};

    if (has_pread_nowait_support.load(std::memory_order_relaxed))
    {
        std::promise<Result> promise;
        std::future<Result> future = promise.get_future();

        size_t bytes_read = 0;
        while (!bytes_read)
        {
            struct iovec io_vec{ .iov_base = request.buf, .iov_len = request.size };
            ssize_t res = syscall(
                SYS_preadv2, fd,
                &io_vec, 1,
                static_cast<long>(request.offset), static_cast<long>(request.offset >> 32),
                RWF_NOWAIT);

            //ssize_t res = ::pread(fd, request.buf, request.size, request.offset);

            if (!res)
            {
                promise.set_value(0);
                return future;
            }

            if (-1 == res)
            {
                if (errno == ENOSYS || errno == EOPNOTSUPP)
                {
                    has_pread_nowait_support.store(false, std::memory_order_relaxed);
                    break;
                }
                else if (errno == EAGAIN)
                {
                    /// Data is not available.
                    //std::cerr << "miss\n";
                    break;
                }
                else if (errno == EINTR)
                {
                    continue;
                }
                else
                {
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
            }
        }

        if (bytes_read)
        {
            //std::cerr << "hit\n";
            promise.set_value(bytes_read);
            return future;
        }
    }
#endif

    auto task = std::make_shared<std::packaged_task<Result()>>([request, fd]
    {
        setThreadName("ThreadPoolRead");

        /// TODO Instrumentation.

        size_t bytes_read = 0;
        while (!bytes_read)
        {
            ssize_t res = ::pread(fd, request.buf, request.size, request.offset);
            if (!res)
                break;

            if (-1 == res && errno != EINTR)
                throwFromErrno(fmt::format("Cannot read from file {}", fd), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);

            bytes_read += res;
        }

        return bytes_read;
    });

    auto future = task->get_future();

    /// ThreadPool is using "bigger is higher priority" instead of "smaller is more priority".
    pool.scheduleOrThrow([task]{ (*task)(); }, -request.priority);

    return future;
}

}
