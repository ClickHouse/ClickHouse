#pragma once

#include <IO/AsynchronousReader.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
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
    #define SYS_preadv2 327 /// TODO: AArch64, PPC64
#endif

#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
}

/** Perform reads from separate thread pool of specified size.
  *
  * Note: doing reads from thread pool is usually bad idea for the following reasons:
  * - for fast devices or when data is in page cache, it is less cache-friendly and less NUMA friendly
  *   and also involves extra synchronization overhead;
  * - for fast devices and lots of small random reads, it cannot utilize the device, because
  *   OS will spent all the time in switching between threads and wasting CPU;
  * - you don't know how many threads do you need, for example, when reading from page cache,
  *   you need the number of threads similar to the number of CPU cores;
  *   when reading from HDD array you need the number of threads as the number of disks times N;
  *   when reading from SSD you may need at least hundreds of threads for efficient random reads,
  *   but it is impractical;
  * For example, this method is used in POSIX AIO that is notoriously useless (in contrast to Linux AIO).
  *
  * This is intended only as example for implementation of readers from remote filesystems,
  * where this method can be feasible.
  */
class ThreadPoolReader final : public IAsynchronousReader
{
private:
    ThreadPool pool;

public:
    ThreadPoolReader(size_t pool_size, size_t queue_size_)
        : pool(pool_size, pool_size, queue_size_)
    {
    }

    std::future<Result> submit(Request request) override
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
                        std::cerr << "miss\n";
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
                std::cerr << "hit\n";
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
        pool.scheduleOrThrow([task]{ (*task)(); }, -request.priority);
        return future;
    }

    ~ThreadPoolReader() override
    {
        /// pool automatically waits for all tasks in destructor.
    }
};

}


