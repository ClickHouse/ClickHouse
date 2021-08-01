#pragma once

#include <IO/AsynchronousReader.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <common/errnoToString.h>
#include <Poco/Event.h>
#include <unordered_map>
#include <mutex>
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
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_SCHEDULE_TASK;
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
    UInt64 counter = 0;

    struct RequestInfo
    {
        bool already_read = false;
        Poco::Event event;
        Result result;
    };

    using Requests = std::unordered_map<UInt64, RequestInfo>;
    Requests requests;
    std::mutex mutex;

    ThreadPool pool;
    size_t queue_size;

public:
    ThreadPoolReader(size_t pool_size, size_t queue_size_)
        : pool(pool_size, pool_size, queue_size_), queue_size(queue_size_)
    {
    }

    RequestID submit(Request request) override
    {
        Requests::iterator it;

        {
            std::lock_guard lock(mutex);

            if (requests.size() >= queue_size)
                throw Exception("Too many read requests in flight", ErrorCodes::CANNOT_SCHEDULE_TASK);

            ++counter;
            it = requests.emplace(std::piecewise_construct, std::forward_as_tuple(counter), std::forward_as_tuple()).first;
        }

        int fd = assert_cast<const LocalFileDescriptor &>(*request.descriptor).fd;

        RequestInfo & info = it->second;

#if defined(__linux__)
        /// Check if data is already in page cache with preadv2 syscall.

        /// TODO ProfileEvents for page cache hits and misses.

        /// We don't want to depend on new Linux kernel.
        static std::atomic<bool> has_preadv2_syscall{true};

        if (has_preadv2_syscall.load(std::memory_order_relaxed))
        {
            size_t bytes_read = 0;
            while (!bytes_read)
            {
                struct iovec io_vec{ .iov_base = request.buf, .iov_len = request.size };
                ssize_t res = syscall(
                    SYS_preadv2, fd,
                    &io_vec, 1,
                    static_cast<long>(request.offset), static_cast<long>(request.offset >> 32),
                    RWF_NOWAIT);

                if (!res)
                {
                    info.already_read = true;
                    break;
                }

                if (-1 == res)
                {
                    if (errno == ENOSYS)
                    {
                        has_preadv2_syscall.store(false, std::memory_order_relaxed);
                        break;
                    }
                    else if (errno == EAGAIN)
                    {
                        /// Data is not available.
                        break;
                    }
                    else if (errno == EINTR)
                    {
                        continue;
                    }
                    else
                    {
                        info.already_read = true;
                        info.result.exception = std::make_exception_ptr(ErrnoException(
                            fmt::format("Cannot read from file {}, {}", fd,
                                errnoToString(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno)),
                            ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno));
                        break;
                    }
                }
                else
                {
                    bytes_read += res;
                    info.already_read = true;
                }
            }

            info.result.size = bytes_read;
        }
#endif

        if (!info.already_read)
        {
            pool.scheduleOrThrow([request, fd, &info]
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
                        {
                            info.result.exception = std::make_exception_ptr(ErrnoException(
                                fmt::format("Cannot read from file {}, {}", fd,
                                    errnoToString(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno)),
                                ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno));
                            break;
                        }

                        bytes_read += res;
                    }

                    info.result.size = bytes_read;
                    info.event.set();
                },
                request.priority);
        }

        return it->first;
    }

    std::optional<Result> wait(RequestID id, std::optional<UInt64> microseconds) override
    {
        Result result;
        Requests::iterator it;

        {
            std::lock_guard lock(mutex);
            it = requests.find(id);
            if (it == requests.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find request by id {}", id);
        }

        if (!it->second.already_read)
        {
            if (microseconds)
            {
                if (!it->second.event.tryWait(*microseconds / 1000))
                    return {};
            }
            else
                it->second.event.wait();
        }

        Result res = it->second.result;

        {
            std::lock_guard lock(mutex);
            requests.erase(it);
        }

        return res;
    }

    ~ThreadPoolReader() override
    {
        /// pool automatically waits for all tasks in destructor.
    }
};

}


