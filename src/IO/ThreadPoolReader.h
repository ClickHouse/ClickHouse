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

        pool.scheduleOrThrow([request, info = &it->second]
            {
                setThreadName("ThreadPoolRead");

                int fd = assert_cast<const LocalFileDescriptor &>(*request.descriptor).fd;

                /// TODO Instrumentation.

                size_t bytes_read = 0;
                while (!bytes_read)
                {
                    ssize_t res = ::pread(fd, request.buf, request.size, request.offset);
                    if (!res)
                        break;

                    if (-1 == res && errno != EINTR)
                    {
                        info->result.exception = std::make_exception_ptr(ErrnoException(
                            fmt::format("Cannot read from file {}, {}", fd,
                                errnoToString(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno)),
                            ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno));
                        break;
                    }

                    if (res > 0)
                        bytes_read += res;
                }

                info->result.size = bytes_read;
                info->event.set();
            },
            request.priority);

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

        if (microseconds)
        {
            if (!it->second.event.tryWait(*microseconds / 1000))
                return {};
        }
        else
            it->second.event.wait();

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


