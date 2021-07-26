#pragma once

#include <IO/AsynchronousReader.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <common/errnoToString.h>
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
    extern const int CANNOT_ADVISE;
}

/** Implementation of IAsynchronousReader that in fact synchronous.
  * The only addition is posix_fadvise.
  */
class SynchronousReader final : public IAsynchronousReader
{
private:
    UInt64 counter = 0;
    std::unordered_map<UInt64, Request> requests;
    std::mutex mutex;

public:
    RequestID submit(Request request) override
    {
        int fd = assert_cast<const LocalFileDescriptor &>(*request.descriptor).fd;
        if (0 != posix_fadvise(fd, request.offset, request.size, POSIX_FADV_WILLNEED))
            throwFromErrno("Cannot posix_fadvise", ErrorCodes::CANNOT_ADVISE);

        std::lock_guard lock(mutex);
        ++counter;
        requests.emplace(counter, request);
        return counter;
    }

    /// Timeout is not implemented.
    std::optional<Result> wait(RequestID id, std::optional<UInt64>) override
    {
        Request request;
        Result result;

        {
            std::lock_guard lock(mutex);
            auto it = requests.find(id);
            if (it == requests.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find request by id {}", id);

            request = it->second;
            requests.erase(it);
        }

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
                result.exception = std::make_exception_ptr(ErrnoException(
                    fmt::format("Cannot read from file {}, {}", fd,
                        errnoToString(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno)),
                    ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno));
                return result;
            }

            if (res > 0)
                bytes_read += res;
        }

        result.size = bytes_read;
        return result;
    }

    ~SynchronousReader() override
    {
        /// Nothing to do, no requests are processed in background.
    }
};

}

