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
public:
    std::future<Result> submit(Request request) override
    {
        int fd = assert_cast<const LocalFileDescriptor &>(*request.descriptor).fd;

#if defined(POSIX_FADV_WILLNEED)
        if (0 != posix_fadvise(fd, request.offset, request.size, POSIX_FADV_WILLNEED))
            throwFromErrno("Cannot posix_fadvise", ErrorCodes::CANNOT_ADVISE);
#endif

        return std::async(std::launch::deferred, [fd, request]
        {
            /// TODO Instrumentation.

            size_t bytes_read = 0;
            while (!bytes_read)
            {
                ssize_t res = ::pread(fd, request.buf, request.size, request.offset);
                if (!res)
                    break;

                if (-1 == res && errno != EINTR)
                    throwFromErrno(fmt::format("Cannot read from file {}", fd), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);

                if (res > 0)
                    bytes_read += res;
            }

            return bytes_read;
        });
    }

    ~SynchronousReader() override
    {
        /// Nothing to do, no requests are processed in background.
    }
};

}

