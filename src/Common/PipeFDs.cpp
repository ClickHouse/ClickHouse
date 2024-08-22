#include <Common/PipeFDs.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <Common/FailPoint.h>

#include <Common/logger_useful.h>
#include <base/errnoToString.h>

#include <unistd.h>
#include <fcntl.h>
#include <algorithm>

namespace DB
{

namespace FailPoints
{
    extern const char lazy_pipe_fds_fail_close[];
}

namespace ErrorCodes
{
    extern const int CANNOT_PIPE;
    extern const int CANNOT_FCNTL;
    extern const int LOGICAL_ERROR;
}

void LazyPipeFDs::open()
{
    for (int & fd : fds_rw)
        if (fd >= 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipe is already opened");

#ifndef OS_DARWIN
    if (0 != pipe2(fds_rw, O_CLOEXEC))
        throw ErrnoException(ErrorCodes::CANNOT_PIPE, "Cannot create pipe");
#else
    if (0 != pipe(fds_rw))
        throw ErrnoException(ErrorCodes::CANNOT_PIPE, "Cannot create pipe");
    if (0 != fcntl(fds_rw[0], F_SETFD, FD_CLOEXEC))
        throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot setup auto-close on exec for read end of pipe");
    if (0 != fcntl(fds_rw[1], F_SETFD, FD_CLOEXEC))
        throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot setup auto-close on exec for write end of pipe");
#endif
}

void LazyPipeFDs::close()
{
    fiu_do_on(FailPoints::lazy_pipe_fds_fail_close,
    {
        throw Exception(ErrorCodes::CANNOT_PIPE, "Manually triggered exception on close");
    });

    for (int & fd : fds_rw)
    {
        if (fd < 0)
            continue;
        if (0 != ::close(fd))
            throw ErrnoException(ErrorCodes::CANNOT_PIPE, "Cannot close pipe");
        fd = -1;
    }
}

PipeFDs::PipeFDs()
{
    open();
}

LazyPipeFDs::~LazyPipeFDs()
{
    try
    {
        close();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void LazyPipeFDs::setNonBlockingWrite()
{
    int flags = fcntl(fds_rw[1], F_GETFL, 0);
    if (-1 == flags)
        throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot get file status flags of pipe");
    if (-1 == fcntl(fds_rw[1], F_SETFL, flags | O_NONBLOCK))
        throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot set non-blocking mode of pipe");
}

void LazyPipeFDs::setNonBlockingRead()
{
    int flags = fcntl(fds_rw[0], F_GETFL, 0);
    if (-1 == flags)
        throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot get file status flags of pipe");
    if (-1 == fcntl(fds_rw[0], F_SETFL, flags | O_NONBLOCK))
        throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot set non-blocking mode of pipe");
}

void LazyPipeFDs::setNonBlockingReadWrite()
{
    setNonBlockingRead();
    setNonBlockingWrite();
}

void LazyPipeFDs::tryIncreaseSize(int desired_size)
{
#if defined(OS_LINUX)
    LoggerPtr log = getLogger("Pipe");

    /** Increase pipe size to avoid slowdown during fine-grained trace collection.
      */
    int pipe_size = fcntl(fds_rw[1], F_GETPIPE_SZ);
    if (-1 == pipe_size)
    {
        if (errno == EINVAL)
        {
            LOG_INFO(log, "Cannot get pipe capacity, {}. Very old Linux kernels have no support for this fcntl.", errnoToString());
            /// It will work nevertheless.
        }
        else
            throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot get pipe capacity");
    }
    else
    {
        for (errno = 0; errno != EPERM && pipe_size < desired_size; pipe_size *= 2)
            if (-1 == fcntl(fds_rw[1], F_SETPIPE_SZ, pipe_size * 2) && errno != EPERM)
                throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot increase pipe capacity to {}", pipe_size * 2);

        LOG_TRACE(log, "Pipe capacity is {}", ReadableSize(std::min(pipe_size, desired_size)));
    }
#else
    (void)desired_size;
#endif
}

}
