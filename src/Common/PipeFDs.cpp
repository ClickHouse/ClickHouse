#include <Common/PipeFDs.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>

#include <base/logger_useful.h>
#include <base/errnoToString.h>

#include <unistd.h>
#include <fcntl.h>
#include <string>
#include <algorithm>


namespace DB
{

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
            throw Exception("Pipe is already opened", ErrorCodes::LOGICAL_ERROR);

#ifndef __APPLE__
    if (0 != pipe2(fds_rw, O_CLOEXEC))
        throwFromErrno("Cannot create pipe", ErrorCodes::CANNOT_PIPE);
#else
    if (0 != pipe(fds_rw))
        throwFromErrno("Cannot create pipe", ErrorCodes::CANNOT_PIPE);
    if (0 != fcntl(fds_rw[0], F_SETFD, FD_CLOEXEC))
        throwFromErrno("Cannot setup auto-close on exec for read end of pipe", ErrorCodes::CANNOT_FCNTL);
    if (0 != fcntl(fds_rw[1], F_SETFD, FD_CLOEXEC))
        throwFromErrno("Cannot setup auto-close on exec for write end of pipe", ErrorCodes::CANNOT_FCNTL);
#endif
}

void LazyPipeFDs::close()
{
    for (int & fd : fds_rw)
    {
        if (fd < 0)
            continue;
        if (0 != ::close(fd))
            throwFromErrno("Cannot close pipe", ErrorCodes::CANNOT_PIPE);
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
        throwFromErrno("Cannot get file status flags of pipe", ErrorCodes::CANNOT_FCNTL);
    if (-1 == fcntl(fds_rw[1], F_SETFL, flags | O_NONBLOCK))
        throwFromErrno("Cannot set non-blocking mode of pipe", ErrorCodes::CANNOT_FCNTL);
}

void LazyPipeFDs::setNonBlockingRead()
{
    int flags = fcntl(fds_rw[0], F_GETFL, 0);
    if (-1 == flags)
        throwFromErrno("Cannot get file status flags of pipe", ErrorCodes::CANNOT_FCNTL);
    if (-1 == fcntl(fds_rw[0], F_SETFL, flags | O_NONBLOCK))
        throwFromErrno("Cannot set non-blocking mode of pipe", ErrorCodes::CANNOT_FCNTL);
}

void LazyPipeFDs::setNonBlockingReadWrite()
{
    setNonBlockingRead();
    setNonBlockingWrite();
}

void LazyPipeFDs::tryIncreaseSize(int desired_size)
{
#if defined(OS_LINUX)
    Poco::Logger * log = &Poco::Logger::get("Pipe");

    /** Increase pipe size to avoid slowdown during fine-grained trace collection.
      */
    int pipe_size = fcntl(fds_rw[1], F_GETPIPE_SZ);
    if (-1 == pipe_size)
    {
        if (errno == EINVAL)
        {
            LOG_INFO(log, "Cannot get pipe capacity, {}. Very old Linux kernels have no support for this fcntl.", errnoToString(ErrorCodes::CANNOT_FCNTL));
            /// It will work nevertheless.
        }
        else
            throwFromErrno("Cannot get pipe capacity", ErrorCodes::CANNOT_FCNTL);
    }
    else
    {
        for (errno = 0; errno != EPERM && pipe_size < desired_size; pipe_size *= 2)
            if (-1 == fcntl(fds_rw[1], F_SETPIPE_SZ, pipe_size * 2) && errno != EPERM)
                throwFromErrno("Cannot increase pipe capacity to " + std::to_string(pipe_size * 2), ErrorCodes::CANNOT_FCNTL);

        LOG_TRACE(log, "Pipe capacity is {}", ReadableSize(std::min(pipe_size, desired_size)));
    }
#else
    (void)desired_size;
#endif
}

}
