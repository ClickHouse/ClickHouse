#include "common/Pipe.h"

void LazyPipe::open()
{
    for (int & fd : fds_rw)
    {
        if (fd >= 0)
        {
            throw std::logic_error("Pipe is already opened");
        }
    }

#ifndef __APPLE__
    if (0 != pipe2(fds_rw, O_CLOEXEC))
        throw std::runtime_error("Cannot create pipe");
#else
    if (0 != pipe(fds_rw))
        throw std::runtime_error("Cannot create pipe");
    if (0 != fcntl(fds_rw[0], F_SETFD, FD_CLOEXEC))
        throw std::runtime_error("Cannot setup auto-close on exec for read end of pipe");
    if (0 != fcntl(fds_rw[1], F_SETFD, FD_CLOEXEC))
        throw std::runtime_error("Cannot setup auto-close on exec for write end of pipe");
#endif
}

void LazyPipe::close()
{
    for (int fd : fds_rw)
    {
        if (fd >= 0)
        {
            ::close(fd);
        }
    }
}

Pipe::Pipe()
{
    open();
}

Pipe::~Pipe()
{
    close();
}
