#include <Storages/Kafka/Clients/Net/Transport.h>

#include <base/defines.h>
#include <base/errnoToString.h>

#include <fcntl.h>
#include <unistd.h>

namespace Kafka::Net
{

WakeupSource::WakeupSource()
{
    int fds[2];
    if (::pipe(fds) != 0)
        throw Exception(ErrorKind::Connection, "Cannot create pipe: {}", errnoToString());
    read_fd = fds[0];
    write_fd = fds[1];
    for (int fd : fds)
    {
        ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK);
        ::fcntl(fd, F_SETFD, FD_CLOEXEC);
    }
}

WakeupSource::~WakeupSource()
{
    if (read_fd >= 0)
    {
        [[maybe_unused]] int err = ::close(read_fd);
        chassert(!err);
    }
    if (write_fd >= 0)
    {
        [[maybe_unused]] int err = ::close(write_fd);
        chassert(!err);
    }
}

void WakeupSource::wakeup()
{
    generation.fetch_add(1);
    const char byte = 1;
    /// Non-blocking write; a full pipe already guarantees pending wakeups.
    [[maybe_unused]] ssize_t rc = ::write(write_fd, &byte, 1);
}

void WakeupSource::drain() const
{
    char buf[64];
    while (::read(read_fd, buf, sizeof(buf)) > 0)
    {
    }
}

}
