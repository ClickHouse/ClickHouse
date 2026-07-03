
#if defined(OS_LINUX)

#include <Common/EventFD.h>
#include <Common/Exception.h>
#include <base/defines.h>
#include <sys/eventfd.h>
#include <unistd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PIPE;
    extern const int CANNOT_READ_FROM_SOCKET;
    extern const int CANNOT_WRITE_TO_SOCKET;
}

EventFD::EventFD()
{
    fd = eventfd(0 /* initval */, 0 /* flags */);
    if (fd == -1)
        throw ErrnoException(ErrorCodes::CANNOT_PIPE, "Cannot create eventfd");
}

uint64_t EventFD::read() const
{
    uint64_t buf = 0;
    while (-1 == ::read(fd, &buf, sizeof(buf)))
    {
        if (errno == EAGAIN)
            break;

        if (errno != EINTR)
            throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_SOCKET, "Cannot read from eventfd");
    }

    return buf;
}

bool EventFD::write(uint64_t increase) const
{
    while (-1 == ::write(fd, &increase, sizeof(increase)))
    {
        if (errno == EAGAIN)
            return false;

        if (errno != EINTR)
            throw ErrnoException(ErrorCodes::CANNOT_WRITE_TO_SOCKET, "Cannot write to eventfd");
    }

    return true;
}

EventFD::~EventFD()
{
    if (fd != -1)
    {
        [[maybe_unused]] int err = close(fd);
        chassert(!err || errno == EINTR);
    }
}

}

#endif
