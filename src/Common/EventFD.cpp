
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
        throwFromErrno("Cannot create eventfd", ErrorCodes::CANNOT_PIPE);
}

uint64_t EventFD::read() const
{
    uint64_t buf = 0;
    while (-1 == ::read(fd, &buf, sizeof(buf)))
    {
        if (errno == EAGAIN)
            break;

        if (errno != EINTR)
            throwFromErrno("Cannot read from eventfd", ErrorCodes::CANNOT_READ_FROM_SOCKET);
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
            throwFromErrno("Cannot write to eventfd", ErrorCodes::CANNOT_WRITE_TO_SOCKET);
    }

    return true;
}

EventFD::~EventFD()
{
    if (fd != -1)
    {
        int err = close(fd);
        chassert(!err || errno == EINTR);
    }
}

}

#endif
