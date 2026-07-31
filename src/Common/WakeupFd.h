#pragma once

#include <Common/Socket.h>

#if !defined(OS_WINDOWS)
#include <Common/PipeFDs.h>
#endif

namespace DB
{

/// Portable async wakeup primitive. Designed for use with IProcessor::schedule().
///
/// A non-blocking self-pipe on Unix. On Windows it is a connected loopback socket pair instead,
/// because a Windows pipe cannot be waited on together with sockets - neither `WSAPoll` nor wepoll
/// accepts anything but a `SOCKET` - and the whole point of this class is to be one more thing an
/// event loop that already watches sockets can wait on. libuv and Asio do the same.
class WakeupFd
{
public:
    WakeupFd();
    ~WakeupFd();

    WakeupFd(const WakeupFd &) = delete;
    WakeupFd & operator=(const WakeupFd &) = delete;
    WakeupFd(WakeupFd &&) = delete;
    WakeupFd & operator=(WakeupFd &&) = delete;

    /// Readable end — register this with epoll/kqueue/poll for POLLIN.
    int fd() const;

    /// Wake any waiter polling on fd().
    /// Idempotent: multiple notify() between drains collapse to "at least one byte is readable".
    void notify() const;

    /// Read and discard all queued wakeup bytes.
    void drain() const;

private:
#if defined(OS_WINDOWS)
    /// Owned, unlike the `Socket` handles passed around elsewhere: these two exist only for this
    /// object and are closed with it.
    Socket read_end;
    Socket write_end;
#else
    PipeFDs pipe;
#endif
};

}
