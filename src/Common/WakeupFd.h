#pragma once

#include <Common/Socket.h>

#if !defined(OS_WINDOWS)
#include <Common/PipeFDs.h>
#endif

#include <base/defines.h>

#if defined(DEBUG_OR_SANITIZER_BUILD) && !defined(OS_WINDOWS)
#include <base/types.h>
#endif

namespace DB
{

#if defined(DEBUG_OR_SANITIZER_BUILD) && !defined(OS_WINDOWS)
/// dev:ino of a descriptor, i.e. which file it actually refers to.
struct FdIdentity
{
    UInt64 dev = 0;
    UInt64 ino = 0;
};
#endif

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
    /// object and are closed with it. There is no counterpart to the identity check below - a
    /// socket handle is not a descriptor number that unrelated code can close and have recycled.
    Socket read_end;
    Socket write_end;
#else
    PipeFDs pipe;

/// If unrelated code closes our fd number (a stale-fd double close), the number gets silently
/// recycled and a blocking read()/write() on it wedges the caller forever (seen as an hour-long
/// streaming-source hang in stress tests). Compare the ends against their identity at construction
/// to catch it, in the builds where aborting on it is what we want.

    enum class PipeEnd
    {
        Read,
        Write,
    };
    void validate(PipeEnd end) const;

#ifdef DEBUG_OR_SANITIZER_BUILD
    FdIdentity read_end_identity;
    FdIdentity write_end_identity;
#endif
#endif
};

}
