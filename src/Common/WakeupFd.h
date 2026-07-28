#pragma once

/// Not built on Windows. This is a self-pipe, and Windows pipes are not pollable alongside
/// sockets - neither `WSAPoll` nor wepoll accepts anything but a `SOCKET` - so the wakeup for a
/// Windows event loop has to be a loopback socket pair (or `PostQueuedCompletionStatus`) rather
/// than this. That belongs with the `Epoll` backend for Windows, which does not exist yet; until
/// then the only consumers, `PollingQueue` and `MergeTreeBoundsSubscription`, are not built
/// there either. See docs/en/development/build-cross-windows.md.
#if !defined(OS_WINDOWS)

#include <Common/PipeFDs.h>

namespace DB
{

/// Portable async wakeup primitive backed by a non-blocking self-pipe.
/// Works on every Unix. Designed for use with IProcessor::schedule().
class WakeupFd
{
public:
    WakeupFd();

    WakeupFd(const WakeupFd &) = delete;
    WakeupFd & operator=(const WakeupFd &) = delete;
    WakeupFd(WakeupFd &&) = delete;
    WakeupFd & operator=(WakeupFd &&) = delete;

    /// Readable end — register this with epoll/kqueue/poll for POLLIN.
    int fd() const { return pipe.fds_rw[0]; }

    /// Wake any waiter polling on fd().
    /// Idempotent: multiple notify() between drains collapse to "at least one byte is readable".
    void notify() const;

    /// Read and discard all queued wakeup bytes.
    void drain() const;

private:
    PipeFDs pipe;
};

}

#endif
