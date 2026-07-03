#pragma once

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
