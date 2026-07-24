#pragma once

#include <Common/PipeFDs.h>

#include <base/defines.h>
#include <base/types.h>

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

#ifdef DEBUG_OR_SANITIZER_BUILD
    /// Throws LOGICAL_ERROR (which aborts in these builds) if the fd no longer matches the pipe end
    /// created by the constructor.
    void validate(int which) const;

    /// dev:ino of each pipe end at construction. If unrelated code closes our fd number (a stale-fd
    /// double close), the number gets silently recycled and a blocking read()/write() on it wedges
    /// the caller forever (seen as an hour-long streaming-source hang in stress tests).
    struct EndIdentity
    {
        UInt64 dev = 0;
        UInt64 ino = 0;
    };
    EndIdentity ends[2];
#endif
};

}
