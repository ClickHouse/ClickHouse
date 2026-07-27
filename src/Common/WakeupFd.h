#pragma once

#include <Common/PipeFDs.h>

#include <base/defines.h>
#include <base/types.h>

#include <optional>

struct PreformattedMessage;

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

#ifdef DEBUG_OR_SANITIZER_BUILD
    /// Returns a description of the problem if the given end of the pipe (0 = read, 1 = write) no
    /// longer matches the descriptor created by the constructor, std::nullopt if it is intact.
    /// notify()/drain() abort on it via validate; public so tests can check without aborting.
    std::optional<PreformattedMessage> checkEnd(int which) const;
#endif

private:
    PipeFDs pipe;

#ifdef DEBUG_OR_SANITIZER_BUILD
    /// Throws LOGICAL_ERROR (which aborts in these builds) if checkEnd reports a problem.
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
