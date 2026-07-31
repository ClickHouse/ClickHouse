#pragma once

#include <Common/PipeFDs.h>

#include <base/defines.h>

#ifdef DEBUG_OR_SANITIZER_BUILD
#include <base/types.h>
#endif

namespace DB
{

#ifdef DEBUG_OR_SANITIZER_BUILD
/// dev:ino of a descriptor, i.e. which file it actually refers to.
struct FdIdentity
{
    UInt64 dev = 0;
    UInt64 ino = 0;
};
#endif

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
};

}
