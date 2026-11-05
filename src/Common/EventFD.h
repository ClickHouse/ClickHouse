#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <cstdint>


namespace DB
{

struct EventFD
{
    EventFD();
    ~EventFD();

    /// Both read() and write() are blocking.
    /// TODO: add non-blocking flag to ctor.
    uint64_t read() const;
    bool write(uint64_t increase = 1) const;

    /// Readable (pollable) descriptor. On Linux it is the eventfd itself and is used for both
    /// read() and write(); on macOS it is the read end of a self-pipe (see write_fd below).
    int fd = -1;

#if defined(OS_DARWIN)
    /// macOS has no eventfd, so EventFD is emulated with a self-pipe: fd is the read end and
    /// write_fd is the write end. The read end is left blocking so read() waits for a write(),
    /// matching the eventfd semantics relied upon by callers.
    int write_fd = -1;
#endif
};

}

#else

namespace DB
{

struct EventFD
{
};

}

#endif
