#pragma once

#include <base/types.h>

#include <cstddef>
#include <cstdint>

namespace DB
{

/// A non-owning handle to a socket, together with the few operations ClickHouse performs on one
/// directly rather than through Poco.
///
/// This type exists because a socket is not the same kind of object on the platforms we build
/// for, and the difference is not one that a typedef alone papers over:
///
///  - On POSIX a socket *is* a file descriptor - a small non-negative `int` - and `read`,
///    `write`, `poll` and `close` accept it like any other. Errors arrive in `errno`.
///  - On Windows a socket is a `SOCKET`: an opaque, pointer-sized, *unsigned* handle that is not
///    a file descriptor at all. Only the Winsock entry points accept it, it is closed with
///    `closesocket` rather than `close`, and errors arrive from `WSAGetLastError` rather than
///    `errno`. Its invalid value is `INVALID_SOCKET`, which is all bits set - so a `SOCKET`
///    stored in an `int` compares equal to `-1` by luck rather than by design, and a valid
///    handle above `INT_MAX` would be corrupted outright.
///
/// Passing socket descriptors around as `int`, as the codebase still does in places, is
/// therefore wrong on Windows rather than merely unclean.
///
/// The handle is **not owned**. Sockets in ClickHouse belong to Poco, which closes them; this is
/// a view onto one, so copying is free and nothing here ever closes anything.
struct Socket
{
    /// Wide enough for a Windows `SOCKET` (`UINT_PTR`) and for a POSIX file descriptor, without
    /// dragging <winsock2.h> - and the macros that come with it - into every consumer.
    /// `Poco::Net::poco_socket_t` converts to this implicitly on both platforms.
#if defined(OS_WINDOWS)
    using Handle = std::uintptr_t;
#else
    using Handle = int;
#endif

    /// `-1` is the invalid handle on both platforms, though for different reasons: it is POSIX's
    /// "not a descriptor", and it is every bit set, which is Windows' `INVALID_SOCKET`.
    static constexpr Handle INVALID = static_cast<Handle>(-1);

    Handle handle = INVALID;

    Socket() = default;
    explicit Socket(Handle handle_) : handle(handle_) { }

    bool isValid() const
    {
#if defined(OS_WINDOWS)
        return handle != INVALID;
#else
        /// A descriptor is any non-negative `int`, not just one that is not `-1`.
        return handle >= 0;
#endif
    }

    /// What to wait for in `poll`, and what it reports back.
    enum Event : int
    {
        None = 0,
        Read = 1,
        Write = 2,
        /// An error or hangup on the socket. Never waited *for* - always reported alongside
        /// whatever was asked for, because both platforms report it unconditionally.
        Error = 4,
    };

    /// Waits until one of `events` is ready on the socket, for at most `timeout_microseconds`.
    /// A zero timeout polls and returns immediately; a negative one waits indefinitely.
    ///
    /// Returns the subset of `events` (plus `Error`) that is ready, or `None` on timeout.
    /// Retries on interruption, so a signal does not shorten the wait. Throws `NetException` if
    /// the socket cannot be waited on at all.
    int poll(int events, Int64 timeout_microseconds) const;

    /// Reads up to `size` bytes without consuming them, and without ever blocking - whether or
    /// not the socket is in non-blocking mode, and without changing that mode, since the socket
    /// belongs to someone else.
    ///
    /// Returns the number of bytes peeked, `0` if the peer has closed the connection, or `-1` if
    /// there was nothing to read (which `lastError` reports as a would-block) or on error.
    Int64 peek(char * buffer, size_t size) const;

    /// The last socket error: `errno` on POSIX, `WSAGetLastError()` on Windows. These are
    /// different numbering spaces, which is why comparing against `EAGAIN` directly does not
    /// work on Windows and `isWouldBlock` exists.
    static int lastError();

    /// Whether `error` means "nothing to do right now" rather than a real failure.
    static bool isWouldBlock(int error);

    /// Whether `error` means the call was interrupted and should be retried.
    static bool isInterrupted(int error);
};

}
