#pragma once

#include <functional>
#include <string>

#include <Poco/Timespan.h>

/// The vocabulary for "this operation would block; wake me when the descriptor is ready" - the
/// callback type, the events it can be waited on for, and which timeout applies.
///
/// Deliberately separate from `AsyncTaskExecutor`, which is the fiber-based executor that
/// consumes these. The socket buffers only ever *name* this vocabulary and never touch a fiber,
/// so they should not have to depend on the executor - and on Windows they cannot, because
/// fibers are not built there while the socket buffers very much are.
#if defined(OS_LINUX) || defined(OS_DARWIN)
#include <Common/Epoll.h>
#endif
#if defined(OS_LINUX)
#include <sys/epoll.h>
#endif

namespace DB
{

enum class AsyncEventTimeoutType : uint8_t
{
    CONNECT,
    RECEIVE,
    SEND,
    NONE,
};

#if defined(OS_LINUX) || defined(OS_DARWIN)
/// The kqueue-backed compatibility shim in <Common/Epoll.h> defines these on macOS, so the
/// values match the `Epoll` flags on both platforms.
enum AsyncEvent
{
    READ = EPOLLIN,
    WRITE = EPOLLOUT,
    ERROR = EPOLLERR,
};
#else
enum AsyncEvent
{
    READ = 1,
    WRITE = 2,
    ERROR = 4,
};
#endif

/// Called with the descriptor to wait on, its timeout, which timeout that is, a description for
/// diagnostics, and a mask of `AsyncEvent`.
using AsyncCallback = std::function<void(int, Poco::Timespan, AsyncEventTimeoutType, const std::string &, uint32_t)>;

}
