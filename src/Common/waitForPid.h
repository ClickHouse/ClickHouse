#pragma once
#include <sys/types.h>

namespace DB
{
enum class ChildState
{
    RUNNING,
    EXITED,
    /// `ECHILD`: the pid was reaped already or is not a child of this process, so it may belong to
    /// anybody now. Neither waited for nor signalled again.
    NOT_OUR_CHILD,
    /// The wait itself failed for another reason. Nothing is known about the child, which is still
    /// ours as far as anyone can tell - unlike `NOT_OUR_CHILD`, it is not to be forgotten.
    UNKNOWN,
};

/// The state of the child `pid`, without reaping it. With `blocking`, waits for it to exit.
ChildState peekChildState(pid_t pid, bool blocking);

enum class WaitForPidResult
{
    EXITED,
    TIMEOUT,
    /// See `ChildState::NOT_OUR_CHILD`.
    NOT_OUR_CHILD,
    /// The wait failed; see `ChildState::UNKNOWN`.
    ERROR,
};

/// Waits up to `timeout_in_milliseconds` for the child `pid` to exit, reaping it. Distinguishes an
/// expired deadline from a failed wait, so a caller cannot retry errors in a busy loop, and a child
/// that is not ours any more from a wait that failed. With `leave_unreaped`, a child that has exited
/// is left a zombie (`waitid` with `WNOWAIT`), so its pid - and the number of the process group it
/// leads - cannot be reused until it is reaped.
WaitForPidResult waitForPidMilliseconds(pid_t pid, size_t timeout_in_milliseconds, bool leave_unreaped = false);

#if defined(OS_LINUX)
int syscall_pidfd_open(pid_t pid);
int syscall_pidfd_send_signal(int pidfd, int sig);

/// `pidfd_open` for a wait on the child `pid`: the descriptor, or -1 with `errno` set. A refusal
/// (`EPERM`, `EACCES`, `ENOSYS` - a seccomp profile) is not an error for a wait, which then polls in
/// short steps, but it is logged, once per process, so that the polling has a stated reason.
int openPidFdForWaiting(pid_t pid);
#endif

}
