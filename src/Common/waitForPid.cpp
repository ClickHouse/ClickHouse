#include <Common/waitForPid.h>
#include <Common/VersionNumber.h>
#include <Poco/Environment.h>
#include <Common/Stopwatch.h>
/// for abortOnFailedAssertion() via chassert() (dependency chain looks odd)
#include <Common/Exception.h>
#include <base/defines.h>
#include <base/scope_guard.h>

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-statement-expression"
#define HANDLE_EINTR(x) ({ \
    decltype(x) eintr_wrapper_result; \
    do { \
        eintr_wrapper_result = (x); \
    } while (eintr_wrapper_result == -1 && errno == EINTR); \
    eintr_wrapper_result; \
})

namespace DB
{

enum PollPidResult
{
    RESTART,
    FAILED
};

}

#if defined(OS_LINUX)

#include <poll.h>
#include <string>

#if !defined(__NR_pidfd_open)
    #if defined(__x86_64__)
        #define SYS_pidfd_open 434
    #elif defined(__aarch64__)
        #define SYS_pidfd_open 434
    #elif defined(__powerpc64__)
        #define SYS_pidfd_open 434
    #elif defined(__riscv)
        #define SYS_pidfd_open 434
    #elif defined(__s390x__)
        #define SYS_pidfd_open 434
    #elif defined(__loongarch64)
        #define SYS_pidfd_open 434
    #elif defined(__e2k__)
        #define SYS_pidfd_open 206
    #else
        #error "Unsupported architecture"
    #endif
#else
    #define SYS_pidfd_open __NR_pidfd_open
#endif

#if !defined(__NR_pidfd_send_signal)
    #define SYS_pidfd_send_signal 424
#else
    #define SYS_pidfd_send_signal __NR_pidfd_send_signal
#endif

namespace DB
{

int syscall_pidfd_open(pid_t pid)
{
    return static_cast<int>(syscall(SYS_pidfd_open, pid, 0));
}

int syscall_pidfd_send_signal(int pidfd, int sig)
{
    return static_cast<int>(syscall(SYS_pidfd_send_signal, pidfd, sig, nullptr, 0));
}

static bool supportsPidFdOpen()
{
    VersionNumber pidfd_open_minimal_version(5, 3, 0);
    VersionNumber linux_version(Poco::Environment::osVersion());
    return linux_version >= pidfd_open_minimal_version;
}

static PollPidResult pollPid(pid_t pid, int timeout_in_ms)
{
    int pid_fd = 0;

    if (supportsPidFdOpen())
    {
        // pidfd_open cannot be interrupted, no EINTR handling

        pid_fd = syscall_pidfd_open(pid);

        if (pid_fd < 0)
        {
            if (errno == ESRCH)
                return PollPidResult::RESTART;

            return PollPidResult::FAILED;
        }
    }
    else
    {
        std::string path = "/proc/" + std::to_string(pid);
        pid_fd = HANDLE_EINTR(open(path.c_str(), O_DIRECTORY));

        if (pid_fd < 0)
        {
            if (errno == ENOENT)
                return PollPidResult::RESTART;

            return PollPidResult::FAILED;
        }
    }

    /// Releases pid_fd on every return path, including poll timeout and error.
    SCOPE_EXIT(
    {
        [[maybe_unused]] int err = close(pid_fd);
        chassert(!err || errno == EINTR);
    });

    struct pollfd pollfd{};
    pollfd.fd = pid_fd;
    pollfd.events = POLLIN;

    /// A signal interrupting `poll` must not restart with the full timeout; returning
    /// RESTART lets `waitForPid` re-evaluate the deadline-bounded remaining budget.
    int ready = poll(&pollfd, 1, timeout_in_ms);

    if (ready < 0 && errno == EINTR)
        return PollPidResult::RESTART;

    /// `ready == 0` is a poll timeout; `ready < 0` (non-EINTR) is a real error.
    /// Both return FAILED: `waitForPid`'s outer deadline loop treats either as
    /// "stop waiting" — the timeout is re-evaluated there, not here.
    if (ready <= 0)
        return PollPidResult::FAILED;

    return PollPidResult::RESTART;
}

#elif defined(OS_DARWIN) || defined(OS_FREEBSD)

#pragma clang diagnostic ignored "-Wreserved-identifier"

#include <sys/event.h>
#include <err.h>

namespace DB
{

static PollPidResult pollPid(pid_t pid, int timeout_in_ms)
{
    int kq = kqueue();
    if (kq == -1)
        return PollPidResult::FAILED;

    /// Releases the kqueue fd on every return path, including early filter-add errors.
    SCOPE_EXIT({ close(kq); });

    struct kevent change{};
    change.ident = 0;

    EV_SET(&change, pid, EVFILT_PROC, EV_ADD, NOTE_EXIT, 0, NULL);

    int event_add_result = HANDLE_EINTR(kevent(kq, &change, 1, NULL, 0, NULL));
    if (event_add_result == -1)
    {
        if (errno == ESRCH)
            return PollPidResult::RESTART;

        return PollPidResult::FAILED;
    }

    struct kevent event{};
    event.ident = 0;

    struct timespec remaining_timespec = {.tv_sec = timeout_in_ms / 1000, .tv_nsec = (timeout_in_ms % 1000) * 1000000};

    /// A signal interrupting `kevent` must not restart with the full timeout; returning
    /// RESTART lets `waitForPid` re-evaluate the deadline-bounded remaining budget.
    int ret = kevent(kq, nullptr, 0, &event, 1, &remaining_timespec);

    if (ret < 0 && errno == EINTR)
        return PollPidResult::RESTART;

    if (ret <= 0)
        return PollPidResult::FAILED;

    return PollPidResult::RESTART;
}
#elif defined(OS_SUNOS)

#include <libproc.h>

namespace DB
{

/// Grab the process, wait for it to change state, and check whether it's
/// terminated.
static PollPidResult pollPid(pid_t pid, int timeout_in_ms)
{
    PollPidResult result = PollPidResult::FAILED;
    int rc, perr;
    struct ps_prochandle *hdl;

    hdl = Pgrab(pid, PGRAB_RETAIN | PGRAB_FORCE | PGRAB_NOSTOP, &perr);
    if (hdl == NULL)
    {
        if (perr == G_NOPROC)
            return PollPidResult::RESTART;
        return PollPidResult::FAILED;
    }

    rc = Pstopstatus(hdl, PCWSTOP, timeout_in_ms);
    if (rc < 0 && errno == ENOENT)
        result = PollPidResult::RESTART;
    if (rc == 0)
    {
        int state = Pstate(hdl);
        if (state == PS_DEAD || state == PS_UNDEAD)
            result = PollPidResult::RESTART;
    }

    Pfree(hdl);
    return result;
}
#else
    #error "Unsupported OS type"
#endif

bool waitForPid(pid_t pid, size_t timeout_in_seconds)
{
    int status = 0;

    Stopwatch watch;

    if (timeout_in_seconds == 0)
    {
        /// If there is no timeout before signal try to waitpid 1 time without block so we can avoid sending
        /// signal if process is already normally terminated.

        int waitpid_res = HANDLE_EINTR(waitpid(pid, &status, WNOHANG));
        bool process_terminated_normally = (waitpid_res == pid);
        return process_terminated_normally;
    }

    /// If timeout is positive, poll until the process exits or the total wall
    /// clock since function entry exceeds the limit. The remaining budget is
    /// derived from the `watch` started at function entry (never reset) so
    /// that the `/proc/<pid>` fallback — whose directory fd is always instantly
    /// ready, causing `pollPid` to return in ~0 ms — still subtracts real
    /// elapsed time and cannot busy-spin until the child exits on its own.

    const Int64 total_timeout_ms = static_cast<Int64>(timeout_in_seconds * 1000);
    while (true)
    {
        int waitpid_res = HANDLE_EINTR(waitpid(pid, &status, WNOHANG));
        bool process_terminated_normally = (waitpid_res == pid);
        if (process_terminated_normally)
            return true;

        if (waitpid_res != 0)
            return false;

        const Int64 remaining_ms = total_timeout_ms - static_cast<Int64>(watch.elapsedMilliseconds());
        if (remaining_ms <= 0)
            return false;

        PollPidResult result = pollPid(pid, static_cast<int>(remaining_ms));
        if (result == PollPidResult::FAILED)
            return false;
    }
}

}
#pragma clang diagnostic pop
