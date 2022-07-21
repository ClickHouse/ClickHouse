#include <Common/waitForPid.h>
#include <Common/VersionNumber.h>
#include <Poco/Environment.h>
#include <Common/Stopwatch.h>

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wgnu-statement-expression"
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
    #elif defined(__ppc64__)
        #define SYS_pidfd_open 434
    #elif defined(__riscv)
        #define SYS_pidfd_open 434
    #else
        #error "Unsupported architecture"
    #endif
#else
    #define SYS_pidfd_open __NR_pidfd_open
#endif

namespace DB
{

static int syscall_pidfd_open(pid_t pid)
{
    return syscall(SYS_pidfd_open, pid, 0);
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

    struct pollfd pollfd;
    pollfd.fd = pid_fd;
    pollfd.events = POLLIN;

    int ready = HANDLE_EINTR(poll(&pollfd, 1, timeout_in_ms));

    if (ready <= 0)
        return PollPidResult::FAILED;

    close(pid_fd);

    return PollPidResult::RESTART;
}

#elif defined(OS_DARWIN) || defined(OS_FREEBSD)

#include <sys/event.h>
#include <err.h>

namespace DB
{

static PollPidResult pollPid(pid_t pid, int timeout_in_ms)
{
    int kq = kqueue();
    if (kq == -1)
        return PollPidResult::FAILED;

    struct kevent change = {.ident = NULL};
    EV_SET(&change, pid, EVFILT_PROC, EV_ADD, NOTE_EXIT, 0, NULL);

    int event_add_result = HANDLE_EINTR(kevent(kq, &change, 1, NULL, 0, NULL));
    if (event_add_result == -1)
    {
        if (errno == ESRCH)
            return PollPidResult::RESTART;

        return PollPidResult::FAILED;
    }

    struct kevent event = {.ident = NULL};
    struct timespec remaining_timespec = {.tv_sec = timeout_in_ms / 1000, .tv_nsec = (timeout_in_ms % 1000) * 1000000};
    int ready = HANDLE_EINTR(kevent(kq, nullptr, 0, &event, 1, &remaining_timespec));
    PollPidResult result = ready < 0 ? PollPidResult::FAILED : PollPidResult::RESTART;

    close(kq);

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

    /// If timeout is positive try waitpid without block in loop until
    /// process is normally terminated or waitpid return error

    int timeout_in_ms = timeout_in_seconds * 1000;
    while (timeout_in_ms > 0)
    {
        int waitpid_res = HANDLE_EINTR(waitpid(pid, &status, WNOHANG));
        bool process_terminated_normally = (waitpid_res == pid);
        if (process_terminated_normally)
            return true;

        if (waitpid_res != 0)
            return false;

        watch.restart();

        PollPidResult result = pollPid(pid, timeout_in_ms);

        if (result == PollPidResult::FAILED)
            return false;

        timeout_in_ms -= watch.elapsedMilliseconds();
    }

    return false;
}

}
#pragma GCC diagnostic pop
