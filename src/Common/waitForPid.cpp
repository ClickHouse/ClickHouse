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
    // pidfd_open cannot be interrupted, no EINTR handling
    return syscall(SYS_pidfd_open, pid, 0);
}

static int dir_pidfd_open(pid_t pid)
{
    std::string path = "/proc/" + std::to_string(pid);
    return HANDLE_EINTR(open(path.c_str(), O_DIRECTORY));
}

static bool supportsPidFdOpen()
{
    VersionNumber pidfd_open_minimal_version(5, 3, 0);
    VersionNumber linux_version(Poco::Environment::osVersion());
    return linux_version >= pidfd_open_minimal_version;
}

static int pidFdOpen(pid_t pid)
{
    // use pidfd_open or just plain old /proc/[pid] open for Linux
    if (supportsPidFdOpen())
    {
        return syscall_pidfd_open(pid);
    }
    else
    {
        return dir_pidfd_open(pid);
    }
}

static int pollPid(pid_t pid, int timeout_in_ms)
{
    struct pollfd pollfd;

    int pid_fd = pidFdOpen(pid);
    if (pid_fd == -1)
    {
        return false;
    }
    pollfd.fd = pid_fd;
    pollfd.events = POLLIN;
    int ready = poll(&pollfd, 1, timeout_in_ms);
    int save_errno = errno;
    close(pid_fd);
    errno = save_errno;
    return ready;
}
#elif defined(OS_DARWIN) || defined(OS_FREEBSD)

#include <sys/event.h>
#include <err.h>

namespace DB
{

static int pollPid(pid_t pid, int timeout_in_ms)
{
    int status = 0;
    int kq = HANDLE_EINTR(kqueue());
    if (kq == -1)
    {
        return false;
    }
    struct kevent change = {.ident = NULL};
    EV_SET(&change, pid, EVFILT_PROC, EV_ADD, NOTE_EXIT, 0, NULL);
    int result = HANDLE_EINTR(kevent(kq, &change, 1, NULL, 0, NULL));
    if (result == -1)
    {
        if (errno != ESRCH)
        {
            return false;
        }
        // check if pid already died while we called kevent()
        if (waitpid(pid, &status, WNOHANG) == pid)
        {
            return true;
        }
        return false;
    }

    struct kevent event = {.ident = NULL};
    struct timespec remaining_timespec = {.tv_sec = timeout_in_ms / 1000, .tv_nsec = (timeout_in_ms % 1000) * 1000000};
    int ready = kevent(kq, nullptr, 0, &event, 1, &remaining_timespec);
    int save_errno = errno;
    close(kq);
    errno = save_errno;
    return ready;
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

        int waitpid_res = waitpid(pid, &status, WNOHANG);
        bool process_terminated_normally = (waitpid_res == pid);
        return process_terminated_normally;
    }

    /// If timeout is positive try waitpid without block in loop until
    /// process is normally terminated or waitpid return error

    int timeout_in_ms = timeout_in_seconds * 1000;
    while (timeout_in_ms > 0)
    {
        int waitpid_res = waitpid(pid, &status, WNOHANG);
        bool process_terminated_normally = (waitpid_res == pid);
        if (process_terminated_normally)
        {
            return true;
        }
        else if (waitpid_res == 0)
        {
            watch.restart();
            int ready = pollPid(pid, timeout_in_ms);
            if (ready <= 0)
            {
                if (errno == EINTR || errno == EAGAIN)
                {
                    timeout_in_ms -= watch.elapsedMilliseconds();
                }
                else
                {
                    return false;
                }
            }
            continue;
        }
        else if (waitpid_res == -1 && errno != EINTR)
        {
            return false;
        }
    }
    return false;
}

}
#pragma GCC diagnostic pop
