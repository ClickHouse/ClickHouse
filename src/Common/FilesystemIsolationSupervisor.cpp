#include <Common/FilesystemIsolationSupervisor.h>

#if defined(OS_LINUX)

#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/FilesystemIsolation.h>
#include <Common/IO.h>
#include <Common/SeccompFilter.h>
#include <Common/waitForPid.h>
#include <IO/ReadHelpers.h>
#include <Common/hasLinuxCapability.h>
#include <base/errnoToString.h>
#include <base/getThreadId.h>
#include <base/scope_guard.h>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/file.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace DB::ErrorCodes
{
    extern const int SYSTEM_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace DB
{

namespace
{

/// The socket to the supervisor outside of the isolation, or -1 if the filesystem is not isolated.
std::atomic<int> supervisor_socket{-1};


/// The signals that the server handles and that are sent to it through its pid file: the requests to terminate, to
/// reopen the logs (`SIGHUP`), to print the stack traces (`SIGTSTP`), and `SIGABRT` for a crash report and a core dump.
constexpr std::array relayed_signals = {SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGTSTP, SIGABRT};

/// The signals that the supervisor and the init of the PID namespace handle. They are blocked in the supervisor before
/// it creates the init, so that none is lost or acts by default before they are read from a `signalfd`.
sigset_t getHandledSignals()
{
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGCHLD);
    for (const int sig : relayed_signals)
        sigaddset(&signals, sig);
    return signals;
}

void writeToStderr(std::string_view message)
{
    writeRetry(STDERR_FILENO, message.data(), message.size());
}

/// The pid file that the supervisor owns: created and locked before the namespaces, so that a server already running
/// stops the startup, and removed when the supervisor exits. The server inside the isolation does not see it, and its
/// directory must not be writable inside (see `checkPidFile`). Still, it is created and removed relative to a
/// descriptor of its directory, without following a symbolic link, and removed only if its name still refers to it.
struct SupervisorPidFile
{
    int directory_fd = -1;
    int fd = -1;
    std::string name;
    dev_t device = 0;
    ino_t inode = 0;

    void close()
    {
        if (fd >= 0)
            closeDescriptor(fd);
        if (directory_fd >= 0)
            closeDescriptor(directory_fd);
        fd = -1;
        directory_fd = -1;
    }

    void remove() const
    {
        struct stat st{};
        if (fd >= 0 && ::fstatat(directory_fd, name.c_str(), &st, AT_SYMLINK_NOFOLLOW) == 0 && st.st_dev == device
            && st.st_ino == inode)
            ::unlinkat(directory_fd, name.c_str(), 0);
    }
};

SupervisorPidFile createPidFile(const std::string & path, pid_t pid)
{
    SupervisorPidFile result;
    const fs::path normal = fs::absolute(path).lexically_normal();
    result.name = normal.filename().string();
    /// Without following a symbolic link on the way, which `checkPidFile` has refused.
    result.directory_fd = ::open("/", O_PATH | O_DIRECTORY | O_CLOEXEC);
    if (result.directory_fd < 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot open /");
    try
    {
        for (const auto & component : normal.parent_path().relative_path())
        {
            if (component.empty() || component == ".")
                continue;
            const int next = ::openat(result.directory_fd, component.c_str(), O_PATH | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
            if (next < 0)
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot open the directory {} on the way to the pid file {}", component.string(), path);
            closeDescriptor(result.directory_fd);
            result.directory_fd = next;
        }
        /// Not blocking on a FIFO.
        result.fd = ::openat(result.directory_fd, result.name.c_str(), O_WRONLY | O_CREAT | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC, 0666);
        if (result.fd < 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot open the pid file {}", path);
        struct stat st{};
        if (::fstat(result.fd, &st) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot stat the pid file {}", path);
        if (!S_ISREG(st.st_mode) || st.st_nlink != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The pid file {} is not a regular file with a single link", path);
        result.device = st.st_dev;
        result.inode = st.st_ino;
        if (::flock(result.fd, LOCK_EX | LOCK_NB) != 0)
        {
            if (errno == EWOULDBLOCK)
                throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot lock file {}. Another server instance in same directory is already running.", path);
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot lock the pid file {}", path);
        }
        if (::ftruncate(result.fd, 0) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot truncate the pid file {}", path);
        const std::string content = std::to_string(pid);
        if (::write(result.fd, content.data(), content.size()) != static_cast<ssize_t>(content.size()))
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot write the pid file {}", path);
    }
    catch (...)
    {
        result.close();
        throw;
    }
    return result;
}

/// Closes every descriptor that is not in `keep`.
void closeOtherDescriptors(std::initializer_list<int> keep)
{
    std::vector<int> to_close;
    {
        DIR * dir = ::opendir("/proc/self/fd");
        if (!dir)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot open /proc/self/fd");
        SCOPE_EXIT({ ::closedir(dir); });
        const int own_fd = ::dirfd(dir);
        while (const dirent * entry = ::readdir(dir)) /// NOLINT(concurrency-mt-unsafe): the directory stream is not shared.
        {
            if (entry->d_name[0] == '.')
                continue;
            const int fd = std::stoi(entry->d_name);
            if (fd != own_fd && std::find(keep.begin(), keep.end(), fd) == keep.end())
                to_close.push_back(fd);
        }
    }
    for (const int fd : to_close)
        closeDescriptor(fd);
}

/// The messages of the server to the supervisor: the notifications for systemd, of which only `READY=1` is relayed (a
/// `MAINPID=` of the watchdog would name a PID inside the namespace, and the supervisor is the main process anyway), and
/// the registration of the process that serves, which gets the signals sent through the pid file. The supervisor answers
/// each with a number after a prefix: the error of the relay, and the PID of the sender outside of the namespace.
constexpr std::string_view serving_registration = "SERVING";
constexpr std::string_view serving_reply_prefix = "SERVING=";
constexpr std::string_view notify_reply_prefix = "NOTIFY=";

struct NotifySocket
{
    int fd = -1;
    sockaddr_un address{};
    socklen_t address_size = 0;
};

/// The socket of systemd from `NOTIFY_SOCKET`, as `systemdNotify` reads it, with a name starting with `@` for an
/// abstract socket.
NotifySocket openNotifySocket()
{
    NotifySocket result;
    const char * path = std::getenv("NOTIFY_SOCKET"); // NOLINT(concurrency-mt-unsafe)
    if (!path || (*path != '/' && *path != '@'))
        return result;
    const size_t length = std::strlen(path);
    if (length < 2 || length >= sizeof(result.address.sun_path))
        throw Exception(ErrorCodes::SYSTEM_ERROR, "Wrong UNIX path \"{}\" in NOTIFY_SOCKET env var", path);
    result.address.sun_family = AF_UNIX;
    std::memcpy(result.address.sun_path, path, length);
    result.address_size = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + length);
    if (*path == '@')
        result.address.sun_path[0] = '\0';
    else
        ++result.address_size; /// With the terminating zero.
    result.fd = ::socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (result.fd < 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Can't create UNIX socket for systemd notify");
    return result;
}

/// Receives one message of the server, at most `max_size` bytes, with the PID of its sender outside of the namespace,
/// and the descriptor passed with it, or -1.
/// Returns the size, 0 for nothing or a message that is dropped, -1 when nothing can be received anymore.
ssize_t receiveFromServer(int fd, char * buf, size_t max_size, pid_t & sender, int & passed_fd)
{
    iovec iov{.iov_base = buf, .iov_len = max_size};
    alignas(cmsghdr) char control[CMSG_SPACE(sizeof(ucred)) + CMSG_SPACE(sizeof(int))];
    sender = 0;
    passed_fd = -1;
    msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);
    const ssize_t size = ::recvmsg(fd, &msg, MSG_DONTWAIT | MSG_CMSG_CLOEXEC);
    if (size < 0)
        return (errno == EAGAIN || errno == EINTR) ? 0 : -1;
    /// The `CMSG_NXTHDR` of musl casts and compares in ways that `-Weverything` refuses.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wcast-align"
#pragma clang diagnostic ignored "-Wsign-compare"
    for (cmsghdr * cmsg = CMSG_FIRSTHDR(&msg); cmsg; cmsg = CMSG_NXTHDR(&msg, cmsg))
    {
        if (cmsg->cmsg_level != SOL_SOCKET)
            continue;
        if (cmsg->cmsg_type == SCM_CREDENTIALS && cmsg->cmsg_len == CMSG_LEN(sizeof(ucred)))
        {
            ucred credentials{};
            std::memcpy(&credentials, CMSG_DATA(cmsg), sizeof(credentials));
            sender = credentials.pid;
        }
        else if (cmsg->cmsg_type == SCM_RIGHTS && cmsg->cmsg_len == CMSG_LEN(sizeof(int)))
            std::memcpy(&passed_fd, CMSG_DATA(cmsg), sizeof(int));
    }
#pragma clang diagnostic pop
    /// The space is for one descriptor: with more, the kernel sets `MSG_CTRUNC` and closes the others.
    if (msg.msg_flags & (MSG_TRUNC | MSG_CTRUNC))
    {
        if (passed_fd >= 0)
            closeDescriptor(passed_fd);
        passed_fd = -1;
        return 0;
    }
    return size;
}

/// Exits with the status of the server: with the same code, or by the same signal, without a core dump of the
/// supervisor, so that a service manager sees the same as without the isolation.
[[noreturn]] void exitWithStatus(int status)
{
    if (WIFEXITED(status))
        ::_exit(WEXITSTATUS(status));
    if (WIFSIGNALED(status))
    {
        const int sig = WTERMSIG(status);
        rlimit no_core{0, 0};
        ::setrlimit(RLIMIT_CORE, &no_core);
        /// The action of `SIGKILL` is always the default one, and cannot be set.
        if (sig != SIGKILL && ::signal(sig, SIG_DFL) == SIG_ERR)
            ::_exit(128 + sig);
        sigset_t signals;
        sigemptyset(&signals);
        sigaddset(&signals, sig);
        ::pthread_sigmask(SIG_UNBLOCK, &signals, nullptr);
        ::kill(::getpid(), sig);
        /// A signal that does not terminate a process.
        ::_exit(128 + sig);
    }
    ::_exit(1);
}

/// The loop of the supervisor, the original process, which stays outside of the namespaces. `init_pid` is the init of
/// the PID namespace, `server_socket` the end of the socket pair to the server, `exit_status_fd` the pipe on which the
/// init writes the status of the server, `signal_fd` the `signalfd` of `getHandledSignals`.
[[noreturn]] void runSupervisor(pid_t init_pid, int server_socket, int exit_status_fd, int signal_fd, const SupervisorPidFile & pid_file)
{
    ::prctl(PR_SET_NAME, "clickhouse-isol", 0, 0, 0);
    /// A signal sent to the supervisor goes straight to the process that serves, as it went when the pid file had its
    /// PID. That process registers itself where the pid file used to be created, and a signal that arrives before is
    /// held until then. It is held by a pidfd, so a reused PID is never signalled, and its PID is the sender that the
    /// kernel attests. The processes inside are in a process group of their own, so a signal to the process group of
    /// the supervisor, such as `SIGINT` on Ctrl+C, reaches the server only from here, once: the server takes a second
    /// termination signal as an immediate termination. Not, with `CLICKHOUSE_WATCHDOG_NO_FORWARD=1`, a signal from the
    /// parent, the service manager, which sends the signals to every process of the service, as systemd does.
    const char * no_forward = std::getenv("CLICKHOUSE_WATCHDOG_NO_FORWARD"); // NOLINT(concurrency-mt-unsafe)
    const bool forwarding_disabled = no_forward && std::string_view(no_forward) == "1";
    const pid_t parent_pid = ::getppid();
    int serving_pidfd = -1;
    uint64_t pending_signals = 0;

    NotifySocket notify;
    try
    {
        notify = openNotifySocket();
        closeOtherDescriptors({STDIN_FILENO, STDOUT_FILENO, STDERR_FILENO, server_socket, exit_status_fd, pid_file.directory_fd, pid_file.fd, signal_fd, notify.fd});
        /// It has nothing to do that needs a capability.
        restrictCapabilities(0);
        installSupervisorSeccompFilter();
    }
    catch (...)
    {
        writeToStderr(fmt::format("Cannot isolate the filesystem: cannot set up the supervisor: {}\n", getCurrentExceptionMessage(false)));
        ::kill(init_pid, SIGKILL);
        pid_file.remove();
        ::_exit(1);
    }

    auto reply = [&](std::string_view prefix, int value)
    {
        char text[64];
        auto * const end = fmt::format_to_n(text, sizeof(text), "{}{}", prefix, value).out;
        ::send(server_socket, text, end - text, MSG_DONTWAIT | MSG_NOSIGNAL);
    };

    /// Held until the next registration if no process serves, as after the server exited and before the watchdog
    /// restarts it (`CLICKHOUSE_WATCHDOG_RESTART=1`).
    auto deliver = [&](int sig)
    {
        if (serving_pidfd >= 0 && syscall_pidfd_send_signal(serving_pidfd, sig) == 0)
            return;
        if (serving_pidfd < 0 || errno == ESRCH)
        {
            if (serving_pidfd >= 0)
                closeDescriptor(serving_pidfd);
            serving_pidfd = -1;
            pending_signals |= uint64_t(1) << sig;
            return;
        }
        char text[128];
        auto * const end = fmt::format_to_n(text, sizeof(text), "Cannot send the signal {} to the server: errno {}\n", sig, errno).out;
        writeToStderr(std::string_view(text, end - text));
    };

    char buf[512];
    pollfd fds[2] = {{.fd = signal_fd, .events = POLLIN, .revents = 0}, {.fd = server_socket, .events = POLLIN, .revents = 0}};
    while (true)
    {
        if (::poll(fds, 2, -1) < 0)
        {
            if (errno == EINTR)
                continue;
            ::kill(init_pid, SIGKILL);
            exitWithStatus(1 << 8);
        }

        if (fds[1].revents)
        {
            pid_t sender = 0;
            int passed_fd = -1;
            const ssize_t size = receiveFromServer(server_socket, buf, sizeof(buf), sender, passed_fd);
            /// Only the registration passes a descriptor.
            if (passed_fd >= 0 && std::string_view(buf, size > 0 ? size : 0) != serving_registration)
            {
                closeDescriptor(passed_fd);
                passed_fd = -1;
            }
            /// Every process of the server has closed its end: stop polling it.
            if (size < 0 || (size == 0 && (fds[1].revents & (POLLHUP | POLLERR))))
                fds[1].fd = -1;
            std::string_view message(buf, size > 0 ? size : 0);
            if (message.ends_with('\n'))
                message.remove_suffix(1);
            if (message.empty() || sender <= 0)
            {
                /// Nothing, or not from a process.
            }
            else if (message == serving_registration)
            {
                /// The process passes a pidfd of itself: a PID looked up afterwards could be of another process by then. A
                /// process inside can only make a pidfd of a process inside, and a signal 0 checks that it is one.
                if (passed_fd < 0 || syscall_pidfd_send_signal(passed_fd, 0) != 0)
                {
                    if (passed_fd >= 0)
                        closeDescriptor(passed_fd);
                    reply(serving_reply_prefix, -1);
                }
                else
                {
                    reply(serving_reply_prefix, sender);
                    if (serving_pidfd >= 0)
                        closeDescriptor(serving_pidfd);
                    serving_pidfd = passed_fd;
                    const uint64_t to_deliver = pending_signals;
                    pending_signals = 0;
                    for (const int sig : relayed_signals)
                    {
                        if (to_deliver & (uint64_t(1) << sig))
                            deliver(sig);
                    }
                }
            }
            else if (message.starts_with("MAINPID="))
                reply(notify_reply_prefix, 0);
            else if (message != "READY=1")
                reply(notify_reply_prefix, EINVAL);
            else if (notify.fd < 0)
                reply(notify_reply_prefix, 0); /// Not started by systemd.
            else
            {
                buf[message.size()] = '\n';
                const ssize_t res = ::sendto(notify.fd, buf, message.size() + 1, MSG_NOSIGNAL, reinterpret_cast<const sockaddr *>(&notify.address), notify.address_size);
                reply(notify_reply_prefix, res < 0 ? errno : 0);
            }
        }

        if (fds[0].revents & POLLIN)
        {
            signalfd_siginfo info{};
            if (::read(signal_fd, &info, sizeof(info)) != sizeof(info))
                continue;
            const int sig = static_cast<int>(info.ssi_signo);
            if (sig == SIGCHLD)
            {
                int status = 0;
                const pid_t pid = ::waitpid(init_pid, &status, WNOHANG);
                if (pid != init_pid || !(WIFEXITED(status) || WIFSIGNALED(status)))
                    continue;
                /// The status of the server, if the init could report it.
                int server_status = 0;
                if (WIFEXITED(status) && WEXITSTATUS(status) == 0
                    && ::read(exit_status_fd, &server_status, sizeof(server_status)) == sizeof(server_status))
                    status = server_status;
                pid_file.remove();
                exitWithStatus(status);
            }
            if (forwarding_disabled && static_cast<pid_t>(info.ssi_pid) == parent_pid)
                continue;
            deliver(sig);
        }
    }
}

/// The init of the PID namespace, the first process in it. It reaps the orphans and exits with the status of the
/// server; the signals for the server come to it from the supervisor directly. If the supervisor dies, so does the
/// init, and with it every process of the namespace. Returns in the server, its only child; does not return in the
/// init itself.
void runInit(int server_socket, int exit_status_fd, int signal_fd, const sigset_t & original_signal_mask)
{
    pid_t server_pid = -1;
    try
    {
        if (::prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot set PR_SET_PDEATHSIG");
        /// Out of the process group of the supervisor, which relays the signals sent to the group.
        if (::setpgid(0, 0) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create a process group");
        /// The supervisor could have died before that: then its end of the socket pair is closed.
        pollfd fd{.fd = server_socket, .events = 0, .revents = 0};
        if (::poll(&fd, 1, 0) > 0 && (fd.revents & POLLHUP))
            ::_exit(1);

        /// `pivot_root` moves the root and the working directory of every process of the mount namespace that are at the
        /// original root to the new one, including the ones of the init, which keeps nothing of the original filesystem.
        if (::chdir("/") != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot change the directory to /");

        server_pid = ::fork();
        if (server_pid < 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot fork the server");
        if (server_pid == 0)
        {
            closeDescriptor(exit_status_fd);
            closeDescriptor(signal_fd);
            updateCurrentThreadIdAfterFork();
            if (const int error = ::pthread_sigmask(SIG_SETMASK, &original_signal_mask, nullptr); error != 0)
            {
                errno = error;
                throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot restore the signal mask");
            }
            /// The notifications go through the supervisor.
            ::unsetenv("NOTIFY_SOCKET"); // NOLINT(concurrency-mt-unsafe)
            supervisor_socket.store(server_socket, std::memory_order_relaxed);
            return;
        }

        ::prctl(PR_SET_NAME, "clickhouse-init", 0, 0, 0);
        closeDescriptor(server_socket);
        /// The init only waits and signals the processes of its own user. It is not dumpable, so the server cannot
        /// ptrace it or reach anything through its `/proc/1` entries.
        restrictCapabilities(0);
        if (::prctl(PR_SET_DUMPABLE, 0, 0, 0, 0) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot set PR_SET_DUMPABLE");
    }
    catch (...)
    {
        writeToStderr(fmt::format("Cannot isolate the filesystem: {}\n", getCurrentExceptionMessage(false)));
        if (server_pid > 0)
            ::kill(server_pid, SIGKILL);
        ::_exit(1);
    }

    while (true)
    {
        signalfd_siginfo info{};
        if (::read(signal_fd, &info, sizeof(info)) != sizeof(info))
            continue;
        if (static_cast<int>(info.ssi_signo) == SIGCHLD)
        {
            /// The init of a PID namespace reaps every orphan in it.
            int status = 0;
            pid_t pid = 0;
            while ((pid = ::waitpid(-1, &status, WNOHANG)) > 0)
            {
                if (pid == server_pid && (WIFEXITED(status) || WIFSIGNALED(status)))
                {
                    [[maybe_unused]] const ssize_t res = ::write(exit_status_fd, &status, sizeof(status));
                    ::_exit(0);
                }
            }
        }
    }
}

}

void closeDescriptor(int fd)
{
    [[maybe_unused]] const int err = ::close(fd);
    chassert(!err || errno == EINTR);
}

/// Leaves only the capabilities in `keep`, and sets `PR_SET_NO_NEW_PRIVS`. The bounding set matters only to a program
/// the process executes, and no program can gain a capability anyway: every mount is `nosuid` as well.
void restrictCapabilities(uint64_t keep)
{
    restrictLinuxCapabilities(keep);
    if (::prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot set PR_SET_NO_NEW_PRIVS");
}

/// Creates a child process in new PID and mount namespaces, as `fork` does: it returns twice. The current process
/// stays in its namespaces. A raw `clone`, because `fork` has no flags for the namespaces. The child has a single
/// thread, like the current process.
pid_t cloneIntoPidAndMountNamespaces()
{
    const unsigned long flags = CLONE_NEWPID | CLONE_NEWNS | SIGCHLD; // NOLINT(google-runtime-int)
#if defined(__s390x__)
    /// The stack goes first on s390x.
    return static_cast<pid_t>(::syscall(SYS_clone, nullptr, flags, nullptr, nullptr, nullptr));
#else
    return static_cast<pid_t>(::syscall(SYS_clone, flags, nullptr, nullptr, nullptr, nullptr));
#endif
}

std::string getSupervisorUnavailableReason()
{
    /// The supervisor holds the process that serves by a pidfd.
    const int pidfd = syscall_pidfd_open(::getpid());
    if (pidfd < 0)
        return fmt::format("cannot create a pidfd, which needs Linux 5.3 or newer: {}", errnoToString(errno));
    closeDescriptor(pidfd);
    return {};
}

/// Without a user namespace: the current process stays outside as the supervisor, creates the init of new PID and
/// mount namespaces, which forks the server, and never returns. Returns in the server only.
void startSupervisedServer(const std::string & pid_file)
{
    SupervisorPidFile supervisor_pid_file = pid_file.empty() ? SupervisorPidFile{} : createPidFile(pid_file, ::getpid());
    int server_sockets[2] = {-1, -1};
    int exit_status_pipe[2] = {-1, -1};
    int signal_fd = -1;
    sigset_t original_signal_mask;
    bool signals_blocked = false;
    pid_t init_pid = -1;
    try
    {
        if (::socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, server_sockets) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create a socket pair");
        const int pass_credentials = 1;
        if (::setsockopt(server_sockets[0], SOL_SOCKET, SO_PASSCRED, &pass_credentials, sizeof(pass_credentials)) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot set SO_PASSCRED");
        /// How long the server waits for an answer of the supervisor.
        const timeval timeout{.tv_sec = 10, .tv_usec = 0};
        if (::setsockopt(server_sockets[1], SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot set SO_RCVTIMEO");
        if (::pipe2(exit_status_pipe, O_CLOEXEC) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create a pipe");
        /// The supervisor and the init both read the signals from the same `signalfd`: each gets its own.
        const sigset_t handled_signals = getHandledSignals();
        if (const int error = ::pthread_sigmask(SIG_BLOCK, &handled_signals, &original_signal_mask); error != 0)
        {
            errno = error;
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot block the signals");
        }
        signals_blocked = true;
        signal_fd = ::signalfd(-1, &handled_signals, SFD_CLOEXEC);
        if (signal_fd < 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create a signalfd");
        init_pid = cloneIntoPidAndMountNamespaces();
        if (init_pid < 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create a process in new namespaces");
    }
    catch (...)
    {
        if (signals_blocked)
            ::pthread_sigmask(SIG_SETMASK, &original_signal_mask, nullptr);
        for (const int fd : {server_sockets[0], server_sockets[1], exit_status_pipe[0], exit_status_pipe[1], signal_fd})
        {
            if (fd >= 0)
                closeDescriptor(fd);
        }
        supervisor_pid_file.remove();
        supervisor_pid_file.close();
        throw;
    }

    if (init_pid > 0)
    {
        closeDescriptor(server_sockets[1]);
        closeDescriptor(exit_status_pipe[1]);
        runSupervisor(init_pid, server_sockets[0], exit_status_pipe[0], signal_fd, supervisor_pid_file);
    }

    closeDescriptor(server_sockets[0]);
    closeDescriptor(exit_status_pipe[0]);
    /// In the init, which has a single thread and runs nothing else before: the directory of the pid file is a way
    /// out of the isolation.
    supervisor_pid_file.close();
    runInit(server_sockets[1], exit_status_pipe[1], signal_fd, original_signal_mask);
}

namespace
{

std::mutex supervisor_request_mutex;

/// Sends `request` to the supervisor and returns the number in its answer after `reply_prefix`, or nothing if the
/// supervisor does not answer within the timeout of the socket. An answer to an earlier request that timed out is
/// skipped.
std::optional<int> requestFromSupervisor(int fd, std::string_view request, std::string_view reply_prefix, int passed_fd = -1)
{
    std::lock_guard lock(supervisor_request_mutex);
    iovec iov{.iov_base = const_cast<char *>(request.data()), .iov_len = request.size()};
    alignas(cmsghdr) char control[CMSG_SPACE(sizeof(int))]{};
    msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    if (passed_fd >= 0)
    {
        msg.msg_control = control;
        msg.msg_controllen = sizeof(control);
        cmsghdr * cmsg = CMSG_FIRSTHDR(&msg);
        cmsg->cmsg_level = SOL_SOCKET;
        cmsg->cmsg_type = SCM_RIGHTS;
        cmsg->cmsg_len = CMSG_LEN(sizeof(int));
        std::memcpy(CMSG_DATA(cmsg), &passed_fd, sizeof(int));
    }
    if (::sendmsg(fd, &msg, MSG_NOSIGNAL) < 0)
        return std::nullopt;
    while (true)
    {
        char buf[64];
        /// Bounded by `SO_RCVTIMEO`; the lock keeps the answers of concurrent requests apart.
        const ssize_t size = ::recv(fd, buf, sizeof(buf), 0); // NOLINT(clang-analyzer-unix.BlockInCriticalSection)
        if (size < 0 && errno == EINTR)
            continue;
        if (size <= 0)
            return std::nullopt;
        const std::string_view reply(buf, size);
        int value = 0;
        if (reply.starts_with(reply_prefix) && tryParse(value, reply.substr(reply_prefix.size())))
            return value;
    }
}

}

bool notifyFilesystemIsolationSupervisor(std::string_view message)
{
    const int fd = supervisor_socket.load(std::memory_order_relaxed);
    if (fd < 0)
        return false;
    const auto error = requestFromSupervisor(fd, message, notify_reply_prefix);
    if (!error)
        throw Exception(ErrorCodes::SYSTEM_ERROR, "Failed to notify systemd: the supervisor of the filesystem isolation did not answer");
    if (*error != 0)
    {
        errno = *error;
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Failed to notify systemd, sendto returned error");
    }
    return true;
}

void registerFilesystemIsolationServingProcess()
{
    const int fd = supervisor_socket.load(std::memory_order_relaxed);
    if (fd < 0)
        return;
    const int pidfd = syscall_pidfd_open(::getpid());
    if (pidfd < 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create a pidfd to register with the supervisor of the filesystem isolation");
    SCOPE_EXIT(closeDescriptor(pidfd));
    const auto host_pid = requestFromSupervisor(fd, serving_registration, serving_reply_prefix, pidfd);
    if (!host_pid || *host_pid <= 0)
        throw Exception(ErrorCodes::SYSTEM_ERROR, "The supervisor of the filesystem isolation did not accept the registration");
    setFilesystemIsolationServingHostPid(*host_pid);
}

}

#endif
