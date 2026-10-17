#include <Common/logger_useful.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/ShellCommandsHolder.h>
#include <Common/UDFProcessRegistry.h>
#include <Common/setThreadName.h>

#include <Common/waitForPid.h>
#include <base/scope_guard.h>
#include <base/sleep.h>

#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_POLL;
}

namespace
{

void closePidFd(int pidfd)
{
    if (pidfd < 0)
        return;
    [[maybe_unused]] int err = ::close(pidfd);
    chassert(!err || errno == EINTR);
}

}

ShellCommandsHolder & ShellCommandsHolder::instance()
{
    static ShellCommandsHolder instance;
    return instance;
}

ShellCommandsHolder::ShellCommandsHolder()
{
    /// The reaper removes the children it reaps from `UDFProcessRegistry`, and runs until this
    /// holder's destructor stops it. Both are function-local statics, destroyed in the reverse order
    /// of their construction: the registry is created here, before this holder is complete, so that
    /// it is destroyed after it, whichever of the two the process asked for first.
    UDFProcessRegistry::instance();
}

/// A command taken out of `shell_commands` - or one that was never put there - is destroyed only
/// once `mutex` is released, by both functions below: `~ShellCommand` may hand a signalled child
/// over to the reaper (`addSignalledChild`), which takes that very mutex.

void ShellCommandsHolder::removeCommand(pid_t pid)
{
    std::unique_ptr<ShellCommand> removed;
    {
        std::lock_guard lock(mutex);
        auto it = shell_commands.find(pid);
        if (it != shell_commands.end())
        {
            removed = std::move(it->second);
            shell_commands.erase(it);
        }
    }
    LOG_TRACE(log, "Try to erase command with the pid {}, is_erased: {}", pid, removed != nullptr);
}

void ShellCommandsHolder::addCommand(std::unique_ptr<ShellCommand> command)
{
    /// What is left to destroy outside the lock: the command itself if it is not inserted, or
    /// the one it replaces.
    std::unique_ptr<ShellCommand> discarded;
    std::lock_guard lock(mutex);
    pid_t command_pid = command->getPid();
    if (command->waitIfProccesTerminated())
    {
        LOG_TRACE(log, "Pid {} already finished. Do not insert it.", command_pid);
        discarded = std::move(command);
        return;
    }

    auto [iterator, is_inserted] = shell_commands.try_emplace(command_pid, std::move(command));
    if (is_inserted)
    {
        LOG_TRACE(log, "Inserted the command with pid {}", command_pid);
        return;
    }

    if (iterator->second->isWaitCalled())
    {
        discarded = std::exchange(iterator->second, std::move(command));
        LOG_TRACE(log, "Replaced the command with pid {}", command_pid);
        return;
    }
    /// We got two active ShellCommand with the same pid.
    /// Probably it is a bug, will try to replace the old shell command with a new one.
    chassert(false);

    LOG_WARNING(log, "The PID already presented in active shell commands, will try to replace with a new one.");

    iterator->second->setDoNotTerminate();
    discarded = std::exchange(iterator->second, std::move(command));
}

void ShellCommandsHolder::addSignalledChild(pid_t pid, std::optional<UInt64> udf_registry_generation)
{
    /// Opened once for the child's whole stay here, so that a round of the reaper costs no
    /// syscall per child; outside the lock. Without one to be had (an older kernel, a seccomp
    /// profile that refuses it) the reaper looks at the child in steps instead.
    int pidfd = -1;
#if defined(OS_LINUX)
    pidfd = openPidFdForWaiting(pid);
#endif
    /// Closed here unless the entry takes it - after the lock is released.
    SCOPE_EXIT({ closePidFd(pidfd); });

    std::lock_guard lock(mutex);
    /// A pid that is here already was reaped by somebody other than the reaper, and reused: the
    /// child it stands for now is this one. Its `pidfd` stands for the old one, which has exited,
    /// and may be in the reaper's `poll` right now: it is closed by the reaper, after that `poll`.
    /// Allocations, which can fail, and fail before anything is taken - as does a reaper thread
    /// that cannot be started, below.
    auto it = signalled_children.find(pid);
    if (it != signalled_children.end())
    {
        released_pidfds.push_back(it->second.pidfd);
        it->second = {.udf_registry_generation = udf_registry_generation, .pidfd = std::exchange(pidfd, -1)};
    }
    else
    {
        it = signalled_children.emplace(pid, SignalledChild{.udf_registry_generation = udf_registry_generation}).first;
        it->second.pidfd = std::exchange(pidfd, -1);
    }

    /// One that is gone already needs no thread to wait for it - nor does any child once the
    /// holder is being destroyed: the process is exiting, and the reaper has been stopped. Only
    /// this child is looked at here, under the mutex that every caller shares: the others are the
    /// reaper's, which is woken for them below anyway.
    if (reapSignalledChild(it))
        return;
    if (reaper_shutdown)
        return;

    if (!reaper)
    {
        try
        {
            /// Opened once, and kept if starting the thread fails, so that the next call can try again.
            if (reaper_wakeup.fds_rw[0] < 0)
            {
                reaper_wakeup.open();
                reaper_wakeup.setNonBlockingReadWrite();
            }
            reaper.emplace([this] { runReaper(); });
        }
        catch (...)
        {
            /// Without the thread nobody would reap this child before the next hand-over, which
            /// may never come: an entry taken here would keep it a zombie, and its entry in
            /// `UDFProcessRegistry` live, for as long as the server runs, while the caller
            /// believes it handed the child over. So the hand-over fails instead: the entry goes,
            /// the exception reaches the caller, and the child stays the caller's to wait for or
            /// to warn about (`ShellCommand::handOverChildToReaper`). The entry this one replaced,
            /// if any, stood for a child that somebody else had reaped, so nothing is lost with it.
            /// By pid, not by `it`: `reapSignalledChild` moved that past the entry. Its `pidfd` was
            /// never in a `poll` - there is no reaper - so it is closed at once.
            auto node = signalled_children.extract(pid);
            closePidFd(node.mapped().pidfd);
            throw;
        }
        /// The thread starts with this child in front of it.
        return;
    }

    /// The child is this holder's now. The reaper is asleep on the children it knew of, so it is
    /// woken to wait for this one too.
    wakeReaper();
}

void ShellCommandsHolder::wakeReaper() noexcept
{
    char byte = 0;
    [[maybe_unused]] ssize_t res = ::write(reaper_wakeup.fds_rw[1], &byte, 1);
}

ShellCommandsHolder::~ShellCommandsHolder()
{
    /// The commands go first, while the rest of this holder is whole: `~ShellCommand` may hand a
    /// child over to the reaper (`addSignalledChild`), and members are destroyed in the reverse
    /// order of their declaration - the reaper's state before `shell_commands`. Outside the lock,
    /// for the same reason as in `removeCommand`.
    ShellCommands commands;
    {
        std::lock_guard lock(mutex);
        commands.swap(shell_commands);
    }
    commands.clear();

    std::optional<std::thread> thread;
    {
        std::lock_guard lock(mutex);
        reaper_shutdown = true;
        if (reaper)
            wakeReaper();
        thread = std::move(reaper);
    }
    if (thread)
        thread->join();

    /// Nothing polls them any more. The children themselves are left as they are: the process is
    /// exiting.
    std::lock_guard lock(mutex);
    closeReleasedPidFds();
    for (auto & [_, child] : signalled_children)
        closePidFd(std::exchange(child.pidfd, -1));
}

bool ShellCommandsHolder::reapSignalledChild(SignalledChildren::iterator & it)
{
    int res = 0;
    do
        res = ::waitpid(it->first, nullptr, WNOHANG);
    while (res < 0 && errno == EINTR);

    /// Still running, or (an error other than `ECHILD`) not known to be gone: tried again later.
    if (res == 0 || (res < 0 && errno != ECHILD))
    {
        ++it;
        return false;
    }

    LOG_TRACE(log, "Reaped the signalled command with the pid {}", it->first);
    if (it->second.udf_registry_generation)
        UDFProcessRegistry::instance().removeIfGenerationMatches(it->first, *it->second.udf_registry_generation);
    /// Never in a `poll` that is still going on: this runs either on the reaper, between its polls,
    /// or for an entry that `addSignalledChild` has just made.
    closePidFd(it->second.pidfd);
    it = signalled_children.erase(it);
    return true;
}

void ShellCommandsHolder::reapSignalledChildren()
{
    for (auto it = signalled_children.begin(); it != signalled_children.end();)
        reapSignalledChild(it);
}

void ShellCommandsHolder::closeReleasedPidFds()
{
    for (int pidfd : released_pidfds)
        closePidFd(pidfd);
    released_pidfds.clear();
}

void ShellCommandsHolder::runReaper()
{
    setThreadName(ThreadName::SHELL_CMD_REAPER);

    /// On a kernel without `pidfd_open` there is nothing to sleep on until a child exits: such a
    /// child is looked at again in steps of this. The same step paces the rounds after one that
    /// failed.
    static constexpr int step_without_pidfd_ms = 100;

    /// What a round polls: the wake-up pipe, then the `pidfd` of every child that has one, and the
    /// pid each of those stands for. Kept from one round to the next, so that their memory is not
    /// allocated anew each time.
    std::vector<pollfd> fds;
    std::vector<pid_t> fd_pids;

    /// Whether the next round has to look at every child: the first one, and one after a round that
    /// failed or whose `poll` was interrupted, when what `poll` reported is not known.
    bool reap_all = true;

    while (true)
    {
        try
        {
            int timeout_ms = -1;
            {
                std::lock_guard lock(mutex);
                if (reaper_shutdown)
                    return;

                /// The last `poll` is over: the descriptors released during it are not in use any more.
                closeReleasedPidFds();

                if (reap_all)
                {
                    reapSignalledChildren();
                }
                else
                {
                    /// Only the children whose `pidfd` the last `poll` found readable - they have
                    /// exited - and those without one, which nothing reports on. A child added since
                    /// was looked at by `addSignalledChild` already, and is in the `poll` below.
                    for (size_t i = 1; i < fds.size(); ++i)
                    {
                        if (fds[i].revents == 0)
                            continue;
                        /// By pid: the entry may have gone, or been replaced, since.
                        if (auto it = signalled_children.find(fd_pids[i]); it != signalled_children.end())
                            reapSignalledChild(it);
                    }
                    for (auto it = signalled_children.begin(); it != signalled_children.end();)
                    {
                        if (it->second.pidfd < 0)
                            reapSignalledChild(it);
                        else
                            ++it;
                    }
                }

                /// Reserved first, so that no `push_back` below allocates.
                fds.reserve(signalled_children.size() + 1);
                fd_pids.reserve(signalled_children.size() + 1);
                fds.clear();
                fd_pids.clear();
                fds.push_back({.fd = reaper_wakeup.fds_rw[0], .events = POLLIN, .revents = 0});
                fd_pids.push_back(0);
                for (const auto & [child_pid, child] : signalled_children)
                {
                    if (child.pidfd < 0)
                    {
                        timeout_ms = step_without_pidfd_ms;
                    }
                    else
                    {
                        fds.push_back({.fd = child.pidfd, .events = POLLIN, .revents = 0});
                        fd_pids.push_back(child_pid);
                    }
                }
                /// Until the `poll` below has reported, nothing is known about these.
                reap_all = true;
            }

            if (::poll(fds.data(), static_cast<nfds_t>(fds.size()), timeout_ms) < 0)
            {
                if (errno != EINTR)
                    throw ErrnoException(ErrorCodes::CANNOT_POLL, "Cannot wait for signalled commands to exit");
            }
            else
            {
                reap_all = false;
            }

            char buffer[64];
            while (::read(reaper_wakeup.fds_rw[0], buffer, sizeof(buffer)) > 0)
            {
            }
        }
        catch (...)
        {
            /// The thread must not end on an exception; whatever it was, the next round tries again
            /// - a step later: what threw (a failed `poll`, or the set to wait on not fitting under
            /// a memory limit) throws again as long as its cause lasts, and a round that started
            /// over at once would spin and log for as long as that is.
            tryLogCurrentException(log);
            reap_all = true;
            sleepForMilliseconds(step_without_pidfd_ms);
        }
    }
}
}
