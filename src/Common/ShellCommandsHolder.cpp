#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/ShellCommandsHolder.h>
#include <Common/UDFProcessRegistry.h>
#include <Common/setThreadName.h>

#include <Common/waitForPid.h>
#include <base/errnoToString.h>
#include <base/scope_guard.h>

#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

namespace DB
{

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

void ShellCommandsHolder::removeCommand(pid_t pid)
{
    std::lock_guard lock(mutex);
    bool is_erased = shell_commands.erase(pid);
    LOG_TRACE(log, "Try to erase command with the pid {}, is_erased: {}", pid, is_erased);
}
void ShellCommandsHolder::addCommand(std::unique_ptr<ShellCommand> command)
{
    std::lock_guard lock(mutex);
    pid_t command_pid = command->getPid();
    if (command->waitIfProccesTerminated())
    {
        LOG_TRACE(log, "Pid {} already finished. Do not insert it.", command_pid);
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
        iterator->second = std::move(command);
        LOG_TRACE(log, "Replaced the command with pid {}", command_pid);
        return;
    }
    /// We got two active ShellCommand with the same pid.
    /// Probably it is a bug, will try to replace the old shell command with a new one.
    chassert(false);

    LOG_WARNING(log, "The PID already presented in active shell commands, will try to replace with a new one.");

    iterator->second->setDoNotTerminate();
    iterator->second = std::move(command);
}

void ShellCommandsHolder::addSignalledChild(pid_t pid, std::optional<UInt64> udf_registry_generation)
{
    std::lock_guard lock(mutex);
    /// A pid that is here already was reaped by somebody other than the reaper, and reused: the
    /// child it stands for now is this one.
    auto [it, inserted] = signalled_children.insert_or_assign(pid, udf_registry_generation);

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
        /// Opened once, and kept if starting the thread fails, so that the next call can try again.
        if (reaper_wakeup.fds_rw[0] < 0)
        {
            reaper_wakeup.open();
            reaper_wakeup.setNonBlockingReadWrite();
        }
        reaper.emplace([this] { runReaper(); });
        return;
    }

    /// The reaper is asleep on the children it knew of: woken to wait for this one too.
    wakeReaper();
}

void ShellCommandsHolder::wakeReaper() noexcept
{
    char byte = 0;
    [[maybe_unused]] ssize_t res = ::write(reaper_wakeup.fds_rw[1], &byte, 1);
}

ShellCommandsHolder::~ShellCommandsHolder()
{
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
    if (it->second)
        UDFProcessRegistry::instance().removeIfGenerationMatches(it->first, *it->second);
    it = signalled_children.erase(it);
    return true;
}

void ShellCommandsHolder::reapSignalledChildren()
{
    for (auto it = signalled_children.begin(); it != signalled_children.end();)
        reapSignalledChild(it);
}

namespace
{

/// What the reaper waits on in one round: its wake-up pipe and the `pidfd` of every child it can
/// get one for, which it owns and closes. Room for every descriptor is reserved before the first is
/// opened, so that putting one into the set never allocates - an allocation that threw would leave
/// it open with nothing to close it.
class ReaperPollSet
{
public:
    ReaperPollSet(int wakeup_fd, size_t max_children)
    {
        fds.reserve(max_children + 1);
        fds.push_back({.fd = wakeup_fd, .events = POLLIN, .revents = 0});
    }

    ~ReaperPollSet()
    {
        for (size_t i = 1; i < fds.size(); ++i)
        {
            [[maybe_unused]] int err = ::close(fds[i].fd);
            chassert(!err || errno == EINTR);
        }
    }

    ReaperPollSet(const ReaperPollSet &) = delete;
    ReaperPollSet & operator=(const ReaperPollSet &) = delete;

    /// Adds the `pidfd` of `pid`; false if there is none to be had (an older kernel, a seccomp
    /// profile that refuses it), and the child has to be looked at in steps instead.
    bool addChild([[maybe_unused]] pid_t pid)
    {
        int pidfd = -1;
#if defined(OS_LINUX)
        pidfd = openPidFdForWaiting(pid);
#endif
        if (pidfd < 0)
            return false;
        fds.push_back({.fd = pidfd, .events = POLLIN, .revents = 0});
        return true;
    }

    /// Until a child exits, the reaper is woken, or `timeout_ms` passes (-1: no timeout).
    int wait(int timeout_ms) { return ::poll(fds.data(), static_cast<nfds_t>(fds.size()), timeout_ms); }

private:
    std::vector<pollfd> fds;
};

}

void ShellCommandsHolder::runReaper()
{
    setThreadName(ThreadName::SHELL_CMD_REAPER);

    /// Without a `pidfd` for some child (an older kernel, a seccomp profile that refuses it) there is
    /// nothing to sleep on until it exits: it is looked at again in steps of this.
    static constexpr int step_without_pidfd_ms = 100;

    while (true)
    {
        try
        {
            std::optional<ReaperPollSet> poll_set;
            int timeout_ms = -1;
            {
                std::lock_guard lock(mutex);
                if (reaper_shutdown)
                    return;

                reapSignalledChildren();

                poll_set.emplace(reaper_wakeup.fds_rw[0], signalled_children.size());
                for (const auto & [pid, _] : signalled_children)
                    if (!poll_set->addChild(pid))
                        timeout_ms = step_without_pidfd_ms;
            }

            if (poll_set->wait(timeout_ms) < 0 && errno != EINTR)
                LOG_WARNING(log, "Cannot wait for signalled commands to exit: {}", errnoToString());

            char buffer[64];
            while (::read(reaper_wakeup.fds_rw[0], buffer, sizeof(buffer)) > 0)
            {
            }
        }
        catch (...)
        {
            /// The thread must not end on an exception; whatever it was, the next round tries again.
            tryLogCurrentException(log);
        }
    }
}
}
