#pragma once

#include <Common/Logger.h>
#include <Common/PipeFDs.h>
#include <Common/ShellCommand.h>
#include <boost/noncopyable.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>


namespace DB
{

/** The holder class for running background shell processes.
*/
class ShellCommandsHolder final : public boost::noncopyable
{
public:
    static ShellCommandsHolder & instance();

    void removeCommand(pid_t pid);
    void addCommand(std::unique_ptr<ShellCommand> command);

    /// A child that `ShellCommand` signalled to terminate, or killed and could not reap, and that
    /// nobody waits for any more: reaped as soon as it exits, by a thread of this holder, so that it
    /// does not stay a zombie for as long as the process runs - and without whoever signalled it
    /// blocking until it exits. `udf_registry_generation` is the child's entry in
    /// `UDFProcessRegistry`, if it has one: removed once the child is reaped, not before, so that a
    /// child that ignores the signal goes on being counted while it lives.
    void addSignalledChild(pid_t pid, std::optional<UInt64> udf_registry_generation);

    ShellCommandsHolder();
    ~ShellCommandsHolder();

private:
    struct SignalledChild
    {
        std::optional<UInt64> udf_registry_generation;
        /// Opened once, when the child is taken over, and closed once it is reaped; -1 when there is
        /// none to be had (an older kernel, a seccomp profile that refuses `pidfd_open`).
        int pidfd = -1;
    };
    using SignalledChildren = std::unordered_map<pid_t, SignalledChild>;

    /// Reaps the signalled child at `it` if it has exited: true, and `it` is moved past the erased
    /// entry; false, and `it` is advanced, otherwise.
    bool reapSignalledChild(SignalledChildren::iterator & it) TSA_REQUIRES(mutex);

    /// Reaps every signalled child that has exited.
    void reapSignalledChildren() TSA_REQUIRES(mutex);

    /// Closes the `pidfd`s in `released_pidfds`.
    void closeReleasedPidFds() TSA_REQUIRES(mutex);

    /// The reaper thread: sleeps until a signalled child exits (on its `pidfd`) or another one is
    /// added (`reaper_wakeup`), and reaps.
    void runReaper();

    /// Wakes the reaper from its wait, to look at the children again. A full pipe already holds a
    /// wake-up, which is all this is, so a write that does not fit is not an error.
    void wakeReaper() noexcept;

    using ShellCommands = std::unordered_map<pid_t, std::unique_ptr<ShellCommand>>;

    std::mutex mutex;
    ShellCommands shell_commands TSA_GUARDED_BY(mutex);
    /// The children `addSignalledChild` took over, with their `UDFProcessRegistry` generations.
    SignalledChildren signalled_children TSA_GUARDED_BY(mutex);
    /// The `pidfd`s of entries replaced while the reaper may be polling them: closed by the reaper
    /// once its `poll` is over, so that their numbers are not reused under it.
    std::vector<int> released_pidfds TSA_GUARDED_BY(mutex);
    std::optional<std::thread> reaper TSA_GUARDED_BY(mutex);
    /// Opened under `mutex` before the reaper starts and never changed after, so the reaper reads it
    /// without the lock.
    LazyPipeFDs reaper_wakeup;
    bool reaper_shutdown TSA_GUARDED_BY(mutex) = false;

    LoggerPtr log = getLogger("ShellCommandsHolder");
};

}
