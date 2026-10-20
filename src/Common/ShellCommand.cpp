/// `wait4` is declared under `_DEFAULT_SOURCE` on Linux glibc, which the
/// `-std=c++23` strict mode otherwise hides. Define it before the first system
/// header that guards it. It is a libc feature-test macro, hence the reserved
/// name; suppress the diagnostics that would otherwise reject our own define.
#if defined(OS_LINUX) && !defined(_DEFAULT_SOURCE)
#   pragma clang diagnostic push
#   pragma clang diagnostic ignored "-Wreserved-macro-identifier"
#   pragma clang diagnostic ignored "-Wunused-macros"
#   define _DEFAULT_SOURCE // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
#   pragma clang diagnostic pop
#endif

#include <sys/resource.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#ifdef __sun
#include <sys/filio.h>  // illumos defines FIONREAD in sys/filio.h, not sys/ioctl.h
#endif
#include <sys/wait.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <exception>
#include <csignal>
#include <limits>

#include <base/scope_guard.h>
#include <base/sleep.h>
#include <Common/logger_useful.h>
#include <base/errnoToString.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/ShellCommand.h>
#include <Common/ShellCommandsHolder.h>
#include <Common/UDFProcessRegistry.h>
#include <Common/PipeFDs.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Common/waitForPid.h>
#include <Common/Stopwatch.h>


namespace
{
    /// The step of preparing the child that failed between `vfork` and `exec`. Reported to the
    /// parent through a close-on-exec pipe rather than as the child's exit code: an exit code is
    /// one byte the command's own exit codes share, so any value chosen for these would sooner or
    /// later diagnose an ordinary `exit 88` as a failed `exec`. The pipe carries the step and the
    /// `errno`, and a successful `exec` closes it, so the parent learns the outcome before it
    /// touches the process at all.
    enum class ChildSetupStep : int
    {
        DUP_STDIN,
        DUP_STDOUT,
        DUP_STDERR,
        EXEC,
        DUP_READ_DESCRIPTOR,
        DUP_WRITE_DESCRIPTOR,
        DUP_INHERITED_DESCRIPTOR,
        CLOSE_INHERITED_DESCRIPTOR,
        SET_PROCESS_GROUP,
    };

    /// What the child writes into the error pipe: small enough for a single write to be atomic.
    struct ChildSetupFailure
    {
        int step;
        int error;
    };

    const char * describe(ChildSetupStep step)
    {
        switch (step)
        {
            case ChildSetupStep::DUP_STDIN: return "dup2 stdin";
            case ChildSetupStep::DUP_STDOUT: return "dup2 stdout";
            case ChildSetupStep::DUP_STDERR: return "dup2 stderr";
            case ChildSetupStep::EXEC: return "execv";
            case ChildSetupStep::DUP_READ_DESCRIPTOR: return "dup2 a read descriptor";
            case ChildSetupStep::DUP_WRITE_DESCRIPTOR: return "dup2 a write descriptor";
            case ChildSetupStep::DUP_INHERITED_DESCRIPTOR: return "dup2 an inherited descriptor";
            case ChildSetupStep::CLOSE_INHERITED_DESCRIPTOR: return "close the original of an inherited descriptor";
            case ChildSetupStep::SET_PROCESS_GROUP: return "setpgid";
        }
        return "prepare";
    }

    /// Runs in the child, between `vfork` and `exec`: nothing but the write and the exit.
    [[noreturn]] void reportChildSetupFailureAndExit(int error_fd, ChildSetupStep step)
    {
        ChildSetupFailure failure{static_cast<int>(step), errno};
        /// Nothing to do about a failed write here: the parent then sees EOF and, since the child
        /// is gone, an exit code of 1 in place of a running command.
        [[maybe_unused]] ssize_t written = ::write(error_fd, &failure, sizeof(failure));
        _exit(1);
    }

    /// Whether the command died of a write into a stdout that was closed under it: killed by
    /// `SIGPIPE`, or exited with `128 + SIGPIPE`, which is how a shell reports a command of its
    /// own that was (a script running `yes` without `exec`).
    /// The exit code is ambiguous: a command may also exit with 141 for its own reasons, and nothing
    /// the server can observe tells the two apart - not even whether the command was still writing,
    /// since a slow producer may pause past the close and only then die on its next write. The
    /// ambiguity is resolved in favour of the consumer: once the server has closed an abandoned
    /// stdout, 141 is taken for the `SIGPIPE` that close causes, so that a `LIMIT` over a command
    /// behind a shell does not fail depending on timing. A command that wants its own failure
    /// reported under `check_exit_code` has to use another exit code.
    bool diedOfSigpipe(int status)
    {
        return (WIFSIGNALED(status) && WTERMSIG(status) == SIGPIPE)
            || (WIFEXITED(status) && WEXITSTATUS(status) == 128 + SIGPIPE);
    }
}

namespace ProfileEvents
{
    extern const Event ExecuteShellCommand;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_FORK;
    extern const int CANNOT_WAITPID;
    extern const int CHILD_WAS_NOT_EXITED_NORMALLY;
    extern const int CANNOT_CREATE_CHILD_PROCESS;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_FCNTL;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
}

ShellCommand::ShellCommand(pid_t pid_, int & in_fd_, int & out_fd_, int & err_fd_, const ShellCommand::Config & config_)
    : in(in_fd_)
    , out(out_fd_)
    , err(err_fd_)
    , pid(pid_)
    , config(config_)
{
}

LoggerPtr ShellCommand::getLogger()
{
    return ::getLogger("ShellCommand");
}

UInt64 ShellCommand::remainingTerminationTimeoutMs()
{
    /// Arm the shared deadline once, on the first waiter (cleanup, or the destructor when
    /// cleanup never ran). Every later waiter subtracts the time already spent so both the
    /// cleanup poll and the destructor wait draw from one `command_termination_timeout`.
    if (termination_deadline_ns == 0)
    {
        const UInt64 timeout_seconds = config.terminate_in_destructor_strategy.wait_for_normal_exit_before_termination_seconds;
        const UInt64 max_seconds = std::numeric_limits<UInt64>::max() / 1000;
        termination_deadline_ns = monotonicDeadlineNs((timeout_seconds < max_seconds ? timeout_seconds : max_seconds) * 1000);
    }

    return millisecondsUntil(termination_deadline_ns);
}

void ShellCommand::endTerminationGracePeriod() noexcept
{
    termination_deadline_ns = clock_gettime_ns();
}

void ShellCommand::discardWithoutGrace() noexcept
{
    endTerminationGracePeriod();
    reap_on_destruction = true;
}

ShellCommand::~ShellCommand()
{
    /// `setDoNotTerminate`: nothing is done to the child, its group included.
    if (do_not_terminate)
        return;

    /// A group of its own is ended as a whole, whatever happened before: a bounded wait that ran
    /// out (`wait_called` is set, the child alive), or the normal exit wait below running out. A
    /// child that exited was reaped only after its group got `SIGKILL` (`killGroupOfExitedChild`).
    SCOPE_EXIT({
        if (config.own_process_group)
            killAndReapNoThrow(/*whole_group=*/ true);
    });

    /// Reaped already, or a bounded wait ran out with the child alive: nothing but the end of its
    /// own group, above.
    if (wait_called)
        return;

    /// No `terminate_in_destructor`: waited for without a bound.
    if (!config.terminate_in_destructor_strategy.terminate_in_destructor)
    {
        tryWaitNoThrow();
        return;
    }

    /// Given the grace period to exit on its own.
    if (waitForExitWithinGracePeriod())
        return;

    /// With a group of its own, the whole group gets `SIGKILL` on the way out (above) instead of
    /// `termination_signal`.
    if (config.own_process_group)
        return;

    /// A discarded command is killed, and reaped, so that it leaves no zombie behind - and no
    /// entry in the registry of UDF processes. `termination_signal` is for a command whose exit
    /// might still matter.
    if (reap_on_destruction)
    {
        killAndReapNoThrow(/*whole_group=*/ false);
        return;
    }

    signalAndHandOverChild();
}

bool ShellCommand::waitForExitWithinGracePeriod()
{
    /// Draw from the shared deadline: the cleanup-side wait may have already spent
    /// most of `command_termination_timeout`, so this wait gets only what remains and
    /// the configured grace period is honored once, not doubled.
    return tryWaitProcessWithTimeout(remainingTerminationTimeoutMs());
}

void ShellCommand::signalAndHandOverChild()
{
    LOG_TRACE(getLogger(), "Will kill shell command pid {} with signal {}", pid, config.terminate_in_destructor_strategy.termination_signal);

    int retcode = kill(pid, config.terminate_in_destructor_strategy.termination_signal);
    if (retcode != 0)
        LOG_WARNING(getLogger(), "Cannot kill shell command pid {}, error: '{}'", pid, errnoToString());

    /// A UDF command is reaped once it goes on the signal, as a command normally does - left alone,
    /// it would stay a zombie, and an entry in `UDFProcessRegistry`, for as long as the server runs -
    /// but not waited for here: a command can take its time over the signal, and whatever destroys
    /// this (a query, a reload of a whole pool) is not to be held up by it. A command that ignores
    /// the signal is left running, as it always was. So is one the signal could not be sent to (it
    /// has dropped privileges the server lacks, say): still a child of the server, and still in the
    /// registry, until it exits and is reaped. Other commands are left as they always were.
    ///
    /// One the reaper would not take is left as every signalled command was before the reaper
    /// existed: signalled, and reaped by nobody. That is what the warning is for; nothing more is
    /// done to it here, since this is the destructor and a signal it was not meant to get is not
    /// the answer to a failed bookkeeping step.
    if (config.register_in_udf_process_registry && !handOverChildToReaper())
        LOG_WARNING(getLogger(), "Shell command pid {} was signalled but will not be reaped", pid);
}

bool ShellCommand::handOverChildToReaper() noexcept
{
    try
    {
        /// A cleanup path: an allocation at the query's memory limit must not be what leaves the
        /// child to nobody.
        LockMemoryExceptionInThread lock(VariableContext::Global);
        ShellCommandsHolder::instance().addSignalledChild(
            pid, config.register_in_udf_process_registry ? std::optional<UInt64>(udf_registry_generation) : std::nullopt);
    }
    catch (...)
    {
        /// Still this object's: whoever called decides what to do with a child that nobody reaps.
        tryLogCurrentException(getLogger(), fmt::format("Cannot hand shell command pid {} over to the reaper", pid));
        return false;
    }

    /// Not this object's to wait for any more, but not forgotten either (`forgetChild`): its entry
    /// in `UDFProcessRegistry` goes with it, and is removed once it is reaped - a child that
    /// ignores the signal is still a process of the server for as long as it lives.
    child_reaped = true;
    wait_called = true;
    return true;
}

void ShellCommand::handOverOrForgetKilledChild() noexcept
{
    if (handOverChildToReaper())
        return;

    /// It has `SIGKILL`, which it cannot ignore: it runs no more code, and is a zombie if it is
    /// anything, which keeps its pid to itself - so it is taken out of `UDFProcessRegistry` now,
    /// and the zombie, which nobody reaps, is what the warning is about.
    LOG_WARNING(getLogger(), "Shell command pid {} was killed but will not be reaped", pid);
    forgetChild();
}

void ShellCommand::tryWaitNoThrow() noexcept
{
    try
    {
        tryWait();
    }
    catch (...)
    {
        tryLogCurrentException(getLogger());
    }
}

bool ShellCommand::tryWaitProcessWithTimeout(size_t timeout_in_milliseconds)
{
    LOG_TRACE(getLogger(), "Try wait for shell command pid {} with timeout {} (milliseconds)", pid, timeout_in_milliseconds);

    wait_called = true;

    /// Before the wait: a child blocked writing into a pipe nobody drains any more gets `EPIPE`
    /// rather than the whole timeout. Without throwing: this is the destructor's path.
    closeStreamsNoThrow();

    /// The exited child of a group stays a zombie until its group has been killed, which keeps the
    /// number of the group from being reused in between.
    switch (waitForPidMilliseconds(pid, timeout_in_milliseconds, /*leave_unreaped=*/ config.own_process_group))
    {
        case WaitForPidResult::EXITED:
            if (config.own_process_group)
            {
                killAndReapNoThrow(/*whole_group=*/ true);
                return child_reaped;
            }
            forgetChild();
            return true;
        case WaitForPidResult::NOT_OUR_CHILD:
            /// Reaped by somebody else: its pid may belong to anybody now, so it is neither waited
            /// for nor signalled - not by the destructor either, which takes this for an exit.
            forgetChild();
            return true;
        case WaitForPidResult::TIMEOUT:
            /// Still ours: signalled by the caller.
            return false;
        case WaitForPidResult::ERROR:
            /// Still ours, as far as anyone can tell: signalled by the caller.
            LOG_WARNING(getLogger(), "Cannot wait for shell command pid {}: {}", pid, errnoToString());
            return false;
    }
}

void ShellCommand::forgetChild()
{
    child_reaped = true;
    wait_called = true;
    if (config.register_in_udf_process_registry)
        UDFProcessRegistry::instance().removeIfGenerationMatches(pid, udf_registry_generation);
}

void ShellCommand::killGroupOfExitedChild()
{
    /// The child is a zombie that nobody has reaped, so the number of its group is still its own.
    if (0 != ::kill(-pid, SIGKILL) && errno != ESRCH)
        LOG_WARNING(getLogger(), "Cannot kill the process group of shell command pid {}, error: '{}'", pid, errnoToString());
}

bool ShellCommand::killAndWaitForChild(pid_t child_pid, bool whole_group)
{
    /// The group first, for what the child may have started already; `ESRCH` is a child that is
    /// gone on its own.
    if (whole_group && 0 != ::kill(-child_pid, SIGKILL) && errno != ESRCH)
        LOG_WARNING(getLogger(), "Cannot kill the process group of shell command pid {}, error: '{}'", child_pid, errnoToString());

    /// And the child itself - which the group does not reach if the child has moved to another
    /// group (`setpgid`).
    if (0 != ::kill(child_pid, SIGKILL) && errno != ESRCH)
        LOG_WARNING(getLogger(), "Cannot kill shell command pid {}, error: '{}'", child_pid, errnoToString());

    /// Nothing to wait out after `SIGKILL`: the child cannot run user code again, and the bound
    /// only covers the time the kernel takes to tear it down - which is not always short: a child
    /// in uninterruptible I/O is not reaped before that I/O ends.
    static constexpr size_t reap_after_kill_timeout_ms = 5000;
    switch (waitForPidMilliseconds(child_pid, reap_after_kill_timeout_ms))
    {
        case WaitForPidResult::EXITED:
        case WaitForPidResult::NOT_OUR_CHILD:
            return true;
        /// The wait failed (run out of descriptors for `pidfd_open`, say), or ran out.
        case WaitForPidResult::ERROR:
        {
            const int saved_errno = errno;
            LOG_WARNING(getLogger(), "Cannot reap shell command pid {} after SIGKILL: {}", child_pid, errnoToString(saved_errno));
            return false;
        }
        case WaitForPidResult::TIMEOUT:
            LOG_WARNING(getLogger(), "Shell command pid {} was not reaped within {} ms after SIGKILL", child_pid, reap_after_kill_timeout_ms);
            return false;
    }
    UNREACHABLE();
}

void ShellCommand::killAndReapNoThrow(bool whole_group) noexcept
{
    /// Once: a second call - the destructor's `SCOPE_EXIT` after `tryWaitProcessWithTimeout` has
    /// already been here - would only wait out the same bound again for a child that `SIGKILL`
    /// did not finish off within it.
    if (child_reaped || kill_and_reap_done)
        return;
    kill_and_reap_done = true;

    try
    {
        /// `child_reaped` can be false for a pid that is not ours any more: a `waitpid` that failed
        /// says nothing about the child, and somebody else may have reaped it. Only a child that
        /// `waitid` still finds, alive or a zombie, keeps its pid - and the number of the group it
        /// leads - to itself, so that a signal reaches this child and nothing else.
        if (peekChildState(pid, /*blocking=*/ false) == ChildState::NOT_OUR_CHILD)
        {
            forgetChild();
            LOG_WARNING(getLogger(), "Shell command pid {} is no longer a child of this process; it is not signalled", pid);
            return;
        }

        wait_called = true;
        if (killAndWaitForChild(pid, whole_group))
            forgetChild();
        else
            /// Still ours, as far as anyone can tell, and not reaped: it is handed to the reaper, or
            /// nobody would ever reap it once this object is gone.
            handOverOrForgetKilledChild();
    }
    catch (...)
    {
        tryLogCurrentException(getLogger());
    }
}

void ShellCommand::logCommand(const char * filename, char * const argv[])
{
    WriteBufferFromOwnString args;
    for (int i = 0; argv != nullptr && argv[i] != nullptr; ++i)
    {
        if (i > 0)
            args << ", ";

        /// NOTE: No escaping is performed.
        args << "'" << argv[i] << "'";
    }
    LOG_TRACE(ShellCommand::getLogger(), "Will start shell command '{}' with arguments {}", filename, args.str());
}

namespace
{

/// The pipes a command is started with: those of its standard streams, and one for each of
/// `Config::read_fds` and `Config::write_fds`, enlarged to `Config::pipe_capacity` if it is set.
struct CommandPipes
{
    explicit CommandPipes(const ShellCommand::Config & config)
    {
        read_pipe_fds.reserve(config.read_fds.size());
        write_pipe_fds.reserve(config.write_fds.size());

        for (size_t i = 0; i < config.read_fds.size(); ++i)
            read_pipe_fds.emplace_back(std::make_unique<PipeFDs>());

        for (size_t i = 0; i < config.write_fds.size(); ++i)
            write_pipe_fds.emplace_back(std::make_unique<PipeFDs>());

        if (config.pipe_capacity)
        {
            if (config.pipe_capacity > static_cast<size_t>(std::numeric_limits<int>::max()))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Pipe capacity {} exceeds maximum supported value {}",
                    config.pipe_capacity,
                    std::numeric_limits<int>::max());

            int pipe_capacity = static_cast<int>(config.pipe_capacity);

            pipe_stdin.tryIncreaseSize(pipe_capacity);

            if (!config.pipe_stdin_only)
            {
                pipe_stdout.tryIncreaseSize(pipe_capacity);
                pipe_stderr.tryIncreaseSize(pipe_capacity);
            }

            for (const auto & fds : read_pipe_fds)
                fds->tryIncreaseSize(pipe_capacity);

            for (const auto & fds : write_pipe_fds)
                fds->tryIncreaseSize(pipe_capacity);
        }
    }

    PipeFDs pipe_stdin;
    PipeFDs pipe_stdout;
    PipeFDs pipe_stderr;

    std::vector<std::unique_ptr<PipeFDs>> read_pipe_fds;
    std::vector<std::unique_ptr<PipeFDs>> write_pipe_fds;
};

/// Every descriptor the child installs - the ends of the standard-stream pipes, of the
/// `read_fds`/`write_fds` pipes, and the inherited descriptors - is handed over in two steps,
/// and the first one happens before `vfork`, where it is allowed to fail with an exception
/// (`planHandovers`, `stageHandovers`). A plain `dup2(parent_fd, child_fd)` in the child is
/// wrong in two ways that nobody can rule out, because the parent's numbers are whatever `pipe`
/// and the caller got: when `parent_fd == child_fd` (the region's `memfd` happened to be
/// created as 3, or the pipe end for `read_fds` `{7}` got 7) `dup2` is a no-op and the
/// descriptor keeps its close-on-exec flag, so `exec` closes it; and when one hand-over's target
/// is another's source (`{3 <- 4}, {4 <- 3}`, or a pipe target that is the number of the next
/// pipe's end) the first `dup2` overwrites what the second was going to copy. So every source is
/// first duplicated to a number above every target, and the child `dup2`s from those copies,
/// which can neither be a target nor be clobbered by one. The copies are close-on-exec: they must
/// not outlive this `exec` in any child, and they are closed in the parent once the child has run.
struct Handover
{
    int child_fd;
    int parent_fd;
    ChildSetupStep step;
};

struct HandoverPlan
{
    std::vector<Handover> handovers;

    /// The first number above every descriptor the child is going to install something under.
    int first_free_fd = 0;
};

/// Lists every hand-over to the child and checks that the targets can be installed as they are.
HandoverPlan planHandovers(const ShellCommand::Config & config, const CommandPipes & pipes)
{
    HandoverPlan plan;
    auto & handovers = plan.handovers;
    handovers.reserve(3 + config.read_fds.size() + config.write_fds.size() + config.inherited_fds.size());

    /// Every number the child is going to install something under, each claimed once. The
    /// standard streams are the child's own; three lists claim the rest - `read_fds`, `write_fds`
    /// and the targets of `inherited_fds` - and a number in two of them (or twice in one) would be
    /// installed twice in the child, the later `dup2` silently replacing the earlier: a pipe the
    /// parent goes on reading from, say, with the region's descriptor sitting where the child
    /// was told to write into it. Refused here, where it is a configuration error with a
    /// message, rather than found in the child.
    handovers.push_back({STDIN_FILENO, pipes.pipe_stdin.fds_rw[0], ChildSetupStep::DUP_STDIN});
    if (!config.pipe_stdin_only)
    {
        handovers.push_back({STDOUT_FILENO, pipes.pipe_stdout.fds_rw[1], ChildSetupStep::DUP_STDOUT});
        handovers.push_back({STDERR_FILENO, pipes.pipe_stderr.fds_rw[1], ChildSetupStep::DUP_STDERR});
    }
    for (size_t i = 0; i < config.read_fds.size(); ++i)
        handovers.push_back({config.read_fds[i], pipes.read_pipe_fds[i]->fds_rw[1], ChildSetupStep::DUP_READ_DESCRIPTOR});
    for (size_t i = 0; i < config.write_fds.size(); ++i)
        handovers.push_back({config.write_fds[i], pipes.write_pipe_fds[i]->fds_rw[0], ChildSetupStep::DUP_WRITE_DESCRIPTOR});
    for (const auto & [child_fd, parent_fd] : config.inherited_fds)
        handovers.push_back({child_fd, parent_fd, ChildSetupStep::DUP_INHERITED_DESCRIPTOR});

    std::vector<int> child_targets;
    child_targets.reserve(handovers.size());
    for (const auto & handover : handovers)
    {
        /// A standard stream the child is given a pipe for is a target like any other, and a clash
        /// with it is caught as a duplicate below; one it is not given a pipe for (`pipe_stdin_only`
        /// leaves 1 and 2 alone) is free to be claimed.
        if (handover.child_fd < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot install descriptor {} in a child as {}", handover.parent_fd, handover.child_fd);
        child_targets.push_back(handover.child_fd);
    }
    std::sort(child_targets.begin(), child_targets.end());
    if (auto duplicate = std::adjacent_find(child_targets.begin(), child_targets.end()); duplicate != child_targets.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Descriptor {} is claimed more than once in the child (by a standard stream, read_fds, write_fds or "
            "inherited_fds)", *duplicate);

    if (child_targets.back() == std::numeric_limits<int>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Descriptor {} cannot be a target in the child: there is no number above it", child_targets.back());
    plan.first_free_fd = child_targets.back() + 1;

    return plan;
}

/// The first step of every hand-over (see `Handover`): its source is duplicated, close-on-exec, to
/// a number above every target, and the copy is appended to `staged_fds`, in the order of
/// `plan.handovers`. The caller closes everything in `staged_fds`, including when this throws
/// halfway.
void stageHandovers(const HandoverPlan & plan, std::vector<int> & staged_fds)
{
    for (const auto & handover : plan.handovers)
    {
        int staged = ::fcntl(handover.parent_fd, F_DUPFD_CLOEXEC, plan.first_free_fd);
        if (staged == -1)
            throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot duplicate descriptor {} to hand it to a child as {}", handover.parent_fd, handover.child_fd);
        staged_fds.push_back(staged);
    }
}

/// Runs in the child, between `vfork` and `exec`: whether the original of the `i`-th of
/// `Config::inherited_fds` is to be left open rather than closed (see `execChild`).
bool leaveInheritedOriginalOpen(const ShellCommand::Config & config, size_t i)
{
    const int parent_fd = config.inherited_fds[i].second;
    bool leave_alone = parent_fd <= STDERR_FILENO;
    for (int fd : config.read_fds)
        leave_alone |= parent_fd == fd;
    for (int fd : config.write_fds)
        leave_alone |= parent_fd == fd;
    for (const auto & [other_child_fd, other_parent_fd] : config.inherited_fds)
        leave_alone |= parent_fd == other_child_fd;
    for (size_t j = 0; j < i; ++j)
        leave_alone |= parent_fd == config.inherited_fds[j].second;
    return leave_alone;
}

/// The child, between `vfork` and `exec`: installs the descriptors and `exec`s the command, or
/// reports the step that failed into `child_error_fd` and exits. It runs on the parent's memory,
/// so it makes only async-signal-safe calls, and allocates, throws, locks and logs nothing:
/// everything it needs was prepared before `vfork`.
[[noreturn]] void execChild(
    const char * filename,
    char * const argv[],
    const ShellCommand::Config & config,
    const std::vector<Handover> & handovers,
    const std::vector<int> & staged_fds,
    int child_error_fd)
{
    /// NOLINTBEGIN(clang-analyzer-unix.Vfork)

    /// Why `_exit` and not `exit`? Because `exit` calls `atexit` and destructors of thread local storage.
    /// And there is a lot of garbage (including, for example, mutex is blocked). And this can not be done after `vfork` - deadlock happens.

    /// Install every descriptor under the number the child expects, `dup2`ing from the staged
    /// copy (see `Handover`). The staged copy is above every target, so this is never a no-op and
    /// never destroys a source. The result has no close-on-exec flag, so it survives the `exec`
    /// below; the staged copy does not, and neither do the pipe ends themselves.
    for (size_t i = 0; i < handovers.size(); ++i)
        if (handovers[i].child_fd != dup2(staged_fds[i], handovers[i].child_fd))
            reportChildSetupFailureAndExit(child_error_fd, handovers[i].step);

    /// The originals must not reach the child either, under their own numbers: the contract
    /// is "this descriptor, under the number it is told", and an original that is not
    /// close-on-exec would otherwise survive the `exec` as a second copy - for a pipe, an
    /// extra reader or writer that keeps the parent from ever seeing EOF. Closed here rather
    /// than required to be close-on-exec, so that the contract does not depend on how the
    /// caller opened the descriptor. An original whose number is itself a target - of any of
    /// the `dup2`s above: the standard streams, `read_fds`, `write_fds` or another inherited
    /// pair - has just been overwritten with the right thing and is left alone; closing it
    /// would take down what was just installed there. And an original handed over under two
    /// numbers (`{10 <- 5}, {11 <- 5}`) is one descriptor, closed once: the second close would
    /// fail on a number that is already free, or worse, hit whatever got that number since.
    for (size_t i = 0; i < config.inherited_fds.size(); ++i)
    {
        if (!leaveInheritedOriginalOpen(config, i) && 0 != ::close(config.inherited_fds[i].second))
            reportChildSetupFailureAndExit(child_error_fd, ChildSetupStep::CLOSE_INHERITED_DESCRIPTOR);
    }

    // Reset the signal mask: it may be non-empty and will be inherited
    // by the child process, which might not expect this.
    sigset_t mask;
    sigemptyset(&mask);
    sigprocmask(0, nullptr, &mask); // NOLINT(concurrency-mt-unsafe)
    sigprocmask(SIG_UNBLOCK, &mask, nullptr); // NOLINT(concurrency-mt-unsafe)

    /// A group of its own, led by this process, so that the destructor can end it together
    /// with whatever it starts (see `Config::own_process_group`). In the child, before `exec`:
    /// by the time `vfork` returns in the parent the group already exists, so the parent can
    /// never signal a group that is not there yet.
    if (config.own_process_group && 0 != ::setpgid(0, 0))
        reportChildSetupFailureAndExit(child_error_fd, ChildSetupStep::SET_PROCESS_GROUP);

    execv(filename, argv);
    /// If the process is running, then `execv` does not return here.

    reportChildSetupFailureAndExit(child_error_fd, ChildSetupStep::EXEC);
    /// NOLINTEND(clang-analyzer-unix.Vfork)
}

}

std::unique_ptr<ShellCommand> ShellCommand::executeImpl(
    const char * filename,
    char * const argv[],
    const Config & config)
{
    logCommand(filename, argv);
    ProfileEvents::increment(ProfileEvents::ExecuteShellCommand);

    CommandPipes pipes(config);

    /// Before `vfork`, everything the child is going to need, and that may fail with an exception.
    const HandoverPlan plan = planHandovers(config, pipes);

    std::vector<int> staged_fds;
    staged_fds.reserve(plan.handovers.size() + 1);
    SCOPE_EXIT({
        for (int fd : staged_fds)
            if (0 != ::close(fd))
                LOG_WARNING(getLogger(), "Cannot close a staged descriptor: {}", errnoToString());
    });
    stageHandovers(plan, staged_fds);

    /// How the child reports a failure of any step of its preparation: a close-on-exec pipe. A
    /// successful `exec` leaves no report; a failure writes the step and the
    /// `errno` and the parent reads those. The child's copy of the write end is staged above every
    /// target like the descriptors above are, so that no `dup2` in the child lands on it - it would
    /// otherwise be silently replaced by whatever was installed under that number, and a later
    /// failure would write its report into a pipe or a region instead. (The pipe itself is opened
    /// with `O_CLOEXEC`, and `F_DUPFD_CLOEXEC` keeps the copy so.) Both of the parent's write ends
    /// are closed before the parent reads, or the read would never see EOF.
    PipeFDs pipe_child_error;
    /// Another concurrent spawn can inherit a writer before Darwin installs `FD_CLOEXEC`.
    /// After `vfork` the report is already available or the child has executed successfully;
    /// receiving it must not depend on every unrelated copy of the writer being closed.
    pipe_child_error.setNonBlockingRead();
    const int child_error_fd = ::fcntl(pipe_child_error.fds_rw[1], F_DUPFD_CLOEXEC, plan.first_free_fd);
    if (child_error_fd == -1)
        throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot duplicate the child error pipe");
    staged_fds.push_back(child_error_fd);

    /// `vfork` must be called directly, not through a pointer obtained with `dlsym`: the compiler
    /// knows `vfork` as a function that returns twice, and only a call it can see as such makes
    /// it keep the stack slots of this frame intact across the child's execution. The child runs
    /// `execChild` from this very frame, and the codegen treats that call as one that never
    /// comes back (it ends with `_exit`), so without the attribute it happily reuses the spill
    /// slot of a value the call no longer needs - the `config` reference, say - for one of its
    /// own temporaries. The child then `exec`s, the parent wakes up, and reads garbage from its
    /// own frame. This is not a theoretical concern: the MemorySanitizer build did exactly that
    /// in the loop over `inherited_fds` (now in `execChild`), back when the child's code was
    /// inline here.
    ///
    /// The pointer from `dlsym` also hid the call from the static analyzer, which has two things
    /// to say about `vfork`. That `posix_spawn` is the safer API: it is, and moving this code to
    /// it is a change of its own; until then this is the one place in the server that spawns,
    /// and it is written with the care `vfork` demands. And that nothing but `exec`/`_exit` may
    /// be called after it: the child (`execChild`) makes only the calls `posix_spawn` itself makes
    /// in its own child - `dup2`, `close`, `sigprocmask`, all async-signal-safe - besides its own
    /// helpers, which allocate nothing, and touches nothing the parent shares beyond the
    /// descriptor table, which is the child's own. Suppressed, not hidden.
    pid_t pid = vfork(); // NOLINT(bugprone-unsafe-functions,cert-msc24-c,cert-msc33-c,clang-analyzer-security.insecureAPI.vfork)

    if (pid == -1)
        throw ErrnoException(ErrorCodes::CANNOT_FORK, "Cannot vfork");

    if (0 == pid)
    {
        /// We are in the freshly created process.
        /// NOLINTBEGIN(clang-analyzer-unix.Vfork)
        execChild(filename, argv, config, plan.handovers, staged_fds, child_error_fd);
        /// NOLINTEND(clang-analyzer-unix.Vfork)
    }

    /// The group, from this side too. A real `vfork` returns here only after the child's own
    /// `setpgid`, but a `vfork` that is a `fork` in disguise (ThreadSanitizer intercepts it) can
    /// return first, and a discard right after would then signal a group that does not exist yet.
    /// Whichever of the two calls comes second finds the work done: here `EACCES` (the child has
    /// `exec`ed already) or `ESRCH` (it has exited) mean exactly that, and are fine.
    if (config.own_process_group && 0 != ::setpgid(pid, pid) && errno != EACCES && errno != ESRCH)
        LOG_WARNING(getLogger(), "Cannot put shell command pid {} into a process group of its own: {}", pid, errnoToString());

    /// Both of the parent's write ends of the error pipe are closed before its report is read.
    if (0 != ::close(child_error_fd))
        LOG_WARNING(getLogger(), "Cannot close the child error pipe: {}", errnoToString());
    staged_fds.pop_back();
    if (0 != ::close(pipe_child_error.fds_rw[1]))
        LOG_WARNING(getLogger(), "Cannot close the child error pipe: {}", errnoToString());
    pipe_child_error.fds_rw[1] = -1;

    checkChildSetupReport(pid, pipe_child_error.fds_rw[0], config.own_process_group);

    /// The child has started, and until `res` owns it nobody does: the constructor allocates (a
    /// buffer for each of the three pipes), and an allocation refused by the query's memory limit
    /// would leave a process that has `exec`ed running with nobody to signal, reap or count it -
    /// and, for a shared-memory UDF, holding the descriptor of a region the server is about to
    /// free and stop charging for. So it is killed and reaped before the failure goes out, like a
    /// child whose start could not be established (`checkChildSetupReport`): the alternative is
    /// leaking the process. From `res` on, its destructor is in charge.
    std::unique_ptr<ShellCommand> res;
    try
    {
        res.reset(new ShellCommand(
            pid,
            pipes.pipe_stdin.fds_rw[1],
            pipes.pipe_stdout.fds_rw[0],
            pipes.pipe_stderr.fds_rw[0],
            config));
    }
    catch (...)
    {
        killAndReapOrphan(pid, config.own_process_group);
        throw;
    }

    if (config.register_in_udf_process_registry)
        res->udf_registry_generation = UDFProcessRegistry::instance().add(pid);

    for (size_t i = 0; i < config.read_fds.size(); ++i)
    {
        auto & fds = *pipes.read_pipe_fds[i];
        auto fd = config.read_fds[i];
        res->read_fds.emplace(fd, fds.fds_rw[0]);
    }

    for (size_t i = 0; i < config.write_fds.size(); ++i)
    {
        auto & fds = *pipes.write_pipe_fds[i];
        auto fd = config.write_fds[i];
        res->write_fds.emplace(fd, fds.fds_rw[1]);
    }

    LOG_TRACE(
        getLogger(),
        "Started shell command '{}' with pid {} and file descriptors: out {}, err {}",
        filename,
        pid,
        res->out.getFD(),
        res->err.getFD());

    return res;
}

void ShellCommand::killAndReapOrphan(pid_t child_pid, bool whole_group) noexcept
{
    /// Runs under the very memory limit that got us here: nothing in it may be refused.
    LockMemoryExceptionInThread lock(VariableContext::Global);

    /// The child is ours and unreaped - it has never been waited for - so its pid, and the number of
    /// the group it leads, are its own, and `SIGKILL` reaches it and nothing else. A child not
    /// reaped within the bound (one in uninterruptible I/O on a stuck NFS or FUSE mount) must not
    /// hold up the query this fails, so it is handed to the reaper. It has no entry in
    /// `UDFProcessRegistry` yet, so there is none to remove when it goes.
    if (killAndWaitForChild(child_pid, whole_group))
        return;

    try
    {
        ShellCommandsHolder::instance().addSignalledChild(child_pid, std::nullopt);
    }
    catch (...)
    {
        tryLogCurrentException(getLogger(), fmt::format("Child process pid {} was killed but will not be reaped", child_pid));
    }
}

void ShellCommand::checkChildSetupReport(pid_t child_pid, int report_fd, bool own_process_group)
{
    /// The child has either `exec`ed or written its report and exited (that is what `vfork`
    /// guarantees by the time it returns in the parent). Read the report without waiting for EOF:
    /// another process may still hold a copy of the write end.

#if defined(THREAD_SANITIZER)
    /// ThreadSanitizer intercepts `vfork` and calls `fork` instead, so here the parent can resume
    /// before the child has got to `exec`, and a read without waiting would take a child that is
    /// about to fail for one that started. Wait for its report or for the end of the pipe: the
    /// write end is close-on-exec, so it goes at `exec` or at exit - in this child, and in any
    /// other one spawned meanwhile that inherited it.
    ///
    /// Bounded all the same: a child spawned meanwhile by another thread that inherited the write
    /// end and stalls before its own `exec` must not hang this one. Past the bound the report
    /// is read as in any other build, without waiting.
    {
        static constexpr UInt64 child_report_wait_ms = 10000;
        const UInt64 deadline_ns = monotonicDeadlineNs(child_report_wait_ms);
        pollfd pfd{};
        pfd.fd = report_fd;
        pfd.events = POLLIN;
        while (true)
        {
            const UInt64 remaining_ms = millisecondsUntil(deadline_ns);
            const int res = remaining_ms == 0 ? 0 : ::poll(&pfd, 1, static_cast<int>(remaining_ms));
            if (res < 0 && errno == EINTR)
                continue;
            if (res == 0)
                LOG_WARNING(
                    getLogger(), "The child error pipe of pid {} neither reported nor closed within {} ms", child_pid, child_report_wait_ms);
            break;
        }
    }
#endif

    ChildSetupFailure failure{};
    ssize_t bytes_read = 0;
    do
        bytes_read = ::read(report_fd, &failure, sizeof(failure));
    while (bytes_read == -1 && errno == EINTR);

    bool child_reported_failure = bytes_read > 0;
    if (bytes_read < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
        handleUnreadableChildReport(child_pid, errno, own_process_group);

    if (child_reported_failure)
    {
        /// The child is gone; reap it so that it does not linger as a zombie, then report. Its group,
        /// if it has one, goes with it: a child that reports runs nothing but `execChild`, which
        /// starts no process, so there is nobody else in the group to end.
        int status = 0;
        while (-1 == ::waitpid(child_pid, &status, 0) && errno == EINTR)
        {
        }

        if (bytes_read == sizeof(failure))
            throw Exception(
                ErrorCodes::CANNOT_CREATE_CHILD_PROCESS,
                "Cannot {} in child process: {}",
                describe(static_cast<ChildSetupStep>(failure.step)),
                errnoToString(failure.error));

        throw Exception(ErrorCodes::CANNOT_CREATE_CHILD_PROCESS, "Cannot prepare child process: incomplete report from it");
    }
}

void ShellCommand::handleUnreadableChildReport(pid_t child_pid, int read_error, bool own_process_group)
{
    /// A read that failed for any other reason than an empty pipe says nothing about the child.
    /// What does is the child itself: by the time `vfork` returned, it had either `exec`ed - and is
    /// running, or has exited since - or written its report and exited. A non-blocking probe
    /// answers without the risk of blocking on a child that is alive and well, which a pool worker
    /// would be for as long as it is not asked to exit. And it reaps nothing (`peekChildState`): a
    /// child that is not reaped keeps its pid, and the number of the group it leads, to itself, so
    /// that the group can still be ended below before its leader goes.
    const ChildState state = peekChildState(child_pid, /*blocking=*/ false);

    if (state == ChildState::RUNNING)
    {
        LOG_WARNING(getLogger(), "Cannot read the child error pipe of pid {} ({}); the child is running, so it has started", child_pid, errnoToString(read_error));
        return;
    }

    /// Not ours any more: somebody else reaped it, and its pid may belong to anybody now, so it is
    /// neither signalled nor waited for.
    if (state == ChildState::NOT_OUR_CHILD)
        throw Exception(
            ErrorCodes::CANNOT_CREATE_CHILD_PROCESS,
            "Cannot prepare child process: its report could not be read ({}), and it is no longer a child of this process",
            errnoToString(read_error));

    /// This call fails either way, so no `ShellCommand` is constructed, and a child left here would
    /// have nobody to wait for it, nobody to signal it and no entry in the registry of UDF
    /// processes. A child that exited, or one nothing is known about (the probe failed), is ended
    /// together with its group before the failure is reported: an exited child may have `exec`ed
    /// and started processes of its own first - for a shared-memory UDF, processes that hold the
    /// region's descriptor, whose pages and charge the caller is about to release. Killing a
    /// zombie costs nothing, and this is the one place where killing is the conservative choice:
    /// the alternative is leaking the process.
    killAndReapOrphan(child_pid, own_process_group);

    if (state == ChildState::EXITED)
        throw Exception(
            ErrorCodes::CANNOT_CREATE_CHILD_PROCESS,
            "Cannot prepare child process: it has exited, and its report could not be read: {}",
            errnoToString(read_error));

    throw Exception(
        ErrorCodes::CANNOT_CREATE_CHILD_PROCESS,
        "Cannot prepare child process: its report could not be read ({}) and whether it started could "
        "not be established; it is signalled",
        errnoToString(read_error));
}


std::unique_ptr<ShellCommand> ShellCommand::execute(const ShellCommand::Config & config)
{
    auto config_copy = config;
    config_copy.command = "/bin/sh";
    config_copy.arguments = {"-c", config.command};

    for (const auto & argument : config.arguments)
        config_copy.arguments.emplace_back(argument);

    return executeDirect(config_copy);
}


std::unique_ptr<ShellCommand> ShellCommand::executeDirect(const ShellCommand::Config & config)
{
    const auto & path = config.command;
    const auto & arguments = config.arguments;

    size_t argv_sum_size = path.size() + 1;
    for (const auto & arg : arguments)
        argv_sum_size += arg.size() + 1;

    std::vector<char *> argv(arguments.size() + 2);
    std::vector<char> argv_data(argv_sum_size);
    WriteBufferFromPointer writer(argv_data.data(), argv_sum_size);

    argv[0] = writer.position();
    writer.write(path.data(), path.size() + 1);

    for (size_t i = 0, size = arguments.size(); i < size; ++i)
    {
        argv[i + 1] = writer.position();
        writer.write(arguments[i].data(), arguments[i].size() + 1);
    }

    writer.finalize();

    argv[arguments.size() + 1] = nullptr;

    return executeImpl(path.data(), argv.data(), config);
}

struct ShellCommand::tryWaitResult
{
    bool is_process_terminated = false;
    int retcode = -1;

    /// The raw `waitpid` status, kept so a caller that asked not to have it decoded here can decode
    /// it later - after it has read whatever the child left in its pipes.
    int raw_status = 0;
};

int ShellCommand::tryWait()
{
    return tryWaitImpl({.blocking = true}).retcode;
}

ShellCommand::tryWaitResult ShellCommand::tryWaitImpl(const TryWaitOptions & options)
{
    const bool blocking = options.blocking;
    if (blocking)
        LOG_TRACE(getLogger(), "Will wait for shell command pid {}", pid);

    ShellCommand::tryWaitResult result;

    int waitpid_flags = ((!blocking) ? WNOHANG : 0);
    int status = 0;
    int waitpid_retcode = -1;
    ::rusage local_rusage{};

    if (config.own_process_group)
    {
        /// Wait for the exit without reaping, and kill the group before the reap: once the child
        /// is reaped, the number of its group may belong to somebody else, and the descendants it
        /// left behind in it would be out of reach.
        const ChildState state = peekChildState(pid, blocking);
        if (state == ChildState::RUNNING)
        {
            result.is_process_terminated = false;
            return result;
        }
        if (state == ChildState::NOT_OUR_CHILD)
        {
            const int saved_errno = errno;
            forgetChild();
            errno = saved_errno;
            throw ErrnoException(ErrorCodes::CANNOT_WAITPID, "Cannot waitid");
        }
        /// The wait failed, but the child is still ours as far as anyone can tell: not forgotten.
        if (state == ChildState::UNKNOWN)
            throw ErrnoException(ErrorCodes::CANNOT_WAITPID, "Cannot waitid");
        killGroupOfExitedChild();
    }

    while (waitpid_retcode < 0)
    {
        /// Reap the child. With `Config::collect_resource_usage` (executable UDFs),
        /// use `wait4` to also collect the child's `rusage`: it is `waitpid` plus an
        /// `rusage` out-parameter and shares its pid/status/options/EINTR semantics.
        /// Without the flag, reap with plain `waitpid` and collect no usage.
        if (config.collect_resource_usage)
            waitpid_retcode = wait4(pid, &status, waitpid_flags, &local_rusage);
        else
            waitpid_retcode = waitpid(pid, &status, waitpid_flags);
        if (waitpid_retcode > 0)
        {
            /// A reaped pid may be reused immediately, so `wait_called` must be set the
            /// moment the child is reaped — before any operation that can throw — so the
            /// destructor never waits on or signals an unrelated process.
            forgetChild();
            if (config.collect_resource_usage)
            {
                child_user_time_us = static_cast<UInt64>(local_rusage.ru_utime.tv_sec) * 1000000ULL
                    + static_cast<UInt64>(local_rusage.ru_utime.tv_usec);
                child_system_time_us = static_cast<UInt64>(local_rusage.ru_stime.tv_sec) * 1000000ULL
                    + static_cast<UInt64>(local_rusage.ru_stime.tv_usec);
                child_resource_usage_captured = true;
            }
            break;
        }
        if (!blocking && !waitpid_retcode)
        {
            result.is_process_terminated = false;
            return result;
        }
        if (errno != EINTR)
        {
            /// Not a child of this process any more: its pid may be reused, so it must not be
            /// signalled or waited for again.
            if (errno == ECHILD)
            {
                const int saved_errno = errno;
                forgetChild();
                errno = saved_errno;
            }
            throw ErrnoException(ErrorCodes::CANNOT_WAITPID, "Cannot waitpid");
        }
    }

    LOG_TRACE(getLogger(), "Wait for shell command pid {} completed with status {}", pid, status);

    result.is_process_terminated = true;
    result.raw_status = status;

    /// Deliberately optional: see the declaration. A caller that still has to read what the child
    /// left in its pipes closes them itself, afterwards.
    if (options.close_streams)
        closeStreams();

    /// When `check_exit_status` is false the caller only wants the reaped `rusage`;
    /// skip decoding/validating the status so a non-zero or signalled child is not
    /// reported as an error.
    if (!options.check_exit_status)
        return result;

    if (WIFEXITED(status))
    {
        /// The return code is the caller's to judge (`handleProcessRetcode`).
        result.retcode = WEXITSTATUS(status);
        return result;
    }

    /// Every status that is not a normal exit throws.
    handleProcessStatus(status);
    return result;
}


void ShellCommand::handleProcessStatus(int status) const
{
    if (WIFEXITED(status))
    {
        handleProcessRetcode(WEXITSTATUS(status));
        return;
    }

    if (WIFSIGNALED(status))
        throw Exception(ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY, "Child process was terminated by signal {}", toString(WTERMSIG(status)));

    if (WIFSTOPPED(status))
        throw Exception(ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY, "Child process was stopped by signal {}", toString(WSTOPSIG(status)));

    throw Exception(ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY, "Child process was not exited normally by unknown reason");
}


void ShellCommand::handleProcessRetcode(int retcode) const
{
    /// Whatever the code, it is the command's own: a failure to prepare or `exec` the child is
    /// reported through the error pipe in `executeImpl` and never gets as far as an exit status.
    if (retcode != EXIT_SUCCESS)
        throw Exception(ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY, "Child process was exited with return code {}", toString(retcode));
}

bool ShellCommand::waitIfProccesTerminated()
{
    auto proc_status = tryWaitImpl({.blocking = false});
    if (proc_status.is_process_terminated)
    {
        handleProcessRetcode(proc_status.retcode);
    }
    return proc_status.is_process_terminated;
}


/// Closes one input of the command. One whose close fails - flushing what is still buffered, when its
/// reader is gone - is closed without it; the failure is kept in `first_failure` unless one is there
/// already, and a failure of that second close is only logged.
static void closeInput(WriteBufferFromFile & input, std::exception_ptr & first_failure, const LoggerPtr & log) noexcept
{
    try
    {
        input.close();
    }
    catch (...)
    {
        if (!first_failure)
            first_failure = std::current_exception();
        try
        {
            input.cancel();
            input.close();
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    }
}

void ShellCommand::closeStreamsNoThrow() noexcept
{
    /// Each on its own, so that one that fails does not leave the others open.
    try
    {
        closeInputs();
    }
    catch (...)
    {
        tryLogCurrentException(getLogger());
    }

    auto close_no_throw = [](auto & stream)
    {
        try
        {
            stream.close();
        }
        catch (...)
        {
            tryLogCurrentException(getLogger());
        }
    };
    close_no_throw(out);
    close_no_throw(err);
    for (auto & [_, fd] : read_fds)
        close_no_throw(fd);
}

void ShellCommand::closeStreams()
{
    in.close();
    out.close();
    err.close();

    for (auto & [_, fd] : write_fds)
        fd.close();

    for (auto & [_, fd] : read_fds)
        fd.close();
}


bool ShellCommand::tryWaitWithoutStatusCheck()
{
    /// A child that closed stdout but has only just called `_exit` is not yet a
    /// zombie, so a single `wait4(WNOHANG)` can miss it and lose its `rusage`. Poll
    /// until the shared termination deadline (`remainingTerminationTimeoutMs`), collecting the
    /// child's usage here via `wait4` instead of leaving it to the destructor's
    /// `waitForPid`, which collects none. The deadline is shared with the destructor,
    /// so a child that lingers past it is not double-charged: the destructor's own wait
    /// gets only the time remaining. A configured timeout of 0 means a single
    /// non-blocking attempt, so this never stalls a query beyond the configuration.
    static constexpr UInt64 poll_step_ms = 5;

    while (true)
    {
        if (tryWaitImpl({.blocking = false, .check_exit_status = false}).is_process_terminated)
            return true;

        const UInt64 remaining_ms = remainingTerminationTimeoutMs();
        if (remaining_ms == 0)
            return false;

        sleepForMilliseconds(std::min(poll_step_ms, remaining_ms));
    }
}


void ShellCommand::readBufferedOutput(int (&drain_fds)[2], const StderrSink & stderr_sink, size_t * stdout_bytes_drained) const
{
    char buffer[4096];
    for (size_t i = 0; i < 2; ++i)
    {
        if (drain_fds[i] < 0)
            continue;

        int available = 0;
        if (0 != ::ioctl(drain_fds[i], FIONREAD, &available))
        {
            /// Not skipped, and not read some other way: what the command said on its way out is
            /// what this read is for, and a pipe whose content cannot even be counted is a broken
            /// descriptor - which has to be seen, not worked around.
            const int saved_errno = errno;
            ErrnoException::throwWithErrno(
                ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, saved_errno,
                "Cannot query the pipe of shell command pid {} for buffered bytes", pid);
        }

        /// Nothing read (`EAGAIN` on a descriptor `FIONREAD` just said holds bytes - the count went
        /// stale - or EOF): the rest is left to the drain that follows, which polls, rather than
        /// retried here on the strength of a count that is wrong.
        while (available > 0 && drain_fds[i] >= 0)
        {
            const size_t bytes = readPipeOnce(
                drain_fds, i, buffer, std::min(sizeof(buffer), static_cast<size_t>(available)), stderr_sink, stdout_bytes_drained);
            if (bytes == 0)
                break;
            available -= static_cast<int>(bytes);
        }
    }
}

size_t ShellCommand::readPipeOnce(
    int (&drain_fds)[2], size_t i, char * buffer, size_t size, const StderrSink & stderr_sink, size_t * stdout_bytes_drained) const
{
    ssize_t res = 0;
    do
        res = ::read(drain_fds[i], buffer, size);
    while (res < 0 && errno == EINTR);

    if (res > 0)
    {
        /// `drain_fds[1]` is `stderr`, and it is the only one a caller can ask for: the child's
        /// `stdout` past this point is output the protocol did not ask for, and reading it is the
        /// whole reason it is read.
        if (i == 1 && stderr_sink)
            stderr_sink(std::string_view(buffer, static_cast<size_t>(res)));
        if (i == 0 && stdout_bytes_drained)
            *stdout_bytes_drained += static_cast<size_t>(res);
        return static_cast<size_t>(res);
    }

    /// Nothing there right now: the descriptor stays, for the next readiness report.
    if (res < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        return 0;

    if (res < 0)
        LOG_WARNING(getLogger(), "Cannot read a pipe of shell command pid {}, error: '{}'", pid, errnoToString());

    /// `res == 0` is EOF, and an error that is not a retryable one will not go away either: this
    /// descriptor has nothing more to give.
    drain_fds[i] = -1;
    return 0;
}

void ShellCommand::drainOutputPipes(
    int (&drain_fds)[2], const StderrSink & stderr_sink, UInt64 budget_ms, size_t * stdout_bytes_drained, int exit_fd) const
{
    static constexpr UInt64 poll_step_ms = 5;
    char discard_buffer[4096];

    const UInt64 deadline_ns = monotonicDeadlineNs(budget_ms);

    while (drain_fds[0] >= 0 || drain_fds[1] >= 0)
    {
        const UInt64 remaining_ms = millisecondsUntil(deadline_ns);
        if (remaining_ms == 0)
            return;

        /// With the child's `pidfd` in the set the poll wakes up for its exit as well, so it can
        /// wait out the whole budget at once; without it, it comes back in short steps to let the
        /// caller look for the exit.
        const UInt64 step_ms = exit_fd >= 0 ? remaining_ms : std::min<UInt64>(poll_step_ms, remaining_ms);

        pollfd pfds[3]{};
        for (size_t i = 0; i < 2; ++i)
        {
            /// `poll` ignores a negative descriptor and leaves its `revents` zero.
            pfds[i].fd = drain_fds[i];
            pfds[i].events = POLLIN;
        }
        pfds[2].fd = exit_fd;
        pfds[2].events = POLLIN;

        const int num_events = ::poll(pfds, 3, static_cast<int>(step_ms));
        if (num_events < 0)
        {
            if (errno == EINTR)
                continue;

            LOG_WARNING(getLogger(), "Cannot poll the pipes of shell command pid {}, error: '{}'", pid, errnoToString());
            return;
        }

        if (num_events == 0)
            continue;

        for (size_t i = 0; i < 2; ++i)
        {
            if (drain_fds[i] < 0)
                continue;

            /// One read per readiness report: `poll` promises only that a single read will not
            /// block, and these descriptors are not necessarily non-blocking.
            if ((pfds[i].revents & POLLIN) != 0)
                readPipeOnce(drain_fds, i, discard_buffer, sizeof(discard_buffer), stderr_sink, stdout_bytes_drained);
            else if ((pfds[i].revents & (POLLHUP | POLLERR | POLLNVAL)) != 0)
                drain_fds[i] = -1;
        }

        /// The child has exited: back to the caller, which reaps it. What it wrote is still read
        /// above first, in this round.
        if (pfds[2].revents != 0)
            return;
    }
}


void ShellCommand::checkCancelled(const std::function<void()> & check_cancelled)
{
    if (!check_cancelled)
        return;

    try
    {
        check_cancelled();
    }
    catch (...)
    {
        endTerminationGracePeriod();
        throw;
    }
}

bool ShellCommand::closeAbandonedStdout(int (&drain_fds)[2], const WaitDrainingOptions & options, size_t stdout_bytes_drained)
{
    /// A caller that does not want the exit status wants only what the child says on stderr on
    /// its way out. Two kinds of child have to be told apart by what they do with stdout in the
    /// meantime. One writes a stray line past the rows it was asked for and then its diagnostic to
    /// stderr: its stdout has to stay open and be read, or that stray write dies on `SIGPIPE` with
    /// the diagnostic still unwritten - and under `stderr_reaction` `throw` that diagnostic is the
    /// whole point. The other never stops writing (`LIMIT` over a command that produces forever):
    /// reading its stdout keeps it alive, and busy, for the whole termination budget, where it
    /// used to die at once on the first write into the closed pipe. The two are told apart by
    /// volume: a stray line or two is a few bytes, a stream is not, so stdout is read up to a
    /// pipe's worth of bytes and closed after that - the next write then hits the closed pipe and
    /// the child dies on `SIGPIPE`, as it did before this wait existed. That is the contract, and
    /// it is a narrower one than "late stderr is always seen": a command that writes more than a
    /// pipe's worth of output past its rows and *then* its diagnostic is treated as the endless
    /// kind, and the diagnostic is lost with it. The alternative - keeping stdout open for as long
    /// as a stderr sink is wanted - would make every `LIMIT` over a streaming command wait out
    /// the whole termination budget under the default `stderr_reaction`, which is the common
    /// case; a diagnostic behind 64 KiB of stray output is not. With the status checked, stdout
    /// stays open unless the caller explicitly abandoned the output early, such as under `LIMIT`.
    /// In that case the exit status is still checked, except for the `SIGPIPE` this close causes.
    static constexpr size_t stray_stdout_limit = 64 * 1024;

    /// An abandoned output is also closed once `command_termination_timeout` has passed,
    /// however little arrived: a producer that writes slowly would otherwise take hours to
    /// reach the limit, and keep the query waiting for all of them. A command that is still
    /// writing then dies on its next write; one that has stopped writing and only takes its
    /// time to exit does not notice, and is waited for as long as it takes.
    const bool stdout_abandoned_for_good = (!options.check_exit_status || options.limit_stdout_drain)
        && (stdout_bytes_drained > stray_stdout_limit || (options.limit_stdout_drain && remainingTerminationTimeoutMs() == 0));
    if (!stdout_abandoned_for_good || drain_fds[0] < 0)
        return false;

    /// A stdout the command has already closed itself is its own EOF, not ours: dropped
    /// from the set, and the exit grace of `waitDrainingOutput` still applies to it.
    bool closed_here = false;
    if (!pipeHasEnded(drain_fds[0]))
    {
        out.close();
        closed_here = true;
    }
    drain_fds[0] = -1;
    return closed_here;
}

void ShellCommand::waitForExitLeavingUnreaped(int exit_fd, UInt64 timeout_ms) const
{
    if (exit_fd >= 0)
    {
        pollfd pfd{};
        pfd.fd = exit_fd;
        pfd.events = POLLIN;
        if (::poll(&pfd, 1, static_cast<int>(timeout_ms)) < 0 && errno != EINTR)
            throw ErrnoException(ErrorCodes::CANNOT_WAITPID, "Cannot wait for shell command pid {}", pid);
        return;
    }

    /// `NOT_OUR_CHILD` is left to the reap at the top of the loop of `waitDrainingOutput`, which
    /// forgets the child.
    if (waitForPidMilliseconds(pid, timeout_ms, /*leave_unreaped=*/ true) == WaitForPidResult::ERROR)
        throw Exception(ErrorCodes::CANNOT_WAITPID, "Cannot wait for shell command pid {}", pid);
}

bool ShellCommand::waitInExitGrace(UInt64 & exit_grace_deadline_ns, int exit_fd) const
{
    static constexpr UInt64 exit_grace_ms = 1000;
    static constexpr UInt64 exit_grace_step_ms = 100;
    if (exit_grace_deadline_ns == 0)
        exit_grace_deadline_ns = monotonicDeadlineNs(exit_grace_ms);
    if (const UInt64 grace_left_ms = millisecondsUntil(exit_grace_deadline_ns))
    {
        waitForExitLeavingUnreaped(exit_fd, std::min(exit_grace_step_ms, grace_left_ms));
        return true;
    }
    return false;
}

bool ShellCommand::waitDrainingOutput(const WaitDrainingOptions & options)
{
    const auto & stderr_sink = options.stderr_sink;
    const bool check_exit_status = options.check_exit_status;
    const bool unbounded_status_wait = options.unbounded_status_wait;

    LOG_TRACE(getLogger(), "Will wait for shell command pid {} while draining its output", pid);
    /// A child that writes past what the protocol asked of it fills the pipe and blocks in `write`.
    /// Nothing reads that pipe any more by the time this is called, so the only way the child ever
    /// reaches its own exit is if the bytes keep being taken off the pipe here and thrown away.
    static constexpr UInt64 poll_step_ms = 5;

    /// The descriptors still worth draining, in the order they are polled. One that has hung up or
    /// reached EOF is dropped out of the set (-1, which `poll` ignores): `poll` reports a hung-up
    /// descriptor immediately and forever, so a child that closed its own output and then lingered
    /// would otherwise spin a core here for the whole termination budget.
    int drain_fds[2] = {out.getFD(), err.getFD()};

    /// What has come off stdout, and whether it has been closed here: a stdout that floods past
    /// what was asked of it, or that the caller abandoned, is closed rather than drained
    /// (`closeAbandonedStdout`).
    size_t stdout_bytes_drained = 0;
    bool stdout_closed_here = false;
    /// Set once the child's output has ended and the budget has run out - see below.
    UInt64 exit_grace_deadline_ns = 0;

    /// The child's `pidfd`, if the kernel gives one: polled together with the pipes, it lets the
    /// wait sleep until there is output or an exit to deal with, coming back only every
    /// `exit_fd_step_ms` to check for cancellation and the budget. Without it the loop comes back
    /// every `poll_step_ms` to look for the exit itself.
    int exit_fd = -1;
#if defined(OS_LINUX)
    exit_fd = openPidFdForWaiting(pid);
#endif
    SCOPE_EXIT({
        if (exit_fd >= 0 && 0 != ::close(exit_fd))
            LOG_WARNING(getLogger(), "Cannot close the pidfd of shell command pid {}: {}", pid, errnoToString());
    });
    static constexpr UInt64 exit_fd_step_ms = 100;
    const UInt64 wait_step_ms = exit_fd >= 0 ? exit_fd_step_ms : poll_step_ms;

    while (true)
    {
        checkCancelled(options.check_cancelled);

        if (closeAbandonedStdout(drain_fds, options, stdout_bytes_drained))
            stdout_closed_here = true;

        /// Reaped WITHOUT closing the pipes. Reaping is what makes the rest of what the child wrote
        /// final - its write ends are gone, so the pipes now hold exactly its last words and
        /// nothing more - but closing the descriptors here would throw those words away unread.
        /// Under `stderr_reaction` `throw` they are the whole reason the caller asked for a sink:
        /// a command that writes `boom` and exits in the same breath must not come out as a
        /// successful query. So: reap, then read to the end, then close.
        /// Reaped with the status check switched off no matter what the caller asked for: decoding
        /// it here can throw - a child killed by a signal does - and that would leave by the same
        /// door the unread bytes are still behind. The status is kept and decoded below, after the
        /// pipes have been read and closed, so `printf boom >&2; kill -TERM $$` reports both the
        /// signal and what the command said before it.
        auto proc_status = tryWaitImpl({.blocking = false, .check_exit_status = false, .close_streams = false});
        if (proc_status.is_process_terminated)
        {
            /// What the pipes hold at this moment is the command's last words, and it is read
            /// whole, with no deadline of any kind: the bytes are counted (`FIONREAD`) and read
            /// exactly - a pipe of a megabyte, a sink that takes its time, a thread that is not
            /// scheduled for a while, none of that may cost a command its `boom` under
            /// `stderr_reaction` `throw`. Nothing after that is the command's: it has exited, so
            /// everything it wrote is in the pipes already, and what may still arrive is written by
            /// a descendant that outlived it and inherited the write end. That is not waited for:
            /// the pipes are closed, and such a descendant gets `EPIPE` on its next write.
            readBufferedOutput(drain_fds, stderr_sink);
            closeStreams();

            /// A `SIGPIPE` after the abandoned stdout was closed here is not the command's failure
            /// but the way it was told to stop: the consumer already has what it wanted, and the
            /// command was still writing, so the status would otherwise depend on whether it
            /// happened to finish before `command_termination_timeout`. Any other status is still
            /// the command's own.
            if (check_exit_status && !(stdout_closed_here && diedOfSigpipe(proc_status.raw_status)))
                handleProcessStatus(proc_status.raw_status);
            return true;
        }

        /// The exit status of a non-pooled command was waited for without a bound before this wait
        /// existed, and a command whose cleanup outlasts `command_termination_timeout` must not
        /// start failing for it: that wait stays unbounded, only drained now (see the header). Only
        /// when the status is wanted, and only for the caller that says so: without the status
        /// this wait is for the child's last words on stderr, and a pooled worker being discarded
        /// was never waited for at all - neither may hang the query (and a pool's slot) forever
        /// over a child that does not exit on stdin EOF.
        const bool unbounded = check_exit_status && unbounded_status_wait;
        const UInt64 remaining_ms = unbounded ? wait_step_ms : remainingTerminationTimeoutMs();
        if (remaining_ms == 0)
        {
            /// Out of time for the exit, not for what has already arrived: whatever the child has
            /// written by now is sitting in the pipes, costs nothing to read (`FIONREAD`, exact
            /// reads), and under `stderr_reaction` `throw` is the verdict this wait exists to
            /// deliver. A grace period of zero in particular must not turn into "the last words
            /// are dropped".
            readBufferedOutput(drain_fds, stderr_sink, &stdout_bytes_drained);

            /// A child whose output has ended - both pipes at EOF, by its own doing - has made its last
            /// write and is, as a rule, on its way out: `exit` closes its descriptors before the
            /// kernel makes it a zombie, so a probe in between finds it alive, and failing a query
            /// for that instant would turn a budget that has run out (or a
            /// `command_termination_timeout` of zero) into a race. Such a child gets a short grace
            /// on top, in steps that come back here - for cancellation, and for the reap above. One
            /// that closed its output and goes on living is not given more than that.
            if (!stdout_closed_here && outputPipesHaveEnded(drain_fds) && waitInExitGrace(exit_grace_deadline_ns, exit_fd))
                continue;
            return false;
        }

        /// Capped so that a child which simply stops writing is still reaped promptly: a pipe that
        /// goes quiet reports nothing until its write end is closed, so without a `pidfd` the loop
        /// must come back to the `waitpid` above on its own.
        const UInt64 step_ms = std::min(remaining_ms, wait_step_ms);

        if (drain_fds[0] < 0 && drain_fds[1] < 0)
        {
            /// Nothing left to drain, only a child that has not exited yet. Wait for its exit
            /// itself (`pidfd`), left unreaped for the `waitpid` above to collect, and come back at
            /// least every `exit_wait_step_ms` to check for cancellation and the budget: there is
            /// nothing on the pipes to come back for sooner.
            static constexpr UInt64 exit_wait_step_ms = 100;
            waitForExitLeavingUnreaped(exit_fd, unbounded ? exit_wait_step_ms : std::min(remaining_ms, exit_wait_step_ms));
            continue;
        }

        drainOutputPipes(drain_fds, stderr_sink, step_ms, &stdout_bytes_drained, exit_fd);
    }
}


Int16 ShellCommand::pendingEvents(int fd) noexcept
{
    pollfd pfd{};
    pfd.fd = fd;
    pfd.events = POLLIN;

    int res = 0;
    do
    {
        pfd.revents = 0;
        res = ::poll(&pfd, 1, 0);
    }
    while (res < 0 && errno == EINTR);

    if (res < 0)
    {
        /// Answered as an error on the pipe, which every caller takes for a process not to be built
        /// on - but said here for what it is, or the discard would be reported as one that exited.
        const int saved_errno = errno;
        try
        {
            LOG_WARNING(getLogger(), "Cannot poll the pipe {} of a command: {}", fd, errnoToString(saved_errno));
        }
        catch (...) // NOLINT(bugprone-empty-catch) Ok: a log line that cannot be written must not fail a noexcept probe
        {
        }
        return POLLERR;
    }
    return static_cast<Int16>(pfd.revents);
}

bool ShellCommand::pipeHasEnded(int fd) noexcept
{
    const Int16 events = pendingEvents(fd);
    return (events & POLLHUP) != 0 && (events & POLLIN) == 0;
}

bool ShellCommand::outputPipesHaveEnded(const int (&drain_fds)[2]) const
{
    for (int fd : drain_fds)
    {
        /// Dropped from the set after its EOF (`drainOutputPipes`), or never there.
        if (fd >= 0 && !pipeHasEnded(fd))
            return false;
    }
    return true;
}

void ShellCommand::closeInputs()
{
    /// Every input is closed even if closing one fails: a command written to exit once all its
    /// inputs are done would otherwise wait for the rest until the destructor closes them. The first
    /// failure is reported after that.
    std::exception_ptr first_failure;
    closeInput(in, first_failure, getLogger());
    for (auto & [_, buffer] : write_fds)
        closeInput(buffer, first_failure, getLogger());
    if (first_failure)
        std::rethrow_exception(first_failure);
}


void ShellCommand::wait()
{
    int retcode = tryWaitImpl({.blocking = true}).retcode;
    handleProcessRetcode(retcode);
}


bool ShellCommand::wasChildResourceUsageCaptured() const noexcept
{
    return child_resource_usage_captured;
}


UInt64 ShellCommand::getChildUserTimeMicroseconds() const noexcept
{
    return child_user_time_us;
}


UInt64 ShellCommand::getChildSystemTimeMicroseconds() const noexcept
{
    return child_system_time_us;
}


}
