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
#include <sys/wait.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <limits>

#include <base/scope_guard.h>
#include <base/sleep.h>
#include <Common/logger_useful.h>
#include <base/errnoToString.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/ShellCommand.h>
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
    const UInt64 now_ns = clock_gettime_ns();

    /// Arm the shared deadline once, on the first waiter (cleanup, or the destructor when
    /// cleanup never ran). Every later waiter subtracts the time already spent so both the
    /// cleanup poll and the destructor wait draw from one `command_termination_timeout`.
    if (termination_deadline_ns == 0)
        termination_deadline_ns
            = now_ns + config.terminate_in_destructor_strategy.wait_for_normal_exit_before_termination_seconds * 1000000000ULL;

    if (now_ns >= termination_deadline_ns)
        return 0;

    return (termination_deadline_ns - now_ns) / 1000000ULL;
}

ShellCommand::~ShellCommand()
{
    if (do_not_terminate)
        return;

    if (wait_called)
        return;

    if (config.terminate_in_destructor_strategy.terminate_in_destructor)
    {
        /// Draw from the shared deadline: the cleanup-side wait may have already spent
        /// most of `command_termination_timeout`, so this wait gets only what remains and
        /// the configured grace period is honored once, not doubled. `waitForPid` is
        /// second-granular, so round the remainder up to keep at least one poll when any
        /// budget is left.
        size_t try_wait_timeout = (remainingTerminationTimeoutMs() + 999) / 1000;
        bool process_terminated_normally = tryWaitProcessWithTimeout(try_wait_timeout);

        if (process_terminated_normally)
            return;

        LOG_TRACE(getLogger(), "Will kill shell command pid {} with signal {}", pid, config.terminate_in_destructor_strategy.termination_signal);

        int retcode = kill(pid, config.terminate_in_destructor_strategy.termination_signal);
        if (retcode != 0)
            LOG_WARNING(getLogger(), "Cannot kill shell command pid {}, error: '{}'", pid, errnoToString());
    }
    else
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
}

bool ShellCommand::tryWaitProcessWithTimeout(size_t timeout_in_seconds)
{
    LOG_TRACE(getLogger(), "Try wait for shell command pid {} with timeout {} (seconds)", pid, timeout_in_seconds);

    wait_called = true;

    in.close();
    out.close();
    err.close();

    for (auto & [_, fd] : write_fds)
        fd.close();

    for (auto & [_, fd] : read_fds)
        fd.close();

    bool process_terminated_normally = waitForPid(pid, timeout_in_seconds);

    if (process_terminated_normally && config.register_in_udf_process_registry)
        UDFProcessRegistry::instance().removeIfGenerationMatches(pid, udf_registry_generation);

    return process_terminated_normally;
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

std::unique_ptr<ShellCommand> ShellCommand::executeImpl(
    const char * filename,
    char * const argv[],
    const Config & config)
{
    logCommand(filename, argv);
    ProfileEvents::increment(ProfileEvents::ExecuteShellCommand);

    PipeFDs pipe_stdin;
    PipeFDs pipe_stdout;
    PipeFDs pipe_stderr;

    std::vector<std::unique_ptr<PipeFDs>> read_pipe_fds;
    std::vector<std::unique_ptr<PipeFDs>> write_pipe_fds;

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

    /// The inherited descriptors are handed over in two steps, and the first one happens here,
    /// before `vfork`, where it is allowed to fail with an exception. A plain `dup2(parent_fd,
    /// child_fd)` in the child is wrong in two ways that a caller cannot rule out: when
    /// `parent_fd == child_fd` (the region's `memfd` happened to be created as 3) `dup2` is a
    /// no-op and the descriptor keeps its close-on-exec flag, so `exec` closes it; and when one
    /// pair's target is another pair's source (`{3 <- 4}, {4 <- 3}`) the first `dup2` overwrites
    /// what the second was going to copy. So every source is first duplicated to a number above
    /// every target - any target, including the ones `read_fds`/`write_fds` claim - and the child
    /// `dup2`s from those copies, which can neither be a target nor be clobbered by one. The copies
    /// are close-on-exec: they must not outlive this `exec` in any child, and they are closed in
    /// the parent once the child has run.
    std::vector<int> staged_inherited_fds;
    staged_inherited_fds.reserve(config.inherited_fds.size());
    SCOPE_EXIT({
        for (int fd : staged_inherited_fds)
            if (0 != ::close(fd))
                LOG_WARNING(getLogger(), "Cannot close a staged inherited descriptor: {}", errnoToString());
    });

    /// The first number above every descriptor the child is going to install something under.
    int first_free_fd = STDERR_FILENO + 1;
    for (const auto & [child_fd, parent_fd] : config.inherited_fds)
    {
        if (child_fd <= STDERR_FILENO)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot hand descriptor {} to a child as {}: 0, 1 and 2 are the child's standard streams", parent_fd, child_fd);
        first_free_fd = std::max(first_free_fd, child_fd + 1);
    }
    for (int fd : config.read_fds)
        first_free_fd = std::max(first_free_fd, fd + 1);
    for (int fd : config.write_fds)
        first_free_fd = std::max(first_free_fd, fd + 1);

    for (const auto & [child_fd, parent_fd] : config.inherited_fds)
    {
        int staged = ::fcntl(parent_fd, F_DUPFD_CLOEXEC, first_free_fd);
        if (staged == -1)
            throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot duplicate descriptor {} to hand it to a child as {}", parent_fd, child_fd);
        staged_inherited_fds.push_back(staged);
    }

    /// How the child reports a failure of any step below: a close-on-exec pipe. A successful
    /// `exec` closes the child's end and the parent reads EOF; a failure writes the step and the
    /// `errno` and the parent reads those. The child's copy of the write end is staged above every
    /// target like the inherited descriptors are, so that no `dup2` below lands on it - it would
    /// otherwise be silently replaced by whatever was installed under that number, and a later
    /// failure would write its report into a pipe or a region instead. (The pipe itself is opened
    /// with `O_CLOEXEC`, and `F_DUPFD_CLOEXEC` keeps the copy so.) Both of the parent's write ends
    /// are closed before the parent reads, or the read would never see EOF.
    PipeFDs pipe_child_error;
    const int child_error_fd = ::fcntl(pipe_child_error.fds_rw[1], F_DUPFD_CLOEXEC, first_free_fd);
    if (child_error_fd == -1)
        throw ErrnoException(ErrorCodes::CANNOT_FCNTL, "Cannot duplicate the child error pipe");
    staged_inherited_fds.push_back(child_error_fd);

    /// `vfork` must be called directly, not through a pointer obtained with `dlsym`: the compiler
    /// knows `vfork` as a function that returns twice, and only a call it can see as such makes
    /// it keep the stack slots of this frame intact across the child's execution. The child runs
    /// the block below on this very frame, and the codegen treats that block as one that never
    /// comes back (it ends with `_exit`), so without the attribute it happily reuses the spill
    /// slot of a value the block no longer needs - the `config` reference, say - for one of its
    /// own temporaries. The child then `exec`s, the parent wakes up, and reads garbage from its
    /// own frame. This is not a theoretical concern: the MemorySanitizer build did exactly that
    /// in the loop over `inherited_fds` below.
    ///
    /// The pointer from `dlsym` also hid the call from the static analyzer, which has two things
    /// to say about `vfork`. That `posix_spawn` is the safer API: it is, and moving this code to
    /// it is a change of its own; until then this is the one place in the server that spawns,
    /// and it is written with the care `vfork` demands. And that nothing but `exec`/`_exit` may
    /// be called after it: the child below makes only the calls `posix_spawn` itself makes in its
    /// own child - `dup2`, `close`, `sigprocmask`, all async-signal-safe - and touches nothing
    /// the parent shares beyond the descriptor table, which is the child's own. Suppressed, not
    /// hidden.
    pid_t pid = vfork(); // NOLINT(bugprone-unsafe-functions,cert-msc24-c,cert-msc33-c,clang-analyzer-security.insecureAPI.vfork)

    if (pid == -1)
        throw ErrnoException(ErrorCodes::CANNOT_FORK, "Cannot vfork");

    if (0 == pid)
    {
        /// We are in the freshly created process.
        /// NOLINTBEGIN(clang-analyzer-unix.Vfork)

        /// Why `_exit` and not `exit`? Because `exit` calls `atexit` and destructors of thread local storage.
        /// And there is a lot of garbage (including, for example, mutex is blocked). And this can not be done after `vfork` - deadlock happens.

        /// Replace the file descriptors with the ends of our pipes.
        if (STDIN_FILENO != dup2(pipe_stdin.fds_rw[0], STDIN_FILENO))
            reportChildSetupFailureAndExit(child_error_fd, ChildSetupStep::DUP_STDIN);

        if (!config.pipe_stdin_only)
        {
            if (STDOUT_FILENO != dup2(pipe_stdout.fds_rw[1], STDOUT_FILENO))
                reportChildSetupFailureAndExit(child_error_fd, ChildSetupStep::DUP_STDOUT);

            if (STDERR_FILENO != dup2(pipe_stderr.fds_rw[1], STDERR_FILENO))
                reportChildSetupFailureAndExit(child_error_fd, ChildSetupStep::DUP_STDERR);
        }

        for (size_t i = 0; i < config.read_fds.size(); ++i)
        {
            auto & fds = *read_pipe_fds[i];
            auto fd = config.read_fds[i];

            if (fd != dup2(fds.fds_rw[1], fd))
                reportChildSetupFailureAndExit(child_error_fd, ChildSetupStep::DUP_READ_DESCRIPTOR);
        }

        for (size_t i = 0; i < config.write_fds.size(); ++i)
        {
            auto & fds = *write_pipe_fds[i];
            auto fd = config.write_fds[i];

            if (fd != dup2(fds.fds_rw[0], fd))
                reportChildSetupFailureAndExit(child_error_fd, ChildSetupStep::DUP_WRITE_DESCRIPTOR);
        }

        for (size_t i = 0; i < config.inherited_fds.size(); ++i)
        {
            /// `dup2` from the staged copy (see above) onto the number the child expects. The
            /// staged copy is above every target, so this is never a no-op and never destroys a
            /// source. The result has no close-on-exec flag, so it survives the `exec` below; the
            /// staged copy does not.
            const int child_fd = config.inherited_fds[i].first;
            if (child_fd != dup2(staged_inherited_fds[i], child_fd))
                reportChildSetupFailureAndExit(child_error_fd, ChildSetupStep::DUP_INHERITED_DESCRIPTOR);
        }

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
            if (!leave_alone && 0 != ::close(parent_fd))
                reportChildSetupFailureAndExit(child_error_fd, ChildSetupStep::CLOSE_INHERITED_DESCRIPTOR);
        }

        // Reset the signal mask: it may be non-empty and will be inherited
        // by the child process, which might not expect this.
        sigset_t mask;
        sigemptyset(&mask);
        sigprocmask(0, nullptr, &mask); // NOLINT(concurrency-mt-unsafe)
        sigprocmask(SIG_UNBLOCK, &mask, nullptr); // NOLINT(concurrency-mt-unsafe)

        execv(filename, argv);
        /// If the process is running, then `execv` does not return here.

        reportChildSetupFailureAndExit(child_error_fd, ChildSetupStep::EXEC);
        /// NOLINTEND(clang-analyzer-unix.Vfork)
    }

    /// The child has either `exec`ed or written its report and exited (that is what `vfork`
    /// guarantees by the time it returns in the parent), so this read does not wait on anything:
    /// once the parent's own write ends are closed, the pipe holds either the report or nothing.
    {
        if (0 != ::close(child_error_fd))
            LOG_WARNING(getLogger(), "Cannot close the child error pipe: {}", errnoToString());
        staged_inherited_fds.pop_back();
        if (0 != ::close(pipe_child_error.fds_rw[1]))
            LOG_WARNING(getLogger(), "Cannot close the child error pipe: {}", errnoToString());
        pipe_child_error.fds_rw[1] = -1;

        ChildSetupFailure failure{};
        ssize_t bytes_read = 0;
        do
            bytes_read = ::read(pipe_child_error.fds_rw[0], &failure, sizeof(failure));
        while (bytes_read == -1 && errno == EINTR);

        if (bytes_read != 0)
        {
            /// The child is gone; reap it so that it does not linger as a zombie, then report.
            int status = 0;
            while (-1 == ::waitpid(pid, &status, 0) && errno == EINTR)
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

    std::unique_ptr<ShellCommand> res(new ShellCommand(
        pid,
        pipe_stdin.fds_rw[1],
        pipe_stdout.fds_rw[0],
        pipe_stderr.fds_rw[0],
        config));

    if (config.register_in_udf_process_registry)
        res->udf_registry_generation = UDFProcessRegistry::instance().add(pid);

    for (size_t i = 0; i < config.read_fds.size(); ++i)
    {
        auto & fds = *read_pipe_fds[i];
        auto fd = config.read_fds[i];
        res->read_fds.emplace(fd, fds.fds_rw[0]);
    }

    for (size_t i = 0; i < config.write_fds.size(); ++i)
    {
        auto & fds = *write_pipe_fds[i];
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
    return tryWaitImpl(true).retcode;
}

ShellCommand::tryWaitResult ShellCommand::tryWaitImpl(bool blocking, bool check_exit_status, bool close_streams)
{
    LOG_TRACE(getLogger(), "Will wait for shell command pid {}", pid);

    ShellCommand::tryWaitResult result;

    int options = ((!blocking) ? WNOHANG : 0);
    int status = 0;
    int waitpid_retcode = -1;
    ::rusage local_rusage{};

    while (waitpid_retcode < 0)
    {
        /// Reap the child. With `Config::collect_resource_usage` (executable UDFs),
        /// use `wait4` to also collect the child's `rusage`: it is `waitpid` plus an
        /// `rusage` out-parameter and shares its pid/status/options/EINTR semantics.
        /// Without the flag, reap with plain `waitpid` and collect no usage.
        if (config.collect_resource_usage)
            waitpid_retcode = wait4(pid, &status, options, &local_rusage);
        else
            waitpid_retcode = waitpid(pid, &status, options);
        if (waitpid_retcode > 0)
        {
            /// A reaped pid may be reused immediately, so `wait_called` must be set the
            /// moment the child is reaped — before any operation that can throw — so the
            /// destructor never waits on or signals an unrelated process.
            wait_called = true;
            if (config.register_in_udf_process_registry)
                UDFProcessRegistry::instance().removeIfGenerationMatches(pid, udf_registry_generation);
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
            throw ErrnoException(ErrorCodes::CANNOT_WAITPID, "Cannot waitpid");
    }

    LOG_TRACE(getLogger(), "Wait for shell command pid {} completed with status {}", pid, status);

    result.is_process_terminated = true;
    result.raw_status = status;

    /// Deliberately optional: see the declaration. A caller that still has to read what the child
    /// left in its pipes closes them itself, afterwards.
    if (close_streams)
        closeStreams();

    /// When `check_exit_status` is false the caller only wants the reaped `rusage`;
    /// skip decoding/validating the status so a non-zero or signalled child is not
    /// reported as an error.
    if (!check_exit_status)
        return result;

    if (WIFEXITED(status))
    {
        result.retcode = WEXITSTATUS(status);
        return result;
    }

    if (WIFSIGNALED(status))
        throw Exception(ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY, "Child process was terminated by signal {}", toString(WTERMSIG(status)));

    if (WIFSTOPPED(status))
        throw Exception(ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY, "Child process was stopped by signal {}", toString(WSTOPSIG(status)));

    throw Exception(ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY, "Child process was not exited normally by unknown reason");
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
    auto proc_status = tryWaitImpl(false);
    if (proc_status.is_process_terminated)
    {
        handleProcessRetcode(proc_status.retcode);
    }
    return proc_status.is_process_terminated;
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
        if (tryWaitImpl(/*blocking=*/false, /*check_exit_status=*/false).is_process_terminated)
            return true;

        const UInt64 remaining_ms = remainingTerminationTimeoutMs();
        if (remaining_ms == 0)
            return false;

        sleepForMilliseconds(std::min(poll_step_ms, remaining_ms));
    }
}


void ShellCommand::drainOutputPipes(
    int (&drain_fds)[2], const StderrSink & stderr_sink, UInt64 budget_ms, bool budget_is_quiet_time, UInt64 max_total_ms) const
{
    static constexpr UInt64 poll_step_ms = 5;
    char discard_buffer[4096];

    const UInt64 start_ns = clock_gettime_ns();
    UInt64 deadline_ns = start_ns + budget_ms * 1000000ULL;
    const UInt64 hard_deadline_ns = max_total_ms ? start_ns + max_total_ms * 1000000ULL : std::numeric_limits<UInt64>::max();

    while (drain_fds[0] >= 0 || drain_fds[1] >= 0)
    {
        const UInt64 now_ns = clock_gettime_ns();
        if (now_ns >= deadline_ns || now_ns >= hard_deadline_ns)
            return;

        const UInt64 step_ms = std::min<UInt64>(poll_step_ms, (deadline_ns - now_ns) / 1000000ULL + 1);

        pollfd pfds[2]{};
        for (size_t i = 0; i < 2; ++i)
        {
            /// `poll` ignores a negative descriptor and leaves its `revents` zero.
            pfds[i].fd = drain_fds[i];
            pfds[i].events = POLLIN;
        }

        const int num_events = ::poll(pfds, 2, static_cast<int>(step_ms));
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

            if ((pfds[i].revents & POLLIN) != 0)
            {
                /// One read per readiness report: `poll` promises only that a single read will not
                /// block, and these descriptors are not necessarily non-blocking.
                const ssize_t res = ::read(drain_fds[i], discard_buffer, sizeof(discard_buffer));
                if (res > 0)
                {
                    /// `drain_fds[1]` is `stderr`, and it is the only one a caller can ask for: the
                    /// child's `stdout` past this point is output the protocol did not ask for, and
                    /// reading it is the whole reason this loop exists.
                    if (i == 1 && stderr_sink)
                        stderr_sink(std::string_view(discard_buffer, static_cast<size_t>(res)));

                    /// Bytes arrived, so the quiet time starts over: a full pipe is read whole
                    /// however long the reads take to get scheduled, and the budget is only ever
                    /// spent waiting for bytes that do not come.
                    if (budget_is_quiet_time)
                        deadline_ns = clock_gettime_ns() + budget_ms * 1000000ULL;
                    continue;
                }

                if (res < 0)
                {
                    if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                        continue;

                    LOG_WARNING(
                        getLogger(), "Cannot drain a pipe of shell command pid {}, error: '{}'", pid, errnoToString());
                }

                /// `res == 0` is EOF, and an error that is not one of the retryable ones will not
                /// go away either: this descriptor has nothing more to give.
                drain_fds[i] = -1;
            }
            else if ((pfds[i].revents & (POLLHUP | POLLERR | POLLNVAL)) != 0)
            {
                drain_fds[i] = -1;
            }
        }
    }
}


void ShellCommand::drainPendingOutput(const StderrSink & stderr_sink, UInt64 budget_ms) const
{
    if (wait_called)
        return;

    int drain_fds[2] = {out.getFD(), err.getFD()};
    drainOutputPipes(drain_fds, stderr_sink, budget_ms);
}


bool ShellCommand::waitDrainingOutput(const StderrSink & stderr_sink, bool check_exit_status)
{
    /// A child that writes past what the protocol asked of it fills the pipe and blocks in `write`.
    /// Nothing reads that pipe any more by the time this is called, so the only way the child ever
    /// reaches its own exit is if the bytes keep being taken off the pipe here and thrown away.
    static constexpr UInt64 poll_step_ms = 5;

    /// The descriptors still worth draining, in the order they are polled. One that has hung up or
    /// reached EOF is dropped out of the set (-1, which `poll` ignores): `poll` reports a hung-up
    /// descriptor immediately and forever, so a child that closed its own output and then lingered
    /// would otherwise spin a core here for the whole termination budget.
    int drain_fds[2] = {out.getFD(), err.getFD()};

    while (true)
    {
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
        auto proc_status = tryWaitImpl(/*blocking=*/ false, /*check_exit_status=*/ false, /*close_streams=*/ false);
        if (proc_status.is_process_terminated)
        {
            /// Given a budget of its own rather than what is left of `command_termination_timeout`.
            /// That budget is about how long the command is allowed to take to *exit*, and it has
            /// already exited - it is routinely zero by this point, and zero here would mean the
            /// bytes are read only when the command happened to be slow. And it is a budget of
            /// quiet time, not of wall time: what the pipes hold is read whole - a pipe of a
            /// megabyte, a sink that takes its time, a thread that is not scheduled for a while,
            /// none of that may cost the command its last words under `stderr_reaction` `throw`.
            /// Only the wait for bytes that do not come is bounded, for the case where a
            /// grandchild inherited the write end and the end never comes; and a grandchild that
            /// keeps the pipe fed instead runs into the hard cap, because what it writes ten
            /// seconds after the command exited is not the command's.
            static constexpr UInt64 post_reap_quiet_ms = 100;
            static constexpr UInt64 post_reap_max_total_ms = 10000;
            drainOutputPipes(drain_fds, stderr_sink, post_reap_quiet_ms, /*budget_is_quiet_time=*/ true, post_reap_max_total_ms);
            closeStreams();

            if (check_exit_status)
                handleProcessStatus(proc_status.raw_status);
            return true;
        }

        const UInt64 remaining_ms = remainingTerminationTimeoutMs();
        if (remaining_ms == 0)
            return false;

        /// Capped so that a child which simply stops writing is still reaped promptly: a pipe that
        /// goes quiet reports nothing until its write end is closed, so the loop must come back to
        /// the `waitpid` above on its own.
        const UInt64 step_ms = std::min(remaining_ms, poll_step_ms);

        if (drain_fds[0] < 0 && drain_fds[1] < 0)
        {
            /// Nothing left to drain, only a child that has not exited yet. Wait out the rest of
            /// the budget in the same steps rather than polling an empty set in a tight loop.
            sleepForMilliseconds(step_ms);
            continue;
        }

        drainOutputPipes(drain_fds, stderr_sink, step_ms);
    }
}

void ShellCommand::wait()
{
    int retcode = tryWaitImpl(true).retcode;
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

