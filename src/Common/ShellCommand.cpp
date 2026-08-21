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
#include <dlfcn.h>
#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <limits>

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
    /// By these return codes from the child process, we learn (for sure) about errors when creating it.
    enum class ReturnCodes : int
    {
        CANNOT_DUP_STDIN            = 0x55555555,   /// The value is not important, but it is chosen so that it's rare to conflict with the program return code.
        CANNOT_DUP_STDOUT           = 0x55555556,
        CANNOT_DUP_STDERR           = 0x55555557,
        CANNOT_EXEC                 = 0x55555558,
        CANNOT_DUP_READ_DESCRIPTOR  = 0x55555559,
        CANNOT_DUP_WRITE_DESCRIPTOR = 0x55555560,
    };
}

namespace ProfileEvents
{
    extern const Event ExecuteShellCommand;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_DLSYM;
    extern const int CANNOT_FORK;
    extern const int CANNOT_WAITPID;
    extern const int CHILD_WAS_NOT_EXITED_NORMALLY;
    extern const int CANNOT_CREATE_CHILD_PROCESS;
    extern const int BAD_ARGUMENTS;
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

#if !defined(USE_MUSL)
    /** Here it is written that with a normal call `vfork`, there is a chance of deadlock in multithreaded programs,
      *  because of the resolving of symbols in the shared library
      * http://www.oracle.com/technetwork/server-storage/solaris10/subprocess-136439.html
      * Therefore, separate the resolving of the symbol from the call.
      */
    static void * real_vfork = dlsym(RTLD_DEFAULT, "vfork");
#else
    /// If we use Musl with static linking, there is no dlsym and no issue with vfork.
    static void * real_vfork = reinterpret_cast<void *>(&vfork);
#endif

    if (!real_vfork)
        throw ErrnoException(ErrorCodes::CANNOT_DLSYM, "Cannot find symbol vfork in myself");

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

    pid_t pid = reinterpret_cast<pid_t(*)()>(real_vfork)();

    if (pid == -1)
        throw ErrnoException(ErrorCodes::CANNOT_FORK, "Cannot vfork");

    if (0 == pid)
    {
        /// We are in the freshly created process.

        /// Why `_exit` and not `exit`? Because `exit` calls `atexit` and destructors of thread local storage.
        /// And there is a lot of garbage (including, for example, mutex is blocked). And this can not be done after `vfork` - deadlock happens.

        /// Replace the file descriptors with the ends of our pipes.
        if (STDIN_FILENO != dup2(pipe_stdin.fds_rw[0], STDIN_FILENO))
            _exit(static_cast<int>(ReturnCodes::CANNOT_DUP_STDIN));

        if (!config.pipe_stdin_only)
        {
            if (STDOUT_FILENO != dup2(pipe_stdout.fds_rw[1], STDOUT_FILENO))
                _exit(static_cast<int>(ReturnCodes::CANNOT_DUP_STDOUT));

            if (STDERR_FILENO != dup2(pipe_stderr.fds_rw[1], STDERR_FILENO))
                _exit(static_cast<int>(ReturnCodes::CANNOT_DUP_STDERR));
        }

        for (size_t i = 0; i < config.read_fds.size(); ++i)
        {
            auto & fds = *read_pipe_fds[i];
            auto fd = config.read_fds[i];

            if (fd != dup2(fds.fds_rw[1], fd))
                _exit(static_cast<int>(ReturnCodes::CANNOT_DUP_READ_DESCRIPTOR));
        }

        for (size_t i = 0; i < config.write_fds.size(); ++i)
        {
            auto & fds = *write_pipe_fds[i];
            auto fd = config.write_fds[i];

            if (fd != dup2(fds.fds_rw[0], fd))
                _exit(static_cast<int>(ReturnCodes::CANNOT_DUP_WRITE_DESCRIPTOR));
        }

        // Reset the signal mask: it may be non-empty and will be inherited
        // by the child process, which might not expect this.
        sigset_t mask;
        sigemptyset(&mask);
        sigprocmask(0, nullptr, &mask); // NOLINT(concurrency-mt-unsafe)
        sigprocmask(SIG_UNBLOCK, &mask, nullptr); // NOLINT(concurrency-mt-unsafe)

        execv(filename, argv);
        /// If the process is running, then `execv` does not return here.

        _exit(static_cast<int>(ReturnCodes::CANNOT_EXEC));
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
};

int ShellCommand::tryWait()
{
    return tryWaitImpl(true).retcode;
}

ShellCommand::tryWaitResult ShellCommand::tryWaitImpl(bool blocking, bool check_exit_status)
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
    in.close();
    out.close();
    err.close();

    for (auto & [_, fd] : write_fds)
        fd.close();

    for (auto & [_, fd] : read_fds)
        fd.close();

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


void ShellCommand::handleProcessRetcode(int retcode) const
{
    if (retcode != EXIT_SUCCESS)
    {
        switch (retcode)
        {
            case static_cast<int>(ReturnCodes::CANNOT_DUP_STDIN):
                throw Exception(ErrorCodes::CANNOT_CREATE_CHILD_PROCESS, "Cannot dup2 stdin of child process");
            case static_cast<int>(ReturnCodes::CANNOT_DUP_STDOUT):
                throw Exception(ErrorCodes::CANNOT_CREATE_CHILD_PROCESS, "Cannot dup2 stdout of child process");
            case static_cast<int>(ReturnCodes::CANNOT_DUP_STDERR):
                throw Exception(ErrorCodes::CANNOT_CREATE_CHILD_PROCESS, "Cannot dup2 stderr of child process");
            case static_cast<int>(ReturnCodes::CANNOT_EXEC):
                throw Exception(ErrorCodes::CANNOT_CREATE_CHILD_PROCESS, "Cannot execv in child process");
            case static_cast<int>(ReturnCodes::CANNOT_DUP_READ_DESCRIPTOR):
                throw Exception(ErrorCodes::CANNOT_CREATE_CHILD_PROCESS, "Cannot dup2 read descriptor of child process");
            case static_cast<int>(ReturnCodes::CANNOT_DUP_WRITE_DESCRIPTOR):
                throw Exception(ErrorCodes::CANNOT_CREATE_CHILD_PROCESS, "Cannot dup2 write descriptor of child process");
            default:
                throw Exception(ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY, "Child process was exited with return code {}", toString(retcode));
        }
    }
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

