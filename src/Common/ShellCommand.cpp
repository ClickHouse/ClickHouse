#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <dlfcn.h>
#include <unistd.h>
#include <time.h>
#include <csignal>

#include <base/logger_useful.h>
#include <base/errnoToString.h>
#include <Common/Exception.h>
#include <Common/ShellCommand.h>
#include <Common/PipeFDs.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>


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

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_DLSYM;
    extern const int CANNOT_FORK;
    extern const int CANNOT_WAITPID;
    extern const int CHILD_WAS_NOT_EXITED_NORMALLY;
    extern const int CANNOT_CREATE_CHILD_PROCESS;
}

ShellCommand::ShellCommand(pid_t pid_, int & in_fd_, int & out_fd_, int & err_fd_, const ShellCommand::Config & config_)
    : in(in_fd_)
    , out(out_fd_)
    , err(err_fd_)
    , pid(pid_)
    , config(config_)
{
}

Poco::Logger * ShellCommand::getLogger()
{
    return &Poco::Logger::get("ShellCommand");
}

ShellCommand::~ShellCommand()
{
    if (wait_called)
        return;

    if (config.terminate_in_destructor_strategy.terminate_in_destructor)
    {
        size_t try_wait_timeout = config.terminate_in_destructor_strategy.wait_for_normal_exit_before_termination_seconds;
        bool process_terminated_normally = tryWaitProcessWithTimeout(try_wait_timeout);

        if (!process_terminated_normally)
        {
            LOG_TRACE(getLogger(), "Will kill shell command pid {} with SIGTERM", pid);

            int retcode = kill(pid, SIGTERM);
            if (retcode != 0)
                LOG_WARNING(getLogger(), "Cannot kill shell command pid {} errno '{}'", pid, errnoToString(retcode));
        }
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
    int status = 0;

    LOG_TRACE(getLogger(), "Try wait for shell command pid ({}) with timeout ({})", pid, timeout_in_seconds);

    wait_called = true;
    struct timespec interval {.tv_sec = 1, .tv_nsec = 0};

    in.close();
    out.close();
    err.close();

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

    while (timeout_in_seconds != 0)
    {
        int waitpid_res = waitpid(pid, &status, WNOHANG);
        bool process_terminated_normally = (waitpid_res == pid);

        if (process_terminated_normally)
            return true;
        else if (waitpid_res == 0)
        {
            --timeout_in_seconds;
            nanosleep(&interval, nullptr);

            continue;
        }
        else if (waitpid_res == -1 && errno != EINTR)
            return false;
    }

    return false;
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

    /** Here it is written that with a normal call `vfork`, there is a chance of deadlock in multithreaded programs,
      *  because of the resolving of symbols in the shared library
      * http://www.oracle.com/technetwork/server-storage/solaris10/subprocess-136439.html
      * Therefore, separate the resolving of the symbol from the call.
      */
    static void * real_vfork = dlsym(RTLD_DEFAULT, "vfork");

    if (!real_vfork)
        throwFromErrno("Cannot find symbol vfork in myself", ErrorCodes::CANNOT_DLSYM);

    PipeFDs pipe_stdin;
    PipeFDs pipe_stdout;
    PipeFDs pipe_stderr;

    std::vector<std::unique_ptr<PipeFDs>> read_pipe_fds;
    std::vector<std::unique_ptr<PipeFDs>> write_pipe_fds;

    for (size_t i = 0; i < config.read_fds.size(); ++i)
        read_pipe_fds.emplace_back(std::make_unique<PipeFDs>());

    for (size_t i = 0; i < config.write_fds.size(); ++i)
        write_pipe_fds.emplace_back(std::make_unique<PipeFDs>());

    pid_t pid = reinterpret_cast<pid_t(*)()>(real_vfork)();

    if (pid == -1)
        throwFromErrno("Cannot vfork", ErrorCodes::CANNOT_FORK);

    if (0 == pid)
    {
        /// We are in the freshly created process.

        /// Why `_exit` and not `exit`? Because `exit` calls `atexit` and destructors of thread local storage.
        /// And there is a lot of garbage (including, for example, mutex is blocked). And this can not be done after `vfork` - deadlock happens.

        /// Replace the file descriptors with the ends of our pipes.
        if (STDIN_FILENO != dup2(pipe_stdin.fds_rw[0], STDIN_FILENO))
            _exit(int(ReturnCodes::CANNOT_DUP_STDIN));

        if (!config.pipe_stdin_only)
        {
            if (STDOUT_FILENO != dup2(pipe_stdout.fds_rw[1], STDOUT_FILENO))
                _exit(int(ReturnCodes::CANNOT_DUP_STDOUT));

            if (STDERR_FILENO != dup2(pipe_stderr.fds_rw[1], STDERR_FILENO))
                _exit(int(ReturnCodes::CANNOT_DUP_STDERR));
        }

        for (size_t i = 0; i < config.read_fds.size(); ++i)
        {
            auto & fds = *read_pipe_fds[i];
            auto fd = config.read_fds[i];

            if (fd != dup2(fds.fds_rw[1], fd))
                _exit(int(ReturnCodes::CANNOT_DUP_READ_DESCRIPTOR));
        }

        for (size_t i = 0; i < config.write_fds.size(); ++i)
        {
            auto & fds = *write_pipe_fds[i];
            auto fd = config.write_fds[i];

            if (fd != dup2(fds.fds_rw[0], fd))
                _exit(int(ReturnCodes::CANNOT_DUP_WRITE_DESCRIPTOR));
        }

        // Reset the signal mask: it may be non-empty and will be inherited
        // by the child process, which might not expect this.
        sigset_t mask;
        sigemptyset(&mask);
        sigprocmask(0, nullptr, &mask);
        sigprocmask(SIG_UNBLOCK, &mask, nullptr);

        execv(filename, argv);
        /// If the process is running, then `execv` does not return here.

        _exit(int(ReturnCodes::CANNOT_EXEC));
    }

    std::unique_ptr<ShellCommand> res(new ShellCommand(
        pid,
        pipe_stdin.fds_rw[1],
        pipe_stdout.fds_rw[0],
        pipe_stderr.fds_rw[0],
        config));

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

    LOG_TRACE(getLogger(), "Started shell command '{}' with pid {}", filename, pid);
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
    WriteBuffer writer(argv_data.data(), argv_sum_size);

    argv[0] = writer.position();
    writer.write(path.data(), path.size() + 1);

    for (size_t i = 0, size = arguments.size(); i < size; ++i)
    {
        argv[i + 1] = writer.position();
        writer.write(arguments[i].data(), arguments[i].size() + 1);
    }

    argv[arguments.size() + 1] = nullptr;

    return executeImpl(path.data(), argv.data(), config);
}


int ShellCommand::tryWait()
{
    wait_called = true;

    in.close();
    out.close();
    err.close();

    LOG_TRACE(getLogger(), "Will wait for shell command pid {}", pid);

    int status = 0;
    while (waitpid(pid, &status, 0) < 0)
    {
        if (errno != EINTR)
            throwFromErrno("Cannot waitpid", ErrorCodes::CANNOT_WAITPID);
    }

    LOG_TRACE(getLogger(), "Wait for shell command pid {} completed with status {}", pid, status);

    if (WIFEXITED(status))
        return WEXITSTATUS(status);

    if (WIFSIGNALED(status))
        throw Exception("Child process was terminated by signal " + toString(WTERMSIG(status)), ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY);

    if (WIFSTOPPED(status))
        throw Exception("Child process was stopped by signal " + toString(WSTOPSIG(status)), ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY);

    throw Exception("Child process was not exited normally by unknown reason", ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY);
}


void ShellCommand::wait()
{
    int retcode = tryWait();

    if (retcode != EXIT_SUCCESS)
    {
        switch (retcode)
        {
            case int(ReturnCodes::CANNOT_DUP_STDIN):
                throw Exception("Cannot dup2 stdin of child process", ErrorCodes::CANNOT_CREATE_CHILD_PROCESS);
            case int(ReturnCodes::CANNOT_DUP_STDOUT):
                throw Exception("Cannot dup2 stdout of child process", ErrorCodes::CANNOT_CREATE_CHILD_PROCESS);
            case int(ReturnCodes::CANNOT_DUP_STDERR):
                throw Exception("Cannot dup2 stderr of child process", ErrorCodes::CANNOT_CREATE_CHILD_PROCESS);
            case int(ReturnCodes::CANNOT_EXEC):
                throw Exception("Cannot execv in child process", ErrorCodes::CANNOT_CREATE_CHILD_PROCESS);
            case int(ReturnCodes::CANNOT_DUP_READ_DESCRIPTOR):
                throw Exception("Cannot dup2 read descriptor of child process", ErrorCodes::CANNOT_CREATE_CHILD_PROCESS);
            case int(ReturnCodes::CANNOT_DUP_WRITE_DESCRIPTOR):
                throw Exception("Cannot dup2 write descriptor of child process", ErrorCodes::CANNOT_CREATE_CHILD_PROCESS);
            default:
                throw Exception("Child process was exited with return code " + toString(retcode), ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY);
        }
    }
}


}
