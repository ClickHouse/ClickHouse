#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <dlfcn.h>
#include <Common/Exception.h>
#include <Common/ShellCommand.h>
#include <Common/PipeFDs.h>
#include <common/logger_useful.h>
#include <IO/WriteHelpers.h>
#include <unistd.h>
#include <csignal>

namespace
{
    /// By these return codes from the child process, we learn (for sure) about errors when creating it.
    enum class ReturnCodes : int
    {
        CANNOT_DUP_STDIN    = 0x55555555,   /// The value is not important, but it is chosen so that it's rare to conflict with the program return code.
        CANNOT_DUP_STDOUT   = 0x55555556,
        CANNOT_DUP_STDERR   = 0x55555557,
        CANNOT_EXEC         = 0x55555558,
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

ShellCommand::ShellCommand(pid_t pid_, int in_fd_, int out_fd_, int err_fd_, bool terminate_in_destructor_)
    : pid(pid_)
    , terminate_in_destructor(terminate_in_destructor_)
    , log(&Poco::Logger::get("ShellCommand"))
    , in(in_fd_)
    , out(out_fd_)
    , err(err_fd_) {}

ShellCommand::~ShellCommand()
{
    if (terminate_in_destructor)
    {
        int retcode = kill(pid, SIGTERM);
        if (retcode != 0)
            LOG_WARNING(log, "Cannot kill pid " << pid << " errno '" << errnoToString(retcode) << "'");
    }
    else if (!wait_called)
        tryWait();
}

std::unique_ptr<ShellCommand> ShellCommand::executeImpl(const char * filename, char * const argv[], bool pipe_stdin_only, bool terminate_in_destructor)
{
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

    pid_t pid = reinterpret_cast<pid_t(*)()>(real_vfork)();

    if (-1 == pid)
        throwFromErrno("Cannot vfork", ErrorCodes::CANNOT_FORK);

    if (0 == pid)
    {
        /// We are in the freshly created process.

        /// Why `_exit` and not `exit`? Because `exit` calls `atexit` and destructors of thread local storage.
        /// And there is a lot of garbage (including, for example, mutex is blocked). And this can not be done after `vfork` - deadlock happens.

        /// Replace the file descriptors with the ends of our pipes.
        if (STDIN_FILENO != dup2(pipe_stdin.fds_rw[0], STDIN_FILENO))
            _exit(int(ReturnCodes::CANNOT_DUP_STDIN));

        if (!pipe_stdin_only)
        {
            if (STDOUT_FILENO != dup2(pipe_stdout.fds_rw[1], STDOUT_FILENO))
                _exit(int(ReturnCodes::CANNOT_DUP_STDOUT));

            if (STDERR_FILENO != dup2(pipe_stderr.fds_rw[1], STDERR_FILENO))
                _exit(int(ReturnCodes::CANNOT_DUP_STDERR));
        }

        execv(filename, argv);
        /// If the process is running, then `execv` does not return here.

        _exit(int(ReturnCodes::CANNOT_EXEC));
    }

    std::unique_ptr<ShellCommand> res(new ShellCommand(pid, pipe_stdin.fds_rw[1], pipe_stdout.fds_rw[0], pipe_stderr.fds_rw[0], terminate_in_destructor));

    /// Now the ownership of the file descriptors is passed to the result.
    pipe_stdin.fds_rw[1] = -1;
    pipe_stdout.fds_rw[0] = -1;
    pipe_stderr.fds_rw[0] = -1;

    return res;
}


std::unique_ptr<ShellCommand> ShellCommand::execute(const std::string & command, bool pipe_stdin_only, bool terminate_in_destructor)
{
    /// Arguments in non-constant chunks of memory (as required for `execv`).
    /// Moreover, their copying must be done before calling `vfork`, so after `vfork` do a minimum of things.
    std::vector<char> argv0("sh", &("sh"[3]));
    std::vector<char> argv1("-c", &("-c"[3]));
    std::vector<char> argv2(command.data(), command.data() + command.size() + 1);

    char * const argv[] = { argv0.data(), argv1.data(), argv2.data(), nullptr };

    return executeImpl("/bin/sh", argv, pipe_stdin_only, terminate_in_destructor);
}


std::unique_ptr<ShellCommand> ShellCommand::executeDirect(const std::string & path, const std::vector<std::string> & arguments, bool terminate_in_destructor)
{
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

    return executeImpl(path.data(), argv.data(), false, terminate_in_destructor);
}


int ShellCommand::tryWait()
{
    wait_called = true;

    int status = 0;
    if (-1 == waitpid(pid, &status, 0))
        throwFromErrno("Cannot waitpid", ErrorCodes::CANNOT_WAITPID);

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
            default:
                throw Exception("Child process was exited with return code " + toString(retcode), ErrorCodes::CHILD_WAS_NOT_EXITED_NORMALLY);
        }
    }
}


}
