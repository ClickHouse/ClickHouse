#pragma once

#include <memory>
#include <unordered_map>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>


namespace DB
{


/** Lets you run the command,
  *  read it stdout and stderr; write to stdin;
  *  wait for completion.
  *
  * The implementation is similar to the popen function from POSIX (see libc source code).
  *
  * The most important difference: uses vfork instead of fork.
  * This is done because fork does not work (with a memory shortage error),
  *  with some overcommit settings, if the address space of the process is more than half the amount of available memory.
  * Also, changing memory maps - a fairly resource-intensive operation.
  *
  * The second difference - allows to work simultaneously with stdin, and with stdout, and with stderr of running process,
  *  and also to obtain the return code and completion status.
  */
class ShellCommand final
{
public:
    ~ShellCommand();

    struct DestructorStrategy final
    {
        explicit DestructorStrategy(bool terminate_in_destructor_, int termination_signal_, size_t wait_for_normal_exit_before_termination_seconds_ = 0)
            : terminate_in_destructor(terminate_in_destructor_), termination_signal(termination_signal_)
            , wait_for_normal_exit_before_termination_seconds(wait_for_normal_exit_before_termination_seconds_)
        {
        }

        bool terminate_in_destructor;
        int termination_signal;

        /// If terminate in destructor is true, command will wait until send SIGTERM signal to created process
        size_t wait_for_normal_exit_before_termination_seconds = 0;
    };

    struct Config
    {
        Config(const std::string & command_) /// NOLINT
            : command(command_)
        {}

        Config(const char * command_) /// NOLINT
            : command(command_)
        {}

        std::string command;

        VectorWithMemoryTracking<std::string> arguments;

        std::vector<int> read_fds;

        std::vector<int> write_fds;

        bool pipe_stdin_only = false;

        size_t pipe_capacity = 0;

        DestructorStrategy terminate_in_destructor_strategy = DestructorStrategy(false, 0);

        /// When true, `tryWaitImpl` reaps with `wait4` and captures the child's
        /// `rusage` (read back via `getChild*`/`wasChildResourceUsageCaptured`).
        /// When false (the default) it reaps with plain `waitpid` and allocates
        /// nothing. Set for executable (non-pool) UDFs, which read the usage.
        bool collect_resource_usage = false;

        /// When true, the child pid is tracked in the global `UDFProcessRegistry`
        /// from spawn until reaped. Off by default; enabled only for executable
        /// and executable_pool UDFs.
        bool register_in_udf_process_registry = false;
    };

    pid_t getPid() const
    {
        return pid;
    }

    bool isWaitCalled() const
    {
        return wait_called;
    }

    void setDoNotTerminate()
    {
        do_not_terminate = true;
    }

    /// True once the child has been reaped by `tryWaitImpl` and its
    /// resource usage was captured.
    bool wasChildResourceUsageCaptured() const noexcept;

    /// User-mode CPU time consumed by the reaped child. Zero if
    /// `wasChildResourceUsageCaptured` returns false.
    UInt64 getChildUserTimeMicroseconds() const noexcept;

    /// Kernel-mode CPU time consumed by the reaped child. Zero if
    /// `wasChildResourceUsageCaptured` returns false.
    UInt64 getChildSystemTimeMicroseconds() const noexcept;

    /// Run the command using /bin/sh -c.
    /// If terminate_in_destructor is true, send terminate signal in destructor and don't wait process.
    static std::unique_ptr<ShellCommand> execute(const Config & config);

    /// Run the executable with the specified arguments. `arguments` - without argv[0].
    /// If terminate_in_destructor is true, send terminate signal in destructor and don't wait process.
    static std::unique_ptr<ShellCommand> executeDirect(const Config & config);

    /// Wait for the process to end, throw an exception if the code is not 0 or if the process was not completed by itself.
    void wait();

    /// Wait for the process to finish, see the return code. To throw an exception if the process was not completed independently.
    int tryWait();

    /// Returns if process terminated.
    /// If process terminated, then handle return code.
    bool waitIfProccesTerminated();

    /// Non-blocking reap: if the child has already terminated, collect its `rusage`
    /// without inspecting the exit status, so a non-zero or signalled exit is not
    /// raised as an error. Returns whether the child was reaped.
    bool tryReapWithoutStatusCheck();

    WriteBufferFromFile in;        /// If the command reads from stdin, do not forget to call in.close() after writing all the data there.
    ReadBufferFromFile out;
    ReadBufferFromFile err;

    std::unordered_map<int, ReadBufferFromFile> read_fds;
    std::unordered_map<int, WriteBufferFromFile> write_fds;
private:

    pid_t pid;
    Config config;
    bool wait_called = false;
    bool do_not_terminate = false;

    /// CPU time of the reaped child, taken from `wait4` rusage and stored by value
    /// at reap time. The reap path performs no allocation, so a memory-limit
    /// `exception` can never fail a query whose child has already exited.
    bool child_resource_usage_captured = false;
    UInt64 child_user_time_us = 0;
    UInt64 child_system_time_us = 0;

    /// Identifies which incarnation of the pid this wrapper owns, so reap removes
    /// only its own entry and never one belonging to a later process that reused
    /// the pid. Stamped by `UDFProcessRegistry::add` at spawn; 0 for non-UDF
    /// commands, which never register.
    UInt64 udf_registry_generation = 0;

    ShellCommand(pid_t pid_, int & in_fd_, int & out_fd_, int & err_fd_, const Config & config);

    bool tryWaitProcessWithTimeout(size_t timeout_in_seconds);
    struct tryWaitResult;

    tryWaitResult tryWaitImpl(bool blocking, bool check_exit_status = true);

    void handleProcessRetcode(int retcode) const;

    static LoggerPtr getLogger();

    /// Print command name and the list of arguments to log. NOTE: No escaping of arguments is performed.
    static void logCommand(const char * filename, char * const argv[]);

    static std::unique_ptr<ShellCommand> executeImpl(const char * filename, char * const argv[], const Config & config);
};


}
