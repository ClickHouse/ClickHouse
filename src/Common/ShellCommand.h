#pragma once

#include <memory>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <unordered_map>


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
        explicit DestructorStrategy(bool terminate_in_destructor_, size_t wait_for_normal_exit_before_termination_seconds_ = 0)
            : terminate_in_destructor(terminate_in_destructor_)
            , wait_for_normal_exit_before_termination_seconds(wait_for_normal_exit_before_termination_seconds_)
        {
        }

        bool terminate_in_destructor;

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

        std::vector<std::string> arguments;

        std::vector<int> read_fds;

        std::vector<int> write_fds;

        bool pipe_stdin_only = false;

        DestructorStrategy terminate_in_destructor_strategy = DestructorStrategy(false);
    };

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

    WriteBufferFromFile in;        /// If the command reads from stdin, do not forget to call in.close() after writing all the data there.
    ReadBufferFromFile out;
    ReadBufferFromFile err;

    std::unordered_map<int, ReadBufferFromFile> read_fds;
    std::unordered_map<int, WriteBufferFromFile> write_fds;
private:

    pid_t pid;
    Config config;
    bool wait_called = false;

    ShellCommand(pid_t pid_, int & in_fd_, int & out_fd_, int & err_fd_, const Config & config);

    bool tryWaitProcessWithTimeout(size_t timeout_in_seconds);

    static Poco::Logger * getLogger();

    /// Print command name and the list of arguments to log. NOTE: No escaping of arguments is performed.
    static void logCommand(const char * filename, char * const argv[]);

    static std::unique_ptr<ShellCommand> executeImpl(const char * filename, char * const argv[], const Config & config);
};


}
