#pragma once

#include <memory>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>


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
class ShellCommand
{
private:
    pid_t pid;
    bool wait_called = false;

    ShellCommand(pid_t pid, int in_fd, int out_fd, int err_fd)
        : pid(pid), in(in_fd), out(out_fd), err(err_fd) {};

    static std::unique_ptr<ShellCommand> executeImpl(const char * filename, char * const argv[], bool pipe_stdin_only);

public:
    WriteBufferFromFile in;        /// If the command reads from stdin, do not forget to call in.close() after writing all the data there.
    ReadBufferFromFile out;
    ReadBufferFromFile err;

    ~ShellCommand();

    /// Run the command using /bin/sh -c
    static std::unique_ptr<ShellCommand> execute(const std::string & command, bool pipe_stdin_only = false);

    /// Run the executable with the specified arguments. `arguments` - without argv[0].
    static std::unique_ptr<ShellCommand> executeDirect(const std::string & path, const std::vector<std::string> & arguments);

    /// Wait for the process to end, throw an exception if the code is not 0 or if the process was not completed by itself.
    void wait();

    /// Wait for the process to finish, see the return code. To throw an exception if the process was not completed independently.
    int tryWait();
};


}
