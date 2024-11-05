#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/BackgroundShellCommandHolder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

LoggerPtr BackgroundShellCommandHolder::getLogger()
{
    return ::getLogger("BackgroundShellCommandHolder");
}


void BackgroundShellCommandHolder::removeCommand(pid_t pid)
{
    std::lock_guard lock(mutex);
    bool is_erased = active_shell_commands.erase(pid);
    LOG_TRACE(getLogger(), "Try to erase command with the pid {}, is_erased: {}", pid, is_erased);
}

void BackgroundShellCommandHolder::addCommand(std::unique_ptr<ShellCommand> command)
{
    std::lock_guard lock(mutex);
    pid_t command_pid = command->getPid();

    auto [iterator, is_inserted] = active_shell_commands.emplace(std::make_pair(command_pid, std::move(command)));
    if (!is_inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't insert process PID {} into active shell commands, because there are running process with same PID", command_pid);

    LOG_TRACE(getLogger(), "Inserted the command with pid {}", command_pid);
}
}
