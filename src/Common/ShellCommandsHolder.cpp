#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/ShellCommandsHolder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

LoggerPtr ShellCommandsHolder::getLogger()
{
    return ::getLogger("ShellCommandsHolder");
}


void ShellCommandsHolder::removeCommand(pid_t pid)
{
    std::lock_guard lock(mutex);
    bool is_erased = shell_commands.erase(pid);
    LOG_TRACE(getLogger(), "Try to erase command with the pid {}, is_erased: {}", pid, is_erased);
}

void ShellCommandsHolder::addCommand(std::unique_ptr<ShellCommand> command)
{
    std::lock_guard lock(mutex);
    pid_t command_pid = command->getPid();
    if (command->waitIfProccesTerminated())
    {
        LOG_TRACE(getLogger(), "Pid {} already finished. Do not insert it.", command_pid);
        return;
    }

    auto [iterator, is_inserted] = shell_commands.try_emplace(command_pid, std::move(command));
    if (is_inserted)
    {
        LOG_TRACE(getLogger(), "Inserted the command with pid {}", command_pid);
        return;
    }

    if (iterator->second->isWaitCalled())
    {
        iterator->second = std::move(command);
        LOG_TRACE(getLogger(), "Replaced the command with pid {}", command_pid);
        return;
    }

    /// We got two active ShellCommand with the same pid.
    /// Probably it is a bug, will try to replace the old shell command with a new one.

    LOG_WARNING(getLogger(), "The PID already presented in active shell commands, will try to replace with a new one.");

    iterator->second->setManuallyTerminated();
    iterator->second = std::move(command);
}
}
