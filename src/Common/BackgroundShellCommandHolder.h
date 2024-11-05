#pragma once

#include <memory>
#include <mutex>

#include <Common/ShellCommand.h>
#include <unordered_map>


namespace DB
{

/** The holder class for running background shell processes.
*/
class BackgroundShellCommandHolder final
{
public:
    void removeCommand(pid_t pid);
    void addCommand(std::unique_ptr<ShellCommand> command);

private:
    using ActiveShellCommandsCollection = std::unordered_map<pid_t, std::unique_ptr<ShellCommand>>;

    std::mutex mutex;
    ActiveShellCommandsCollection active_shell_commands TSA_GUARDED_BY(mutex);

    static LoggerPtr getLogger();
};

}
