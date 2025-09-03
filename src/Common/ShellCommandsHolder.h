#pragma once

#include <Common/ShellCommand.h>
#include <boost/noncopyable.hpp>
#include <memory>
#include <mutex>
#include <unordered_map>


namespace DB
{

/** The holder class for running background shell processes.
*/
class ShellCommandsHolder final : public boost::noncopyable
{
public:
    static ShellCommandsHolder & instance();

    void removeCommand(pid_t pid);
    void addCommand(std::unique_ptr<ShellCommand> command);

private:
    using ShellCommands = std::unordered_map<pid_t, std::unique_ptr<ShellCommand>>;

    std::mutex mutex;
    ShellCommands shell_commands TSA_GUARDED_BY(mutex);

    LoggerPtr log = getLogger("ShellCommandsHolder");
};

}
