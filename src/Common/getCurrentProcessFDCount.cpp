#include <Common/getCurrentProcessFDCount.h>
#include <Common/ShellCommand.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <unistd.h>
#include <fmt/format.h>
#include <IO/ReadHelpers.h>


int getCurrentProcessFDCount()
{
    int result = -1;
#if defined(__linux__)  || defined(__APPLE__)
    using namespace DB;

    Int32 pid = getpid();
    std::unique_ptr<ShellCommand> command;

    /// First try procfs
    String by_procfs = fmt::format("ls /proc/{}/fd | wc -l", pid);
    command = ShellCommand::execute(by_procfs);

    try
    {
        readIntText(result, command->out);
        command->wait();
    }
    catch (...)
    {
        /// Then try lsof command
        String by_lsof = fmt::format("lsof -p {} | wc -l", pid);
        command = ShellCommand::execute(by_procfs);

        try
        {
            readIntText(result, command->out);
            command->wait();
        }
        catch (...)
        {
        }
    }
#endif
    return result;
}
