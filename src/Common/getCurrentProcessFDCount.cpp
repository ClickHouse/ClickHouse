#include <Common/getCurrentProcessFDCount.h>
#include <Common/ShellCommand.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <unistd.h>
#include <fmt/format.h>
#include <IO/ReadHelpers.h>


int getCurrentProcessFDCount()
{
#if defined(__linux__)  || defined(__APPLE__)
    using namespace DB;

    Int32 pid = getpid();
    std::unique_ptr<ShellCommand> command;

    /// First try procfs
    String by_procfs = fmt::format("ls /proc/{}/fd | wc -l", pid);
    command = ShellCommand::execute(by_procfs);

    try
    {
        command->wait();
    }
    catch (...)
    {
        /// Then try lsof command
        String by_lsof = fmt::format("lsof -p {} | wc -l", pid);
        command = ShellCommand::execute(by_procfs);

        try
        {
            command->wait();
        }
        catch (...)
        {
            return -1;
        }
    }

    WriteBufferFromOwnString out;
    copyData(command->out, out);

    if (!out.str().empty())
    {
        return parse<Int32>(out.str());
    }

    return -1;
#else
    return -1;
#endif

}
