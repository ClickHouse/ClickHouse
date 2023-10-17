#include <Common/getCurrentProcessFDCount.h>
#include <Common/ShellCommand.h>
#include <IO/WriteBufferFromString.h>
#include <unistd.h>
#include <fmt/format.h>
#include <IO/ReadHelpers.h>
#include <filesystem>


Int64 getCurrentProcessFDCount()
{
    namespace fs = std::filesystem;
    Int64 result = -1;
#if defined(OS_LINUX)  || defined(OS_DARWIN)
    using namespace DB;

    Int32 pid = getpid();

    auto proc_fs_path = fmt::format("/proc/{}/fd", pid);
    if (fs::exists(proc_fs_path))
    {
        result = std::distance(fs::directory_iterator(proc_fs_path), fs::directory_iterator{});
    }
    else if (fs::exists("/dev/fd"))
    {
        result = std::distance(fs::directory_iterator("/dev/fd"), fs::directory_iterator{});
    }
    else
    {
        /// Then try lsof command
        String by_lsof = fmt::format("lsof -p {} | wc -l", pid);
        auto command = ShellCommand::execute(by_lsof);

        try
        {
            readIntText(result, command->out);
            command->wait();
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
        }
    }

#endif
    return result;
}
