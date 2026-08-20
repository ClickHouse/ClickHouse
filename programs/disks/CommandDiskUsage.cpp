#include <ICommand.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

#include <stack>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int FILE_DOESNT_EXIST;
}

class CommandDiskUsage final : public ICommand
{
public:
    CommandDiskUsage()
        : ICommand("CommandDiskUsage")
    {
        command_name = "du";
        description = "Print the total size in bytes for a file or directory.";
        options_description.add_options()("path", po::value<String>(), "File or directory to report the size of.")(
            "human-readable,h", "Print sizes in a human-readable format");
        positional_options_description.add("path", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        const auto & disk = client.getCurrentDiskWithPath();
        String path = getValueFromCommandLineOptionsWithDefault<String>(options, "path", ".");
        bool human_readable = options.contains("human-readable");

        const String relative = disk.getRelativeFromRoot(path);
        if (!disk.getDisk()->existsFile(relative) && !disk.isDirectory(path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path {} on disk {} doesn't exist", path, disk.getDisk()->getName());

        LOG_INFO(log, "Computing disk usage of '{}' at disk '{}'", path, disk.getDisk()->getName());
        UInt64 size = computeSize(disk, path);

        if (human_readable)
            std::cout << formatReadableSizeWithBinarySuffix(size) << "\n";
        else
            std::cout << size << "\n";
    }

private:
    /// Returns the size of a file, or the total size of all files contained in a directory recursively.
    UInt64 computeSize(const DiskWithPath & disk, const String & path) const
    {
        UInt64 total = 0;
        std::stack<String> stack;
        stack.push(path);

        while (!stack.empty())
        {
            const String current = std::move(stack.top());
            stack.pop();

            const String relative = disk.getRelativeFromRoot(current);

            if (disk.getDisk()->existsDirectory(relative))
            {
                for (const auto & file_name : disk.listAllFilesByPath(current))
                    stack.push(current.ends_with("/") ? current + file_name : current + "/" + file_name);
            }
            else
            {
                // Wrap getFileSize in try/catch, in case file disappears while traversing.
                try
                {
                    total += disk.getDisk()->getFileSize(relative);
                }
                catch (const Exception & e)
                {
                    /// Throw for failures related to object storage, metadata, permissions.
                    if (e.code() != ErrorCodes::FILE_DOESNT_EXIST)
                        throw;
                    LOG_WARNING(log, "File '{}' disappeared while traversing, skipping", current);
                    continue;
                }
            }
        }
        return total;
    }
};

CommandPtr makeCommandDiskUsage()
{
    return std::make_shared<DB::CommandDiskUsage>();
}

}
