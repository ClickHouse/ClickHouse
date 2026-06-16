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
    CommandDiskUsage() : ICommand("CommandDiskUsage")
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

        if (!disk.isDirectory(path) && !disk.getDisk()->existsFile(disk.getRelativeFromRoot(path)))
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

            /// listAllFilesByPath does not throw on a file: it returns an empty list.
            /// A non-empty list means a directory; an empty list means a file or an empty directory.
            const auto children = disk.listAllFilesByPath(current);
            if (!children.empty())
            {
                for (const auto & file_name : children)
                    stack.push(current.ends_with("/") ? current + file_name : current + "/" + file_name);
            }
            /// An empty list is either a file or an empty directory. Only files have a size; an
            /// empty directory contributes 0. Check this explicitly so the result does not depend
            /// on whether the disk's getFileSize raises an exception or reports FILE_DOESNT_EXIST
            /// for directories.
            else if (!disk.isDirectory(current))
            {
                try
                {
                    total += disk.getDisk()->getFileSize(disk.getRelativeFromRoot(current));
                }
                catch (const Exception & e)
                {
                    /// Be tolerant of files that disappear while we traverse: skip them.
                    /// Any other failure (object storage, metadata, permissions, ...) must
                    /// not be swallowed, or `du` would silently report an undercount.
                    if (e.code() != ErrorCodes::FILE_DOESNT_EXIST)
                        throw;
                    LOG_WARNING(log, "File '{}' disappeared while traversing, skipping", current);
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
