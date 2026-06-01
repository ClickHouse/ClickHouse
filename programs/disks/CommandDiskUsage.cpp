#include <iostream>
#include <DisksClient.h>
#include <ICommand.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace DB
{

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

        LOG_INFO(log, "Computing disk usage of '{}' at disk '{}'", path, disk.getDisk()->getName());
        UInt64 size = computeSize(disk, path);

        if (human_readable)
            std::cout << formatReadableSizeWithBinarySuffix(size) << "\n";
        else
            std::cout << size << "\n";
    }

private:
    /// Returns the size of a file, or the total size of all files contained in a directory recursively.
    static UInt64 computeSize(const DiskWithPath & disk, const String & path)
    {
        if (!disk.isDirectory(path))
            return disk.getDisk()->getFileSize(disk.getRelativeFromRoot(path));

        UInt64 total = 0;
        for (const auto & file_name : disk.listAllFilesByPath(path))
        {
            const String child = path.ends_with("/") ? path + file_name : path + "/" + file_name;
            total += computeSize(disk, child);
        }
        return total;
    }
};

CommandPtr makeCommandDiskUsage()
{
    return std::make_shared<DB::CommandDiskUsage>();
}

}
