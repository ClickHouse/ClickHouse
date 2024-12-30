#include <Interpreters/Context.h>
#include "Common/Exception.h"
#include <Common/TerminalSize.h>
#include "DisksClient.h"
#include "ICommand.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


class CommandCopy final : public ICommand
{
public:
    explicit CommandCopy() : ICommand()
    {
        command_name = "copy";
        description = "Recursively copy data from `path-from` to `path-to`";
        options_description.add_options()(
            "disk-from", po::value<String>(), "disk from which we copy is executed (default value is a current disk)")(
            "disk-to", po::value<String>(), "disk to which copy is executed (default value is a current disk)")(
            "path-from", po::value<String>(), "path from which copy is executed (mandatory, positional)")(
            "path-to", po::value<String>(), "path to which copy is executed (mandatory, positional)")(
            "recursive,r", "recursively copy the directory (required to remove a directory)");
        positional_options_description.add("path-from", 1);
        positional_options_description.add("path-to", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        auto disk_from = getDiskWithPath(client, options, "disk-from");
        auto disk_to = getDiskWithPath(client, options, "disk-to");
        String path_from = disk_from.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path-from"));
        String path_to = disk_to.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path-to"));
        bool recursive = options.count("recursive");

        if (disk_from.getDisk()->existsFile(path_from))
        {
            auto target_location = getTargetLocation(path_from, disk_to, path_to);
            if (!disk_to.getDisk()->existsDirectory(target_location))
            {
                disk_from.getDisk()->copyFile(
                    path_from,
                    *disk_to.getDisk(),
                    target_location,
                    /* read_settings= */ {},
                    /* write_settings= */ {},
                    /* cancellation_hook= */ {});
            }
            else
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "cannot overwrite directory {} with non-directory {}", target_location, path_from);
            }
        }
        else if (disk_from.getDisk()->existsDirectory(path_from))
        {
            if (!recursive)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "--recursive not specified; omitting directory {}", path_from);
            }
            auto target_location = getTargetLocation(path_from, disk_to, path_to);

            if (disk_to.getDisk()->existsFile(target_location))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "cannot overwrite non-directory {} with directory {}", path_to, target_location);
            }
            if (!disk_to.getDisk()->existsDirectory(target_location))
            {
                disk_to.getDisk()->createDirectory(target_location);
            }
            disk_from.getDisk()->copyDirectoryContent(
                path_from,
                disk_to.getDisk(),
                target_location,
                /* read_settings= */ {},
                /* write_settings= */ {},
                /* cancellation_hook= */ {});
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "cannot stat '{}' on disk '{}': No such file or directory",
                path_from,
                disk_from.getDisk()->getName());
        }
    }
};

CommandPtr makeCommandCopy()
{
    return std::make_shared<DB::CommandCopy>();
}

}
