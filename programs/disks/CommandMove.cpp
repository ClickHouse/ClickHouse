#include <Interpreters/Context.h>
#include <ICommand.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


class CommandMove final : public ICommand
{
public:
    CommandMove() : ICommand("CommandMove")
    {
        command_name = "move";
        description = "Move file or directory from `from_path` to `to_path`";
        options_description.add_options()("path-from", po::value<String>(), "path from which we copy (mandatory, positional)")(
            "path-to", po::value<String>(), "path to which we copy (mandatory, positional)");
        positional_options_description.add("path-from", 1);
        positional_options_description.add("path-to", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        auto disk = client.getCurrentDiskWithPath();

        String path_from = disk.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path-from"));
        String path_to = disk.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path-to"));

        if (disk.getDisk()->existsFile(path_from))
        {
            LOG_INFO(log, "Moving file from '{}' to '{}' at disk '{}'", path_from, path_to, disk.getDisk()->getName());
            disk.getDisk()->moveFile(path_from, path_to);
        }
        else if (disk.getDisk()->existsDirectory(path_from))
        {
            auto target_location = getTargetLocation(path_from, disk, path_to);
            if (!disk.getDisk()->existsDirectory(target_location))
            {
                LOG_INFO(log, "Moving directory from '{}' to '{}' at disk '{}'", path_from, target_location, disk.getDisk()->getName());
                disk.getDisk()->createDirectory(target_location);
                disk.getDisk()->moveDirectory(path_from, target_location);
            }
            else
            {
                if (disk.getDisk()->existsFile(target_location))
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "cannot overwrite non-directory '{}' with directory '{}'", target_location, path_from);
                }
                if (!disk.getDisk()->isDirectoryEmpty(target_location))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "cannot move '{}' to '{}': Directory not empty", path_from, target_location);
                }
                LOG_INFO(log, "Moving directory from '{}' to '{}' at disk '{}'", path_from, target_location, disk.getDisk()->getName());
                disk.getDisk()->moveDirectory(path_from, target_location);
            }
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "cannot stat '{}' on disk: '{}': No such file or directory",
                path_from,
                disk.getDisk()->getName());
        }
    }
};

CommandPtr makeCommandMove()
{
    return std::make_shared<DB::CommandMove>();
}
}
