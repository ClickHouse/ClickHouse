#include <Interpreters/Context.h>
#include "ICommand.h"

namespace DB
{

class CommandMove final : public ICommand
{
public:
    CommandMove()
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

        if (disk.getDisk()->isFile(path_from))
            disk.getDisk()->moveFile(path_from, path_from);
        else
            disk.getDisk()->moveDirectory(path_from, path_from);
    }
};

CommandPtr makeCommandMove()
{
    return std::make_unique<DB::CommandMove>();
}

}
