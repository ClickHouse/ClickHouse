#include <Interpreters/Context.h>
#include "ICommand.h"

namespace DB
{

class CommandLink final : public ICommand
{
public:
    CommandLink()
    {
        command_name = "link";
        description = "Create hardlink from `from_path` to `to_path`";
        options_description.add_options()(
            "path-from", po::value<String>(), "the path from which a hard link will be created (mandatory, positional)")(
            "path-to", po::value<String>(), "the path where a hard link will be created (mandatory, positional)");
        positional_options_description.add("path-from", 1);
        positional_options_description.add("path-to", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        auto disk = client.getCurrentDiskWithPath();

        const String & path_from = disk.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path-from"));
        const String & path_to = disk.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path-to"));

        disk.getDisk()->createHardLink(path_from, path_to);
    }
};

CommandPtr makeCommandLink()
{
    return std::make_shared<DB::CommandLink>();
}

}
