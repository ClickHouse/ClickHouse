#include <optional>
#include <Interpreters/Context.h>
#include "Common/Exception.h"
#include <Common/TerminalSize.h>
#include "DisksApp.h"
#include "ICommand.h"

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

class CommandSwitchDisk final : public ICommand
{
public:
    explicit CommandSwitchDisk() : ICommand()
    {
        command_name = "switch-disk";
        // options_description.emplace(createOptionsDescription("Allowed options", getTerminalWidth()));
        description = "Change disk";
        // options_description->add_options()("recursive", "recursively list all directories");
        options_description.add_options()("disk", po::value<String>(), "the disk to switch to (mandatory, positional)")(
            "path", po::value<String>(), "the path to switch on the disk");
        positional_options_description.add("disk", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        String disk = getValueFromCommandLineOptions<String>(options, "disk");
        std::optional<String> path = getValueFromCommandLineOptionsWithOptional<String>(options, "path");

        if (!client.switchToDisk(disk, path))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Unable to switch to disk: {}, path: {}", disk, path.has_value() ? path.value() : "NO PATH");
        }
    }
};

CommandPtr makeCommandSwitchDisk()
{
    return std::make_unique<DB::CommandSwitchDisk>();
}
}
