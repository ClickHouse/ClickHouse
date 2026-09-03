#include <optional>
#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>
#include <DisksApp.h>
#include <ICommand.h>

namespace DB
{

class CommandSwitchDisk final : public ICommand
{
public:
    explicit CommandSwitchDisk() : ICommand("CommandSwitchDisk")
    {
        command_name = "switch-disk";
        description = "Switch disk (makes sense only in interactive mode)";
        options_description.add_options()("disk", po::value<String>(), "the disk to switch to (mandatory, positional)")(
            "path", po::value<String>(), "the path to switch on the disk");
        positional_options_description.add("disk", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        String disk = getValueFromCommandLineOptions<String>(options, "disk");
        std::optional<String> path = getValueFromCommandLineOptionsWithOptional<String>(options, "path");

        client.addDisk(disk, path);
        client.switchToDisk(disk, path);
    }
};

CommandPtr makeCommandSwitchDisk()
{
    return std::make_shared<DB::CommandSwitchDisk>();
}
}
