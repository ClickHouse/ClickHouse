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


class CommandInit final : public ICommand
{
public:
    explicit CommandInit() : ICommand()
    {
        command_name = "init";
        description = "Initializes a disk described in the configuration file";
        options_description.add_options()("disk", po::value<String>(), "disk name to be initialized (mandatory, positional)")(
            "path", po::value<String>(), "the path to establish on the initialized disk")(
            "switch", "switch to the initialized disk after initialization");
        positional_options_description.add("disk", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        const auto disk_name = getValueFromCommandLineOptionsThrow<String>(options, "disk");
        const bool switch_to_disk = options.count("switch");
        const std::optional<String> path = getValueFromCommandLineOptionsWithOptional<String>(options, "path");

        client.addDisk(disk_name, std::nullopt);
        if (switch_to_disk)
        {
            client.switchToDisk(disk_name, path);
        } 
    }
};

CommandPtr makeCommandInit()
{
    return std::make_shared<DB::CommandInit>();
}

}
