#include <DisksApp.h>
#include <ICommand.h>

#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>

namespace DB
{

class CommandHelp final : public ICommand
{
public:
    explicit CommandHelp(const DisksApp & disks_app_) : ICommand("CommandHelp"), disks_app(disks_app_)
    {
        command_name = "help";
        description = "Print help message about available commands";
        options_description.add_options()(
            "command", po::value<String>(), "A command to help with (optional, positional), if not specified, help lists all the commands");
        positional_options_description.add("command", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & /*client*/) override
    {
        std::optional<String> command = getValueFromCommandLineOptionsWithOptional<String>(options, "command");
        if (command.has_value())
        {
            disks_app.printCommandHelpMessage(command.value());
        }
        else
        {
            disks_app.printAvailableCommandsHelpMessage();
        }
    }

    const DisksApp & disks_app;
};

CommandPtr makeCommandHelp(const DisksApp & disks_app)
{
    return std::make_shared<DB::CommandHelp>(disks_app);
}

}
