#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>
#include "DisksApp.h"
#include "DisksClient.h"
#include "ICommand.h"

namespace DB
{

class CommandTouch final : public ICommand
{
public:
    explicit CommandTouch() : ICommand()
    {
        command_name = "touch";
        description = "Create a file by path";
        options_description.add_options()("path", po::value<String>(), "the path of listing (mandatory, positional)");
        positional_options_description.add("path", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        auto disk = client.getCurrentDiskWithPath();
        String path = getValueFromCommandLineOptionsThrow<String>(options, "path");

        disk.getDisk()->createFile(disk.getRelativeFromRoot(path));
    }
};

CommandPtr makeCommandTouch()
{
    return std::make_shared<DB::CommandTouch>();
}
}
