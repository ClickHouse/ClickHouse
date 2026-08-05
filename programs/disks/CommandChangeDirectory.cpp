#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>
#include <DisksApp.h>
#include <DisksClient.h>
#include <ICommand.h>

namespace DB
{

class CommandChangeDirectory final : public ICommand
{
public:
    explicit CommandChangeDirectory() : ICommand("CommandChangeDirectory")
    {
        command_name = "cd";
        description = "Change directory (makes sense only in interactive mode)";
        options_description.add_options()("path", po::value<String>(), "the path to which we want to change (mandatory, positional)")(
            "disk", po::value<String>(), "A disk where the path is changed (without disk switching)");
        positional_options_description.add("path", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        DiskWithPath & disk = getDiskWithPath(client, options, "disk");
        String path = getValueFromCommandLineOptionsThrow<String>(options, "path");
        disk.setPath(path);
    }
};

CommandPtr makeCommandChangeDirectory()
{
    return std::make_shared<DB::CommandChangeDirectory>();
}

}
