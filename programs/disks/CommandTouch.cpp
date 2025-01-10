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
        const auto & disk = client.getCurrentDiskWithPath();
        String path = getValueFromCommandLineOptionsThrow<String>(options, "path");

        if (!disk.getDisk()->existsFileOrDirectory(disk.getRelativeFromRoot(path)))
        {
            LOG_INFO(&Poco::Logger::get("CommandTouch"), "Creating file at path: {}", disk.getRelativeFromRoot(path));
            disk.getDisk()->createFile(disk.getRelativeFromRoot(path));
        }
        else if (disk.getDisk()->existsFile(disk.getRelativeFromRoot(path)))
        {
            LOG_WARNING(&Poco::Logger::get("CommandTouch"), "File already exists at path: {}", disk.getRelativeFromRoot(path));
        }
        else if (disk.getDisk()->existsDirectory(disk.getRelativeFromRoot(path)))
        {
            LOG_WARNING(&Poco::Logger::get("CommandTouch"), "Directory already exists at path: {}", disk.getRelativeFromRoot(path));
        }
    }
};

CommandPtr makeCommandTouch()
{
    return std::make_shared<DB::CommandTouch>();
}
}
