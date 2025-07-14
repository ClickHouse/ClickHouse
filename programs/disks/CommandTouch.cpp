#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>
#include <DisksApp.h>
#include <DisksClient.h>
#include <ICommand.h>
#include <Common/logger_useful.h>

namespace DB
{

class CommandTouch final : public ICommand
{
public:
    explicit CommandTouch() : ICommand("CommandTouch")
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

        const auto & disk_impl = disk.getDisk();
        const auto & relative_path = disk.getRelativeFromRoot(path);
        if (disk_impl->existsFile(relative_path))
        {
            LOG_WARNING(&Poco::Logger::get("CommandTouch"), "File already exists at path: {}", relative_path);
        }
        else if (disk_impl->existsDirectory(relative_path))
        {
            LOG_WARNING(&Poco::Logger::get("CommandTouch"), "Directory already exists at path: {}", relative_path);
        }
        else
        {
            LOG_INFO(&Poco::Logger::get("CommandTouch"), "Creating file at path: {}", relative_path);
            disk_impl->createFile(relative_path);
        }
    }
};

CommandPtr makeCommandTouch()
{
    return std::make_shared<DB::CommandTouch>();
}
}
