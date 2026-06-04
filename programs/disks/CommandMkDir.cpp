#include <ICommand.h>

#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>
#include <Common/logger_useful.h>

namespace DB
{

class CommandMkDir final : public ICommand
{
public:
    CommandMkDir() : ICommand("CommandMkDir")
    {
        command_name = "mkdir";
        description = "Creates a directory";
        options_description.add_options()("parents", "recursively create directories")(
            "path", po::value<String>(), "the path on which directory should be created (mandatory, positional)");
        positional_options_description.add("path", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        bool recursive = options.count("parents");
        const auto & disk = client.getCurrentDiskWithPath();

        String path = disk.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path"));

        if (recursive)
        {
            LOG_INFO(log, "Creating directory '{}' on disk '{}' recursively", path, disk.getDisk()->getName());
            disk.getDisk()->createDirectories(path);
        }
        else
        {
            LOG_INFO(log, "Creating directory '{}' on disk '{}'", path, disk.getDisk()->getName());
            disk.getDisk()->createDirectory(path);
        }
    }
};

CommandPtr makeCommandMkDir()
{
    return std::make_shared<DB::CommandMkDir>();
}

}
