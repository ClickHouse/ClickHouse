#include "ICommand.h"

#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandMkDir final : public ICommand
{
public:
    CommandMkDir()
    {
        command_name = "mkdir";
        command_option_description.emplace(createOptionsDescription("Allowed options", getTerminalWidth()));
        description = "Create a directory";
        usage = "mkdir [OPTION]... <PATH>";
        command_option_description->add_options()
            ("recursive", "recursively create directories");
    }

    void processOptions(
        Poco::Util::LayeredConfiguration & config,
        po::variables_map & options) const override
    {
        if (options.count("recursive"))
            config.setBool("recursive", true);
    }

    void execute(
        const std::vector<String> & command_arguments,
        std::shared_ptr<DiskSelector> & disk_selector,
        Poco::Util::LayeredConfiguration & config) override
    {
        if (command_arguments.size() != 1)
        {
            printHelpMessage();
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Bad Arguments");
        }

        String disk_name = config.getString("disk", "default");

        const String & path = command_arguments[0];

        DiskPtr disk = disk_selector->get(disk_name);

        String relative_path = validatePathAndGetAsRelative(path);
        bool recursive = config.getBool("recursive", false);

        if (recursive)
            disk->createDirectories(relative_path);
        else
            disk->createDirectory(relative_path);
    }
};
}

std::unique_ptr <DB::ICommand> makeCommandMkDir()
{
    return std::make_unique<DB::CommandMkDir>();
}
