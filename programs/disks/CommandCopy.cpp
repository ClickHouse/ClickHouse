#include "ICommand.h"
#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandCopy final : public ICommand
{
public:
    CommandCopy()
    {
        command_name = "copy";
        command_option_description.emplace(createOptionsDescription("Allowed options", getTerminalWidth()));
        description = "Recursively copy data from `FROM_PATH` to `TO_PATH`";
        usage = "copy [OPTION]... <FROM_PATH> <TO_PATH>";
        command_option_description->add_options()
            ("disk-from", po::value<String>(), "disk from which we copy")
            ("disk-to", po::value<String>(), "disk to which we copy");
    }

    void processOptions(
        Poco::Util::LayeredConfiguration & config,
        po::variables_map & options) const override
    {
        if (options.count("disk-from"))
            config.setString("disk-from", options["disk-from"].as<String>());
        if (options.count("disk-to"))
            config.setString("disk-to", options["disk-to"].as<String>());
    }

    void execute(
        const std::vector<String> & command_arguments,
        std::shared_ptr<DiskSelector> & disk_selector,
        Poco::Util::LayeredConfiguration & config) override
    {
        if (command_arguments.size() != 2)
        {
            printHelpMessage();
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Bad Arguments");
        }

        String disk_name_from = config.getString("disk-from", config.getString("disk", "default"));
        String disk_name_to = config.getString("disk-to", config.getString("disk", "default"));

        const String & path_from = command_arguments[0];
        const String & path_to =  command_arguments[1];

        DiskPtr disk_from = disk_selector->get(disk_name_from);
        DiskPtr disk_to = disk_selector->get(disk_name_to);

        String relative_path_from = validatePathAndGetAsRelative(path_from);
        String relative_path_to = validatePathAndGetAsRelative(path_to);

        disk_from->copyDirectoryContent(relative_path_from, disk_to, relative_path_to, /* read_settings= */ {}, /* write_settings= */ {}, /* cancellation_hook= */ {});
    }
};
}

std::unique_ptr <DB::ICommand> makeCommandCopy()
{
    return std::make_unique<DB::CommandCopy>();
}
