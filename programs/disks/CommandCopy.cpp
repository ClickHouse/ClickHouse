#pragma once

#include "ICommand.h"
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandCopy : public ICommand
{
public:
    CommandCopy()
    {
        command_name = "copy";
        command_option_description.emplace(createOptionsDescription("Allowed options", getTerminalWidth()));
        description = "Recursively copy data containing at `from_path` to `to_path`\nPath should be in format './' or './path' or 'path'";
        usage = "copy [OPTION]... <FROM_PATH> <TO_PATH>";
        command_option_description->add_options()
            ("diskFrom", po::value<String>(), "set name for disk from which we do operations")
            ("diskTo", po::value<String>(), "set name for disk to which we do operations")
            ;

    }

    void processOptions(
        Poco::Util::LayeredConfiguration & config,
        po::variables_map & options) const override
    {
        if (options.count("diskFrom"))
            config.setString("diskFrom", options["diskFrom"].as<String>());
        if (options.count("diskTo"))
            config.setString("diskTo", options["diskTo"].as<String>());
    }

    void execute(
        const std::vector<String> & command_arguments,
        DB::ContextMutablePtr & global_context,
        Poco::Util::LayeredConfiguration & config) override
    {
        if (command_arguments.size() != 2)
        {
            printHelpMessage();
            throw DB::Exception("Bad Arguments", DB::ErrorCodes::BAD_ARGUMENTS);
        }

        String disk_name_from = config.getString("diskFrom", config.getString("disk", "default"));
        String disk_name_to = config.getString("diskTo", config.getString("disk", "default"));

        String path_from = command_arguments[0];
        String path_to =  command_arguments[1];

        DiskPtr disk_from = global_context->getDisk(disk_name_from);
        DiskPtr disk_to = global_context->getDisk(disk_name_to);

        String full_path_from = fullPathWithValidate(disk_from, path_from);
        String full_path_to = fullPathWithValidate(disk_to, path_to);

        disk_from->copy(full_path_from, disk_to, full_path_to);
    }
};
}

std::unique_ptr <DB::ICommand> makeCommandCopy()
{
    return std::make_unique<DB::CommandCopy>();
}
