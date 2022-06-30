#pragma once

#include "ICommand.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandLink : public ICommand
{
public:
    CommandLink()
    {
        command_name = "link";
        command_option_description.emplace(createOptionsDescription("Help Message for link", getTerminalWidth()));
        description = "Create hardlink from `from_path` to `to_path`\nPath should be in format './' or './path' or 'path'";
        usage = "Usage: link [OPTION]... <FROM_PATH> <TO_PATH>";
    }

    void processOptions(
        Poco::Util::LayeredConfiguration &,
        po::variables_map &) const override{}

    void executeImpl(
        const DB::ContextMutablePtr & global_context,
        const Poco::Util::LayeredConfiguration & config) const override
    {
        if (pos_arguments.size() != 2)
        {
            printHelpMessage();
            throw DB::Exception("Bad Arguments", DB::ErrorCodes::BAD_ARGUMENTS);
        }

        String disk_name = config.getString("disk", "default");

        String path_from = pos_arguments[0];
        String path_to = pos_arguments[1];

        DiskPtr disk = global_context->getDisk(disk_name);

        String full_path_from = fullPathWithValidate(disk, path_from);
        String full_path_to = fullPathWithValidate(disk, path_to);

        disk->createHardLink(full_path_from, full_path_to);
    }
};
}

std::unique_ptr <DB::ICommand> makeCommandLink()
{
    return std::make_unique<DB::CommandLink>();
}
