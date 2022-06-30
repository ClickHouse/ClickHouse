#pragma once

#include "ICommand.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandListDisks : public ICommand
{
public:
    CommandListDisks()
    {
        command_name = "list-disks";
        command_option_description.emplace(createOptionsDescription("Help Message for list-disks", getTerminalWidth()));
        description = "List disks names";
        usage = "Usage: list-disks [OPTION]";
    }

    void processOptions(
        Poco::Util::LayeredConfiguration &,
        po::variables_map &) const override{}

    void executeImpl(
        const DB::ContextMutablePtr & global_context,
        const Poco::Util::LayeredConfiguration &) const override
    {
        if (pos_arguments.size() != 0)
        {
            printHelpMessage();
            throw DB::Exception("Bad Arguments", DB::ErrorCodes::BAD_ARGUMENTS);
        }

        for (const auto & [disk_name, _] : global_context->getDisksMap())
            std::cout << disk_name << '\n';
    }
};
}

std::unique_ptr <DB::ICommand> makeCommandListDisks()
{
    return std::make_unique<DB::CommandListDisks>();
}
