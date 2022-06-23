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
        description = "List disks names";
        usage = "list-disks [OPTION]";
    }

    void processOptions(
        Poco::Util::LayeredConfiguration &,
        po::variables_map &) const override
    {}

    void execute(
        const std::vector<String> & command_arguments,
        DB::ContextMutablePtr & global_context,
        Poco::Util::LayeredConfiguration &) override
    {
        if (!command_arguments.empty())
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
