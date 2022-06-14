#pragma once

#include "ICommand.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandList : public ICommand
{
public:
    CommandList()
    {
        command_name = "list";
        command_option_description.emplace(createOptionsDescription("Help Message for list", getTerminalWidth()));
        description = "List files (the default disk is used by default)\nPath should be in format './' or './path' or 'path'";
        usage = "Usage: list [OPTION]... <PATH>...";
    }

    void processOptions(
        Poco::Util::LayeredConfiguration &,
        po::variables_map &) const override{}

    void executeImpl(
        const DB::ContextMutablePtr & global_context,
        const Poco::Util::LayeredConfiguration & config) const override
    {
        if (pos_arguments.size() != 1)
        {
            printHelpMessage();
            throw DB::Exception("Bad Arguments", DB::ErrorCodes::BAD_ARGUMENTS);
        }

        String disk_name = config.getString("disk", "default");

        String path =  pos_arguments[0];

        std::vector<String> file_names;
        DiskPtr disk = global_context->getDisk(disk_name);

        String full_path = fullPathWithValidate(disk, path);

        disk->listFiles(full_path, file_names);

        for (const auto & file_name : file_names)
            std::cout << file_name << '\n';
    }
};
}

std::unique_ptr <DB::ICommand> makeCommandList()
{
    return std::make_unique<DB::CommandList>();
}
