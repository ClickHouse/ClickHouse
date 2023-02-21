#pragma once

#include "ICommand.h"
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandMkDir : public ICommand
{
public:
    CommandMkDir()
    {
        command_name = "mkdir";
        command_option_description.emplace(createOptionsDescription("Allowed options", getTerminalWidth()));
        description = "Create directory or directories recursively";
        usage = "mkdir [OPTION]... <PATH>";
        command_option_description->add_options()
            ("recursive", "recursively create directories")
            ;
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
        DB::ContextMutablePtr & global_context,
        Poco::Util::LayeredConfiguration & config) override
    {
        if (command_arguments.size() != 1)
        {
            printHelpMessage();
            throw DB::Exception("Bad Arguments", DB::ErrorCodes::BAD_ARGUMENTS);
        }

        String disk_name = config.getString("disk", "default");

        String path = command_arguments[0];

        DiskPtr disk = global_context->getDisk(disk_name);

        String full_path = fullPathWithValidate(disk, path);
        bool recursive = config.getBool("recursive", false);

        if (recursive)
            disk->createDirectories(full_path);
        else
            disk->createDirectory(full_path);
    }
};
}

std::unique_ptr <DB::ICommand> makeCommandMkDir()
{
    return std::make_unique<DB::CommandMkDir>();
}
