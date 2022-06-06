#pragma once

#include "ICommand.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandWrite : public ICommand
{
public:
    CommandWrite()
    {
        command_name = "write";
        command_option_description.emplace(createOptionsDescription("Help Message for write", getTerminalWidth()));
        description = "Write File `from_path` or stdin to `to_path`";
        usage = "Usage: write [OPTION]... <FROM_PATH> <TO_PATH>\nor\nstdin | write [OPTION]... <TO_PATH>\nPath should be in format './' or './path' or 'path'";
        command_option_description->add_options()
            ("input", po::value<String>(), "set path to file to which we are write")
            ;
    }

    void processOptions(
        Poco::Util::AbstractConfiguration & config,
        po::variables_map & options) const override
    {
        if (options.count("input"))
            config.setString("input", options["input"].as<String>());
    }

    void executeImpl(
        const DB::ContextMutablePtr & global_context,
        const Poco::Util::AbstractConfiguration & config) const override
    {
        if (pos_arguments.size() != 1)
        {
            printHelpMessage();
            throw DB::Exception("Bad Arguments", DB::ErrorCodes::BAD_ARGUMENTS);
        }

        String disk_name = config.getString("disk", "default");

        String path = pos_arguments[0];

        DiskPtr disk = global_context->getDisk(disk_name);

        String full_path = fullPathWithValidate(disk, path);

        String path_input = config.getString("input", "");
        std::unique_ptr<ReadBufferFromFileBase> in;
        if (path_input.empty())
        {
            in = std::make_unique<ReadBufferFromFileDescriptor>(STDIN_FILENO);
        }
        else
        {
            String full_path_input = fullPathWithValidate(disk, path_input);
            in = disk->readFile(full_path_input);
        }

        auto out = disk->writeFile(full_path);
        copyData(*in, *out);
        out->finalize();
        return;
    }
};
}

std::unique_ptr <DB::ICommand> makeCommandWrite()
{
    return std::make_unique<DB::CommandWrite>();
}
