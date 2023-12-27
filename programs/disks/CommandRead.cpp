#include "ICommand.h"
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Common/TerminalSize.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandRead final : public ICommand
{
public:
    CommandRead()
    {
        command_name = "read";
        command_option_description.emplace(createOptionsDescription("Allowed options", getTerminalWidth()));
        description = "read File `from_path` to `to_path` or to stdout\nPath should be in format './' or './path' or 'path'";
        usage = "read [OPTION]... <FROM_PATH> <TO_PATH>\nor\nread [OPTION]... <FROM_PATH>";
        command_option_description->add_options()
            ("output", po::value<String>(), "set path to file to which we are read")
            ;
    }

    void processOptions(
        Poco::Util::LayeredConfiguration & config,
        po::variables_map & options) const override
    {
        if (options.count("output"))
            config.setString("output", options["output"].as<String>());
    }

    void execute(
        const std::vector<String> & command_arguments,
        DB::ContextMutablePtr & global_context,
        Poco::Util::LayeredConfiguration & config) override
    {
        if (command_arguments.size() != 1)
        {
            printHelpMessage();
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Bad Arguments");
        }

        String disk_name = config.getString("disk", "default");

        DiskPtr disk = global_context->getDisk(disk_name);

        String relative_path = validatePathAndGetAsRelative(command_arguments[0]);

        String path_output = config.getString("output", "");

        if (!path_output.empty())
        {
            String relative_path_output = validatePathAndGetAsRelative(path_output);

            auto in = disk->readFile(relative_path);
            auto out = disk->writeFile(relative_path_output);
            copyData(*in, *out);
            out->finalize();
            return;
        }
        else
        {
            auto in = disk->readFile(relative_path);
            std::unique_ptr<WriteBufferFromFileBase> out = std::make_unique<WriteBufferFromFileDescriptor>(STDOUT_FILENO);
            copyData(*in, *out);
        }
    }
};
}

std::unique_ptr <DB::ICommand> makeCommandRead()
{
    return std::make_unique<DB::CommandRead>();
}
