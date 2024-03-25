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
        description = "Read a file from `FROM_PATH` to `TO_PATH`";
        usage = "read [OPTION]... <FROM_PATH> [<TO_PATH>]";
        command_option_description->add_options()
            ("output", po::value<String>(), "file to which we are reading, defaults to `stdout`");
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
       std::shared_ptr<DiskSelector> & disk_selector,
        Poco::Util::LayeredConfiguration & config) override
    {
        if (command_arguments.size() != 1)
        {
            printHelpMessage();
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Bad Arguments");
        }

        String disk_name = config.getString("disk", "default");

        DiskPtr disk = disk_selector->get(disk_name);

        String relative_path = validatePathAndGetAsRelative(command_arguments[0]);

        String path_output = config.getString("output", "");

        if (!path_output.empty())
        {
            String relative_path_output = validatePathAndGetAsRelative(path_output);

            auto in = disk->readFile(relative_path);
            auto out = disk->writeFile(relative_path_output);
            copyData(*in, *out);
            out->finalize();
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
