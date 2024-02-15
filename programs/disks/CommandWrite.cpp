#include "ICommand.h"
#include <Interpreters/Context.h>

#include <Common/TerminalSize.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandWrite final : public ICommand
{
public:
    CommandWrite()
    {
        command_name = "write";
        command_option_description.emplace(createOptionsDescription("Allowed options", getTerminalWidth()));
        description = "Write a file from `FROM_PATH` to `TO_PATH`";
        usage = "write [OPTION]... [<FROM_PATH>] <TO_PATH>";
        command_option_description->add_options()
            ("input", po::value<String>(), "file from which we are reading, defaults to `stdin`");
    }

    void processOptions(
        Poco::Util::LayeredConfiguration & config,
        po::variables_map & options) const override
    {
        if (options.count("input"))
            config.setString("input", options["input"].as<String>());
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

        const String & path = command_arguments[0];

        DiskPtr disk = disk_selector->get(disk_name);

        String relative_path = validatePathAndGetAsRelative(path);

        String path_input = config.getString("input", "");
        std::unique_ptr<ReadBufferFromFileBase> in;
        if (path_input.empty())
        {
            in = std::make_unique<ReadBufferFromFileDescriptor>(STDIN_FILENO);
        }
        else
        {
            String relative_path_input = validatePathAndGetAsRelative(path_input);
            in = disk->readFile(relative_path_input);
        }

        auto out = disk->writeFile(relative_path);
        copyData(*in, *out);
        out->finalize();
    }
};
}

std::unique_ptr <DB::ICommand> makeCommandWrite()
{
    return std::make_unique<DB::CommandWrite>();
}
