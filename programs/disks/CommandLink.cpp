#include "ICommand.h"
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandLink final : public ICommand
{
public:
    CommandLink()
    {
        command_name = "link";
        description = "Create hardlink from `from_path` to `to_path`\nPath should be in format './' or './path' or 'path'";
        usage = "link [OPTION]... <FROM_PATH> <TO_PATH>";
    }

    void processOptions(
        Poco::Util::LayeredConfiguration &,
        po::variables_map &) const override
    {
    }

    void execute(
        const std::vector<String> & command_arguments,
        DB::ContextMutablePtr & global_context,
        Poco::Util::LayeredConfiguration & config) override
    {
        if (command_arguments.size() != 2)
        {
            printHelpMessage();
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Bad Arguments");
        }

        String disk_name = config.getString("disk", "default");

        const String & path_from = command_arguments[0];
        const String & path_to = command_arguments[1];

        DiskPtr disk = global_context->getDisk(disk_name);

        String relative_path_from = validatePathAndGetAsRelative(path_from);
        String relative_path_to = validatePathAndGetAsRelative(path_to);

        disk->createHardLink(relative_path_from, relative_path_to);
    }
};
}

std::unique_ptr <DB::ICommand> makeCommandLink()
{
    return std::make_unique<DB::CommandLink>();
}
