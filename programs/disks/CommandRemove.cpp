#include "ICommand.h"
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandRemove final : public ICommand
{
public:
    CommandRemove()
    {
        command_name = "remove";
        description = "Remove file or directory with all children. Throws exception if file doesn't exists.\nPath should be in format './' or './path' or 'path'";
        usage = "remove [OPTION]... <PATH>";
    }

    void processOptions(
        Poco::Util::LayeredConfiguration &,
        po::variables_map &) const override
    {}

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

        const String & path = command_arguments[0];

        DiskPtr disk = global_context->getDisk(disk_name);

        String relative_path = validatePathAndGetAsRelative(path);

        disk->removeRecursive(relative_path);
    }
};
}

std::unique_ptr <DB::ICommand> makeCommandRemove()
{
    return std::make_unique<DB::CommandRemove>();
}
