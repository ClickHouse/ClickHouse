#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>
#include "DisksClient.h"
#include "ICommand.h"

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

class CommandCopy final : public ICommand
{
public:
    explicit CommandCopy() : ICommand()
    {
        command_name = "copy";
        description = "Recursively copy data from `FROM_PATH` to `TO_PATH`";
        options_description.add_options()("disk-from", po::value<String>(), "disk from which we copy")(
            "disk-to", po::value<String>(), "disk to which we copy")(
            "path-from", po::value<String>(), "path from which we copy (mandatory, positional)")(
            "path-to", po::value<String>(), "path to which we copy (mandatory, positional)");
        positional_options_description.add("path-from", 1);
        positional_options_description.add("path-to", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        auto disk_from = getDiskWithPath(client, options, "disk-from");
        auto disk_to = getDiskWithPath(client, options, "disk-to");
        String path_from = disk_from.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path-from"));
        String path_to = disk_to.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path-to"));

        disk_from.getDisk()->copyDirectoryContent(
            path_from, disk_to.getDisk(), path_to, /* read_settings= */ {}, /* write_settings= */ {}, /* cancellation_hook= */ {});
    }
};

CommandPtr makeCommandCopy()
{
    return std::make_unique<DB::CommandCopy>();
}
}
