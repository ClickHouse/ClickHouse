#include <Interpreters/Context.h>
#include "ICommand.h"

namespace DB
{

class CommandRemove final : public ICommand
{
public:
    CommandRemove()
    {
        command_name = "remove";
        description = "Remove file or directory with all children. Throws exception if file doesn't exists";
        options_description.add_options()("path", po::value<String>(), "path from which we copy (mandatory, positional)");
        positional_options_description.add("path", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        auto disk = client.getCurrentDiskWithPath();
        const String & path = disk.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path"));
        disk.getDisk()->removeRecursive(path);
    }
};

CommandPtr makeCommandRemove()
{
    return std::make_unique<DB::CommandRemove>();
}

}
