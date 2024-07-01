#include <Interpreters/Context.h>
#include "Common/Exception.h"
#include "ICommand.h"

namespace DB
{

class CommandRemove final : public ICommand
{
public:
    CommandRemove()
    {
        command_name = "remove";
        description = "Remove file or directory. Throws exception if file doesn't exists";
        options_description.add_options()("path", po::value<String>(), "path from which we copy (mandatory, positional)")(
            "recursive", "recursively removes the directory (required to remove a directory)");
        positional_options_description.add("path", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        auto disk = client.getCurrentDiskWithPath();
        const String & path = disk.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path"));
        bool recursive = options.count("recursive");
        if (!disk.getDisk()->exists(path))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path {} on disk {} doesn't exist", path, disk.getDisk()->getName());
        }
        else if (disk.getDisk()->isDirectory(path))
        {
            if (!recursive)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "cannot remove '{}': Is a directory", path);
            }
            else
            {
                disk.getDisk()->removeRecursive(path);
            }
        }
        else
        {
            disk.getDisk()->removeFileIfExists(path);
        }
    }
};

CommandPtr makeCommandRemove()
{
    return std::make_unique<DB::CommandRemove>();
}

}
