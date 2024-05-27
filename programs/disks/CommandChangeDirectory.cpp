#include <Interpreters/Context.h>
#include "Common/Exception.h"
#include <Common/TerminalSize.h>
#include "DisksApp.h"
#include "DisksClient.h"
#include "ICommand.h"

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

class CommandChangeDirectory final : public ICommand
{
public:
    explicit CommandChangeDirectory() : ICommand()
    {
        command_name = "cd";
        description = "Change directory";
        options_description.add_options()("path", po::value<String>(), "the path of listing  (mandatory, positional)")(
            "disk", po::value<String>(), "A disk where the path is changed");
        positional_options_description.add("path", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        DiskWithPath & disk = getDiskWithPath(client, options, "disk");
        // std::cerr << "Disk name: " << disk.getDisk()->getName() << std::endl;
        String path = getValueFromCommandLineOptionsThrow<String>(options, "path");
        // std::cerr << "Disk path: " << path << std::endl;
        disk.setPath(path);
    }
};

CommandPtr makeCommandChangeDirectory()
{
    return std::make_unique<DB::CommandChangeDirectory>();
}

}
