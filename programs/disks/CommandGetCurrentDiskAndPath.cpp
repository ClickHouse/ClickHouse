#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>
#include "DisksApp.h"
#include "DisksClient.h"
#include "ICommand.h"

namespace DB
{

class CommandGetCurrentDiskAndPath final : public ICommand
{
public:
    explicit CommandGetCurrentDiskAndPath() : ICommand()
    {
        command_name = "current_disk_with_path";
        description = "Prints current disk and path (which coincide with the prompt)";
    }

    void executeImpl(const CommandLineOptions &, DisksClient & client) override
    {
        auto disk = client.getCurrentDiskWithPath();
        std::cout << "Disk: " << disk.getDisk()->getName() << "\nPath: " << disk.getCurrentPath() << std::endl;
    }
};

CommandPtr makeCommandGetCurrentDiskAndPath()
{
    return std::make_shared<DB::CommandGetCurrentDiskAndPath>();
}
}
