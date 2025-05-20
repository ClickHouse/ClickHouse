#include <algorithm>
#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>
#include "DisksClient.h"
#include "ICommand.h"

namespace DB
{

class CommandListDisks final : public ICommand
{
public:
    explicit CommandListDisks() : ICommand()
    {
        command_name = "list-disks";
        description = "Lists all available disks";
    }

    void executeImpl(const CommandLineOptions &, DisksClient & client) override
    {
        std::vector<String> sorted_and_selected{};
        for (const auto & disk_name : client.getAllDiskNames())
        {
            sorted_and_selected.push_back(disk_name + ":" + client.getDiskWithPath(disk_name).getAbsolutePath(""));
        }
        std::sort(sorted_and_selected.begin(), sorted_and_selected.end());
        for (const auto & disk_name : sorted_and_selected)
        {
            std::cout << disk_name << "\n";
        }
    }

private:
};

CommandPtr makeCommandListDisks()
{
    return std::make_shared<DB::CommandListDisks>();
}
}
