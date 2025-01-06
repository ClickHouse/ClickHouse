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
        const std::vector<String> initialized_disks = client.getInitializedDiskNames();
        std::vector<String> sorted_and_selected_disk_state;
        sorted_and_selected_disk_state.reserve(initialized_disks.size());
        std::vector<String> uninitialized_disks = client.getUninitializedDiskNames();


        for (const auto & disk_name : initialized_disks)
        {
            sorted_and_selected_disk_state.push_back(disk_name + ":" + client.getDiskWithPath(disk_name).getAbsolutePath(""));
        }
        std::sort(sorted_and_selected_disk_state.begin(), sorted_and_selected_disk_state.end());
        if (!sorted_and_selected_disk_state.empty())
        {
            std::cout << "Initialized disks:\n";
            for (const auto & disk_state : sorted_and_selected_disk_state)
            {
                std::cout << disk_state << "\n";
            }
            std::cout << "\n";
        }

        if (!uninitialized_disks.empty())
        {
            std::cout << "Uninitialized disks:\n";
            std::sort(uninitialized_disks.begin(), uninitialized_disks.end());
            for (const auto & disk_name : uninitialized_disks)
            {
                std::cout << disk_name << "\n";
            }
            std::cout << "\n";
        }
    }

private:
};

CommandPtr makeCommandListDisks()
{
    return std::make_shared<DB::CommandListDisks>();
}
}
