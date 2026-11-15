#include <algorithm>
#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>
#include <DisksClient.h>
#include <ICommand.h>

#include <fmt/ranges.h>

namespace DB
{

class CommandListDisks final : public ICommand
{
public:
    explicit CommandListDisks() : ICommand("CommandListDisks")
    {
        command_name = "list-disks";
        description = "Lists all available disks";
    }

    void executeImpl(const CommandLineOptions &, DisksClient & client) override
    {
        const std::vector<String> initialized_disks = client.getInitializedDiskNames();
        std::set<String> sorted_and_selected_disk_state;


        for (const auto & disk_name : initialized_disks)
        {
            sorted_and_selected_disk_state.insert(disk_name + ":" + client.getDiskWithPath(disk_name).getAbsolutePath(""));
        }
        if (!sorted_and_selected_disk_state.empty())
        {
            std::cout << "Initialized disks:\n" << fmt::format("{}", fmt::join(sorted_and_selected_disk_state, "\n")) << "\n";
        }

        std::vector<String> uninitialized_disks = client.getUninitializedDiskNames();
        if (!uninitialized_disks.empty())
        {
            std::set<String> sorted_uninitialized_disks(uninitialized_disks.begin(), uninitialized_disks.end());
            std::cout << "Uninitialized disks:\n" << fmt::format("{}", fmt::join(sorted_uninitialized_disks, "\n")) << "\n";
        }
    }

private:
};

CommandPtr makeCommandListDisks()
{
    return std::make_shared<DB::CommandListDisks>();
}
}
