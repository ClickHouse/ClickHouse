#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>
#include "DisksApp.h"
#include "DisksClient.h"
#include "ICommand.h"

namespace DB
{

class CommandList final : public ICommand
{
public:
    explicit CommandList() : ICommand()
    {
        command_name = "list";
        description = "List files at path[s]";
        options_description.add_options()("recursive", "recursively list the directory")("all", "show hidden files")(
            "path", po::value<String>(), "the path of listing (mandatory, positional)");
        positional_options_description.add("path", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        bool recursive = options.count("recursive");
        bool show_hidden = options.count("all");
        auto disk = client.getCurrentDiskWithPath();
        String path = getValueFromCommandLineOptionsWithDefault<String>(options, "path", ".");

        if (recursive)
            listRecursive(disk, path, show_hidden);
        else
            list(disk, path, show_hidden);
    }

private:
    static void list(const DiskWithPath & disk, const std::string & path, bool show_hidden)
    {
        std::vector<String> file_names = disk.listAllFilesByPath(path);
        std::vector<String> selected_and_sorted_file_names{};

        for (const auto & file_name : file_names)
            if (show_hidden || (!file_name.starts_with('.')))
                selected_and_sorted_file_names.push_back(file_name);

        std::sort(selected_and_sorted_file_names.begin(), selected_and_sorted_file_names.end());
        for (const auto & file_name : selected_and_sorted_file_names)
        {
            std::cout << file_name << "\n";
        }
    }

    static void listRecursive(const DiskWithPath & disk, const std::string & relative_path, bool show_hidden)
    {
        std::vector<String> file_names = disk.listAllFilesByPath(relative_path);
        std::vector<String> selected_and_sorted_file_names{};

        std::cout << relative_path << ":\n";

        for (const auto & file_name : file_names)
            if (show_hidden || (!file_name.starts_with('.')))
                selected_and_sorted_file_names.push_back(file_name);

        std::sort(selected_and_sorted_file_names.begin(), selected_and_sorted_file_names.end());
        for (const auto & file_name : selected_and_sorted_file_names)
        {
            std::cout << file_name << "\n";
        }
        std::cout << "\n";

        for (const auto & file_name : selected_and_sorted_file_names)
        {
            auto path = [&]() -> String
            {
                if (relative_path.ends_with("/"))
                    return relative_path + file_name;

                return relative_path + "/" + file_name;
            }();
            if (disk.isDirectory(path))
            {
                listRecursive(disk, path, show_hidden);
            }
        }
    }
};

CommandPtr makeCommandList()
{
    return std::make_shared<DB::CommandList>();
}
}
