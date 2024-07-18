#include "ICommand.h"
#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandList final : public ICommand
{
public:
    CommandList()
    {
        command_name = "list";
        command_option_description.emplace(createOptionsDescription("Allowed options", getTerminalWidth()));
        description = "List files at path[s]";
        usage = "list [OPTION]... <PATH>...";
        command_option_description->add_options()
            ("recursive", "recursively list all directories");
    }

    void processOptions(
        Poco::Util::LayeredConfiguration & config,
        po::variables_map & options) const override
    {
        if (options.count("recursive"))
            config.setBool("recursive", true);
    }

    void execute(
        const std::vector<String> & command_arguments,
        std::shared_ptr<DiskSelector> & disk_selector,
        Poco::Util::LayeredConfiguration & config) override
    {
        if (command_arguments.size() != 1)
        {
            printHelpMessage();
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Bad Arguments");
        }

        String disk_name = config.getString("disk", "default");

        const String & path =  command_arguments[0];

        DiskPtr disk = disk_selector->get(disk_name);

        String relative_path = validatePathAndGetAsRelative(path);

        bool recursive = config.getBool("recursive", false);

        if (recursive)
            listRecursive(disk, relative_path);
        else
            list(disk, relative_path);
    }

private:
    static void list(const DiskPtr & disk, const std::string & relative_path)
    {
        std::vector<String> file_names;
        disk->listFiles(relative_path, file_names);

        for (const auto & file_name : file_names)
            std::cout << file_name << '\n';
    }

    static void listRecursive(const DiskPtr & disk, const std::string & relative_path)
    {
        std::vector<String> file_names;
        disk->listFiles(relative_path, file_names);

        std::cout << relative_path << ":\n";

        if (!file_names.empty())
        {
            for (const auto & file_name : file_names)
                std::cout << file_name << '\n';
            std::cout << "\n";
        }

        for (const auto & file_name : file_names)
        {
            auto path = relative_path.empty() ? file_name : (relative_path + "/" + file_name);
            if (disk->isDirectory(path))
                listRecursive(disk, path);
        }
    }
};
}

std::unique_ptr <DB::ICommand> makeCommandList()
{
    return std::make_unique<DB::CommandList>();
}
