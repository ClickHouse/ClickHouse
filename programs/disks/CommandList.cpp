#pragma once

#include "ICommand.h"
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandList : public ICommand
{
public:
    CommandList()
    {
        command_name = "list";
        command_option_description.emplace(createOptionsDescription("Allowed options", getTerminalWidth()));
        description = "List files (the default disk is used by default)\nPath should be in format './' or './path' or 'path'";
        usage = "list [OPTION]... <PATH>...";
        command_option_description->add_options()
            ("recursive", "recursively list all directories")
            ;
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
        DB::ContextMutablePtr & global_context,
        Poco::Util::LayeredConfiguration & config) override
    {
        if (command_arguments.size() != 1)
        {
            printHelpMessage();
            throw DB::Exception("Bad Arguments", DB::ErrorCodes::BAD_ARGUMENTS);
        }

        String disk_name = config.getString("disk", "default");

        String path =  command_arguments[0];

        DiskPtr disk = global_context->getDisk(disk_name);

        String full_path = fullPathWithValidate(disk, path);

        bool recursive = config.getBool("recursive", false);

        if (recursive)
            listRecursive(disk, full_path);
        else
            list(disk, full_path);
    }

private:
    static void list(const DiskPtr & disk, const std::string & full_path)
    {
        std::vector<String> file_names;
        disk->listFiles(full_path, file_names);

        for (const auto & file_name : file_names)
            std::cout << file_name << '\n';
    }

    static void listRecursive(const DiskPtr & disk, const std::string & full_path)
    {
        std::vector<String> file_names;
        disk->listFiles(full_path, file_names);

        std::cout << full_path << ":\n";
        for (const auto & file_name : file_names)
            std::cout << file_name << '\n';
        std::cout << "\n";

        for (const auto & file_name : file_names)
        {
            auto path = full_path + "/" + file_name;
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
