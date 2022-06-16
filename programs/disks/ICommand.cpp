#include "ICommand.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ICommand::printHelpMessage() const
{
    std::cout << "Command: " << command_name << '\n';
    std::cout << "Description: " << description << '\n';
    std::cout << "Usage: " << usage << '\n';

    if (command_option_description)
    {
        auto options = *command_option_description;
        if (!options.options().empty())
            std::cout << options << '\n';
    }
}

String ICommand::fullPathWithValidate(const DiskPtr & disk, const String & path)
{
    String full_path = (fs::absolute(disk->getPath()) / path).lexically_normal();
    String disk_path = fs::path(disk->getPath());

    if (!full_path.starts_with(disk_path))
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS, "Path {} must be inside disk path {}", path, disk->getPath());

    return full_path;
}

}
