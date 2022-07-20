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

void ICommand::addOptions(ProgramOptionsDescription & options_description)
{
    if (!command_option_description || command_option_description->options().empty())
        return;

    options_description.add(*command_option_description);
}

String ICommand::fullPathWithValidate(const DiskPtr & disk, const String & path)
{
    if (fs::path(path).lexically_normal().string() != path)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Path {} is not normalized", path);

    String disk_path = fs::canonical(fs::path(disk->getPath())) / "";
    String full_path = (fs::absolute(disk_path) / path).lexically_normal();

    if (!full_path.starts_with(disk_path))
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS, "Path {} must be inside disk path {}", full_path, disk_path);

    return path;
}

}
