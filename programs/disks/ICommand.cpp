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

String ICommand::validatePathAndGetAsRelative(const String & path)
{
    /// If path contain non-normalized symbols like ../ or something like
    /// this it can be dangerous, disallow such paths. Also since clickhouse-disks
    /// is not an interactive program (don't track you current path) it's OK to disallow them.
    if (path.find("..") != std::string::npos || fs::path(path).lexically_normal().string() != path)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Path {} is not normalized", path);

    /// If path is absolute we should keep it as relative inside disk, so disk will look like
    /// an ordinary filesystem with root.
    if (fs::path(path).is_absolute())
        return path.substr(1);

    return path;
}

}
