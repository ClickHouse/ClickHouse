#include "ICommand.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ICommand::printHelpMessage() const
{
    std::cout << command_name << '\n';
    std::cout << description << '\n';
    std::cout << usage << '\n';
    std::cout << command_option_description.value() << '\n';
}

String ICommand::fullPathWithValidate(const DiskPtr & disk, const String & path)
{
    String full_path = (fs::absolute(disk->getPath()) / path).lexically_normal();
    String disk_path = fs::path(disk->getPath());
    if (full_path.find(disk_path) == String::npos)
        throw DB::Exception("Error path", DB::ErrorCodes::BAD_ARGUMENTS);
    return full_path;
}

void ICommand::execute(
    const std::vector<String> & command_arguments,
    const DB::ContextMutablePtr & global_context,
    Poco::Util::AbstractConfiguration & config,
    po::variables_map & options)
{
    command_option_description->add_options()
        ("help,h", "print help message for list")
        ("command_arguments", po::value<std::vector<String>>(&pos_arguments), "command arguments for command")
        ;
    positional_options_description.add("command_arguments", -1);

    auto parser = po::command_line_parser(command_arguments).options(command_option_description.value()).positional(positional_options_description).allow_unregistered();
    po::parsed_options parsed = parser.run();
    po::store(parsed, options);
    po::notify(options);

    if (options.count("help"))
    {
        printHelpMessage();
        exit(0);
    }
    processOptions(config, options);

    executeImpl(global_context, config);
}

}
