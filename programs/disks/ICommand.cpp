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
    if (!full_path.starts_with(disk_path))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Path {} must be inside disk path {}", path, disk->getPath());
    return full_path;
}

void ICommand::execute(
    const std::vector<String> & command_arguments,
    DB::ContextMutablePtr & global_context,
    Poco::Util::LayeredConfiguration & config)
{
    po::variables_map options;

    command_option_description->add_options()
        ("help,h", "print help message for list")
        ("config-file,C", po::value<String>(), "set config file")
        ("command_arguments", po::value<std::vector<String>>(&pos_arguments), "command arguments for command")
        ;
    positional_options_description.add("command_arguments", -1);

    auto parser = po::command_line_parser(command_arguments).options(command_option_description.value()).positional(positional_options_description).allow_unregistered();
    po::parsed_options parsed = parser.run();
    po::store(parsed, options);
    po::notify(options);

    if (options.count("config-file") && config.has("config-file"))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Can be only one config-file");

    if (options.count("help"))
    {
        printHelpMessage();
        exit(0);
    }
    processOptions(config, options);

    String config_path = config.getString("config-file", "/etc/clickhouse-server/config.xml");
    DB::ConfigProcessor config_processor(config_path, false, false);
    config_processor.setConfigPath(fs::path(config_path).parent_path());
    auto loaded_config = config_processor.loadConfig();
    config.add(loaded_config.configuration.duplicate(), false, false);

    String path = config.getString("path", DBMS_DEFAULT_PATH);
    global_context->setPath(path);

    executeImpl(global_context, config);
}

}
