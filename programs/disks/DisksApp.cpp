#include "DisksApp.h"

#include <Disks/registerDisks.h>

#include <base/argsToConfig.h>

#include <Formats/registerFormats.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

size_t DisksApp::findCommandPos(std::vector<String> & common_arguments)
{
    for (size_t i = 0; i < common_arguments.size(); i++)
        if (supported_commands.contains(common_arguments[i]))
            return i + 1;
    return common_arguments.size();
}

void DisksApp::printHelpMessage(std::optional<ProgramOptionsDescription> & command_option_description)
{
    std::optional<ProgramOptionsDescription> help_description =
        createOptionsDescription("Help Message for clickhouse-disks", getTerminalWidth());

    help_description->add(command_option_description.value());

    std::cout << "ClickHouse disk management tool\n";
    std::cout << "Usage: ./clickhouse-disks [OPTION]\n";
    std::cout << "clickhouse-disks\n\n";

    for (const auto & command : supported_commands)
        std::cout << command_descriptions[command]->command_name
                  << "\t"
                  << command_descriptions[command]->description
                  << "\n\n";

    std::cout << command_option_description.value() << '\n';
}

String DisksApp::getDefaultConfigFileName()
{
    return "/etc/clickhouse-server/config.xml";
}

void DisksApp::addOptions(std::optional<ProgramOptionsDescription>  & options_description,
                          boost::program_options::positional_options_description & positional_options_description
)
{
    options_description->add_options()
        ("help,h", "Print common help message")
        ("config-file,C", po::value<String>(), "Set config file")
        ("disk", po::value<String>(), "Set disk name")
        ("command_name", po::value<String>(&command_name), "Name for command to do")
        ("send-logs", "Send logs")
        ("log-level", "Logging level")
        ;

    positional_options_description.add("command_name", 1);

    supported_commands = {"list-disks", "list", "move", "remove", "link", "copy", "write", "read"};

    command_descriptions.emplace("list-disks", makeCommandListDisks());
    command_descriptions.emplace("list", makeCommandList());
    command_descriptions.emplace("move", makeCommandMove());
    command_descriptions.emplace("remove", makeCommandRemove());
    command_descriptions.emplace("link", makeCommandLink());
    command_descriptions.emplace("copy", makeCommandCopy());
    command_descriptions.emplace("write", makeCommandWrite());
    command_descriptions.emplace("read", makeCommandRead());
}

void DisksApp::processOptions()
{
    if (options.count("config-file"))
        config().setString("config-file", options["config-file"].as<String>());
    if (options.count("disk"))
        config().setString("disk", options["disk"].as<String>());
}

void DisksApp::init(std::vector<String> & common_arguments)
{
    stopOptionsProcessing();

    std::optional<ProgramOptionsDescription> options_description;
    options_description.emplace(createOptionsDescription("clickhouse-disks", getTerminalWidth()));

    po::positional_options_description positional_options_description;

    addOptions(options_description, positional_options_description);

    size_t command_pos = findCommandPos(common_arguments);
    std::vector<String> global_flags(command_pos);
    command_arguments.resize(common_arguments.size() - command_pos);
    copy(common_arguments.begin(), common_arguments.begin() + command_pos, global_flags.begin());
    copy(common_arguments.begin() + command_pos, common_arguments.end(), command_arguments.begin());

    parseAndCheckOptions(options_description, positional_options_description, global_flags);

    po::notify(options);

    if (options.count("help"))
    {
        printHelpMessage(options_description);
        exit(0);
    }

    if (!supported_commands.contains(command_name))
    {
        printHelpMessage(options_description);
        throw DB::Exception("Bad Arguments", DB::ErrorCodes::BAD_ARGUMENTS);
    }

    processOptions();
}

void DisksApp::parseAndCheckOptions(
    std::optional<ProgramOptionsDescription> & options_description,
    boost::program_options::positional_options_description & positional_options_description,
    std::vector<String> & arguments)
{
    auto parser = po::command_line_parser(arguments)
        .options(options_description.value())
        .positional(positional_options_description)
        .allow_unregistered();
    po::parsed_options parsed = parser.run();
    po::store(parsed, options);
}

int DisksApp::main(const std::vector<String> & /*args*/)
{
    if (config().has("send-logs"))
    {
        auto log_level = config().getString("log-level", "trace");
        Poco::Logger::root().setLevel(Poco::Logger::parseLevel(log_level));

        auto log_path = config().getString("logger.clickhouse-disks", "/var/log/clickhouse-server/clickhouse-disks.log");
        Poco::Logger::root().setChannel(new Poco::FileChannel(log_path));
    }

    if (config().has("config-file") || fs::exists(getDefaultConfigFileName()))
    {
        String config_path = config().getString("config-file", getDefaultConfigFileName());
        ConfigProcessor config_processor(config_path, false, false);
        config_processor.setConfigPath(fs::path(config_path).parent_path());
        auto loaded_config = config_processor.loadConfig();
        config().add(loaded_config.configuration.duplicate(), false, false);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No config-file specifiged");
    }

    registerDisks();
    registerFormats();

    shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::DISKS);

    String path = config().getString("path", DBMS_DEFAULT_PATH);
    global_context->setPath(path);

    auto & command = command_descriptions[command_name];

    auto parser = po::command_line_parser(command_arguments).options(command->getCommandOptions()).allow_unregistered();
    po::parsed_options parsed = parser.run();
    auto positional_arguments = po::collect_unrecognized(parsed.options, po::collect_unrecognized_mode::include_positional);

    command->execute(positional_arguments, global_context, config());

    return Application::EXIT_OK;
}

}

int mainEntryClickHouseDisks(int argc, char ** argv)
{
    try
    {
        DB::DisksApp app;
        std::vector<String> common_arguments{argv + 1, argv + argc};
        app.init(common_arguments);
        return app.run();
    }
    catch (const DB::Exception & e)
    {
        std::cerr << DB::getExceptionMessage(e, false) << std::endl;
        return 1;
    }
    catch (const boost::program_options::error & e)
    {
        std::cerr << "Bad arguments: " << e.what() << std::endl;
        return DB::ErrorCodes::BAD_ARGUMENTS;
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << std::endl;
        return 1;
    }
}
