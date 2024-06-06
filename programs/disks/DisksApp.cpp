#include "DisksApp.h"
#include <Client/ClientBase.h>
#include <Client/ReplxxLineReader.h>
#include "Common/Exception.h"
#include "Common/filesystemHelpers.h"
#include <Common/Config/ConfigProcessor.h>
#include "DisksClient.h"
#include "ICommand.h"

#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>

#include <Disks/registerDisks.h>

#include <Formats/registerFormats.h>
#include <Common/TerminalSize.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
};

LineReader::Patterns DisksApp::query_extenders = {"\\"};
LineReader::Patterns DisksApp::query_delimiters = {""};
String DisksApp::word_break_characters = " \t\v\f\a\b\r\n";

CommandPtr DisksApp::getCommandByName(const String & command) const
{
    try
    {
        if (auto it = aliases.find(command); it != aliases.end())
            return command_descriptions.at(it->second);

        return command_descriptions.at(command);
    }
    catch (std::out_of_range &)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The command `{}` is unknown", command);
    }
}

std::vector<String> DisksApp::getEmptyCompletion(CommandPtr command_) const
{
    auto answer = [&]() -> std::vector<String>
    {
        if (multidisk_commands.contains(command_->command_name))
        {
            return client->getAllFilesByPatternFromAllDisks("");
        }
        else
        {
            return client->getCurrentDiskWithPath().getAllFilesByPattern("");
        }
    }();
    for (const auto & disk_name : client->getAllDiskNames())
    {
        answer.push_back(disk_name);
    }
    for (const auto & option : command_->options_description.options())
    {
        answer.push_back("--" + option->long_name());
    }
    std::sort(answer.begin(), answer.end());
    return answer;
}

std::vector<String> DisksApp::getCompletions(const String & prefix) const
{
    auto arguments = po::split_unix(prefix, word_break_characters);
    if (arguments.empty())
    {
        return {};
    }
    if (word_break_characters.contains(prefix.back()))
    {
        CommandPtr command;
        try
        {
            command = getCommandByName(arguments[0]);
        }
        catch (...)
        {
            return {arguments.back()};
        }
        return getEmptyCompletion(command);
    }
    else if (arguments.size() == 1)
    {
        String command_prefix = arguments[0];
        std::vector<String> answer{};
        for (const auto & [word, _] : command_descriptions)
        {
            if (word.starts_with(command_prefix))
            {
                answer.push_back(word);
            }
        }
        if (!answer.empty())
        {
            return answer;
        }
        for (const auto & [word, _] : aliases)
        {
            if (word.starts_with(command_prefix))
            {
                answer.push_back(word);
            }
        }
        if (!answer.empty())
        {
            return answer;
        }
        return {command_prefix};
    }
    else
    {
        String last_token = arguments.back();
        CommandPtr command;
        try
        {
            command = getCommandByName(arguments[0]);
        }
        catch (...)
        {
            return {last_token};
        }
        auto answer = [&]() -> std::vector<String>
        {
            if (multidisk_commands.contains(command->command_name))
            {
                return client->getAllFilesByPatternFromAllDisks(last_token);
            }
            else
            {
                return client->getCurrentDiskWithPath().getAllFilesByPattern(last_token);
            }
        }();

        for (const auto & disk_name : client->getAllDiskNames())
        {
            if (disk_name.starts_with(last_token))
            {
                answer.push_back(disk_name);
            }
        }
        for (const auto & option : command->options_description.options())
        {
            String option_sign = "--" + option->long_name();
            if (option_sign.starts_with(last_token))
            {
                answer.push_back(option_sign);
            }
        }
        if (!answer.empty())
        {
            return answer;
        }
        else
        {
            return {last_token};
        }
    }
}

bool DisksApp::processQueryText(const String & text)
{
    if (exit_strings.find(text) != exit_strings.end())
        return false;
    CommandPtr command;
    try
    {
        auto arguments = po::split_unix(text, word_break_characters);
        command = getCommandByName(arguments[0]);
        arguments.erase(arguments.begin());
        command->execute(arguments, *client);
    }
    catch (DB::Exception & err)
    {
        int code = getCurrentExceptionCode();
        if (code == ErrorCodes::LOGICAL_ERROR)
        {
            throw err;
        }
        else if (code == ErrorCodes::BAD_ARGUMENTS)
        {
            std::cerr << err.message() << "\n"
                      << "\n";
            if (command.get())
            {
                std::cerr << "COMMAND: " << command->command_name << "\n";
                std::cerr << command->options_description << "\n";
            }
            else
            {
                printAvailableCommandsHelpMessage();
            }
        }
        else
        {
            std::cerr << err.message() << "\n";
        }
    }
    catch (std::exception & err)
    {
        std::cerr << err.what() << "\n";
    }

    return true;
}

void DisksApp::runInteractiveReplxx()
{
    ReplxxLineReader lr(
        suggest,
        history_file,
        /* multiline= */ false,
        query_extenders,
        query_delimiters,
        word_break_characters.c_str(),
        /* highlighter_= */ {});
    lr.enableBracketedPaste();

    while (true)
    {
        DiskWithPath disk_with_path = client->getCurrentDiskWithPath();
        String prompt = "\x1b[1;34m" + disk_with_path.getDisk()->getName() + "\x1b[0m:" + "\x1b[1;31m" + disk_with_path.getCurrentPath()
            + "\x1b[0m$ ";

        auto input = lr.readLine(prompt, "\x1b[1;31m:-] \x1b[0m");
        if (input.empty())
            break;

        if (!processQueryText(input))
            break;
    }
}

void DisksApp::parseAndCheckOptions(
    const std::vector<String> & arguments, const ProgramOptionsDescription & options_description, CommandLineOptions & options)
{
    auto parser = po::command_line_parser(arguments).options(options_description).allow_unregistered();
    po::parsed_options parsed = parser.run();
    po::store(parsed, options);
}

void DisksApp::addOptions()
{
    options_description.add_options()("help,h", "Print common help message")("config-file,C", po::value<String>(), "Set config file")(
        "disk", po::value<String>(), "Set disk name")("save-logs", "Save logs to a file")(
        "log-level", po::value<String>(), "Logging level")("query,q", po::value<String>(), "Query for a non-interactive mode");

    command_descriptions.emplace("list-disks", makeCommandListDisks());
    command_descriptions.emplace("copy", makeCommandCopy());
    command_descriptions.emplace("list", makeCommandList());
    command_descriptions.emplace("cd", makeCommandChangeDirectory());
    command_descriptions.emplace("move", makeCommandMove());
    command_descriptions.emplace("remove", makeCommandRemove());
    command_descriptions.emplace("link", makeCommandLink());
    command_descriptions.emplace("write", makeCommandWrite());
    command_descriptions.emplace("read", makeCommandRead());
    command_descriptions.emplace("mkdir", makeCommandMkDir());
    command_descriptions.emplace("switch-disk", makeCommandSwitchDisk());
#ifdef CLICKHOUSE_CLOUD
    command_descriptions.emplace("packed-io", makeCommandPackedIO());
#endif
    for (const auto & [command_name, command_ptr] : command_descriptions)
    {
        if (command_name != command_ptr->command_name)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Command name inside map doesn't coincide with actual command name");
        }
    }
}

void DisksApp::processOptions()
{
    if (options.count("config-file"))
        config().setString("config-file", options["config-file"].as<String>());
    if (options.count("disk"))
        config().setString("disk", options["disk"].as<String>());
    if (options.count("save-logs"))
        config().setBool("save-logs", true);
    if (options.count("log-level"))
        config().setString("log-level", options["log-level"].as<String>());
    if (options.count("query"))
        query = std::optional{options["query"].as<String>()};
}


void DisksApp::printEntryHelpMessage()
{
    std::cout << "ClickHouse disk management tool\n";
    std::cout << options_description << '\n';
}


void DisksApp::printAvailableCommandsHelpMessage()
{
    std::cout << "\x1b[1;33mAvailable commands:\x1b[0m\n";
    std::vector<std::pair<String, String>> commands_with_aliases_and_descrtiptions{};
    size_t maximal_command_length = 0;
    for (const auto & [current_command, _] : command_descriptions)
    {
        std::string command_string = command_descriptions[current_command]->command_name;
        bool need_comma = false;
        for (const auto & [alias_name, alias_command_name] : aliases)
        {
            if (alias_command_name == current_command)
            {
                if (std::exchange(need_comma, true))
                    command_string += ",";
                else
                    command_string += "(";
                command_string += alias_name;
            }
        }
        command_string += (need_comma ? ")" : "");
        maximal_command_length = std::max(maximal_command_length, command_string.size());
        commands_with_aliases_and_descrtiptions.push_back({std::move(command_string), command_descriptions[current_command]->command_name});
    }
    for (const auto & [command_with_aliases, description] : commands_with_aliases_and_descrtiptions)
    {
        std::cout << "\x1b[1;32m" << command_with_aliases << "\x1b[0m"
                  << std::string(maximal_command_length + 2 - command_with_aliases.size(), ' ') << description << "\n";
    }
}

void DisksApp::initializeHistoryFile()
{
    String home_path;
    const char * home_path_cstr = getenv("HOME"); // NOLINT(concurrency-mt-unsafe)
    if (home_path_cstr)
        home_path = home_path_cstr;
    if (config().has("history-file"))
        history_file = config().getString("history-file");
    else
        history_file = home_path + "/.disks-file-history";

    if (!history_file.empty() && !fs::exists(history_file))
    {
        try
        {
            FS::createFile(history_file);
        }
        catch (const ErrnoException & e)
        {
            if (e.getErrno() != EEXIST)
                throw;
        }
    }
}

void DisksApp::init(const std::vector<String> & common_arguments)
{
    addOptions();
    parseAndCheckOptions(common_arguments, options_description, options);

    po::notify(options);

    if (options.count("help"))
    {
        printEntryHelpMessage();
        printAvailableCommandsHelpMessage();
        exit(0); // NOLINT(concurrency-mt-unsafe)
    }

    processOptions();
}

String DisksApp::getDefaultConfigFileName()
{
    return "/etc/clickhouse-server/config.xml";
}

int DisksApp::main(const std::vector<String> & /*args*/)
{
    std::vector<std::string> keys;
    config().keys(keys);
    if (config().has("config-file") || fs::exists(getDefaultConfigFileName()))
    {
        String config_path = config().getString("config-file", getDefaultConfigFileName());
        ConfigProcessor config_processor(config_path, false, false);
        ConfigProcessor::setConfigPath(fs::path(config_path).parent_path());
        auto loaded_config = config_processor.loadConfig();
        config().add(loaded_config.configuration.duplicate(), false, false);
    }
    else
    {
        printEntryHelpMessage();
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No config-file specified");
    }

    config().keys(keys);
    initializeHistoryFile();

    if (config().has("save-logs"))
    {
        auto log_level = config().getString("log-level", "trace");
        Poco::Logger::root().setLevel(Poco::Logger::parseLevel(log_level));

        auto log_path = config().getString("logger.clickhouse-disks", "/var/log/clickhouse-server/clickhouse-disks.log");
        Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::FileChannel>(new Poco::FileChannel(log_path)));
    }
    else
    {
        auto log_level = config().getString("log-level", "none");
        Poco::Logger::root().setLevel(Poco::Logger::parseLevel(log_level));
    }

    registerDisks(/* global_skip_access_check= */ true);
    registerFormats();

    auto shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::DISKS);

    String path = config().getString("path", DBMS_DEFAULT_PATH);
    global_context->setPath(path);

    String main_disk = config().getString("disk", "default");

    auto validator = [](const Poco::Util::AbstractConfiguration &, const std::string &, const std::string &) { return true; };

    constexpr auto config_prefix = "storage_configuration.disks";
    auto disk_selector = std::make_shared<DiskSelector>(std::unordered_set{"cache", "encrypted"});
    disk_selector->initialize(config(), config_prefix, global_context, validator);

    std::vector<std::pair<DiskPtr, std::optional<String>>> disks_with_path;

    for (const auto & [_, disk_ptr] : disk_selector->getDisksMap())
    {
        disks_with_path.emplace_back(
            disk_ptr, (disk_ptr->getName() == "local") ? std::optional{fs::current_path().string()} : std::nullopt);
    }


    client = std::make_unique<DisksClient>(std::move(disks_with_path), main_disk);

    suggest.setCompletionsCallback([&](const String & prefix, size_t /* prefix_length */) { return getCompletions(prefix); });

    if (!query.has_value())
    {
        runInteractiveReplxx();
    }
    else
    {
        processQueryText(query.value());
    }

    return Application::EXIT_OK;
}

DisksApp::~DisksApp()
{
    if (global_context)
        global_context->shutdown();
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
        return 0;
    }
    catch (const boost::program_options::error & e)
    {
        std::cerr << "Bad arguments: " << e.what() << std::endl;
        return 0;
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << std::endl;
        return 0;
    }
}
