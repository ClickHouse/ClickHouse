#include "Core/BaseSettingsProgramOptions.h"
#include <Client/ClientBaseApplication.h>
#include <Client/LineReader.h>
#include <Client/ClientBaseHelpers.h>
#include <Client/TestHint.h>
#include <Client/InternalTextLogs.h>
#include <Client/TestTags.h>

#include <base/argsToConfig.h>
#include <base/safeExit.h>
#include <base/scope_guard.h>
#include <Core/Block.h>
#include <Core/Protocol.h>
#include <Common/DateLUT.h>
#include <Common/MemoryTracker.h>
#include <Common/scope_guard_safe.h>
#include <Common/Exception.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/typeid_cast.h>
#include <Common/UTF8Helpers.h>
#include <Common/TerminalSize.h>
#include <Common/clearPasswordFromCommandLine.h>
#include <Common/filesystemHelpers.h>
#include <Common/tryGetFileNameByFileDescriptor.h>
#include <Common/NetException.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Formats/FormatFactory.h>

#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/Kusto/ParserKQLStatement.h>

#include <Processors/Formats/Impl/NullFormat.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Interpreters/ProfileEventsExt.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/CompressionMethod.h>
#include <IO/ForkWriteBuffer.h>

#include <Access/AccessControl.h>
#include <Storages/ColumnsDescription.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <iostream>
#include <filesystem>
#include <map>
#include <unordered_map>

#include <Common/config_version.h>


using namespace std::literals;


namespace CurrentMetrics
{
    extern const Metric MemoryTracking;
}

namespace DB
{


namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_SET_SIGNAL_HANDLER;
    extern const int UNRECOGNIZED_ARGUMENTS;
}

static ClientInfo::QueryKind parseQueryKind(const String & query_kind)
{
    if (query_kind == "initial_query")
        return ClientInfo::QueryKind::INITIAL_QUERY;
    if (query_kind == "secondary_query")
        return ClientInfo::QueryKind::SECONDARY_QUERY;
    if (query_kind == "no_query")
        return ClientInfo::QueryKind::NO_QUERY;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown query kind {}", query_kind);
}

}


namespace DB
{


/// This signal handler is set only for SIGINT and SIGQUIT.
void interruptSignalHandler(int signum)
{
    if (ClientBaseApplication::getInstance().tryStopQuery())
        safeExit(128 + signum);
}


ClientBaseApplication::~ClientBaseApplication() = default;

ClientBaseApplication::ClientBaseApplication() : ClientBase(STDIN_FILENO, STDOUT_FILENO, STDERR_FILENO, std::cin, std::cout, std::cerr) {}


void ClientBaseApplication::setupSignalHandler()
{
    ClientBaseApplication::getInstance().stopQuery();

    struct sigaction new_act;
    memset(&new_act, 0, sizeof(new_act));

    new_act.sa_handler = interruptSignalHandler;
    new_act.sa_flags = 0;

#if defined(OS_DARWIN)
    sigemptyset(&new_act.sa_mask);
#else
    if (sigemptyset(&new_act.sa_mask))
        throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler.");
#endif

    if (sigaction(SIGINT, &new_act, nullptr))
        throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler.");

    if (sigaction(SIGQUIT, &new_act, nullptr))
        throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler.");
}


namespace
{

/// Define transparent hash to we can use
/// std::string_view with the containers
struct TransparentStringHash
{
    using is_transparent = void;
    size_t operator()(std::string_view txt) const
    {
        return std::hash<std::string_view>{}(txt);
    }
};

/*
 * This functor is used to parse command line arguments and replace dashes with underscores,
 * allowing options to be specified using either dashes or underscores.
 */
class OptionsAliasParser
{
public:
    explicit OptionsAliasParser(const boost::program_options::options_description& options)
    {
        options_names.reserve(options.options().size());
        for (const auto& option : options.options())
            options_names.insert(option->long_name());
    }

    /*
     * Parses arguments by replacing dashes with underscores, and matches the resulting name with known options
     * Implements boost::program_options::ext_parser logic
     */
    std::pair<std::string, std::string> operator()(const std::string& token) const
    {
        if (token.find("--") != 0)
            return {};
        std::string arg = token.substr(2);

        // divide token by '=' to separate key and value if options style=long_allow_adjacent
        auto pos_eq = arg.find('=');
        std::string key = arg.substr(0, pos_eq);

        if (options_names.contains(key))
            // option does not require any changes, because it is already correct
            return {};

        std::replace(key.begin(), key.end(), '-', '_');
        if (!options_names.contains(key))
            // after replacing '-' with '_' argument is still unknown
            return {};

        std::string value;
        if (pos_eq != std::string::npos && pos_eq < arg.size())
            value = arg.substr(pos_eq + 1);

        return {key, value};
    }

private:
    std::unordered_set<std::string> options_names;
};

}


void ClientBaseApplication::parseAndCheckOptions(OptionsDescription & options_description, po::variables_map & options, Arguments & arguments)
{
    if (allow_repeated_settings)
        addProgramOptionsAsMultitokens(cmd_settings, options_description.main_description.value());
    else
        addProgramOptions(cmd_settings, options_description.main_description.value());

    if (allow_merge_tree_settings)
    {
        /// Add merge tree settings manually, because names of some settings
        /// may clash. Query settings have higher priority and we just
        /// skip ambiguous merge tree settings.
        auto & main_options = options_description.main_description.value();

        std::unordered_set<std::string, TransparentStringHash, std::equal_to<>> main_option_names;
        for (const auto & option : main_options.options())
            main_option_names.insert(option->long_name());

        for (const auto & setting : cmd_merge_tree_settings.all())
        {
            const auto add_setting = [&](const std::string_view name)
            {
                if (auto it = main_option_names.find(name); it != main_option_names.end())
                    return;

                if (allow_repeated_settings)
                    addProgramOptionAsMultitoken(cmd_merge_tree_settings, main_options, name, setting);
                else
                    addProgramOption(cmd_merge_tree_settings, main_options, name, setting);
            };

            const auto & setting_name = setting.getName();

            add_setting(setting_name);

            const auto & settings_to_aliases = MergeTreeSettings::Traits::settingsToAliases();
            if (auto it = settings_to_aliases.find(setting_name); it != settings_to_aliases.end())
            {
                for (const auto alias : it->second)
                {
                    add_setting(alias);
                }
            }
        }
    }

    /// Parse main commandline options.
    auto parser = po::command_line_parser(arguments)
                      .options(options_description.main_description.value())
                      .extra_parser(OptionsAliasParser(options_description.main_description.value()))
                      .allow_unregistered();
    po::parsed_options parsed = parser.run();

    /// Check unrecognized options without positional options.
    auto unrecognized_options = po::collect_unrecognized(parsed.options, po::collect_unrecognized_mode::exclude_positional);
    if (!unrecognized_options.empty())
    {
        auto hints = this->getHints(unrecognized_options[0]);
        if (!hints.empty())
            throw Exception(ErrorCodes::UNRECOGNIZED_ARGUMENTS, "Unrecognized option '{}'. Maybe you meant {}",
                            unrecognized_options[0], toString(hints));

        throw Exception(ErrorCodes::UNRECOGNIZED_ARGUMENTS, "Unrecognized option '{}'", unrecognized_options[0]);
    }

    /// Check positional options (options after ' -- ', ex: clickhouse-client -- <options>).
    if (std::ranges::count_if(parsed.options, [](const auto & op){ return !op.unregistered && op.string_key.empty() && !op.original_tokens[0].starts_with("--"); }) > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Positional options are not supported.");

    po::store(parsed, options);
}


void ClientBaseApplication::addMultiquery(std::string_view query, Arguments & common_arguments) const
{
    common_arguments.emplace_back("--multiquery");
    common_arguments.emplace_back("-q");
    common_arguments.emplace_back(query);
}

Poco::Util::LayeredConfiguration & ClientBaseApplication::getClientConfiguration()
{
    return config();
}

void ClientBaseApplication::init(int argc, char ** argv)
{
    namespace po = boost::program_options;

    /// Don't parse options with Poco library, we prefer neat boost::program_options.
    stopOptionsProcessing();

    stdin_is_a_tty = isatty(STDIN_FILENO);
    stdout_is_a_tty = isatty(STDOUT_FILENO);
    stderr_is_a_tty = isatty(STDERR_FILENO);
    terminal_width = getTerminalWidth();

    Arguments common_arguments{""}; /// 0th argument is ignored.
    std::vector<Arguments> external_tables_arguments;
    std::vector<Arguments> hosts_and_ports_arguments;

    readArguments(argc, argv, common_arguments, external_tables_arguments, hosts_and_ports_arguments);

    /// Support for Unicode dashes
    /// Interpret Unicode dashes as default double-hyphen
    for (auto & arg : common_arguments)
    {
        // replace em-dash(U+2014)
        boost::replace_all(arg, "—", "--");
        // replace en-dash(U+2013)
        boost::replace_all(arg, "–", "--");
        // replace mathematical minus(U+2212)
        boost::replace_all(arg, "−", "--");
    }


    po::variables_map options;
    OptionsDescription options_description;
    options_description.main_description.emplace(createOptionsDescription("Main options", terminal_width));

    /// Common options for clickhouse-client and clickhouse-local.
    options_description.main_description->add_options()
        ("help", "produce help message")
        ("version,V", "print version information and exit")
        ("version-clean", "print version in machine-readable format and exit")

        ("config-file,C", po::value<std::string>(), "config-file path")

        ("query,q", po::value<std::string>(), "query")
        ("queries-file", po::value<std::vector<std::string>>()->multitoken(),
            "file path with queries to execute; multiple files can be specified (--queries-file file1 file2...)")
        ("multiquery,n", "If specified, multiple queries separated by semicolons can be listed after --query. For convenience, it is also possible to omit --query and pass the queries directly after --multiquery.")
        ("multiline,m", "If specified, allow multiline queries (do not send the query on Enter)")
        ("database,d", po::value<std::string>(), "database")
        ("query_kind", po::value<std::string>()->default_value("initial_query"), "One of initial_query/secondary_query/no_query")
        ("query_id", po::value<std::string>(), "query_id")

        ("history_file", po::value<std::string>(), "path to history file")

        ("stage", po::value<std::string>()->default_value("complete"), "Request query processing up to specified stage: complete,fetch_columns,with_mergeable_state,with_mergeable_state_after_aggregation,with_mergeable_state_after_aggregation_and_limit")
        ("progress", po::value<ProgressOption>()->implicit_value(ProgressOption::TTY, "tty")->default_value(ProgressOption::DEFAULT, "default"), "Print progress of queries execution - to TTY: tty|on|1|true|yes; to STDERR non-interactive mode: err; OFF: off|0|false|no; DEFAULT - interactive to TTY, non-interactive is off")

        ("disable_suggestion,A", "Disable loading suggestion data. Note that suggestion data is loaded asynchronously through a second connection to ClickHouse server. Also it is reasonable to disable suggestion if you want to paste a query with TAB characters. Shorthand option -A is for those who get used to mysql client.")
        ("time,t", "print query execution time to stderr in non-interactive mode (for benchmarks)")

        ("echo", "in batch mode, print query before execution")
        ("verbose", "print query and other debugging info")

        ("log-level", po::value<std::string>(), "log level")
        ("server_logs_file", po::value<std::string>(), "put server logs into specified file")

        ("suggestion_limit", po::value<int>()->default_value(10000),
            "Suggestion limit for how many databases, tables and columns to fetch.")

        ("format,f", po::value<std::string>(), "default output format")
        ("vertical,E", "vertical output format, same as --format=Vertical or FORMAT Vertical or \\G at end of command")
        ("highlight", po::value<bool>()->default_value(true), "enable or disable basic syntax highlight in interactive command line")

        ("ignore-error", "do not stop processing in multiquery mode")
        ("stacktrace", "print stack traces of exceptions")
        ("hardware-utilization", "print hardware utilization information in progress bar")
        ("print-profile-events", po::value(&profile_events.print)->zero_tokens(), "Printing ProfileEvents packets")
        ("profile-events-delay-ms", po::value<UInt64>()->default_value(profile_events.delay_ms), "Delay between printing `ProfileEvents` packets (-1 - print only totals, 0 - print every single packet)")
        ("processed-rows", "print the number of locally processed rows")

        ("interactive", "Process queries-file or --query query and start interactive mode")
        ("pager", po::value<std::string>(), "Pipe all output into this command (less or similar)")
        ("max_memory_usage_in_client", po::value<int>(), "Set memory limit in client/local server")
    ;

    addOptions(options_description);

    auto getter = [](const auto & op)
    {
        String op_long_name = op->long_name();
        return "--" + String(op_long_name);
    };

    if (options_description.main_description)
    {
        const auto & main_options = options_description.main_description->options();
        std::transform(main_options.begin(), main_options.end(), std::back_inserter(cmd_options), getter);
    }

    if (options_description.external_description)
    {
        const auto & external_options = options_description.external_description->options();
        std::transform(external_options.begin(), external_options.end(), std::back_inserter(cmd_options), getter);
    }

    parseAndCheckOptions(options_description, options, common_arguments);
    po::notify(options);

    if (options.count("version") || options.count("V"))
    {
        showClientVersion();
        exit(0); // NOLINT(concurrency-mt-unsafe)
    }

    if (options.count("version-clean"))
    {
        std::cout << VERSION_STRING;
        exit(0); // NOLINT(concurrency-mt-unsafe)
    }

    /// Output of help message.
    if (options.count("help")
        || (options.count("host") && options["host"].as<std::string>() == "elp")) /// If user writes -help instead of --help.
    {
        printHelpMessage(options_description, false);
        exit(0); // NOLINT(concurrency-mt-unsafe)
    }

    /// Common options for clickhouse-client and clickhouse-local.
    if (options.count("time"))
        getClientConfiguration().setBool("print-time-to-stderr", true);
    if (options.count("query"))
        getClientConfiguration().setString("query", options["query"].as<std::string>());
    if (options.count("query_id"))
        getClientConfiguration().setString("query_id", options["query_id"].as<std::string>());
    if (options.count("database"))
        getClientConfiguration().setString("database", options["database"].as<std::string>());
    if (options.count("config-file"))
        getClientConfiguration().setString("config-file", options["config-file"].as<std::string>());
    if (options.count("queries-file"))
        queries_files = options["queries-file"].as<std::vector<std::string>>();
    if (options.count("multiline"))
        getClientConfiguration().setBool("multiline", true);
    if (options.count("multiquery"))
        getClientConfiguration().setBool("multiquery", true);
    if (options.count("ignore-error"))
        getClientConfiguration().setBool("ignore-error", true);
    if (options.count("format"))
        getClientConfiguration().setString("format", options["format"].as<std::string>());
    if (options.count("vertical"))
        getClientConfiguration().setBool("vertical", true);
    if (options.count("stacktrace"))
        getClientConfiguration().setBool("stacktrace", true);
    if (options.count("print-profile-events"))
        getClientConfiguration().setBool("print-profile-events", true);
    if (options.count("profile-events-delay-ms"))
        getClientConfiguration().setUInt64("profile-events-delay-ms", options["profile-events-delay-ms"].as<UInt64>());
    /// Whether to print the number of processed rows at
    if (options.count("processed-rows"))
        getClientConfiguration().setBool("print-num-processed-rows", true);
    if (options.count("progress"))
    {
        switch (options["progress"].as<ProgressOption>())
        {
            case DEFAULT:
                getClientConfiguration().setString("progress", "default");
                break;
            case OFF:
                getClientConfiguration().setString("progress", "off");
                break;
            case TTY:
                getClientConfiguration().setString("progress", "tty");
                break;
            case ERR:
                getClientConfiguration().setString("progress", "err");
                break;
        }
    }
    if (options.count("echo"))
        getClientConfiguration().setBool("echo", true);
    if (options.count("disable_suggestion"))
        getClientConfiguration().setBool("disable_suggestion", true);
    if (options.count("suggestion_limit"))
        getClientConfiguration().setInt("suggestion_limit", options["suggestion_limit"].as<int>());
    if (options.count("highlight"))
        getClientConfiguration().setBool("highlight", options["highlight"].as<bool>());
    if (options.count("history_file"))
        getClientConfiguration().setString("history_file", options["history_file"].as<std::string>());
    if (options.count("verbose"))
        getClientConfiguration().setBool("verbose", true);
    if (options.count("interactive"))
        getClientConfiguration().setBool("interactive", true);
    if (options.count("pager"))
        getClientConfiguration().setString("pager", options["pager"].as<std::string>());

    if (options.count("log-level"))
        Poco::Logger::root().setLevel(options["log-level"].as<std::string>());
    if (options.count("server_logs_file"))
        server_logs_file = options["server_logs_file"].as<std::string>();

    query_processing_stage = QueryProcessingStage::fromString(options["stage"].as<std::string>());
    query_kind = parseQueryKind(options["query_kind"].as<std::string>());
    profile_events.print = options.count("print-profile-events");
    profile_events.delay_ms = options["profile-events-delay-ms"].as<UInt64>();

    this->processOptions(options_description, options, external_tables_arguments, hosts_and_ports_arguments);
    {
        std::unordered_set<std::string> alias_names;
        alias_names.reserve(options_description.main_description->options().size());
        for (const auto& option : options_description.main_description->options())
            alias_names.insert(option->long_name());
        argsToConfig(common_arguments, getClientConfiguration(), 100, &alias_names);
    }

    clearPasswordFromCommandLine(argc, argv);

    /// Limit on total memory usage
    size_t max_client_memory_usage = getClientConfiguration().getInt64("max_memory_usage_in_client", 0 /*default value*/);
    if (max_client_memory_usage != 0)
    {
        total_memory_tracker.setHardLimit(max_client_memory_usage);
        total_memory_tracker.setDescription("(total)");
        total_memory_tracker.setMetric(CurrentMetrics::MemoryTracking);
    }
}

}
