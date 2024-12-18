#include <Client/ClientApplicationBase.h>

#include <base/argsToConfig.h>
#include <base/safeExit.h>
#include <Common/clearPasswordFromCommandLine.h>
#include <Common/TerminalSize.h>
#include <Common/Exception.h>
#include <Common/SignalHandlers.h>

#include <Common/config_version.h>
#include "config.h"

#include <unordered_set>
#include <string>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>

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

/// This signal handler is set only for SIGINT and SIGQUIT.
void interruptSignalHandler(int signum)
{
    /// Signal handler might be called even before the setup is fully finished
    /// and client application started to process the query.
    /// Because of that we have to manually check it.
    if (auto * instance = ClientApplicationBase::instanceRawPtr(); instance)
        if (auto * base = dynamic_cast<ClientApplicationBase *>(instance); base)
            if (base->tryStopQuery())
                safeExit(128 + signum);
}

ClientApplicationBase::~ClientApplicationBase()
{
    try
    {
        writeSignalIDtoSignalPipe(SignalListener::StopThread);
        signal_listener_thread.join();
        HandledSignals::instance().reset();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

ClientApplicationBase::ClientApplicationBase() : ClientBase(STDIN_FILENO, STDOUT_FILENO, STDERR_FILENO, std::cin, std::cout, std::cerr) {}

ClientApplicationBase & ClientApplicationBase::getInstance()
{
    return dynamic_cast<ClientApplicationBase&>(Poco::Util::Application::instance());
}

void ClientApplicationBase::setupSignalHandler()
{
    ClientApplicationBase::getInstance().stopQuery();

    struct sigaction new_act;
    memset(&new_act, 0, sizeof(new_act));

    new_act.sa_handler = interruptSignalHandler;
    new_act.sa_flags = 0;

#if defined(OS_DARWIN)
    sigemptyset(&new_act.sa_mask);
#else
    if (sigemptyset(&new_act.sa_mask))
        throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler");
#endif

    if (sigaction(SIGINT, &new_act, nullptr))
        throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler");

    if (sigaction(SIGQUIT, &new_act, nullptr))
        throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler");
}

void ClientApplicationBase::addMultiquery(std::string_view query, Arguments & common_arguments) const
{
    common_arguments.emplace_back("--multiquery");
    common_arguments.emplace_back("-q");
    common_arguments.emplace_back(query);
}

Poco::Util::LayeredConfiguration & ClientApplicationBase::getClientConfiguration()
{
    return config();
}

void ClientApplicationBase::init(int argc, char ** argv)
{
    namespace po = boost::program_options;

    /// Don't parse options with Poco library, we prefer neat boost::program_options.
    stopOptionsProcessing();

    stdin_is_a_tty = isatty(STDIN_FILENO);
    stdout_is_a_tty = isatty(STDOUT_FILENO);
    stderr_is_a_tty = isatty(STDERR_FILENO);
    terminal_width = getTerminalWidth();

    std::vector<Arguments> external_tables_arguments;
    Arguments common_arguments = {""}; /// 0th argument is ignored.
    std::vector<Arguments> hosts_and_ports_arguments;

    if (argc)
        argv0 = argv[0];
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


    OptionsDescription options_description;
    options_description.main_description.emplace(createOptionsDescription("Main options", terminal_width));

    /// Common options for clickhouse-client and clickhouse-local.
    options_description.main_description->add_options()
        ("help", "print usage summary, combine with --verbose to display all options")
        ("verbose", "print query and other debugging info")
        ("version,V", "print version information and exit")
        ("version-clean", "print version in machine-readable format and exit")

        ("config-file,C", po::value<std::string>(), "config-file path")

        ("proto_caps", po::value<std::string>(), "enable/disable chunked protocol: chunked_optional, notchunked, notchunked_optional, send_chunked, send_chunked_optional, send_notchunked, send_notchunked_optional, recv_chunked, recv_chunked_optional, recv_notchunked, recv_notchunked_optional")

        ("query,q", po::value<std::vector<std::string>>()->multitoken(), R"(Query. Can be specified multiple times (--query "SELECT 1" --query "SELECT 2") or once with multiple comma-separated queries (--query "SELECT 1; SELECT 2;"). In the latter case, INSERT queries with non-VALUE format must be separated by empty lines.)")
        ("queries-file", po::value<std::vector<std::string>>()->multitoken(), "file path with queries to execute; multiple files can be specified (--queries-file file1 file2...)")
        ("multiquery,n", "Obsolete, does nothing")
        ("multiline,m", "If specified, allow multiline queries (do not send the query on Enter)")
        ("database,d", po::value<std::string>(), "database")
        ("query_kind", po::value<std::string>()->default_value("initial_query"), "One of initial_query/secondary_query/no_query")
        ("query_id", po::value<std::string>(), "query_id")

        ("history_file", po::value<std::string>(), "path to history file")

        ("stage", po::value<std::string>()->default_value("complete"), "Request query processing up to specified stage: complete,fetch_columns,with_mergeable_state,with_mergeable_state_after_aggregation,with_mergeable_state_after_aggregation_and_limit")
        ("progress", po::value<ProgressOption>()->implicit_value(ProgressOption::TTY, "tty")->default_value(ProgressOption::DEFAULT, "default"), "Print progress of queries execution - to TTY: tty|on|1|true|yes; to STDERR non-interactive mode: err; OFF: off|0|false|no; DEFAULT - interactive to TTY, non-interactive is off")

        ("disable_suggestion,A", "Disable loading suggestion data. Note that suggestion data is loaded asynchronously through a second connection to ClickHouse server. Also it is reasonable to disable suggestion if you want to paste a query with TAB characters. Shorthand option -A is for those who get used to mysql client.")
        ("wait_for_suggestions_to_load", "Load suggestion data synchonously.")
        ("time,t", "print query execution time to stderr in non-interactive mode (for benchmarks)")
        ("memory-usage", po::value<std::string>()->implicit_value("default")->default_value("none"), "print memory usage to stderr in non-interactive mode (for benchmarks). Values: 'none', 'default', 'readable'")

        ("echo", "in batch mode, print query before execution")

        ("log-level", po::value<std::string>(), "log level")
        ("server_logs_file", po::value<std::string>(), "put server logs into specified file")

        ("suggestion_limit", po::value<int>()->default_value(10000), "Suggestion limit for how many databases, tables and columns to fetch.")

        ("format,f", po::value<std::string>(), "default output format (and input format for clickhouse-local)")
        ("output-format", po::value<std::string>(), "default output format (this option has preference over --format)")

        ("vertical,E", "vertical output format, same as --format=Vertical or FORMAT Vertical or \\G at end of command")
        ("highlight", po::value<bool>()->default_value(true), "enable or disable basic syntax highlight in interactive command line")

        ("ignore-error", "do not stop processing when an error occurs")
        ("stacktrace", "print stack traces of exceptions")
        ("hardware-utilization", "print hardware utilization information in progress bar")
        ("print-profile-events", po::value(&profile_events.print)->zero_tokens(), "Printing ProfileEvents packets")
        ("profile-events-delay-ms", po::value<UInt64>()->default_value(profile_events.delay_ms), "Delay between printing `ProfileEvents` packets (-1 - print only totals, 0 - print every single packet)")
        ("processed-rows", "print the number of locally processed rows")

        ("interactive", "Process queries-file or --query query and start interactive mode")
        ("pager", po::value<std::string>(), "Pipe all output into this command (less or similar)")
        ("max_memory_usage_in_client", po::value<std::string>(), "Set memory limit in client/local server")

        ("client_logs_file", po::value<std::string>(), "Path to a file for writing client logs. Currently we only have fatal logs (when the client crashes)")
    ;

    addOptions(options_description);

    OptionsDescription options_description_non_verbose = options_description;

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

    po::variables_map options;
    parseAndCheckOptions(options_description, options, common_arguments);
    po::notify(options);

    if (options.count("version") || options.count("V"))
    {
        showClientVersion();
        exit(0); // NOLINT(concurrency-mt-unsafe)
    }

    if (options.count("version-clean"))
    {
        output_stream << VERSION_STRING;
        exit(0); // NOLINT(concurrency-mt-unsafe)
    }

    if (options.count("verbose"))
        getClientConfiguration().setBool("verbose", true);

    /// Output of help message.
    if (options.count("help")
        || (options.count("host") && options["host"].as<std::string>() == "elp")) /// If user writes -help instead of --help.
    {
        if (getClientConfiguration().getBool("verbose", false))
            printHelpMessage(options_description, true);
        else
            printHelpMessage(options_description_non_verbose, false);
        exit(0); // NOLINT(concurrency-mt-unsafe)
    }

    /// Common options for clickhouse-client and clickhouse-local.

    /// Output execution time to stderr in batch mode.
    if (options.count("time"))
        getClientConfiguration().setBool("print-time-to-stderr", true);
    if (options.count("memory-usage"))
    {
        const auto & memory_usage_mode = options["memory-usage"].as<std::string>();
        if (memory_usage_mode != "none" && memory_usage_mode != "default" && memory_usage_mode != "readable")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown memory-usage mode: {}", memory_usage_mode);
        getClientConfiguration().setString("print-memory-to-stderr", memory_usage_mode);
    }

    if (options.count("query"))
        queries = options["query"].as<std::vector<std::string>>();
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
    if (options.count("ignore-error"))
        getClientConfiguration().setBool("ignore-error", true);
    if (options.count("format"))
        getClientConfiguration().setString("format", options["format"].as<std::string>());
    if (options.count("output-format"))
        getClientConfiguration().setString("output-format", options["output-format"].as<std::string>());
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
    if (options.count("wait_for_suggestions_to_load"))
        getClientConfiguration().setBool("wait_for_suggestions_to_load", true);
    if (options.count("suggestion_limit"))
        getClientConfiguration().setInt("suggestion_limit", options["suggestion_limit"].as<int>());
    if (options.count("highlight"))
        getClientConfiguration().setBool("highlight", options["highlight"].as<bool>());
    if (options.count("history_file"))
        getClientConfiguration().setString("history_file", options["history_file"].as<std::string>());
    if (options.count("interactive"))
        getClientConfiguration().setBool("interactive", true);
    if (options.count("pager"))
        getClientConfiguration().setString("pager", options["pager"].as<std::string>());

    if (options.count("log-level"))
        Poco::Logger::root().setLevel(options["log-level"].as<std::string>());
    if (options.count("server_logs_file"))
        server_logs_file = options["server_logs_file"].as<std::string>();

    if (options.count("proto_caps"))
    {
        std::string proto_caps_str = options["proto_caps"].as<std::string>();

        std::vector<std::string_view> proto_caps;
        splitInto<','>(proto_caps, proto_caps_str);

        for (auto cap_str : proto_caps)
        {
            std::string direction;

            if (cap_str.starts_with("send_"))
            {
                direction = "send";
                cap_str = cap_str.substr(std::string_view("send_").size());
            }
            else if (cap_str.starts_with("recv_"))
            {
                direction = "recv";
                cap_str = cap_str.substr(std::string_view("recv_").size());
            }

            if (cap_str != "chunked" && cap_str != "notchunked" && cap_str != "chunked_optional" && cap_str != "notchunked_optional")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "proto_caps option is incorrect ({})", proto_caps_str);

            if (direction.empty())
            {
                config().setString("proto_caps.send", std::string(cap_str));
                config().setString("proto_caps.recv", std::string(cap_str));
            }
            else
                config().setString("proto_caps." + direction, std::string(cap_str));
        }
    }

    query_processing_stage = QueryProcessingStage::fromString(options["stage"].as<std::string>());
    query_kind = parseQueryKind(options["query_kind"].as<std::string>());
    profile_events.print = options.count("print-profile-events");
    profile_events.delay_ms = options["profile-events-delay-ms"].as<UInt64>();

    processOptions(options_description, options, external_tables_arguments, hosts_and_ports_arguments);
    {
        std::unordered_set<std::string> alias_names;
        alias_names.reserve(options_description.main_description->options().size());
        for (const auto& option : options_description.main_description->options())
            alias_names.insert(option->long_name());
        argsToConfig(common_arguments, getClientConfiguration(), 100, &alias_names);
    }

    clearPasswordFromCommandLine(argc, argv);

    /// Limit on total memory usage
    std::string max_client_memory_usage = getClientConfiguration().getString("max_memory_usage_in_client", "0" /*default value*/);
    if (max_client_memory_usage != "0")
    {
        UInt64 max_client_memory_usage_int = parseWithSizeSuffix<UInt64>(max_client_memory_usage.c_str(), max_client_memory_usage.length());

        total_memory_tracker.setHardLimit(max_client_memory_usage_int);
        total_memory_tracker.setDescription("(total)");
        total_memory_tracker.setMetric(CurrentMetrics::MemoryTracking);
    }

    /// Print stacktrace in case of crash
    HandledSignals::instance().setupTerminateHandler();
    HandledSignals::instance().setupCommonDeadlySignalHandlers();
    /// We don't setup signal handlers for SIGINT, SIGQUIT, SIGTERM because we don't
    /// have an option for client to shutdown gracefully.

    fatal_channel_ptr = new Poco::SplitterChannel;
    fatal_console_channel_ptr = new Poco::ConsoleChannel;
    fatal_channel_ptr->addChannel(fatal_console_channel_ptr);
    if (options.count("client_logs_file"))
    {
        fatal_file_channel_ptr = new Poco::SimpleFileChannel(options["client_logs_file"].as<std::string>());
        fatal_channel_ptr->addChannel(fatal_file_channel_ptr);
    }

    fatal_log = createLogger("ClientBase", fatal_channel_ptr.get(), Poco::Message::PRIO_FATAL);
    signal_listener = std::make_unique<SignalListener>(nullptr, fatal_log);
    signal_listener_thread.start(*signal_listener);

#if USE_GWP_ASAN
    GWPAsan::initFinished();
#endif

}


}
