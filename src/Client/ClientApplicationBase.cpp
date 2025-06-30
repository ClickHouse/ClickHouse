#include <Client/ClientApplicationBase.h>

#include <base/argsToConfig.h>
#include <base/safeExit.h>
#include <Common/clearPasswordFromCommandLine.h>
#include <Common/TerminalSize.h>
#include <Common/Exception.h>
#include <Common/SignalHandlers.h>
#include <Client/JwtProvider.h>

#include <Common/config_version.h>
#include "config.h"

#include <unordered_set>
#include <string>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>

// For HTTP client, JSON, and device flow
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/Dynamic/Var.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <cstdlib>

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
    extern const int SUPPORT_IS_DISABLED;
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

bool ClientApplicationBase::isEmbeeddedClient() const
{
    return false;
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

void ClientApplicationBase::addOptionsToHints(const OptionsDescription & options_description)
{
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

    if (options_description.hosts_and_ports_description)
    {
        const auto & hosts_and_ports_description = options_description.hosts_and_ports_description->options();
        std::transform(hosts_and_ports_description.begin(), hosts_and_ports_description.end(), std::back_inserter(cmd_options), getter);
    }
}

void ClientApplicationBase::init(int argc, char ** argv)
{
    namespace po = boost::program_options;

    /// Don't parse options with Poco library, we prefer neat boost::program_options.
    stopOptionsProcessing();

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
    addCommonOptions(options_description);
    addExtraOptions(options_description);
    /// Copy them to be able to print a simplified version of the help message.
    OptionsDescription options_description_non_verbose = options_description;
    addSettingsToProgramOptionsAndSubscribeToChanges(options_description);
    addOptionsToHints(options_description);

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

    /// If user writes -help instead of --help.
    bool user_made_a_typo = options.count("host") && options["host"].as<std::string>() == "elp";
    if (options.count("help") || user_made_a_typo)
    {
        if (options.count("verbose"))
            printHelpMessage(options_description);
        else
            printHelpMessage(options_description_non_verbose);
        exit(0); // NOLINT(concurrency-mt-unsafe)
    }

    addOptionsToTheClientConfiguration(options);

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
        total_memory_tracker.setDescription("Global");
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
        if (isEmbeeddedClient())
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Writing logs to a file is disabled in an embedded mode.");

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
