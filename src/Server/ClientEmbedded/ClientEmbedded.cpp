#if defined(OS_LINUX)

#include <Server/ClientEmbedded/ClientEmbedded.h>

#include <base/getFQDNOrHostName.h>
#include <Interpreters/Session.h>
#include <Interpreters/Context.h>
#include <Common/setThreadName.h>
#include <Common/Config/ConfigHelper.h>
#include <Common/Exception.h>
#include <Core/Settings.h>

#include <iomanip>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace Setting
{
    extern const SettingsUInt64 max_insert_block_size;
}


ClientEmbedded::ClientEmbedded(
    std::unique_ptr<Session> && session_,
    int in_fd_,
    int out_fd_,
    int err_fd_,
    std::istream & input_stream_,
    std::ostream & output_stream_,
    std::ostream & error_stream_)
    : ClientBase(in_fd_, out_fd_, err_fd_, input_stream_, output_stream_, error_stream_), session(std::move(session_))
{
    global_context = session->makeSessionContext();
    configuration = ConfigHelper::createEmpty();
    layered_configuration = new Poco::Util::LayeredConfiguration();
    layered_configuration->addWriteable(configuration, 0);
}

void ClientEmbedded::printHelpMessage(const OptionsDescription & options_description)
{
    output_stream << "Welcome to the ClickHouse embedded client!" << "\n";
    output_stream << "This client runs on the server side inside the ClickHouse's main process." << "\n";

    if (options_description.main_description.has_value())
        output_stream << options_description.main_description.value() << "\n";
    if (options_description.external_description.has_value())
        output_stream << options_description.external_description.value() << "\n";
    if (options_description.hosts_and_ports_description.has_value())
        output_stream << options_description.hosts_and_ports_description.value() << "\n";

    output_stream << "All settings are documented at https://clickhouse.com/docs/en/operations/settings/settings.\n\n";
    output_stream << "See also: https://clickhouse.com/docs/en/integrations/sql-clients/cli\n";
}


void ClientEmbedded::processError(std::string_view) const
{
    if (ignore_error)
        return;

    if (is_interactive)
    {
        String message;
        if (server_exception)
        {
            message = getExceptionMessage(*server_exception, print_stack_trace, true);
        }
        else if (client_exception)
        {
            message = client_exception->message();
        }

        error_stream << fmt::format("Received exception\n{}\n\n", message);
    }
    else
    {
        if (server_exception)
            server_exception->rethrow();
        if (client_exception)
            client_exception->rethrow();
    }
}


void ClientEmbedded::cleanup()
{
    try
    {
        connection.reset();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void ClientEmbedded::connect()
{
    if (!session)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Error creating connection without session object");
    }
    connection_parameters = ConnectionParameters::createForEmbedded(session->sessionContext()->getUserName(), default_database);
    connection = LocalConnection::createConnection(
        connection_parameters, std::move(session), std_in.get(), need_render_progress, need_render_profile_events, server_display_name);
    if (!default_database.empty())
    {
        connection->setDefaultDatabase(default_database);
    }
}

Poco::Util::LayeredConfiguration & ClientEmbedded::getClientConfiguration()
{
    chassert(layered_configuration);
    return *layered_configuration;
}


bool ClientEmbedded::isEmbeeddedClient() const
{
    return true;
}


int ClientEmbedded::run(const NameToNameMap & envVars, const String & first_query)
try
{
    setThreadName("LocalServerPty");

    output_stream << std::fixed << std::setprecision(3);
    error_stream << std::fixed << std::setprecision(3);

    /**
    * To pass the environment variables through the SSH protocol you need to follow
    * the format: ssh -o SetEnv="key1=value1 key2=value2"
    * But the whole code is used to work with command line options, so we reconstruct them back.
    */
    Arguments arguments;
    arguments.reserve(envVars.size() * 2);
    for (const auto & [key, value] : envVars)
    {
        arguments.emplace_back("--" + key);
        arguments.emplace_back(value);
    }

    OptionsDescription options_description;
    addCommonOptions(options_description);
    addSettingsToProgramOptionsAndSubscribeToChanges(options_description);

    po::variables_map options;
    auto parser = po::command_line_parser(arguments)
                      .options(options_description.main_description.value())
                      .allow_unregistered();
    auto parsed = parser.run();
    po::store(parsed, options);
    po::notify(options);

    if (options.count("version") || options.count("V"))
    {
        showClientVersion();
        cleanup();
        return 0;
    }

    if (options.count("help"))
    {
        printHelpMessage(options_description);
        cleanup();
        return 0;
    }

    addOptionsToTheClientConfiguration(options);

    /// Apply settings specified as command line arguments (read environment variables).
    global_context = session->sessionContext();
    global_context->setApplicationType(Context::ApplicationType::SERVER);
    global_context->setSettings(*cmd_settings);

    is_interactive = stdin_is_a_tty;
    /// If a query is passed via SSH - just append it to the list of queries to execute:
    /// ssh -i ~/.ssh/id_rsa default@localhost -p 9022 "SELECT 1"
    if (!first_query.empty())
        queries.push_back(first_query);

    delayed_interactive = is_interactive && !queries.empty();
    if (!is_interactive || delayed_interactive)
    {
        echo_queries = getClientConfiguration().getBool("echo", false);
        ignore_error = getClientConfiguration().getBool("ignore-error", false);
    }

    load_suggestions = true;
    wait_for_suggestions_to_load = true;
    server_display_name = getFQDNOrHostName();
    prompt = format("{} :) ", global_context->getConfigRef().getString("display_name", server_display_name));
    query_processing_stage = QueryProcessingStage::Enum::Complete;
    pager = getClientConfiguration().getString("pager", "");
    enable_highlight = getClientConfiguration().getBool("highlight", true);
    multiline = getClientConfiguration().has("multiline");
    print_stack_trace = getClientConfiguration().getBool("stacktrace", false);
    default_database = getClientConfiguration().getString("database", "");

    setDefaultFormatsAndCompressionFromConfiguration();

    initTTYBuffer(toProgressOption(getClientConfiguration().getString("progress", "default")),
        toProgressOption(getClientConfiguration().getString("progress-table", "default")));
    initKeystrokeInterceptor();

    initClientContext(session->sessionContext());
    /// Note, QueryScope will be initialized in the LocalConnection

    if (is_interactive)
    {
        clearTerminal();
        showClientVersion();
        error_stream << std::endl;
    }

    connect();


    if (is_interactive && !delayed_interactive)
    {
        runInteractive();
    }
    else
    {
        runNonInteractive();

        if (delayed_interactive)
            runInteractive();
    }

    cleanup();
    return 0;
}
catch (const DB::Exception & e)
{
    cleanup();

    error_stream << getExceptionMessage(e, print_stack_trace, true) << std::endl;
    auto code = DB::getCurrentExceptionCode();
    return static_cast<UInt8>(code) ? code : 1;
}
catch (...)
{
    cleanup();

    error_stream << getCurrentExceptionMessage(false) << std::endl;
    auto code = DB::getCurrentExceptionCode();
    return static_cast<UInt8>(code) ? code : 1;
}

}

#endif
