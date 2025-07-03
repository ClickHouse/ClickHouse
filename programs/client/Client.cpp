#include <Client.h>
#include <Client/ConnectionString.h>
#include <Core/Protocol.h>
#include <Core/Settings.h>
#include <boost/algorithm/string/replace.hpp>
#include <boost/program_options.hpp>
#include <Common/ThreadStatus.h>

#include <Access/AccessControl.h>

#include <Columns/ColumnString.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/getClientConfigPath.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/TerminalSize.h>
#include <Common/config_version.h>
#include <Common/formatReadable.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>

#include <Client/JWTProvider.h>

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>

#include <Storages/MergeTree/MergeTreeSettings.h>

#include <Poco/Util/Application.h>

#include <filesystem>

#include "config.h"

#if USE_BUZZHOUSE
#   include <Client/BuzzHouse/Generator/ExternalIntegrations.h>
#   include <Client/BuzzHouse/Generator/FuzzConfig.h>
#endif

namespace fs = std::filesystem;
using namespace std::literals;


namespace DB
{
namespace Setting
{
extern const SettingsDialect dialect;
extern const SettingsBool use_client_time_zone;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int NETWORK_ERROR;
    extern const int AUTHENTICATION_FAILED;
    extern const int REQUIRED_PASSWORD;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int USER_EXPIRED;
}

Client::Client()
{
    fuzzer = QueryFuzzer(randomSeed(), &std::cout, &std::cerr);
}


Client::~Client() = default;

void Client::processError(std::string_view query) const
{
    if (server_exception)
    {
        fmt::print(
            stderr,
            "Received exception from server (version {}):\n{}\n",
            server_version,
            getExceptionMessageForLogging(*server_exception, print_stack_trace, true));

        if (server_exception->code() == ErrorCodes::USER_EXPIRED)
        {
            server_exception->rethrow();
        }

        if (is_interactive)
        {
            fmt::print(stderr, "\n");
        }
        else
        {
            fmt::print(stderr, "(query: {})\n", query);
        }
    }

    if (client_exception)
    {
        fmt::print(stderr, "Error on processing query: {}\n", client_exception->message());

        if (is_interactive)
        {
            fmt::print(stderr, "\n");
        }
        else
        {
            fmt::print(stderr, "(query: {})\n", query);
        }
    }

    // A debug check -- at least some exception must be set, if the error
    // flag is set, and vice versa.
    assert(have_error == (client_exception || server_exception));
}


void Client::showWarnings()
{
    try
    {
        std::vector<String> messages = loadWarningMessages();
        if (!messages.empty())
        {
            output_stream << "Warnings:" << std::endl;
            for (const auto & message : messages)
                output_stream << " * " << message << std::endl;
            output_stream << std::endl;
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ignore exception
    }
}

void Client::parseConnectionsCredentials(Poco::Util::AbstractConfiguration & config, const std::string & connection_name)
{
    std::optional<String> default_connection_name;
    if (hosts_and_ports.empty())
    {
        if (config.has("host"))
            default_connection_name = config.getString("host");
    }
    else
    {
        default_connection_name = hosts_and_ports.front().host;
    }

    String connection;
    if (!connection_name.empty())
        connection = connection_name;
    else
        connection = default_connection_name.value_or("localhost");

    Strings keys;
    config.keys("connections_credentials", keys);
    bool connection_found = false;
    for (const auto & key : keys)
    {
        const String & prefix = "connections_credentials." + key;

        const String & name = config.getString(prefix + ".name", "");
        if (name != connection)
            continue;
        connection_found = true;

        String connection_hostname;
        if (config.has(prefix + ".hostname"))
            connection_hostname = config.getString(prefix + ".hostname");
        else
            connection_hostname = name;

        config.setString("host", connection_hostname);
        if (config.has(prefix + ".port"))
            config.setInt("port", config.getInt(prefix + ".port"));
        if (config.has(prefix + ".secure"))
        {
            bool secure = config.getBool(prefix + ".secure");
            if (secure)
                config.setBool("secure", true);
            else
                config.setBool("no-secure", true);
        }
        if (config.has(prefix + ".user"))
            config.setString("user", config.getString(prefix + ".user"));
        if (config.has(prefix + ".password"))
            config.setString("password", config.getString(prefix + ".password"));
        if (config.has(prefix + ".database"))
            config.setString("database", config.getString(prefix + ".database"));
        if (config.has(prefix + ".history_file"))
        {
            String history_file = config.getString(prefix + ".history_file");
            if (history_file.starts_with("~") && !home_path.empty())
                history_file = home_path + "/" + history_file.substr(1);
            config.setString("history_file", history_file);
        }
        if (config.has(prefix + ".history_max_entries"))
        {
            config.setUInt("history_max_entries", history_max_entries);
        }
        if (config.has(prefix + ".accept-invalid-certificate"))
            config.setBool("accept-invalid-certificate", config.getBool(prefix + ".accept-invalid-certificate"));
        if (config.has(prefix + ".prompt"))
            config.setString("prompt", config.getString(prefix + ".prompt"));
    }

    if (!connection_name.empty() && !connection_found)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "No such connection '{}' in connections_credentials", connection);
}

/// Make query to get all server warnings
std::vector<String> Client::loadWarningMessages()
{
    /// Older server versions cannot execute the query loading warnings.
    constexpr UInt64 min_server_revision_to_load_warnings = DBMS_MIN_PROTOCOL_VERSION_WITH_VIEW_IF_PERMITTED;

    if (server_revision < min_server_revision_to_load_warnings)
        return {};

    std::vector<String> messages;
    connection->sendQuery(connection_parameters.timeouts,
                          "SELECT * FROM viewIfPermitted(SELECT message FROM system.warnings ELSE null('message String'))",
                          {} /* query_parameters */,
                          "" /* query_id */,
                          QueryProcessingStage::Complete,
                          &client_context->getSettingsRef(),
                          &client_context->getClientInfo(), false, {}, {});
    while (true)
    {
        Packet packet = connection->receivePacket();
        switch (packet.type)
        {
            case Protocol::Server::Data:
                if (packet.block)
                {
                    const ColumnString & column = typeid_cast<const ColumnString &>(*packet.block.getByPosition(0).column);

                    size_t rows = packet.block.rows();
                    for (size_t i = 0; i < rows; ++i)
                        messages.emplace_back(column[i].safeGet<String>());
                }
                continue;

            case Protocol::Server::Progress:
            case Protocol::Server::ProfileInfo:
            case Protocol::Server::Totals:
            case Protocol::Server::Extremes:
            case Protocol::Server::Log:
                continue;

            case Protocol::Server::Exception:
                packet.exception->rethrow();
                return messages;

            case Protocol::Server::EndOfStream:
                return messages;

            case Protocol::Server::ProfileEvents:
                continue;

            default:
                throw Exception(
                    ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from server {}", packet.type, connection->getDescription());
        }
    }
}


Poco::Util::LayeredConfiguration & Client::getClientConfiguration()
{
    return config();
}

void Client::initialize(Poco::Util::Application & self)
{
    Poco::Util::Application::initialize(self);

    const char * home_path_cstr = getenv("HOME"); // NOLINT(concurrency-mt-unsafe)
    if (home_path_cstr)
        home_path = home_path_cstr;

    std::optional<std::string> config_path;
    if (config().has("config-file"))
        config_path.emplace(config().getString("config-file"));
    else
        config_path = getClientConfigPath(home_path);
    if (config_path.has_value())
    {
        ConfigProcessor config_processor(*config_path);
        auto loaded_config = config_processor.loadConfig();
        parseConnectionsCredentials(*loaded_config.configuration, config().getString("connection", ""));
        config().add(loaded_config.configuration);
    }
    else if (config().has("connection"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "--connection was specified, but config does not exist");

    if (config().has("accept-invalid-certificate"))
    {
        config().setString("openSSL.client.invalidCertificateHandler.name", "AcceptCertificateHandler");
        config().setString("openSSL.client.verificationMode", "none");
    }

    /** getenv is thread-safe in Linux glibc and in all sane libc implementations.
      * But the standard does not guarantee that subsequent calls will not rewrite the value by returned pointer.
      *
      * man getenv:
      *
      * As typically implemented, getenv() returns a pointer to a string within the environment list.
      * The caller must take care not to modify this string, since that would change the environment of
      * the process.
      *
      * The implementation of getenv() is not required to be reentrant. The string pointed to by the return value of getenv()
      * may be statically allocated, and can be modified by a subsequent call to getenv(), putenv(3), setenv(3), or unsetenv(3).
      */

    const char * env_user = getenv("CLICKHOUSE_USER"); // NOLINT(concurrency-mt-unsafe)
    if (env_user && !config().has("user"))
        config().setString("user", env_user);

    const char * env_password = getenv("CLICKHOUSE_PASSWORD"); // NOLINT(concurrency-mt-unsafe)
    if (env_password && !config().has("password"))
        config().setString("password", env_password);

    /// settings and limits could be specified in config file, but passed settings has higher priority
    for (const auto & setting : client_context->getSettingsRef().getUnchangedNames())
    {
        String name{setting};
        if (config().has(name))
            client_context->setSetting(name, config().getString(name));
    }

    /// Set path for format schema files
    if (config().has("format_schema_path"))
        client_context->setFormatSchemaPath(fs::weakly_canonical(config().getString("format_schema_path")));

    /// Set the path for google proto files
    if (config().has("google_protos_path"))
        client_context->setGoogleProtosPath(fs::weakly_canonical(config().getString("google_protos_path")));
}


int Client::main(const std::vector<std::string> & /*args*/)
try
{
    setupSignalHandler();

    output_stream << std::fixed << std::setprecision(3);
    error_stream << std::fixed << std::setprecision(3);

    registerFormats();
    registerFunctions();
    registerAggregateFunctions();

    processConfig();
    adjustSettings(client_context);

    initTTYBuffer(
        toProgressOption(config().getString("progress", "default")), toProgressOption(config().getString("progress-table", "default")));
    initKeystrokeInterceptor();

    /// Includes delayed_interactive.
    if (is_interactive)
    {
        clearTerminal();
        showClientVersion();
    }

#if USE_JWT_CPP && USE_SSL
    if (config().getBool("login", false))
    {
        login();
    }
#endif

    try
    {
        connect();
    }
    catch (const Exception & e)
    {
        if ((e.code() != ErrorCodes::AUTHENTICATION_FAILED && e.code() != ErrorCodes::REQUIRED_PASSWORD) ||
            config().has("password") ||
            config().getBool("ask-password", false) ||
            !is_interactive)
            throw;

        config().setBool("ask-password", true);
        connect();
    }

    /// Show warnings at the beginning of connection.
    if (is_interactive && !config().has("no-warnings"))
        showWarnings();

    /// Set user password complexity rules
    auto & access_control = client_context->getAccessControl();
    access_control.setPasswordComplexityRules(connection->getPasswordComplexityRules());

    if (is_interactive && !delayed_interactive && !buzz_house)
    {
        runInteractive();
    }
    else
    {
        connection->setDefaultDatabase(connection_parameters.default_database);

        runNonInteractive();

        // If exception code isn't zero, we should return non-zero return
        // code anyway.
        const auto * exception = server_exception ? server_exception.get() : client_exception.get();

        if (exception)
        {
            return exception->code() != 0 ? exception->code() : -1;
        }

        if (have_error)
        {
            // Shouldn't be set without an exception, but check it just in
            // case so that at least we don't lose an error.
            return -1;
        }

        if (delayed_interactive)
            runInteractive();
    }

    return 0;
}
catch (Exception & e)
{
    bool need_print_stack_trace = config().getBool("stacktrace", false) && e.code() != ErrorCodes::NETWORK_ERROR;
    std::cerr << getExceptionMessageForLogging(e, need_print_stack_trace, true) << std::endl << std::endl;
    /// If exception code isn't zero, we should return non-zero return code anyway.
    return static_cast<UInt8>(e.code()) ? e.code() : -1;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(false) << std::endl;
    return getCurrentExceptionCode();
}

#if USE_JWT_CPP && USE_SSL
void Client::login()
{
    std::string host = hosts_and_ports.front().host;
    std::string auth_url = getClientConfiguration().getString("auth-url", "");
    std::string client_id = getClientConfiguration().getString("auth-client-id", "");

    if ((auth_url.empty() || client_id.empty()) && !isCloudEndpoint(host))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Could not retrieve authentication endpoints for host '{}'. Please specify --auth-url and --auth-client-id if you are "
            "not using ClickHouse Cloud.",
            host);
    }

    jwt_provider = createJwtProvider(auth_url, client_id, host, output_stream, error_stream);
    if (jwt_provider)
    {
        std::string jwt = jwt_provider->getJWT();
        if (!jwt.empty())
        {
            getClientConfiguration().setString("jwt", jwt);
        }
        else
        {
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Login failed. Please check your credentials and try again.");
        }
    }
}
#endif

void Client::connect()
{
    String server_name;
    UInt64 server_version_major = 0;
    UInt64 server_version_minor = 0;
    UInt64 server_version_patch = 0;

    if (hosts_and_ports.empty())
    {
        String host = config().getString("host", "localhost");
        UInt16 port = ConnectionParameters::getPortFromConfig(config(), host);
        hosts_and_ports.emplace_back(HostAndPort{host, port});
    }

    for (size_t attempted_address_index = 0; attempted_address_index < hosts_and_ports.size(); ++attempted_address_index)
    {
        try
        {
            const auto host = ConnectionParameters::Host{hosts_and_ports[attempted_address_index].host};
            const auto database = ConnectionParameters::Database{default_database};

            connection_parameters = ConnectionParameters(
                config(), host, database, hosts_and_ports[attempted_address_index].port);

#if USE_JWT_CPP && USE_SSL
            connection_parameters.jwt_provider = jwt_provider;
#endif

            if (is_interactive)
                output_stream << "Connecting to "
                          << (!connection_parameters.default_database.empty()
                                  ? "database " + connection_parameters.default_database + " at "
                                  : "")
                          << connection_parameters.host << ":" << connection_parameters.port
                          << (!connection_parameters.user.empty() ? " as user " + connection_parameters.user : "") << "." << std::endl;

            connection = Connection::createConnection(connection_parameters, client_context);

            if (max_client_network_bandwidth)
            {
                ThrottlerPtr throttler = std::make_shared<Throttler>(max_client_network_bandwidth, 0, "");
                connection->setThrottler(throttler);
            }

            connection->getServerVersion(
                connection_parameters.timeouts,
                server_name,
                server_version_major,
                server_version_minor,
                server_version_patch,
                server_revision);
            config().setString("host", connection_parameters.host);
            config().setInt("port", connection_parameters.port);

            settings_from_server = assert_cast<Connection &>(*connection).settingsFromServer();

            break;
        }
        catch (Exception & e)
        {
            /// This problem can't be fixed with reconnection so it is not attempted
            if (e.code() == ErrorCodes::AUTHENTICATION_FAILED || e.code() == ErrorCodes::REQUIRED_PASSWORD)
                throw;

            if (attempted_address_index == hosts_and_ports.size() - 1)
                throw;

            if (is_interactive)
            {
                std::cerr << "Connection attempt to database at " << connection_parameters.host << ":" << connection_parameters.port
                          << " resulted in failure" << std::endl
                          << getExceptionMessageForLogging(e, false) << std::endl
                          << "Attempting connection to the next provided address" << std::endl;
            }
        }
    }

    server_version = toString(server_version_major) + "." + toString(server_version_minor) + "." + toString(server_version_patch);
    load_suggestions
        = is_interactive && (server_revision >= Suggest::MIN_SERVER_REVISION) && !config().getBool("disable_suggestion", false);
    wait_for_suggestions_to_load = config().getBool("wait_for_suggestions_to_load", false);
    if (load_suggestions)
    {
        suggestion_limit = config().getInt("suggestion_limit");
    }

    server_display_name = connection->getServerDisplayName(connection_parameters.timeouts);
    if (server_display_name.empty())
        server_display_name = config().getString("host", "localhost");

    if (is_interactive)
    {
        output_stream << "Connected to " << server_name << " server version " << server_version << "." << std::endl << std::endl;

#if not CLICKHOUSE_CLOUD
        auto client_version_tuple = std::make_tuple(VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH);
        auto server_version_tuple = std::make_tuple(server_version_major, server_version_minor, server_version_patch);

        if (client_version_tuple < server_version_tuple)
        {
            output_stream << "ClickHouse client version is older than ClickHouse server. "
                      << "It may lack support for new features." << std::endl
                      << std::endl;
        }
        else if (client_version_tuple > server_version_tuple && server_display_name != "clickhouse-cloud")
        {
            output_stream << "ClickHouse server version is older than ClickHouse client. "
                      << "It may indicate that the server is out of date and can be upgraded." << std::endl
                      << std::endl;
        }
#endif
    }

    if (!client_context->getSettingsRef()[Setting::use_client_time_zone])
    {
        const auto & time_zone = connection->getServerTimezone(connection_parameters.timeouts);
        if (!time_zone.empty())
        {
            try
            {
                DateLUT::setDefaultTimezone(time_zone);
            }
            catch (...)
            {
                std::cerr << "Warning: could not switch to server time zone: " << time_zone
                          << ", reason: " << getCurrentExceptionMessage(/* with_stacktrace = */ false) << std::endl
                          << "Proceeding with local time zone." << std::endl
                          << std::endl;
            }
        }
        else
        {
            std::cerr << "Warning: could not determine server time zone. "
                      << "Proceeding with local time zone." << std::endl
                      << std::endl;
        }
    }

    /// A custom prompt can be specified
    /// - directly (possible as CLI parameter or in client.xml as top-level <prompt>...</prompt> or within client.xml's connection credentials)
    /// - via prompt_by_server_display_name (only possible in client.xml as top-level <prompt>...</prompt>).
    if (config().has("prompt"))
        prompt = config().getString("prompt");
    else if (config().has("prompt_by_server_display_name"))
    {
        if (config().has("prompt_by_server_display_name.default"))
            prompt = config().getRawString("prompt_by_server_display_name.default");

        Strings keys;
        config().keys("prompt_by_server_display_name", keys);
        for (const auto & key : keys)
        {
            if (key != "default" && server_display_name.contains(key))
            {
                prompt = config().getRawString("prompt_by_server_display_name." + key);
                break;
            }
        }
    }
    else
    {
        prompt = "{display_name}";
    }

    /// Prompt may contain escape sequences including \e[ or \x1b[ sequences to set terminal color.
    {
        String prompt_escaped;
        ReadBufferFromString in(prompt);
        readEscapedString(prompt_escaped, in);
        prompt = prompt_escaped;
    }

    /// Substitute placeholders in the form of {name}:
    const std::map<String, String> prompt_substitutions{
        {"host", connection_parameters.host},
        {"port", toString(connection_parameters.port)},
        {"user", connection_parameters.user},
        {"display_name", server_display_name},
    };

    for (const auto & [key, value] : prompt_substitutions)
        boost::replace_all(prompt, "{" + key + "}", value);

    prompt = appendSmileyIfNeeded(prompt);
}

// Prints changed settings to stderr. Useful for debugging fuzzing failures.
void Client::printChangedSettings() const
{
    auto print_changes = [](const auto & changes, std::string_view settings_name)
    {
        if (!changes.empty())
        {
            fmt::print(stderr, "Changed {}: ", settings_name);
            for (size_t i = 0; i < changes.size(); ++i)
            {
                if (i)
                    fmt::print(stderr, ", ");
                fmt::print(stderr, "{} = '{}'", changes[i].name, toString(changes[i].value));
            }

            fmt::print(stderr, "\n");
        }
        else
        {
            fmt::print(stderr, "No changed {}.\n", settings_name);
        }
    };

    print_changes(client_context->getSettingsRef().changes(), "settings");
    print_changes(cmd_merge_tree_settings->changes(), "MergeTree settings");
}


void Client::printHelpMessage(const OptionsDescription & options_description)
{
    if (options_description.main_description.has_value())
        output_stream << options_description.main_description.value() << "\n";
    if (options_description.external_description.has_value())
        output_stream << options_description.external_description.value() << "\n";
    if (options_description.hosts_and_ports_description.has_value())
        output_stream << options_description.hosts_and_ports_description.value() << "\n";

    output_stream << "All settings are documented at https://clickhouse.com/docs/operations/settings/settings.\n";
    output_stream << "In addition, --param_name=value can be specified for substitution of parameters for parameterized queries.\n";
    output_stream << "\nSee also: https://clickhouse.com/docs/en/integrations/sql-clients/cli\n";
}


void Client::addExtraOptions(OptionsDescription & options_description)
{
    /// Main commandline options related to client functionality and all parameters from Settings.
    options_description.main_description->add_options()("config,c", po::value<std::string>(), "config-file path (another shorthand)")(
        "connection", po::value<std::string>(), "connection to use (from the client config), by default connection name is hostname")(
        "secure,s", "Use TLS connection")("no-secure", "Don't use TLS connection")(
        "user,u", po::value<std::string>()->default_value("default"), "user")("password", po::value<std::string>(), "password")(
        "ask-password",
        "ask-password")("ssh-key-file", po::value<std::string>(), "File containing the SSH private key for authenticate with the server.")(
        "ssh-key-passphrase", po::value<std::string>(), "Passphrase for the SSH private key specified by --ssh-key-file.")(
        "quota_key", po::value<std::string>(), "A string to differentiate quotas when the user have keyed quotas configured on server")(
        "jwt", po::value<std::string>(), "Use JWT for authentication")

        ("max_client_network_bandwidth",
         po::value<int>(),
         "the maximum speed of data exchange over the network for the client in bytes per second.")(
            "compression",
            po::value<bool>(),
            "enable or disable compression (enabled by default for remote communication and disabled for localhost communication).")

            ("query-fuzzer-runs",
             po::value<int>()->default_value(0),
             "After executing every SELECT query, do random mutations in it and run again specified number of times. This is used for "
             "testing to discover unexpected corner cases.")("create-query-fuzzer-runs", po::value<int>()->default_value(0), "")(
                "buzz-house-config", po::value<std::string>(), "Path to configuration file for BuzzHouse")(
                "interleave-queries-file",
                po::value<std::vector<std::string>>()->multitoken(),
                "file path with queries to execute before every file from 'queries-file'; multiple files can be specified (--queries-file "
                "file1 file2...); this is needed to enable more aggressive fuzzing of newly added tests (see 'query-fuzzer-runs' option)")

                ("opentelemetry-traceparent",
                 po::value<std::string>(),
                 "OpenTelemetry traceparent header as described by W3C Trace Context recommendation")(
                    "opentelemetry-tracestate",
                    po::value<std::string>(),
                    "OpenTelemetry tracestate header as described by W3C Trace Context recommendation")

                    ("no-warnings", "disable warnings when client connects to server")
        /// TODO: Left for compatibility as it's used in upgrade check, remove after next release and use server setting ignore_drop_queries_probability
        ("fake-drop", "Ignore all DROP queries, should be used only for testing")(
            "accept-invalid-certificate",
            "Ignore certificate verification errors, equal to config parameters "
            "openSSL.client.invalidCertificateHandler.name=AcceptCertificateHandler and openSSL.client.verificationMode=none");

    /// Commandline options related to external tables.

    options_description.external_description.emplace(createOptionsDescription("External tables options", terminal_width));
    options_description.external_description->add_options()("file", po::value<std::string>(), "data file or - for stdin")(
        "name", po::value<std::string>()->default_value("_data"), "name of the table")(
        "format", po::value<std::string>()->default_value("TabSeparated"), "data format")(
        "structure", po::value<std::string>(), "structure")("types", po::value<std::string>(), "types");

    /// Commandline options related to hosts and ports.
    options_description.hosts_and_ports_description.emplace(createOptionsDescription("Hosts and ports options", terminal_width));
    options_description.hosts_and_ports_description->add_options()(
        "host,h",
        po::value<String>()->default_value("localhost"),
        "Server hostname. Multiple hosts can be passed via multiple arguments"
        "Example of usage: '--host host1 --host host2 --port port2 --host host3 ...'"
        "Each '--port port' will be attached to the last seen host that doesn't have a port yet,"
        "if there is no such host, the port will be attached to the next first host or to default host.")(
        "port", po::value<UInt16>(), "server ports");
}


void Client::processOptions(
    const OptionsDescription & options_description,
    const CommandLineOptions & options,
    const std::vector<Arguments> & external_tables_arguments,
    const std::vector<Arguments> & hosts_and_ports_arguments)
{
    namespace po = boost::program_options;

    size_t number_of_external_tables_with_stdin_source = 0;
    for (size_t i = 0; i < external_tables_arguments.size(); ++i)
    {
        /// Parse commandline options related to external tables.
        po::parsed_options parsed_tables
            = po::command_line_parser(external_tables_arguments[i]).options(options_description.external_description.value()).run();
        po::variables_map external_options;
        po::store(parsed_tables, external_options);

        try
        {
            external_tables.emplace_back(external_options);
            if (external_tables.back().file == "-")
                ++number_of_external_tables_with_stdin_source;
            if (number_of_external_tables_with_stdin_source > 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Two or more external tables has stdin (-) set as --file field");
        }
        catch (Exception & e)
        {
            std::cerr << getExceptionMessageForLogging(e, false) << std::endl;
            std::cerr << "Table №" << i << std::endl << std::endl;
            /// Avoid the case when error exit code can possibly overflow to normal (zero).
            auto exit_code = e.code() % 256;
            if (exit_code == 0)
                exit_code = 255;
            _exit(exit_code);
        }
    }

    for (const auto & hosts_and_ports_argument : hosts_and_ports_arguments)
    {
        /// Parse commandline options related to external tables.
        po::parsed_options parsed_hosts_and_ports
            = po::command_line_parser(hosts_and_ports_argument).options(options_description.hosts_and_ports_description.value()).run();
        po::variables_map host_and_port_options;
        po::store(parsed_hosts_and_ports, host_and_port_options);
        std::string host = host_and_port_options["host"].as<std::string>();
        std::optional<UInt16> port
            = !host_and_port_options["port"].empty() ? std::make_optional(host_and_port_options["port"].as<UInt16>()) : std::nullopt;
        hosts_and_ports.emplace_back(HostAndPort{host, port});
    }

    send_external_tables = true;

    shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::CLIENT);
    global_context->setSettings(*cmd_settings);

    /// Copy settings-related program options to config.
    /// TODO: Is this code necessary?
    global_context->getSettingsRef().addToClientOptions(config(), options, allow_repeated_settings);

    if (options.count("config-file") && options.count("config"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Two or more configuration files referenced in arguments");

    if (options.count("config"))
        config().setString("config-file", options["config"].as<std::string>());
    if (options.count("connection"))
        config().setString("connection", options["connection"].as<std::string>());
    if (options.count("interleave-queries-file"))
        interleave_queries_files = options["interleave-queries-file"].as<std::vector<std::string>>();
    if (options.count("secure"))
        config().setBool("secure", true);
    if (options.count("no-secure"))
        config().setBool("no-secure", true);
    if (options.count("user") && !options["user"].defaulted())
        config().setString("user", options["user"].as<std::string>());
    if (options.count("password"))
        config().setString("password", options["password"].as<std::string>());
    if (options.count("ask-password"))
        config().setBool("ask-password", true);
    if (options.count("ssh-key-file"))
        config().setString("ssh-key-file", options["ssh-key-file"].as<std::string>());
    if (options.count("ssh-key-passphrase"))
        config().setString("ssh-key-passphrase", options["ssh-key-passphrase"].as<std::string>());
    if (options.count("quota_key"))
        config().setString("quota_key", options["quota_key"].as<std::string>());
    if (options.count("max_client_network_bandwidth"))
        max_client_network_bandwidth = options["max_client_network_bandwidth"].as<int>();
    if (options.count("compression"))
        config().setBool("compression", options["compression"].as<bool>());
    if (options.count("no-warnings"))
        config().setBool("no-warnings", true);
    if (options.count("fake-drop"))
        config().setString("ignore_drop_queries_probability", "1");
    if (options.count("jwt"))
    {
        if (!options["user"].defaulted())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "User and JWT flags can't be specified together");
        config().setString("jwt", options["jwt"].as<std::string>());
        config().setString("user", "");
    }
    if (options["login"].as<bool>())
    {
        if (!options["user"].defaulted())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "User and login flags can't be specified together");
        if (config().has("jwt"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "JWT and login flags can't be specified together");
        config().setString("user", "");
    }
    if (options.count("accept-invalid-certificate"))
    {
        config().setString("openSSL.client.invalidCertificateHandler.name", "AcceptCertificateHandler");
        config().setString("openSSL.client.verificationMode", "none");
    }
    else
        config().setString("openSSL.client.invalidCertificateHandler.name", "RejectCertificateHandler");

    query_fuzzer_runs = options["query-fuzzer-runs"].as<int>();
    buzz_house_options_path = options.count("buzz-house-config") ? options["buzz-house-config"].as<std::string>() : "";
    buzz_house = !query_fuzzer_runs && !buzz_house_options_path.empty();
    if (query_fuzzer_runs || !buzz_house_options_path.empty())
    {
        // Ignore errors in parsing queries.
        config().setBool("ignore-error", true);
        ignore_error = true;
#if USE_BUZZHOUSE
        if (!buzz_house_options_path.empty())
        {
            fuzz_config = std::make_unique<BuzzHouse::FuzzConfig>(this, buzz_house_options_path);
            external_integrations = std::make_unique<BuzzHouse::ExternalIntegrations>(*fuzz_config);

            if (query_fuzzer_runs && fuzz_config->seed)
            {
                fuzzer.setSeed(fuzz_config->seed);
            }
        }
#endif
        if (query_fuzzer_runs)
        {
            fmt::print(stdout, "Using seed {} for AST fuzzer\n", fuzzer.getSeed());
        }
    }

    if ((create_query_fuzzer_runs = options["create-query-fuzzer-runs"].as<int>()))
    {
        // Ignore errors in parsing queries.
        config().setBool("ignore-error", true);

        global_context->setSetting("allow_suspicious_low_cardinality_types", true);
        ignore_error = true;
    }

    if (options.count("opentelemetry-traceparent"))
    {
        String traceparent = options["opentelemetry-traceparent"].as<std::string>();
        String error;
        if (!global_context->getClientTraceContext().parseTraceparentHeader(traceparent, error))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse OpenTelemetry traceparent '{}': {}", traceparent, error);
    }

    if (options.count("opentelemetry-tracestate"))
        global_context->getClientTraceContext().tracestate = options["opentelemetry-tracestate"].as<std::string>();

    initClientContext(Context::createCopy(global_context));
    /// Initialize query context for the current thread to avoid sharing global context (i.e. for obtaining session_timezone)
    query_scope.emplace(client_context);


    /// Allow to pass-through unknown settings to the server.
    client_context->getAccessControl().allowAllSettings();
}


void Client::processConfig()
{
    if (!queries.empty() && config().has("queries-file"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Options '--query' and '--queries-file' cannot be specified at the same time");

    /// Batch mode is enabled if one of the following is true:
    /// - -q (--query) command line option is present.
    ///   The value of the option is used as the text of query (or of multiple queries).
    ///   If stdin is not a terminal, INSERT data for the first query is read from it.
    /// - stdin is not a terminal. In this case queries are read from it.
    /// - --queries-file command line option is present.
    ///   The value of the option is used as file with query (or of multiple queries) to execute.

    delayed_interactive = config().has("interactive") && (!queries.empty() || config().has("queries-file"));
    if (stdin_is_a_tty && (delayed_interactive || (queries.empty() && queries_files.empty())))
    {
        is_interactive = true;
    }
    else
    {
        echo_queries = config().getBool("echo", false);
        ignore_error = config().getBool("ignore-error", false);

        query_id = config().getString("query_id", "");
        if (!query_id.empty())
            client_context->setCurrentQueryId(query_id);
    }

    if (is_interactive || delayed_interactive)
    {
        if (home_path.empty())
        {
            const char * home_path_cstr = getenv("HOME"); // NOLINT(concurrency-mt-unsafe)
            if (home_path_cstr)
                home_path = home_path_cstr;
        }

        /// Load command history if present.
        if (config().has("history_file"))
            history_file = config().getString("history_file");
        else
        {
            auto * history_file_from_env = getenv("CLICKHOUSE_HISTORY_FILE"); // NOLINT(concurrency-mt-unsafe)
            if (history_file_from_env)
                history_file = history_file_from_env;
            else if (!home_path.empty())
                history_file = home_path + "/.clickhouse-client-history";
        }
    }

    pager = config().getString("pager", "");
    enable_highlight = config().getBool("highlight", true);
    multiline = config().has("multiline");
    print_stack_trace = config().getBool("stacktrace", false);
    default_database = config().getString("database", "");

    setDefaultFormatsAndCompressionFromConfiguration();
}


void Client::readArguments(
    int argc,
    char ** argv,
    Arguments & common_arguments,
    std::vector<Arguments> & external_tables_arguments,
    std::vector<Arguments> & hosts_and_ports_arguments)
{
    bool has_connection_string
        = argc >= 2 && tryParseConnectionString(std::string_view(argv[1]), common_arguments, hosts_and_ports_arguments);
    int start_argument_index = has_connection_string ? 2 : 1;

    /** We allow different groups of arguments:
        * - common arguments;
        * - arguments for any number of external tables each in form "--external args...",
        *   where possible args are file, name, format, structure, types;
        * - param arguments for prepared statements.
        * Split these groups before processing.
        */
    bool in_external_group = false;

    std::string prev_host_arg;
    std::string prev_port_arg;

    for (int arg_num = start_argument_index; arg_num < argc; ++arg_num)
    {
        std::string_view arg = argv[arg_num];

        if (has_connection_string)
            checkIfCmdLineOptionCanBeUsedWithConnectionString(arg);

        if (arg == "--external")
        {
            in_external_group = true;
            external_tables_arguments.emplace_back(Arguments{""});
        }
        /// Options with value after equal sign.
        else if (
            in_external_group
            && (arg.starts_with("--file=") || arg.starts_with("--name=") || arg.starts_with("--format=") || arg.starts_with("--structure=")
                || arg.starts_with("--types=")))
        {
            external_tables_arguments.back().emplace_back(arg);
        }
        /// Options with value after whitespace.
        else if (in_external_group && (arg == "--file" || arg == "--name" || arg == "--format" || arg == "--structure" || arg == "--types"))
        {
            if (arg_num + 1 < argc)
            {
                external_tables_arguments.back().emplace_back(arg);
                ++arg_num;
                arg = argv[arg_num];
                external_tables_arguments.back().emplace_back(arg);
            }
            else
                break;
        }
        else
        {
            in_external_group = false;
            if (arg == "--file"sv || arg == "--name"sv || arg == "--structure"sv || arg == "--types"sv)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter must be in external group, try add --external before {}", arg);

            /// Parameter arg after underline or dash.
            if (arg.starts_with("--param_") || arg.starts_with("--param-"))
            {
                auto param_continuation = arg.substr(strlen("--param_"));
                auto equal_pos = param_continuation.find_first_of('=');

                if (equal_pos == std::string::npos)
                {
                    /// param_name value
                    ++arg_num;
                    if (arg_num >= argc)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter requires value");
                    arg = argv[arg_num];
                    query_parameters.emplace(String(param_continuation), String(arg));
                }
                else
                {
                    if (equal_pos == 0)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter name cannot be empty");

                    /// param_name=value
                    query_parameters.emplace(param_continuation.substr(0, equal_pos), param_continuation.substr(equal_pos + 1));
                }
            }
            else if (arg.starts_with("--host") || arg.starts_with("-h"))
            {
                std::string host_arg;
                /// --host host
                if (arg == "--host" || arg == "-h")
                {
                    ++arg_num;
                    if (arg_num >= argc)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Host argument requires value");
                    arg = argv[arg_num];
                    host_arg = "--host=";
                    host_arg.append(arg);
                }
                else
                    host_arg = arg;

                /// --port port1 --host host1
                if (!prev_port_arg.empty())
                {
                    hosts_and_ports_arguments.push_back({host_arg, prev_port_arg});
                    prev_port_arg.clear();
                }
                else
                {
                    /// --host host1 --host host2
                    if (!prev_host_arg.empty())
                        hosts_and_ports_arguments.push_back({prev_host_arg});

                    prev_host_arg = host_arg;
                }
            }
            else if (arg.starts_with("--port"))
            {
                auto port_arg = String{arg};
                /// --port port
                if (arg == "--port")
                {
                    port_arg.push_back('=');
                    ++arg_num;
                    if (arg_num >= argc)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Port argument requires value");
                    arg = argv[arg_num];
                    port_arg.append(arg);
                }

                /// --host host1 --port port1
                if (!prev_host_arg.empty())
                {
                    hosts_and_ports_arguments.push_back({port_arg, prev_host_arg});
                    prev_host_arg.clear();
                }
                else
                {
                    /// --port port1 --port port2
                    if (!prev_port_arg.empty())
                        hosts_and_ports_arguments.push_back({prev_port_arg});

                    prev_port_arg = port_arg;
                }
            }
            else if (arg == "--allow_repeated_settings")
                allow_repeated_settings = true;
            else if (arg == "--allow_merge_tree_settings")
                allow_merge_tree_settings = true;
            else if (arg == "--password" && ((arg_num + 1) >= argc || std::string_view(argv[arg_num + 1]).starts_with('-')))
            {
                common_arguments.emplace_back(arg);
                /// if the value of --password is omitted, the password will be asked before
                /// connection start
                common_arguments.emplace_back(ConnectionParameters::ASK_PASSWORD);
            }
            else
                common_arguments.emplace_back(arg);
        }
    }
    if (!prev_host_arg.empty())
        hosts_and_ports_arguments.push_back({prev_host_arg});
    if (!prev_port_arg.empty())
        hosts_and_ports_arguments.push_back({prev_port_arg});
}

}


#pragma clang diagnostic ignored "-Wunused-function"
#pragma clang diagnostic ignored "-Wmissing-declarations"

int mainEntryClickHouseClient(int argc, char ** argv)
{
    DB::MainThreadStatus::getInstance();

    try
    {
        DB::Client client;
        // Initialize command line options
        client.init(argc, argv);
        return client.run();
    }
    catch (DB::Exception & e)
    {
        std::cerr << DB::getExceptionMessageForLogging(e, false) << std::endl;
        auto code = DB::getCurrentExceptionCode();
        return static_cast<UInt8>(code) ? code : 1;
    }
    catch (const boost::program_options::error & e)
    {
        std::cerr << "Bad arguments: " << e.what() << std::endl;
        return DB::ErrorCodes::BAD_ARGUMENTS;
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
        auto code = DB::getCurrentExceptionCode();
        return static_cast<UInt8>(code) ? code : 1;
    }
}
