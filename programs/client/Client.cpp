#include <cstdlib>
#include <fcntl.h>
#include <map>
#include <iostream>
#include <iomanip>
#include <optional>
#include <string_view>
#include <Common/scope_guard_safe.h>
#include <boost/program_options.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <filesystem>
#include <string>
#include "Client.h"
#include "Core/Protocol.h"

#include <base/find_symbols.h>

#include <Common/config_version.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <Common/TerminalSize.h>
#include <Common/Config/configReadClient.h>

#include <Core/QueryProcessingStage.h>
#include <Columns/ColumnString.h>
#include <Poco/Util/Application.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/UseSSL.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>

#include <Interpreters/InterpreterSetQuery.h>

#include <Functions/registerFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Formats/registerFormats.h>

#ifndef __clang__
#pragma GCC optimize("-fno-var-tracking-assignments")
#endif

namespace fs = std::filesystem;
using namespace std::literals;


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int SYNTAX_ERROR;
    extern const int TOO_DEEP_RECURSION;
    extern const int NETWORK_ERROR;
    extern const int AUTHENTICATION_FAILED;
}


void Client::processError(const String & query) const
{
    if (server_exception)
    {
        fmt::print(stderr, "Received exception from server (version {}):\n{}\n",
                server_version,
                getExceptionMessage(*server_exception, print_stack_trace, true));
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
            std::cout << "Warnings:" << std::endl;
            for (const auto & message : messages)
                std::cout << " * " << message << std::endl;
            std::cout << std::endl;
        }
    }
    catch (...)
    {
        /// Ignore exception
    }
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
                          &global_context->getSettingsRef(),
                          &global_context->getClientInfo(), false, {});
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
                        messages.emplace_back(column.getDataAt(i).toString());
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
                throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from server {}",
                    packet.type, connection->getDescription());
        }
    }
}


void Client::initialize(Poco::Util::Application & self)
{
    Poco::Util::Application::initialize(self);

    const char * home_path_cstr = getenv("HOME");
    if (home_path_cstr)
        home_path = home_path_cstr;

    configReadClient(config(), home_path);

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

    const char * env_user = getenv("CLICKHOUSE_USER");
    if (env_user)
        config().setString("user", env_user);

    const char * env_password = getenv("CLICKHOUSE_PASSWORD");
    if (env_password)
        config().setString("password", env_password);

    // global_context->setApplicationType(Context::ApplicationType::CLIENT);
    global_context->setQueryParameters(query_parameters);

    size_t query_cache_size_in_bytes = config().getUInt64("query_cache_size_in_bytes", 1 * (1ULL << 20));
    global_context->setQueryCache(query_cache_size_in_bytes);

    /// settings and limits could be specified in config file, but passed settings has higher priority
    for (const auto & setting : global_context->getSettingsRef().allUnchanged())
    {
        const auto & name = setting.getName();
        if (config().has(name))
            global_context->setSetting(name, config().getString(name));
    }

    /// Set path for format schema files
    if (config().has("format_schema_path"))
        global_context->setFormatSchemaPath(fs::weakly_canonical(config().getString("format_schema_path")));
}


int Client::main(const std::vector<std::string> & /*args*/)
try
{
    UseSSL use_ssl;
    MainThreadStatus::getInstance();
    setupSignalHandler();

    std::cout << std::fixed << std::setprecision(3);
    std::cerr << std::fixed << std::setprecision(3);

    registerFormats();
    registerFunctions();
    registerAggregateFunctions();

    processConfig();

    /// Includes delayed_interactive.
    if (is_interactive)
    {
        clearTerminal();
        showClientVersion();
    }

    connect();

    /// Show warnings at the beginning of connection.
    if (is_interactive && !config().has("no-warnings"))
        showWarnings();

    if (is_interactive && !delayed_interactive)
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
catch (const Exception & e)
{
    bool need_print_stack_trace = config().getBool("stacktrace", false) && e.code() != ErrorCodes::NETWORK_ERROR;
    std::cerr << getExceptionMessage(e, need_print_stack_trace, true) << std::endl << std::endl;
    /// If exception code isn't zero, we should return non-zero return code anyway.
    return e.code() ? e.code() : -1;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(false) << std::endl;
    return getCurrentExceptionCode();
}


void Client::connect()
{
    String server_name;
    UInt64 server_version_major = 0;
    UInt64 server_version_minor = 0;
    UInt64 server_version_patch = 0;

    if (hosts_and_ports.empty())
    {
        String host = config().getString("host", "localhost");
        UInt16 port = ConnectionParameters::getPortFromConfig(config());
        hosts_and_ports.emplace_back(HostAndPort{host, port});
    }

    for (size_t attempted_address_index = 0; attempted_address_index < hosts_and_ports.size(); ++attempted_address_index)
    {
        try
        {
            connection_parameters = ConnectionParameters(
                config(), hosts_and_ports[attempted_address_index].host, hosts_and_ports[attempted_address_index].port);

            if (is_interactive)
                std::cout << "Connecting to "
                          << (!connection_parameters.default_database.empty() ? "database " + connection_parameters.default_database + " at "
                                                                              : "")
                          << connection_parameters.host << ":" << connection_parameters.port
                          << (!connection_parameters.user.empty() ? " as user " + connection_parameters.user : "") << "." << std::endl;

            connection = Connection::createConnection(connection_parameters, global_context);

            if (max_client_network_bandwidth)
            {
                ThrottlerPtr throttler = std::make_shared<Throttler>(max_client_network_bandwidth, 0, "");
                connection->setThrottler(throttler);
            }

            connection->getServerVersion(
                connection_parameters.timeouts, server_name, server_version_major, server_version_minor, server_version_patch, server_revision);
            config().setString("host", connection_parameters.host);
            config().setInt("port", connection_parameters.port);
            break;
        }
        catch (const Exception & e)
        {
            /// It is typical when users install ClickHouse, type some password and instantly forget it.
            /// This problem can't be fixed with reconnection so it is not attempted
            if ((connection_parameters.user.empty() || connection_parameters.user == "default")
                && e.code() == DB::ErrorCodes::AUTHENTICATION_FAILED)
            {
                std::cerr << std::endl
                          << "If you have installed ClickHouse and forgot password you can reset it in the configuration file." << std::endl
                          << "The password for default user is typically located at /etc/clickhouse-server/users.d/default-password.xml" << std::endl
                          << "and deleting this file will reset the password." << std::endl
                          << "See also /etc/clickhouse-server/users.xml on the server where ClickHouse is installed." << std::endl
                          << std::endl;
                throw;
            }
            else
            {
                if (attempted_address_index == hosts_and_ports.size() - 1)
                    throw;

                if (is_interactive)
                {
                    std::cerr << "Connection attempt to database at "
                              << connection_parameters.host << ":" << connection_parameters.port
                              << " resulted in failure"
                              << std::endl
                              << getExceptionMessage(e, false)
                              << std::endl
                              << "Attempting connection to the next provided address"
                              << std::endl;
                }
            }
        }
    }

    server_version = toString(server_version_major) + "." + toString(server_version_minor) + "." + toString(server_version_patch);
    load_suggestions = is_interactive && (server_revision >= Suggest::MIN_SERVER_REVISION) && !config().getBool("disable_suggestion", false);

    if (server_display_name = connection->getServerDisplayName(connection_parameters.timeouts); server_display_name.empty())
        server_display_name = config().getString("host", "localhost");

    if (is_interactive)
    {
        std::cout << "Connected to " << server_name << " server version " << server_version << " revision " << server_revision << "."
                    << std::endl << std::endl;

        auto client_version_tuple = std::make_tuple(VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH);
        auto server_version_tuple = std::make_tuple(server_version_major, server_version_minor, server_version_patch);

        if (client_version_tuple < server_version_tuple)
        {
            std::cout << "ClickHouse client version is older than ClickHouse server. "
                        << "It may lack support for new features." << std::endl
                        << std::endl;
        }
        else if (client_version_tuple > server_version_tuple)
        {
            std::cout << "ClickHouse server version is older than ClickHouse client. "
                        << "It may indicate that the server is out of date and can be upgraded." << std::endl
                        << std::endl;
        }
    }

    if (!global_context->getSettingsRef().use_client_time_zone)
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

    prompt_by_server_display_name = config().getRawString("prompt_by_server_display_name.default", "{display_name} :) ");

    Strings keys;
    config().keys("prompt_by_server_display_name", keys);
    for (const String & key : keys)
    {
        if (key != "default" && server_display_name.find(key) != std::string::npos)
        {
            prompt_by_server_display_name = config().getRawString("prompt_by_server_display_name." + key);
            break;
        }
    }

    /// Prompt may contain escape sequences including \e[ or \x1b[ sequences to set terminal color.
    {
        String unescaped_prompt_by_server_display_name;
        ReadBufferFromString in(prompt_by_server_display_name);
        readEscapedString(unescaped_prompt_by_server_display_name, in);
        prompt_by_server_display_name = std::move(unescaped_prompt_by_server_display_name);
    }

    /// Prompt may contain the following substitutions in a form of {name}.
    std::map<String, String> prompt_substitutions{
        {"host", connection_parameters.host},
        {"port", toString(connection_parameters.port)},
        {"user", connection_parameters.user},
        {"display_name", server_display_name},
    };

    /// Quite suboptimal.
    for (const auto & [key, value] : prompt_substitutions)
        boost::replace_all(prompt_by_server_display_name, "{" + key + "}", value);
}


// Prints changed settings to stderr. Useful for debugging fuzzing failures.
void Client::printChangedSettings() const
{
    const auto & changes = global_context->getSettingsRef().changes();
    if (!changes.empty())
    {
        fmt::print(stderr, "Changed settings: ");
        for (size_t i = 0; i < changes.size(); ++i)
        {
            if (i)
            {
                fmt::print(stderr, ", ");
            }
            fmt::print(stderr, "{} = '{}'", changes[i].name, toString(changes[i].value));
        }
        fmt::print(stderr, "\n");
    }
    else
    {
        fmt::print(stderr, "No changed settings.\n");
    }
}


static bool queryHasWithClause(const IAST & ast)
{
    if (const auto * select = dynamic_cast<const ASTSelectQuery *>(&ast); select && select->with())
    {
        return true;
    }

    // This full recursive walk is somewhat excessive, because most of the
    // children are not queries, but on the other hand it will let us to avoid
    // breakage when the AST structure changes and some new variant of query
    // nesting is added. This function is used in fuzzer, so it's better to be
    // defensive and avoid weird unexpected errors.
    for (const auto & child : ast.children)
    {
        if (queryHasWithClause(*child))
        {
            return true;
        }
    }

    return false;
}


/// Returns false when server is not available.
bool Client::processWithFuzzing(const String & full_query)
{
    ASTPtr orig_ast;

    try
    {
        const char * begin = full_query.data();
        orig_ast = parseQuery(begin, begin + full_query.size(), true);
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::SYNTAX_ERROR &&
            e.code() != ErrorCodes::TOO_DEEP_RECURSION)
            throw;
    }

    if (!orig_ast)
    {
        // Can't continue after a parsing error
        return true;
    }

    // `USE db` should not be executed
    // since this will break every query after `DROP db`
    if (orig_ast->as<ASTUseQuery>())
    {
        return true;
    }

    // Don't repeat:
    // - INSERT -- Because the tables may grow too big.
    // - CREATE -- Because first we run the unmodified query, it will succeed,
    //             and the subsequent queries will fail.
    //             When we run out of fuzzer errors, it may be interesting to
    //             add fuzzing of create queries that wraps columns into
    //             LowCardinality or Nullable.
    //             Also there are other kinds of create queries such as CREATE
    //             DICTIONARY, we could fuzz them as well.
    // - DROP   -- No point in this (by the same reasons).
    // - SET    -- The time to fuzz the settings has not yet come
    //             (see comments in Client/QueryFuzzer.cpp)
    size_t this_query_runs = query_fuzzer_runs;
    if (orig_ast->as<ASTInsertQuery>() ||
        orig_ast->as<ASTCreateQuery>() ||
        orig_ast->as<ASTDropQuery>() ||
        orig_ast->as<ASTSetQuery>())
    {
        this_query_runs = 1;
    }

    String query_to_execute;
    ASTPtr parsed_query;

    ASTPtr fuzz_base = orig_ast;
    for (size_t fuzz_step = 0; fuzz_step < this_query_runs; ++fuzz_step)
    {
        fmt::print(stderr, "Fuzzing step {} out of {}\n", fuzz_step, this_query_runs);

        ASTPtr ast_to_process;
        try
        {
            WriteBufferFromOwnString dump_before_fuzz;
            fuzz_base->dumpTree(dump_before_fuzz);
            auto base_before_fuzz = fuzz_base->formatForErrorMessage();

            ast_to_process = fuzz_base->clone();

            WriteBufferFromOwnString dump_of_cloned_ast;
            ast_to_process->dumpTree(dump_of_cloned_ast);

            // Run the original query as well.
            if (fuzz_step > 0)
            {
                fuzzer.fuzzMain(ast_to_process);
            }

            auto base_after_fuzz = fuzz_base->formatForErrorMessage();

            // Check that the source AST didn't change after fuzzing. This
            // helps debug AST cloning errors, where the cloned AST doesn't
            // clone all its children, and erroneously points to some source
            // child elements.
            if (base_before_fuzz != base_after_fuzz)
            {
                printChangedSettings();

                fmt::print(
                    stderr,
                    "Base before fuzz: {}\n"
                    "Base after fuzz: {}\n",
                    base_before_fuzz,
                    base_after_fuzz);
                fmt::print(stderr, "Dump before fuzz:\n{}\n", dump_before_fuzz.str());
                fmt::print(stderr, "Dump of cloned AST:\n{}\n", dump_of_cloned_ast.str());
                fmt::print(stderr, "Dump after fuzz:\n");

                WriteBufferFromOStream cerr_buf(std::cerr, 4096);
                fuzz_base->dumpTree(cerr_buf);
                cerr_buf.next();

                fmt::print(
                    stderr,
                    "Found error: IAST::clone() is broken for some AST node. This is a bug. The original AST ('dump before fuzz') and its cloned copy ('dump of cloned AST') refer to the same nodes, which must never happen. This means that their parent node doesn't implement clone() correctly.");

                exit(1);
            }

            auto fuzzed_text = ast_to_process->formatForErrorMessage();
            if (fuzz_step > 0 && fuzzed_text == base_before_fuzz)
            {
                fmt::print(stderr, "Got boring AST\n");
                continue;
            }

            parsed_query = ast_to_process;
            query_to_execute = parsed_query->formatForErrorMessage();
            processParsedSingleQuery(full_query, query_to_execute, parsed_query);
        }
        catch (...)
        {
            // Some functions (e.g. protocol parsers) don't throw, but
            // set last_exception instead, so we'll also do it here for
            // uniformity.
            // Surprisingly, this is a client exception, because we get the
            // server exception w/o throwing (see onReceiveException()).
            client_exception = std::make_unique<Exception>(getCurrentExceptionMessage(print_stack_trace), getCurrentExceptionCode());
            have_error = true;
        }

        const auto * exception = server_exception ? server_exception.get() : client_exception.get();
        // Sometimes you may get TOO_DEEP_RECURSION from the server,
        // and TOO_DEEP_RECURSION should not fail the fuzzer check.
        if (have_error && exception->code() == ErrorCodes::TOO_DEEP_RECURSION)
        {
            have_error = false;
            server_exception.reset();
            client_exception.reset();
            return true;
        }

        if (have_error)
        {
            fmt::print(stderr, "Error on processing query '{}': {}\n", ast_to_process->formatForErrorMessage(), exception->message());

            // Try to reconnect after errors, for two reasons:
            // 1. We might not have realized that the server died, e.g. if
            //    it sent us a <Fatal> trace and closed connection properly.
            // 2. The connection might have gotten into a wrong state and
            //    the next query will get false positive about
            //    "Unknown packet from server".
            try
            {
                connection->forceConnected(connection_parameters.timeouts);
            }
            catch (...)
            {
                // Just report it, we'll terminate below.
                fmt::print(stderr,
                    "Error while reconnecting to the server: {}\n",
                    getCurrentExceptionMessage(true));

                // The reconnection might fail, but we'll still be connected
                // in the sense of `connection->isConnected() = true`,
                // in case when the requested database doesn't exist.
                // Disconnect manually now, so that the following code doesn't
                // have any doubts, and the connection state is predictable.
                connection->disconnect();
            }
        }

        if (!connection->isConnected())
        {
            // Probably the server is dead because we found an assertion
            // failure. Fail fast.
            fmt::print(stderr, "Lost connection to the server.\n");

            // Print the changed settings because they might be needed to
            // reproduce the error.
            printChangedSettings();

            return false;
        }

        // Check that after the query is formatted, we can parse it back,
        // format again and get the same result. Unfortunately, we can't
        // compare the ASTs, which would be more sensitive to errors. This
        // double formatting check doesn't catch all errors, e.g. we can
        // format query incorrectly, but to a valid SQL that we can then
        // parse and format into the same SQL.
        // There are some complicated cases where we can generate the SQL
        // which we can't parse:
        // * first argument of lambda() replaced by fuzzer with
        //   something else, leading to constructs such as
        //   arrayMap((min(x) + 3) -> x + 1, ....)
        // * internals of Enum replaced, leading to:
        //   Enum(equals(someFunction(y), 3)).
        // And there are even the cases when we can parse the query, but
        // it's logically incorrect and its formatting is a mess, such as
        // when `lambda()` function gets substituted into a wrong place.
        // To avoid dealing with these cases, run the check only for the
        // queries we were able to successfully execute.
        // Another caveat is that sometimes WITH queries are not executed,
        // if they are not referenced by the main SELECT, so they can still
        // have the aforementioned problems. Disable this check for such
        // queries, for lack of a better solution.
        // There is also a problem that fuzzer substitutes positive Int64
        // literals or Decimal literals, which are then parsed back as
        // UInt64, and suddenly duplicate alias substitition starts or stops
        // working (ASTWithAlias::formatImpl) or something like that.
        // So we compare not even the first and second formatting of the
        // query, but second and third.
        // If you have to add any more workarounds to this check, just remove
        // it altogether, it's not so useful.
        if (parsed_query && !have_error && !queryHasWithClause(*parsed_query))
        {
            ASTPtr ast_2;
            try
            {
                const auto * tmp_pos = query_to_execute.c_str();

                ast_2 = parseQuery(tmp_pos, tmp_pos + query_to_execute.size(), false /* allow_multi_statements */);
            }
            catch (Exception & e)
            {
                if (e.code() != ErrorCodes::SYNTAX_ERROR &&
                    e.code() != ErrorCodes::TOO_DEEP_RECURSION)
                    throw;
            }

            if (ast_2)
            {
                const auto text_2 = ast_2->formatForErrorMessage();
                const auto * tmp_pos = text_2.c_str();
                const auto ast_3 = parseQuery(tmp_pos, tmp_pos + text_2.size(),
                    false /* allow_multi_statements */);
                const auto text_3 = ast_3->formatForErrorMessage();
                if (text_3 != text_2)
                {
                    fmt::print(stderr, "Found error: The query formatting is broken.\n");

                    printChangedSettings();

                    fmt::print(stderr,
                        "Got the following (different) text after formatting the fuzzed query and parsing it back:\n'{}'\n, expected:\n'{}'\n",
                        text_3, text_2);
                    fmt::print(stderr, "In more detail:\n");
                    fmt::print(stderr, "AST-1 (generated by fuzzer):\n'{}'\n", parsed_query->dumpTree());
                    fmt::print(stderr, "Text-1 (AST-1 formatted):\n'{}'\n", query_to_execute);
                    fmt::print(stderr, "AST-2 (Text-1 parsed):\n'{}'\n", ast_2->dumpTree());
                    fmt::print(stderr, "Text-2 (AST-2 formatted):\n'{}'\n", text_2);
                    fmt::print(stderr, "AST-3 (Text-2 parsed):\n'{}'\n", ast_3->dumpTree());
                    fmt::print(stderr, "Text-3 (AST-3 formatted):\n'{}'\n", text_3);
                    fmt::print(stderr, "Text-3 must be equal to Text-2, but it is not.\n");

                    exit(1);
                }
            }
        }

        // The server is still alive so we're going to continue fuzzing.
        // Determine what we're going to use as the starting AST.
        if (have_error)
        {
            // Query completed with error, keep the previous starting AST.
            // Also discard the exception that we now know to be non-fatal,
            // so that it doesn't influence the exit code.
            server_exception.reset();
            client_exception.reset();
            have_error = false;
        }
        else if (ast_to_process->formatForErrorMessage().size() > 500)
        {
            // ast too long, start from original ast
            fmt::print(stderr, "Current AST is too long, discarding it and using the original AST as a start\n");
            fuzz_base = orig_ast;
        }
        else
        {
            // fuzz starting from this successful query
            fmt::print(stderr, "Query succeeded, using this AST as a start\n");
            fuzz_base = ast_to_process;
        }
    }

    return true;
}


void Client::printHelpMessage(const OptionsDescription & options_description)
{
    std::cout << options_description.main_description.value() << "\n";
    std::cout << options_description.external_description.value() << "\n";
    std::cout << options_description.hosts_and_ports_description.value() << "\n";
    std::cout << "In addition, --param_name=value can be specified for substitution of parameters for parametrized queries.\n";
}


void Client::addOptions(OptionsDescription & options_description)
{
    /// Main commandline options related to client functionality and all parameters from Settings.
    options_description.main_description->add_options()
        ("config,c", po::value<std::string>(), "config-file path (another shorthand)")
        ("secure,s", "Use TLS connection")
        ("user,u", po::value<std::string>()->default_value("default"), "user")
        /** If "--password [value]" is used but the value is omitted, the bad argument exception will be thrown.
            * implicit_value is used to avoid this exception (to allow user to type just "--password")
            * Since currently boost provides no way to check if a value has been set implicitly for an option,
            * the "\n" is used to distinguish this case because there is hardly a chance a user would use "\n"
            * as the password.
            */
        ("password", po::value<std::string>()->implicit_value("\n", ""), "password")
        ("ask-password", "ask-password")
        ("quota_key", po::value<std::string>(), "A string to differentiate quotas when the user have keyed quotas configured on server")

        ("max_client_network_bandwidth", po::value<int>(), "the maximum speed of data exchange over the network for the client in bytes per second.")
        ("compression", po::value<bool>(), "enable or disable compression (enabled by default for remote communication and disabled for localhost communication).")

        ("query-fuzzer-runs", po::value<int>()->default_value(0), "After executing every SELECT query, do random mutations in it and run again specified number of times. This is used for testing to discover unexpected corner cases.")
        ("interleave-queries-file", po::value<std::vector<std::string>>()->multitoken(),
            "file path with queries to execute before every file from 'queries-file'; multiple files can be specified (--queries-file file1 file2...); this is needed to enable more aggressive fuzzing of newly added tests (see 'query-fuzzer-runs' option)")

        ("opentelemetry-traceparent", po::value<std::string>(), "OpenTelemetry traceparent header as described by W3C Trace Context recommendation")
        ("opentelemetry-tracestate", po::value<std::string>(), "OpenTelemetry tracestate header as described by W3C Trace Context recommendation")

        ("no-warnings", "disable warnings when client connects to server")
        ("fake-drop", "Ignore all DROP queries, should be used only for testing")
    ;

    /// Commandline options related to external tables.

    options_description.external_description.emplace(createOptionsDescription("External tables options", terminal_width));
    options_description.external_description->add_options()
    (
        "file", po::value<std::string>(), "data file or - for stdin"
    )
    (
        "name", po::value<std::string>()->default_value("_data"), "name of the table"
    )
    (
        "format", po::value<std::string>()->default_value("TabSeparated"), "data format"
    )
    (
        "structure", po::value<std::string>(), "structure"
    )
    (
        "types", po::value<std::string>(), "types"
    );

    /// Commandline options related to hosts and ports.
    options_description.hosts_and_ports_description.emplace(createOptionsDescription("Hosts and ports options", terminal_width));
    options_description.hosts_and_ports_description->add_options()
        ("host,h", po::value<String>()->default_value("localhost"),
         "Server hostname. Multiple hosts can be passed via multiple arguments"
         "Example of usage: '--host host1 --host host2 --port port2 --host host3 ...'"
         "Each '--port port' will be attached to the last seen host that doesn't have a port yet,"
         "if there is no such host, the port will be attached to the next first host or to default host.")
         ("port", po::value<UInt16>(), "server ports")
    ;
}


void Client::processOptions(const OptionsDescription & options_description,
                            const CommandLineOptions & options,
                            const std::vector<Arguments> & external_tables_arguments,
                            const std::vector<Arguments> & hosts_and_ports_arguments)
{
    namespace po = boost::program_options;

    size_t number_of_external_tables_with_stdin_source = 0;
    for (size_t i = 0; i < external_tables_arguments.size(); ++i)
    {
        /// Parse commandline options related to external tables.
        po::parsed_options parsed_tables = po::command_line_parser(external_tables_arguments[i]).options(
            options_description.external_description.value()).run();
        po::variables_map external_options;
        po::store(parsed_tables, external_options);

        try
        {
            external_tables.emplace_back(external_options);
            if (external_tables.back().file == "-")
                ++number_of_external_tables_with_stdin_source;
            if (number_of_external_tables_with_stdin_source > 1)
                throw Exception("Two or more external tables has stdin (-) set as --file field", ErrorCodes::BAD_ARGUMENTS);
        }
        catch (const Exception & e)
        {
            std::cerr << getExceptionMessage(e, false) << std::endl;
            std::cerr << "Table №" << i << std::endl << std::endl;
            /// Avoid the case when error exit code can possibly overflow to normal (zero).
            auto exit_code = e.code() % 256;
            if (exit_code == 0)
                exit_code = 255;
            exit(exit_code);
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
        std::optional<UInt16> port = !host_and_port_options["port"].empty()
                                     ? std::make_optional(host_and_port_options["port"].as<UInt16>())
                                     : std::nullopt;
        hosts_and_ports.emplace_back(HostAndPort{host, port});
    }

    send_external_tables = true;

    shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::CLIENT);

    global_context->setSettings(cmd_settings);

    /// Copy settings-related program options to config.
    /// TODO: Is this code necessary?
    for (const auto & setting : global_context->getSettingsRef().all())
    {
        const auto & name = setting.getName();
        if (options.count(name))
        {
            if (allow_repeated_settings)
                config().setString(name, options[name].as<Strings>().back());
            else
                config().setString(name, options[name].as<String>());
        }
    }

    if (options.count("config-file") && options.count("config"))
        throw Exception("Two or more configuration files referenced in arguments", ErrorCodes::BAD_ARGUMENTS);

    if (options.count("config"))
        config().setString("config-file", options["config"].as<std::string>());
    if (options.count("interleave-queries-file"))
        interleave_queries_files = options["interleave-queries-file"].as<std::vector<std::string>>();
    if (options.count("secure"))
        config().setBool("secure", true);
    if (options.count("user") && !options["user"].defaulted())
        config().setString("user", options["user"].as<std::string>());
    if (options.count("password"))
        config().setString("password", options["password"].as<std::string>());
    if (options.count("ask-password"))
        config().setBool("ask-password", true);
    if (options.count("quota_key"))
        config().setString("quota_key", options["quota_key"].as<std::string>());
    if (options.count("max_client_network_bandwidth"))
        max_client_network_bandwidth = options["max_client_network_bandwidth"].as<int>();
    if (options.count("compression"))
        config().setBool("compression", options["compression"].as<bool>());
    if (options.count("no-warnings"))
        config().setBool("no-warnings", true);
    if (options.count("fake-drop"))
        fake_drop = true;

    if ((query_fuzzer_runs = options["query-fuzzer-runs"].as<int>()))
    {
        // Fuzzer implies multiquery.
        config().setBool("multiquery", true);
        // Ignore errors in parsing queries.
        config().setBool("ignore-error", true);
        ignore_error = true;
    }

    if (options.count("opentelemetry-traceparent"))
    {
        String traceparent = options["opentelemetry-traceparent"].as<std::string>();
        String error;
        if (!global_context->getClientInfo().client_trace_context.parseTraceparentHeader(traceparent, error))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse OpenTelemetry traceparent '{}': {}", traceparent, error);
    }

    if (options.count("opentelemetry-tracestate"))
        global_context->getClientInfo().client_trace_context.tracestate = options["opentelemetry-tracestate"].as<std::string>();
}


void Client::processConfig()
{
    /// Batch mode is enabled if one of the following is true:
    /// - -e (--query) command line option is present.
    ///   The value of the option is used as the text of query (or of multiple queries).
    ///   If stdin is not a terminal, INSERT data for the first query is read from it.
    /// - stdin is not a terminal. In this case queries are read from it.
    /// - --queries-file command line option is present.
    ///   The value of the option is used as file with query (or of multiple queries) to execute.

    delayed_interactive = config().has("interactive") && (config().has("query") || config().has("queries-file"));
    if (stdin_is_a_tty
        && (delayed_interactive || (!config().has("query") && queries_files.empty())))
    {
        is_interactive = true;
    }
    else
    {
        need_render_progress = config().getBool("progress", false);
        echo_queries = config().getBool("echo", false);
        ignore_error = config().getBool("ignore-error", false);

        auto query_id = config().getString("query_id", "");
        if (!query_id.empty())
            global_context->setCurrentQueryId(query_id);
    }
    print_stack_trace = config().getBool("stacktrace", false);
    logging_initialized = true;

    if (config().has("multiquery"))
        is_multiquery = true;

    is_default_format = !config().has("vertical") && !config().has("format");
    if (config().has("vertical"))
        format = config().getString("format", "Vertical");
    else
        format = config().getString("format", is_interactive ? "PrettyCompact" : "TabSeparated");

    format_max_block_size = config().getInt("format_max_block_size", global_context->getSettingsRef().max_block_size);

    insert_format = "Values";

    /// Setting value from cmd arg overrides one from config
    if (global_context->getSettingsRef().max_insert_block_size.changed)
        insert_format_max_block_size = global_context->getSettingsRef().max_insert_block_size;
    else
        insert_format_max_block_size = config().getInt("insert_format_max_block_size", global_context->getSettingsRef().max_insert_block_size);

    ClientInfo & client_info = global_context->getClientInfo();
    client_info.setInitialQuery();
    client_info.quota_key = config().getString("quota_key", "");
    client_info.query_kind = query_kind;
}


void Client::readArguments(
    int argc,
    char ** argv,
    Arguments & common_arguments,
    std::vector<Arguments> & external_tables_arguments,
    std::vector<Arguments> & hosts_and_ports_arguments)
{
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

    for (int arg_num = 1; arg_num < argc; ++arg_num)
    {
        std::string_view arg = argv[arg_num];

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

            /// Parameter arg after underline.
            if (arg.starts_with("--param_"))
            {
                auto param_continuation = arg.substr(strlen("--param_"));
                auto equal_pos = param_continuation.find_first_of('=');

                if (equal_pos == std::string::npos)
                {
                    /// param_name value
                    ++arg_num;
                    if (arg_num >= argc)
                        throw Exception("Parameter requires value", ErrorCodes::BAD_ARGUMENTS);
                    arg = argv[arg_num];
                    query_parameters.emplace(String(param_continuation), String(arg));
                }
                else
                {
                    if (equal_pos == 0)
                        throw Exception("Parameter name cannot be empty", ErrorCodes::BAD_ARGUMENTS);

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
                        throw Exception("Host argument requires value", ErrorCodes::BAD_ARGUMENTS);
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
                        throw Exception("Port argument requires value", ErrorCodes::BAD_ARGUMENTS);
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


#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wmissing-declarations"

int mainEntryClickHouseClient(int argc, char ** argv)
{
    try
    {
        DB::Client client;
        client.init(argc, argv);
        return client.run();
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
