#include <stdlib.h>
#include <fcntl.h>
#include <signal.h>
#include <map>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <unordered_set>
#include <algorithm>
#include <optional>
#include <common/scope_guard_safe.h>
#include <boost/program_options.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <Poco/String.h>
#include <filesystem>
#include <string>
#include "Client.h"

#include <common/argsToConfig.h>
#include <common/find_symbols.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif
#include <Common/ClickHouseRevision.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <Common/NetException.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/PODArray.h>
#include <Common/TerminalSize.h>
#include <Common/Config/configReadClient.h>
#include <Common/InterruptListener.h>
#include "Common/MemoryTracker.h"

#include <Core/QueryProcessingStage.h>
#include <Client/Connection.h>
#include <Columns/ColumnString.h>
#include "Columns/ColumnsNumber.h"
#include <Poco/Util/Application.h>

#include <Processors/Formats/IInputFormat.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromOStream.h>

#include <DataStreams/NullBlockOutputStream.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/formatAST.h>

#include <Interpreters/InterpreterSetQuery.h>

#include <Functions/registerFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Formats/registerFormats.h>
#include <Formats/FormatFactory.h>

#ifndef __clang__
#pragma GCC optimize("-fno-var-tracking-assignments")
#endif

namespace CurrentMetrics
{
    extern const Metric Revision;
    extern const Metric VersionInteger;
    extern const Metric MemoryTracking;
    extern const Metric MaxDDLEntryID;
}

namespace fs = std::filesystem;


namespace DB
{
namespace ErrorCodes
{
    extern const int NO_DATA_TO_INSERT;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
    extern const int CLIENT_OUTPUT_FORMAT_SPECIFIED;
    extern const int INVALID_USAGE_OF_INPUT;
    extern const int DEADLOCK_AVOIDED;
    extern const int SYNTAX_ERROR;
    extern const int TOO_DEEP_RECURSION;
    extern const int NETWORK_ERROR;
    extern const int UNRECOGNIZED_ARGUMENTS;
}


static bool queryHasWithClause(const IAST * ast)
{
    if (const auto * select = dynamic_cast<const ASTSelectQuery *>(ast); select && select->with())
    {
        return true;
    }

    // This full recursive walk is somewhat excessive, because most of the
    // children are not queries, but on the other hand it will let us to avoid
    // breakage when the AST structure changes and some new variant of query
    // nesting is added. This function is used in fuzzer, so it's better to be
    // defensive and avoid weird unexpected errors.
    // clang-tidy is confused by this function: it thinks that if `select` is
    // nullptr, `ast` is also nullptr, and complains about nullptr dereference.
    // NOLINTNEXTLINE
    for (const auto & child : ast->children)
    {
        if (queryHasWithClause(child.get()))
        {
            return true;
        }
    }

    return false;
}


/// Make query to get all server warnings
std::vector<String> Client::loadWarningMessages()
{
    std::vector<String> messages;
    connection->sendQuery(connection_parameters.timeouts, "SELECT message FROM system.warnings", "" /* query_id */, QueryProcessingStage::Complete, nullptr, nullptr, false);
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
                continue;
            case Protocol::Server::ProfileInfo:
                continue;
            case Protocol::Server::Totals:
                continue;
            case Protocol::Server::Extremes:
                continue;
            case Protocol::Server::Log:
                continue;

            case Protocol::Server::Exception:
                packet.exception->rethrow();
                return messages;

            case Protocol::Server::EndOfStream:
                return messages;

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

    // global_context->setApplicationType(Context::ApplicationType::CLIENT);
    global_context->setQueryParameters(query_parameters);

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


void Client::loadSuggestionData(Suggest & suggest)
{
    if (server_revision >= Suggest::MIN_SERVER_REVISION && !config().getBool("disable_suggestion", false))
    {
        /// Load suggestion data from the server.
        suggest.load(connection_parameters, config().getInt("suggestion_limit"));
    }
}


int Client::mainImpl()
{
    try
    {
        MainThreadStatus::getInstance();

        /// Limit on total memory usage
        size_t max_client_memory_usage = config().getInt64("max_memory_usage_in_client", 0 /*default value*/);

        if (max_client_memory_usage != 0)
        {
            total_memory_tracker.setHardLimit(max_client_memory_usage);
            total_memory_tracker.setDescription("(total)");
            total_memory_tracker.setMetric(CurrentMetrics::MemoryTracking);
        }

        registerFormats();
        registerFunctions();
        registerAggregateFunctions();

        processConfig();
        connect();

        if (is_interactive)
        {
            /// Load Warnings at the beginning of connection
            if (!config().has("no-warnings"))
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

            auto try_process_query_text = [&](std::function<bool()> func)
            {
                try
                {
                    return func();
                }
                catch (const Exception & e)
                {
                    /// We don't need to handle the test hints in the interactive mode.
                    bool print_stack_trace = config().getBool("stacktrace", false);
                    std::cerr << "Exception on client:" << std::endl << getExceptionMessage(e, print_stack_trace, true) << std::endl << std::endl;

                    client_exception = std::make_unique<Exception>(e);
                }

                if (client_exception)
                {
                    /// client_exception may have been set above or elsewhere.
                    /// Client-side exception during query execution can result in the loss of
                    /// sync in the connection protocol.
                    /// So we reconnect and allow to enter the next query.
                    reconnectIfNeeded();
                }

                return true;
            };

            runInteractive(try_process_query_text);
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
        }
    }
    catch (const Exception & e)
    {
        bool print_stack_trace = config().getBool("stacktrace", false) && e.code() != ErrorCodes::NETWORK_ERROR;
        std::cerr << getExceptionMessage(e, print_stack_trace, true) << std::endl << std::endl;
        /// If exception code isn't zero, we should return non-zero return code anyway.
        return e.code() ? e.code() : -1;
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false) << std::endl;
        return getCurrentExceptionCode();
    }

    return 0;
}


void Client::connect()
{
    connection_parameters = ConnectionParameters(config());

    if (is_interactive)
        std::cout << "Connecting to "
                    << (!connection_parameters.default_database.empty() ? "database " + connection_parameters.default_database + " at "
                                                                        : "")
                    << connection_parameters.host << ":" << connection_parameters.port
                    << (!connection_parameters.user.empty() ? " as user " + connection_parameters.user : "") << "." << std::endl;

    connection = std::make_unique<Connection>(
        connection_parameters.host,
        connection_parameters.port,
        connection_parameters.default_database,
        connection_parameters.user,
        connection_parameters.password,
        "", /* cluster */
        "", /* cluster_secret */
        "client",
        connection_parameters.compression,
        connection_parameters.security);

    String server_name;
    UInt64 server_version_major = 0;
    UInt64 server_version_minor = 0;
    UInt64 server_version_patch = 0;

    if (max_client_network_bandwidth)
    {
        ThrottlerPtr throttler = std::make_shared<Throttler>(max_client_network_bandwidth, 0, "");
        connection->setThrottler(throttler);
    }

    connection->getServerVersion(
        connection_parameters.timeouts, server_name, server_version_major, server_version_minor, server_version_patch, server_revision);

    server_version = toString(server_version_major) + "." + toString(server_version_minor) + "." + toString(server_version_patch);

    if (server_display_name = connection->getServerDisplayName(connection_parameters.timeouts); server_display_name.empty())
        server_display_name = config().getString("host", "localhost");

    if (is_interactive)
    {
        std::cout << "Connected to " << server_name << " server version " << server_version << " revision " << server_revision << "."
                    << std::endl
                    << std::endl;

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


void Client::reportQueryError(const String & query) const
{
    if (server_exception)
    {
        bool print_stack_trace = config().getBool("stacktrace", false);
        std::cerr << "Received exception from server (version " << server_version << "):" << std::endl
                  << getExceptionMessage(*server_exception, print_stack_trace, true) << std::endl;
        if (is_interactive)
            std::cerr << std::endl;
    }

    if (client_exception)
    {
        fmt::print(stderr, "Error on processing query '{}':\n{}\n", query, client_exception->message());
        if (is_interactive)
            fmt::print(stderr, "\n");
    }

    // A debug check -- at least some exception must be set, if the error
    // flag is set, and vice versa.
    assert(have_error == (client_exception || server_exception));
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

    // Don't repeat inserts, the tables grow too big. Also don't repeat
    // creates because first we run the unmodified query, it will succeed,
    // and the subsequent queries will fail. When we run out of fuzzer
    // errors, it may be interesting to add fuzzing of create queries that
    // wraps columns into LowCardinality or Nullable. Also there are other
    // kinds of create queries such as CREATE DICTIONARY, we could fuzz
    // them as well. Also there is no point fuzzing DROP queries.
    size_t this_query_runs = query_fuzzer_runs;
    if (orig_ast->as<ASTInsertQuery>() || orig_ast->as<ASTCreateQuery>() || orig_ast->as<ASTDropQuery>())
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
            processSingleQueryImpl(full_query, query_to_execute, parsed_query);
        }
        catch (...)
        {
            // Some functions (e.g. protocol parsers) don't throw, but
            // set last_exception instead, so we'll also do it here for
            // uniformity.
            // Surprisingly, this is a client exception, because we get the
            // server exception w/o throwing (see onReceiveException()).
            client_exception = std::make_unique<Exception>(getCurrentExceptionMessage(true), getCurrentExceptionCode());
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
        if (!have_error && !queryHasWithClause(parsed_query.get()))
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


void Client::readArguments(int argc, char ** argv, Arguments & common_arguments, std::vector<Arguments> & external_tables_arguments)
{
    /** We allow different groups of arguments:
        * - common arguments;
        * - arguments for any number of external tables each in form "--external args...",
        *   where possible args are file, name, format, structure, types;
        * - param arguments for prepared statements.
        * Split these groups before processing.
        */

    bool in_external_group = false;
    for (int arg_num = 1; arg_num < argc; ++arg_num)
    {
        const char * arg = argv[arg_num];

        if (0 == strcmp(arg, "--external"))
        {
            in_external_group = true;
            external_tables_arguments.emplace_back(Arguments{""});
        }
        /// Options with value after equal sign.
        else if (in_external_group
            && (0 == strncmp(arg, "--file=", strlen("--file=")) || 0 == strncmp(arg, "--name=", strlen("--name="))
                || 0 == strncmp(arg, "--format=", strlen("--format=")) || 0 == strncmp(arg, "--structure=", strlen("--structure="))
                || 0 == strncmp(arg, "--types=", strlen("--types="))))
        {
            external_tables_arguments.back().emplace_back(arg);
        }
        /// Options with value after whitespace.
        else if (in_external_group
            && (0 == strcmp(arg, "--file") || 0 == strcmp(arg, "--name") || 0 == strcmp(arg, "--format")
                || 0 == strcmp(arg, "--structure") || 0 == strcmp(arg, "--types")))
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

            /// Parameter arg after underline.
            if (startsWith(arg, "--param_"))
            {
                const char * param_continuation = arg + strlen("--param_");
                const char * equal_pos = strchr(param_continuation, '=');

                if (equal_pos == param_continuation)
                    throw Exception("Parameter name cannot be empty", ErrorCodes::BAD_ARGUMENTS);

                if (equal_pos)
                {
                    /// param_name=value
                    query_parameters.emplace(String(param_continuation, equal_pos), String(equal_pos + 1));
                }
                else
                {
                    /// param_name value
                    ++arg_num;
                    if (arg_num >= argc)
                        throw Exception("Parameter requires value", ErrorCodes::BAD_ARGUMENTS);
                    arg = argv[arg_num];
                    query_parameters.emplace(String(param_continuation), String(arg));
                }
            }
            else
                common_arguments.emplace_back(arg);
        }
    }

}


void Client::printHelpMessage(const OptionsDescription & options_description)
{
    std::cout << options_description.main_description.value() << "\n";
    std::cout << options_description.external_description.value() << "\n";
    std::cout << "In addition, --param_name=value can be specified for substitution of parameters for parametrized queries.\n";
}


void Client::addAndCheckOptions(OptionsDescription & options_description, po::variables_map & options, Arguments & arguments)
{
    /// Main commandline options related to client functionality and all parameters from Settings.

    options_description.main_description.emplace(createOptionsDescription("Main options", terminal_width));
    options_description.main_description->add_options()
        ("help", "produce help message")
        ("config-file,C", po::value<std::string>(), "config-file path")
        ("config,c", po::value<std::string>(), "config-file path (another shorthand)")
        ("host,h", po::value<std::string>()->default_value("localhost"), "server host")
        ("port", po::value<int>()->default_value(9000), "server port")
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
        ("stage", po::value<std::string>()->default_value("complete"), "Request query processing up to specified stage: complete,fetch_columns,with_mergeable_state,with_mergeable_state_after_aggregation,with_mergeable_state_after_aggregation_and_limit")
        ("query_id", po::value<std::string>(), "query_id")
        ("query,q", po::value<std::string>(), "query")
        ("database,d", po::value<std::string>(), "database")
        ("pager", po::value<std::string>(), "pager")
        ("disable_suggestion,A", "Disable loading suggestion data. Note that suggestion data is loaded asynchronously through a second connection to ClickHouse server. Also it is reasonable to disable suggestion if you want to paste a query with TAB characters. Shorthand option -A is for those who get used to mysql client.")
        ("suggestion_limit", po::value<int>()->default_value(10000),
            "Suggestion limit for how many databases, tables and columns to fetch.")
        ("multiline,m", "multiline")
        ("multiquery,n", "multiquery")
        ("queries-file", po::value<std::vector<std::string>>()->multitoken(),
            "file path with queries to execute; multiple files can be specified (--queries-file file1 file2...)")
        ("format,f", po::value<std::string>(), "default output format")
        ("testmode,T", "enable test hints in comments")
        ("ignore-error", "do not stop processing in multiquery mode")
        ("vertical,E", "vertical output format, same as --format=Vertical or FORMAT Vertical or \\G at end of command")
        ("time,t", "print query execution time to stderr in non-interactive mode (for benchmarks)")
        ("stacktrace", "print stack traces of exceptions")
        ("progress", "print progress even in non-interactive mode")
        ("version,V", "print version information and exit")
        ("version-clean", "print version in machine-readable format and exit")
        ("echo", "in batch mode, print query before execution")
        ("max_client_network_bandwidth", po::value<int>(), "the maximum speed of data exchange over the network for the client in bytes per second.")
        ("compression", po::value<bool>(), "enable or disable compression")
        ("highlight", po::value<bool>()->default_value(true), "enable or disable basic syntax highlight in interactive command line")
        ("log-level", po::value<std::string>(), "client log level")
        ("server_logs_file", po::value<std::string>(), "put server logs into specified file")
        ("query-fuzzer-runs", po::value<int>()->default_value(0), "After executing every SELECT query, do random mutations in it and run again specified number of times. This is used for testing to discover unexpected corner cases.")
        ("interleave-queries-file", po::value<std::vector<std::string>>()->multitoken(),
            "file path with queries to execute before every file from 'queries-file'; multiple files can be specified (--queries-file file1 file2...); this is needed to enable more aggressive fuzzing of newly added tests (see 'query-fuzzer-runs' option)")
        ("opentelemetry-traceparent", po::value<std::string>(), "OpenTelemetry traceparent header as described by W3C Trace Context recommendation")
        ("opentelemetry-tracestate", po::value<std::string>(), "OpenTelemetry tracestate header as described by W3C Trace Context recommendation")
        ("history_file", po::value<std::string>(), "path to history file")
        ("no-warnings", "disable warnings when client connects to server")
        ("max_memory_usage_in_client", po::value<int>(), "sets memory limit in client")
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

    cmd_settings.addProgramOptions(options_description.main_description.value());
    /// Parse main commandline options.
    po::parsed_options parsed = po::command_line_parser(arguments).options(options_description.main_description.value()).run();
    auto unrecognized_options = po::collect_unrecognized(parsed.options, po::collect_unrecognized_mode::include_positional);
    if (unrecognized_options.size() > 1)
        throw Exception(ErrorCodes::UNRECOGNIZED_ARGUMENTS, "Unrecognized option '{}'", unrecognized_options[1]);
    po::store(parsed, options);
}


void Client::processOptions(const OptionsDescription & options_description,
                            const CommandLineOptions & options,
                            const std::vector<Arguments> & external_tables_arguments)
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
            config().setString(name, options[name].as<String>());
    }

    if (options.count("config-file") && options.count("config"))
        throw Exception("Two or more configuration files referenced in arguments", ErrorCodes::BAD_ARGUMENTS);

    query_processing_stage = QueryProcessingStage::fromString(options["stage"].as<std::string>());

    /// Save received data into the internal config.
    if (options.count("config-file"))
        config().setString("config-file", options["config-file"].as<std::string>());
    if (options.count("config"))
        config().setString("config-file", options["config"].as<std::string>());
    if (options.count("host") && !options["host"].defaulted())
        config().setString("host", options["host"].as<std::string>());
    if (options.count("query_id"))
        config().setString("query_id", options["query_id"].as<std::string>());
    if (options.count("query"))
        config().setString("query", options["query"].as<std::string>());
    if (options.count("queries-file"))
        queries_files = options["queries-file"].as<std::vector<std::string>>();
    if (options.count("interleave-queries-file"))
        interleave_queries_files = options["interleave-queries-file"].as<std::vector<std::string>>();
    if (options.count("database"))
        config().setString("database", options["database"].as<std::string>());
    if (options.count("pager"))
        config().setString("pager", options["pager"].as<std::string>());
    if (options.count("port") && !options["port"].defaulted())
        config().setInt("port", options["port"].as<int>());
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
    if (options.count("multiline"))
        config().setBool("multiline", true);
    if (options.count("multiquery"))
        config().setBool("multiquery", true);
    if (options.count("testmode"))
        config().setBool("testmode", true);
    if (options.count("ignore-error"))
        config().setBool("ignore-error", true);
    if (options.count("format"))
        config().setString("format", options["format"].as<std::string>());
    if (options.count("vertical"))
        config().setBool("vertical", true);
    if (options.count("stacktrace"))
        config().setBool("stacktrace", true);
    if (options.count("progress"))
        config().setBool("progress", true);
    if (options.count("echo"))
        config().setBool("echo", true);
    if (options.count("time"))
        print_time_to_stderr = true;
    if (options.count("max_client_network_bandwidth"))
        max_client_network_bandwidth = options["max_client_network_bandwidth"].as<int>();
    if (options.count("compression"))
        config().setBool("compression", options["compression"].as<bool>());
    if (options.count("server_logs_file"))
        server_logs_file = options["server_logs_file"].as<std::string>();
    if (options.count("disable_suggestion"))
        config().setBool("disable_suggestion", true);
    if (options.count("suggestion_limit"))
        config().setInt("suggestion_limit", options["suggestion_limit"].as<int>());
    if (options.count("highlight"))
        config().setBool("highlight", options["highlight"].as<bool>());
    if (options.count("history_file"))
        config().setString("history_file", options["history_file"].as<std::string>());
    if (options.count("no-warnings"))
        config().setBool("no-warnings", true);

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
    /// - -qf (--queries-file) command line option is present.
    ///   The value of the option is used as file with query (or of multiple queries) to execute.
    if (stdin_is_a_tty && !config().has("query") && queries_files.empty())
    {
        if (config().has("query") && config().has("queries-file"))
            throw Exception("Specify either `query` or `queries-file` option", ErrorCodes::BAD_ARGUMENTS);

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
    catch (const boost::program_options::error & e)
    {
        std::cerr << "Bad arguments: " << e.what() << std::endl;
        return 1;
    }
    catch (const DB::Exception & e)
    {
        std::cerr << DB::getExceptionMessage(e, false) << std::endl;
        return 1;
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << std::endl;
        return 1;
    }
}
