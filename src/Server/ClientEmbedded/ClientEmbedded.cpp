#include "ClientEmbedded.h"

#include <base/getFQDNOrHostName.h>
#include <Interpreters/Session.h>
#include <boost/algorithm/string/replace.hpp>
#include "Common/setThreadName.h"
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

template<typename T>
T getEnvOption(const NameToNameMap & envVars, const String & key, T defaultValue)
{
    auto it = envVars.find(key);
    return it == envVars.end() ? defaultValue : parse<T>(it->second);
}

}


void ClientEmbedded::processError(const String &) const
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
        connection_parameters, std::move(session), need_render_progress, need_render_profile_events, server_display_name);
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


int ClientEmbedded::run(const NameToNameMap & envVars, const String & first_query)
{
try
{
    setThreadName("LocalServerPty");

    query_processing_stage = QueryProcessingStage::Enum::Complete;

    print_stack_trace = getEnvOption<bool>(envVars, "stacktrace", false);

    output_stream << std::fixed << std::setprecision(3);
    error_stream << std::fixed << std::setprecision(3);

    is_interactive = stdin_is_a_tty;
    static_query = first_query.empty() ? getEnvOption<String>(envVars, "query", "") : first_query;
    delayed_interactive = is_interactive && !static_query.empty();
    if (!is_interactive || delayed_interactive)
    {
        echo_queries = getEnvOption<bool>(envVars, "echo", false) || getEnvOption<bool>(envVars, "verbose", false);
        ignore_error = getEnvOption<bool>(envVars, "ignore_error", false);
        is_multiquery = true;
    }
    load_suggestions = (is_interactive || delayed_interactive) && !getEnvOption<bool>(envVars, "disable_suggestion", false);
    if (load_suggestions)
    {
        suggestion_limit = getEnvOption<Int32>(envVars, "suggestion_limit", 10000);
    }


    enable_highlight = getEnvOption<bool>(envVars, "highlight", true);
    multiline = getEnvOption<bool>(envVars, "multiline", false);

    default_database = getEnvOption<String>(envVars, "database", "");

    format = getEnvOption<String>(envVars, "output-format", getEnvOption<String>(envVars, "format", is_interactive ? "PrettyCompact" : "TSV"));
    // TODO: Fix
    // insert_format = "Values";
    insert_format_max_block_size = getEnvOption<size_t>(envVars, "insert_format_max_block_size",
        global_context->getSettingsRef().max_insert_block_size);


    server_display_name = getEnvOption<String>(envVars, "display_name", getFQDNOrHostName());
    prompt_by_server_display_name = getEnvOption<String>(envVars, "prompt_by_server_display_name", "{display_name} :) ");
    std::map<String, String> prompt_substitutions{{"display_name", server_display_name}};
    for (const auto & [key, value] : prompt_substitutions)
        boost::replace_all(prompt_by_server_display_name, "{" + key + "}", value);
    initTTYBuffer(toProgressOption(getEnvOption<String>(envVars, "progress", "default")));

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
    return e.code() ? e.code() : -1;
}
catch (...)
{
    cleanup();

    error_stream << getCurrentExceptionMessage(false) << std::endl;
    return getCurrentExceptionCode();
}
}


}
