#include "WebTerminalSession.h"

#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Common/SettingsChanges.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_USER;
}

WebTerminalSession::WebTerminalSession(Context & context_)
    : context(context_)
{
}

void WebTerminalSession::applySettings(const HTMLForm & params)
{
    SettingsChanges settings_changes;
    for (const auto & [setting_name, setting_value] : params)
        settings_changes.push_back({setting_name, setting_value});

    context.checkSettingsConstraints(settings_changes);
    context.applySettingsChanges(settings_changes);
}

void WebTerminalSession::verifyingAuthenticatedToken(const HTMLForm & params)
{
    std::unique_lock<std::mutex> lock(mutex);

    const auto & client_authenticated_token = params.get("authenticated_token");

    if (!authenticated_token || client_authenticated_token != *authenticated_token)
        throw Exception("Currently querying with wrong token.", ErrorCodes::UNKNOWN_USER);
}

static inline auto makeResponseWriteBuffer(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, Settings & settings)
{
    /// The client can pass a HTTP header indicating supported compression method (gzip or deflate).
    String http_response_compression_methods = request.get("Accept-Encoding", "");
    CompressionMethod http_response_compression_method = CompressionMethod::None;

    if (!http_response_compression_methods.empty())
    {
        /// If client supports brotli - it's preferred.
        /// Both gzip and deflate are supported. If the client supports both, gzip is preferred.
        /// NOTE parsing of the list of methods is slightly incorrect.

        if (std::string::npos != http_response_compression_methods.find("br"))
            http_response_compression_method = CompressionMethod::Brotli;
        else if (std::string::npos != http_response_compression_methods.find("gzip"))
            http_response_compression_method = CompressionMethod::Gzip;
        else if (std::string::npos != http_response_compression_methods.find("deflate"))
            http_response_compression_method = CompressionMethod::Zlib;
    }

    /// TODO:
    unsigned keep_alive_timeout = 10; //configuration.getUInt("web_console.keep_alive_timeout", 10);
    bool has_compress_client = http_response_compression_method != CompressionMethod::None;
    auto output = std::make_shared<WriteBufferFromHTTPServerResponse>(
        request, response, keep_alive_timeout, has_compress_client, http_response_compression_method, false);

    output->addHeaderCORS(settings.add_http_cors_header && !request.get("Origin", "").empty());
    output->setCompression(has_compress_client && settings.enable_http_compression);

    if (has_compress_client)
        output->setCompressionLevel(settings.http_zlib_compression_level);

    return output;
}

void WebTerminalSession::login(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, const HTMLForm & params)
{
    std::unique_lock<std::mutex> lock(mutex);
    const auto & user_name = params.get("user_name");
    const auto & user_password = params.get("user_password", "");

    context.makeSessionContext();
    CurrentThread::QueryScope query_scope(context);
    /// TODO: maybe we should also get quota_key from the web console configuration file
    context.setUser(user_name, user_password, request.clientAddress(), params.get("quota_key", ""));
    const auto & response_buffer = makeResponseWriteBuffer(request, response, context.getSettingsRef());

    authenticated_token.emplace(generateRandomString(6));
    *response_buffer << "{" << DB::double_quote << "type" << ":" << DB::double_quote << "login"
        << "," << DB::double_quote << "authenticated_token" << ":" << DB::double_quote << *authenticated_token << "}";
}

void WebTerminalSession::output(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, const HTMLForm & params)
{
    verifyingAuthenticatedToken(params);

    const auto & query_id = params.get("running_query_id");
    const auto & set_progress_header = [&] (const ProgressValues & progress_values, size_t rows_in_set)
    {
        WriteBufferFromOwnString out;
        out << "{" << DB::double_quote << "read_rows" << ":" << DB::double_quote << progress_values.read_rows
            << "," << DB::double_quote << "read_bytes" << ":" << DB::double_quote << progress_values.read_bytes
            << "," << DB::double_quote << "rows_in_set" << ":" << DB::double_quote << rows_in_set
            << "," << DB::double_quote << "total_rows_to_read" << ":" << DB::double_quote << progress_values.total_rows_to_read << "}";
        response.set("X-ClickHouse-Progress", out.str());
    };

    auto session_query = session_queries.get(query_id, "Not found query to pull output, query id:" + query_id);

    const auto & response_buffer = makeResponseWriteBuffer(request, response, context.getSettingsRef());
    if (session_query->getOutput(response_buffer, set_progress_header) && !response.sent())
    {
        session_queries.erase(query_id);
        response.setStatusAndReason(Poco::Net::HTTPServerResponse::HTTP_RESET_CONTENT);
    }
}

void WebTerminalSession::cancelQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, const HTMLForm & params)
{
    verifyingAuthenticatedToken(params);

    const auto & query_id = params.get("running_query_id");
    auto session_query = session_queries.get(query_id, "Not found query to cancel, query id:" + query_id);
    session_query->cancelQuery();

    const auto & response_buffer = makeResponseWriteBuffer(request, response, context.getSettingsRef());
    *response_buffer << "{" << DB::double_quote << "type" << ":" << DB::double_quote << "cancel_query"
        << "," << DB::double_quote << "successfully" << ":" << "true" << "}";
}

void WebTerminalSession::executeQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, const HTMLForm & params)
{
    verifyingAuthenticatedToken(params);

    const auto & execute_query = params.get("execute_query");
    const auto & vertical_output = params.get("has_vertical_output_suffix", "0") == "1";

    /// TODO: 0 is unlimit
    const auto & current_query_id = generateRandomString(16);
    size_t max_execution_time = context.getConfigRef().getUInt64("web_terminal.max_execution_time", 300);
    max_execution_time = std::max(max_execution_time, size_t(context.getSettingsRef().max_execution_time.totalSeconds()));

    const auto & current_query_executor = session_queries.emplace(
        current_query_id, std::chrono::seconds(max_execution_time), context, execute_query, current_query_id, vertical_output);

    current_query_executor->getEchoQuery(makeResponseWriteBuffer(request, response, context.getSettingsRef()));
}

void WebTerminalSession::configuration(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, const HTMLForm &)
{
    const auto & config = context.getConfigRef();
    const auto & response_buffer = makeResponseWriteBuffer(request, response, context.getSettingsRef());

    *response_buffer << "{" << DB::double_quote << "title" << ":" << DB::double_quote << config.getString("web_terminal.title")
         << "," << DB::double_quote << "prompt" << ":" << DB::double_quote << config.getString("web_terminal.prompt")
         << "," << DB::double_quote << "greetings" << ":" << DB::double_quote << config.getString("web_terminal.greetings") << "}";
}

String generateRandomString(size_t size)
{
    static const char generate_chars[62] = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
        'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
        'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g',
        'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
        's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

    srand(time(nullptr));
    WriteBufferFromOwnString buffer;

    for (size_t index = 0; index < size; ++index)
        writeChar(generate_chars[rand() % 62], buffer);

    return buffer.str();
}

ExpireUnorderedMap<String, WebTerminalSession> & getWebTerminalSessions()
{
    static ExpireUnorderedMap<String, WebTerminalSession> instance;
    return instance;
}

}
