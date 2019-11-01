#include <Interpreters/CustomHTTP/HTTPMatchExecutor.h>

#include <ext/scope_guard.h>
#include <Common/Exception.h>
#include <Common/SettingsChanges.h>
#include <Core/ExternalTable.h>
#include <Interpreters/Context.h>
#include <Interpreters/CustomHTTP/HTTPMatchExecutorDefault.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <IO/CascadeWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/WriteBufferFromTemporaryFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/BrotliReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ZlibInflatingReadBuffer.h>
#include <DataStreams/IBlockInputStream.h>
#include <Poco/Net/HTTPServerRequestImpl.h>
#include <Compression/CompressedReadBuffer.h>
#include <Interpreters/CustomHTTP/HTTPStreamsWithInput.h>
#include <Interpreters/CustomHTTP/HTTPStreamsWithOutput.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int REQUIRED_PASSWORD;
    extern const int INVALID_SESSION_TIMEOUT;
    extern const int UNKNOWN_COMPRESSION_METHOD;
}


namespace
{
    duration parseSessionTimeout(const HTMLForm & params, size_t default_session_timeout, size_t max_session_timeout)
    {
        size_t session_timeout = default_session_timeout;

        if (params.has("session_timeout"))
        {
            std::string session_timeout_str = params.get("session_timeout");

            ReadBufferFromString buf(session_timeout_str);
            if (!tryReadIntText(session_timeout, buf) || !buf.eof())
                throw Exception("Invalid session timeout: '" + session_timeout_str + "'", ErrorCodes::INVALID_SESSION_TIMEOUT);

            if (session_timeout > max_session_timeout)
                throw Exception(
                    "Session timeout '" + session_timeout_str + "' is larger than max_session_timeout: " + toString(max_session_timeout) +
                    ". Maximum session timeout could be modified in configuration file.", ErrorCodes::INVALID_SESSION_TIMEOUT);
        }

        return std::chrono::seconds(session_timeout);
    }
}


void HTTPMatchExecutor::execute(Context & context, HTTPServerRequest & request, HTTPServerResponse & response, HTMLForm & params, HTTPStreamsWithOutput & used_output) const
{
    authentication(context, request, params);

    std::shared_ptr<Context> session_context;
    String session_id = params.get("session_id", "");
    duration session_timeout = parseSessionTimeout(params, 1800, 3600);

    SCOPE_EXIT({ detachSessionContext(session_context, session_id, session_timeout); });
    session_context = attachSessionContext(context, params, session_id, session_timeout);

    initClientInfo(context, request);
    used_output.attachRequestAndResponse(context, request, response, params, /* TODO: keep_alive_time_out */ 0);

    HTTPStreamsWithInput used_input(request, params);
    collectParamsAndApplySettings(request, params, context);

    Settings & query_settings = context.getSettingsRef();
    used_input.attachSettings(context, query_settings, request);
    used_output.attachSettings(context, query_settings, request);

    String execute_query_string = getExecuteQuery(params);
    ReadBufferPtr query_in_buffer = std::make_shared<ReadBufferFromString>(execute_query_string);

    ReadBufferPtr in = query_in_buffer;
    if (!needParsePostBody(request, params) || !context.getExternalTables().empty())
        in = std::make_shared<ConcatReadBuffer>(*query_in_buffer, *used_input.in_maybe_internal_compressed);

    executeQuery(*in, *used_output.out_maybe_delayed_and_compressed, /* allow_into_outfile = */ false, context,
        [&response] (const String & content_type) { response.setContentType(content_type); },
        [&response] (const String & current_query_id) { response.add("X-ClickHouse-Query-Id", current_query_id); });

    used_output.finalize();
}

void HTTPMatchExecutor::initClientInfo(Context & context, HTTPServerRequest & request) const
{

    ClientInfo & client_info = context.getClientInfo();
    client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    client_info.interface = ClientInfo::Interface::HTTP;

    /// Query sent through HTTP interface is initial.
    client_info.initial_user = client_info.current_user;
    client_info.initial_query_id = client_info.current_query_id;
    client_info.initial_address = client_info.current_address;

    ClientInfo::HTTPMethod http_method = ClientInfo::HTTPMethod::UNKNOWN;
    if (request.getMethod() == Poco::Net::HTTPServerRequest::HTTP_GET)
        http_method = ClientInfo::HTTPMethod::GET;
    else if (request.getMethod() == Poco::Net::HTTPServerRequest::HTTP_POST)
        http_method = ClientInfo::HTTPMethod::POST;

    client_info.http_method = http_method;
    client_info.http_user_agent = request.get("User-Agent", "");
}

void HTTPMatchExecutor::authentication(Context & context, HTTPServerRequest & request, HTMLForm & params) const
{
    auto user = request.get("X-ClickHouse-User", "");
    auto password = request.get("X-ClickHouse-Key", "");
    auto quota_key = request.get("X-ClickHouse-Quota", "");

    if (user.empty() && password.empty() && quota_key.empty())
    {
        /// User name and password can be passed using query parameters
        /// or using HTTP Basic auth (both methods are insecure).
        if (request.hasCredentials())
        {
            Poco::Net::HTTPBasicCredentials credentials(request);

            user = credentials.getUsername();
            password = credentials.getPassword();
        }
        else
        {
            user = params.get("user", "default");
            password = params.get("password", "");
        }

        quota_key = params.get("quota_key", "");
    }
    else
    {
        /// It is prohibited to mix different authorization schemes.
        if (request.hasCredentials()
            || params.has("user")
            || params.has("password")
            || params.has("quota_key"))
        {
            throw Exception("Invalid authentication: it is not allowed to use X-ClickHouse HTTP headers and other authentication methods simultaneously", ErrorCodes::REQUIRED_PASSWORD);
        }
    }

    std::string query_id = params.get("query_id", "");
    context.setUser(user, password, request.clientAddress(), quota_key);
    context.setCurrentQueryId(query_id);
}

std::shared_ptr<Context> HTTPMatchExecutor::attachSessionContext(
    Context & context, HTMLForm & params, const String & session_id, const duration & session_timeout) const
{
    if (!session_id.empty())
    {
        std::string session_check = params.get("session_check", "");
        auto session_context = context.acquireSession(session_id, session_timeout, session_check == "1");

        context = *session_context;
        context.setSessionContext(*session_context);
        return session_context;
    }
    return {};
}

void HTTPMatchExecutor::detachSessionContext(std::shared_ptr<Context> & session_context, const String & session_id, const duration & session_timeout) const
{
    if (session_context)
        session_context->releaseSession(session_id, session_timeout);
}

void HTTPMatchExecutor::collectParamsAndApplySettings(HTTPServerRequest & request, HTMLForm & params, Context & context) const
{
    static const NameSet reserved_param_names{
        "compress", "decompress", "user", "password", "quota_key", "query_id", "stacktrace", "buffer_size", "wait_end_of_query",
        "session_id", "session_timeout", "session_check"};

    Names reserved_param_suffixes;

    auto param_could_be_skipped = [&] (const String & name)
    {
        if (reserved_param_names.count(name))
            return true;

        for (const String & suffix : reserved_param_suffixes)
        {
            if (endsWith(name, suffix))
                return true;
        }

        return false;
    };

    /// Settings can be overridden in the query.
    /// Some parameters (database, default_format, everything used in the code above) do not
    /// belong to the Settings class.

    /// 'readonly' setting values mean:
    /// readonly = 0 - any query is allowed, client can change any setting.
    /// readonly = 1 - only readonly queries are allowed, client can't change settings.
    /// readonly = 2 - only readonly queries are allowed, client can change any setting except 'readonly'.

    /// In theory if initially readonly = 0, the client can change any setting and then set readonly
    /// to some other value.
    /// Only readonly queries are allowed for HTTP GET requests.
    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
    {
        Settings & settings = context.getSettingsRef();

        if (settings.readonly == 0)
            settings.readonly = 2;
    }

    bool has_multipart = startsWith(request.getContentType().data(), "multipart/form-data");

    if (has_multipart || needParsePostBody(request, params))
    {
        ExternalTablesHandler handler(context, params);
        params.load(request, request.stream(), handler);

        if (has_multipart)
        {
            /// Skip unneeded parameters to avoid confusing them later with context settings or query parameters.
            reserved_param_suffixes.reserve(3);
            /// It is a bug and ambiguity with `date_time_input_format` and `low_cardinality_allow_in_native_format` formats/settings.
            reserved_param_suffixes.emplace_back("_format");
            reserved_param_suffixes.emplace_back("_types");
            reserved_param_suffixes.emplace_back("_structure");
        }
    }

    SettingsChanges settings_changes;
    for (const auto & [key, value] : params)
    {
        if (key == "database")
            context.setCurrentDatabase(value);
        else if (key == "default_format")
            context.setDefaultFormat(value);
        else if (!param_could_be_skipped(key) && !acceptQueryParam(context, key, value))
            settings_changes.push_back({key, value}); /// All other query parameters are treated as settings.
    }

    /// For external data we also want settings
    context.checkSettingsConstraints(settings_changes);
    context.applySettingsChanges(settings_changes);
}

}
