#include <Server/HTTPHandler.h>

#include <Access/Authentication.h>
#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/ExternalTable.h>
#include <DataStreams/IBlockInputStream.h>
#include <Disks/StoragePolicy.h>
#include <IO/CascadeWriteBuffer.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromTemporaryFile.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryParameterVisitor.h>
#include <Interpreters/executeQuery.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/HTTPHandlerRequestFilter.h>
#include <Server/IServer.h>
#include <Common/SettingsChanges.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <common/getFQDNOrHostName.h>
#include <common/scope_guard.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#include <Poco/Base64Decoder.h>
#include <Poco/Base64Encoder.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPStream.h>
#include <Poco/Net/NetException.h>
#include <Poco/MemoryStream.h>
#include <Poco/StreamCopier.h>
#include <Poco/String.h>

#include <chrono>
#include <iomanip>
#include <sstream>


namespace DB
{

namespace ErrorCodes
{

    extern const int LOGICAL_ERROR;
    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_COMPILE_REGEXP;

    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
    extern const int TOO_DEEP_AST;
    extern const int TOO_BIG_AST;
    extern const int UNEXPECTED_AST_STRUCTURE;

    extern const int SYNTAX_ERROR;

    extern const int INCORRECT_DATA;
    extern const int TYPE_MISMATCH;

    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_FUNCTION;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNKNOWN_TYPE;
    extern const int UNKNOWN_STORAGE;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_SETTING;
    extern const int UNKNOWN_DIRECTION_OF_SORTING;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int UNKNOWN_FORMAT;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int UNKNOWN_TYPE_OF_QUERY;
    extern const int NO_ELEMENTS_IN_CONFIG;

    extern const int QUERY_IS_TOO_LARGE;

    extern const int NOT_IMPLEMENTED;
    extern const int SOCKET_TIMEOUT;

    extern const int UNKNOWN_USER;
    extern const int WRONG_PASSWORD;
    extern const int REQUIRED_PASSWORD;
    extern const int AUTHENTICATION_FAILED;

    extern const int BAD_REQUEST_PARAMETER;
    extern const int INVALID_SESSION_TIMEOUT;
    extern const int HTTP_LENGTH_REQUIRED;
}

static String base64Decode(const String & encoded)
{
    String decoded;
    Poco::MemoryInputStream istr(encoded.data(), encoded.size());
    Poco::Base64Decoder decoder(istr);
    Poco::StreamCopier::copyToString(decoder, decoded);
    return decoded;
}

static String base64Encode(const String & decoded)
{
    std::ostringstream ostr; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ostr.exceptions(std::ios::failbit);
    Poco::Base64Encoder encoder(ostr);
    encoder.rdbuf()->setLineLength(0);
    encoder << decoded;
    encoder.close();
    return ostr.str();
}

static Poco::Net::HTTPResponse::HTTPStatus exceptionCodeToHTTPStatus(int exception_code)
{
    using namespace Poco::Net;

    if (exception_code == ErrorCodes::REQUIRED_PASSWORD)
    {
        return HTTPResponse::HTTP_UNAUTHORIZED;
    }
    else if (exception_code == ErrorCodes::UNKNOWN_USER ||
             exception_code == ErrorCodes::WRONG_PASSWORD ||
             exception_code == ErrorCodes::AUTHENTICATION_FAILED)
    {
        return HTTPResponse::HTTP_FORBIDDEN;
    }
    else if (exception_code == ErrorCodes::CANNOT_PARSE_TEXT ||
             exception_code == ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE ||
             exception_code == ErrorCodes::CANNOT_PARSE_QUOTED_STRING ||
             exception_code == ErrorCodes::CANNOT_PARSE_DATE ||
             exception_code == ErrorCodes::CANNOT_PARSE_DATETIME ||
             exception_code == ErrorCodes::CANNOT_PARSE_NUMBER ||
             exception_code == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED ||
             exception_code == ErrorCodes::UNKNOWN_ELEMENT_IN_AST ||
             exception_code == ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE ||
             exception_code == ErrorCodes::TOO_DEEP_AST ||
             exception_code == ErrorCodes::TOO_BIG_AST ||
             exception_code == ErrorCodes::UNEXPECTED_AST_STRUCTURE ||
             exception_code == ErrorCodes::SYNTAX_ERROR ||
             exception_code == ErrorCodes::INCORRECT_DATA ||
             exception_code == ErrorCodes::TYPE_MISMATCH)
    {
        return HTTPResponse::HTTP_BAD_REQUEST;
    }
    else if (exception_code == ErrorCodes::UNKNOWN_TABLE ||
             exception_code == ErrorCodes::UNKNOWN_FUNCTION ||
             exception_code == ErrorCodes::UNKNOWN_IDENTIFIER ||
             exception_code == ErrorCodes::UNKNOWN_TYPE ||
             exception_code == ErrorCodes::UNKNOWN_STORAGE ||
             exception_code == ErrorCodes::UNKNOWN_DATABASE ||
             exception_code == ErrorCodes::UNKNOWN_SETTING ||
             exception_code == ErrorCodes::UNKNOWN_DIRECTION_OF_SORTING ||
             exception_code == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION ||
             exception_code == ErrorCodes::UNKNOWN_FORMAT ||
             exception_code == ErrorCodes::UNKNOWN_DATABASE_ENGINE ||
             exception_code == ErrorCodes::UNKNOWN_TYPE_OF_QUERY)
    {
        return HTTPResponse::HTTP_NOT_FOUND;
    }
    else if (exception_code == ErrorCodes::QUERY_IS_TOO_LARGE)
    {
        return HTTPResponse::HTTP_REQUESTENTITYTOOLARGE;
    }
    else if (exception_code == ErrorCodes::NOT_IMPLEMENTED)
    {
        return HTTPResponse::HTTP_NOT_IMPLEMENTED;
    }
    else if (exception_code == ErrorCodes::SOCKET_TIMEOUT ||
             exception_code == ErrorCodes::CANNOT_OPEN_FILE)
    {
        return HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
    }
    else if (exception_code == ErrorCodes::HTTP_LENGTH_REQUIRED)
    {
        return HTTPResponse::HTTP_LENGTH_REQUIRED;
    }

    return HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
}


static std::chrono::steady_clock::duration parseSessionTimeout(
    const Poco::Util::AbstractConfiguration & config,
    const HTMLForm & params)
{
    unsigned session_timeout = config.getInt("default_session_timeout", 60);

    if (params.has("session_timeout"))
    {
        unsigned max_session_timeout = config.getUInt("max_session_timeout", 3600);
        std::string session_timeout_str = params.get("session_timeout");

        ReadBufferFromString buf(session_timeout_str);
        if (!tryReadIntText(session_timeout, buf) || !buf.eof())
            throw Exception("Invalid session timeout: '" + session_timeout_str + "'", ErrorCodes::INVALID_SESSION_TIMEOUT);

        if (session_timeout > max_session_timeout)
            throw Exception("Session timeout '" + session_timeout_str + "' is larger than max_session_timeout: " + toString(max_session_timeout)
                + ". Maximum session timeout could be modified in configuration file.",
                ErrorCodes::INVALID_SESSION_TIMEOUT);
    }

    return std::chrono::seconds(session_timeout);
}


void HTTPHandler::pushDelayedResults(Output & used_output)
{
    std::vector<WriteBufferPtr> write_buffers;
    std::vector<ReadBufferPtr> read_buffers;
    std::vector<ReadBuffer *> read_buffers_raw_ptr;

    auto * cascade_buffer = typeid_cast<CascadeWriteBuffer *>(used_output.out_maybe_delayed_and_compressed.get());
    if (!cascade_buffer)
        throw Exception("Expected CascadeWriteBuffer", ErrorCodes::LOGICAL_ERROR);

    cascade_buffer->getResultBuffers(write_buffers);

    if (write_buffers.empty())
        throw Exception("At least one buffer is expected to overwrite result into HTTP response", ErrorCodes::LOGICAL_ERROR);

    for (auto & write_buf : write_buffers)
    {
        IReadableWriteBuffer * write_buf_concrete;
        ReadBufferPtr reread_buf;

        if (write_buf
            && (write_buf_concrete = dynamic_cast<IReadableWriteBuffer *>(write_buf.get()))
            && (reread_buf = write_buf_concrete->tryGetReadBuffer()))
        {
            read_buffers.emplace_back(reread_buf);
            read_buffers_raw_ptr.emplace_back(reread_buf.get());
        }
    }

    if (!read_buffers_raw_ptr.empty())
    {
        ConcatReadBuffer concat_read_buffer(read_buffers_raw_ptr);
        copyData(concat_read_buffer, *used_output.out_maybe_compressed);
    }
}


HTTPHandler::HTTPHandler(IServer & server_, const std::string & name)
    : server(server_)
    , log(&Poco::Logger::get(name))
{
    server_display_name = server.config().getString("display_name", getFQDNOrHostName());
}


/// We need d-tor to be present in this translation unit to make it play well with some
/// forward decls in the header. Other than that, the default d-tor would be OK.
HTTPHandler::~HTTPHandler()
{
    (void)this;
}


bool HTTPHandler::authenticateUser(
    ContextMutablePtr context,
    HTTPServerRequest & request,
    HTMLForm & params,
    HTTPServerResponse & response)
{
    using namespace Poco::Net;

    /// The user and password can be passed by headers (similar to X-Auth-*),
    /// which is used by load balancers to pass authentication information.
    std::string user = request.get("X-ClickHouse-User", "");
    std::string password = request.get("X-ClickHouse-Key", "");
    std::string quota_key = request.get("X-ClickHouse-Quota", "");

    std::string spnego_challenge;

    if (user.empty() && password.empty() && quota_key.empty())
    {
        /// User name and password can be passed using query parameters
        /// or using HTTP Basic auth (both methods are insecure).
        if (request.hasCredentials())
        {
            /// It is prohibited to mix different authorization schemes.
            if (params.has("user") || params.has("password"))
                throw Exception("Invalid authentication: it is not allowed to use Authorization HTTP header and authentication via parameters simultaneously", ErrorCodes::AUTHENTICATION_FAILED);

            std::string scheme;
            std::string auth_info;
            request.getCredentials(scheme, auth_info);

            if (Poco::icompare(scheme, "Basic") == 0)
            {
                HTTPBasicCredentials credentials(auth_info);
                user = credentials.getUsername();
                password = credentials.getPassword();
            }
            else if (Poco::icompare(scheme, "Negotiate") == 0)
            {
                spnego_challenge = auth_info;

                if (spnego_challenge.empty())
                    throw Exception("Invalid authentication: SPNEGO challenge is empty", ErrorCodes::AUTHENTICATION_FAILED);
            }
            else
            {
                throw Exception("Invalid authentication: '" + scheme + "' HTTP Authorization scheme is not supported", ErrorCodes::AUTHENTICATION_FAILED);
            }
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
        if (request.hasCredentials() || params.has("user") || params.has("password") || params.has("quota_key"))
            throw Exception("Invalid authentication: it is not allowed to use X-ClickHouse HTTP headers and other authentication methods simultaneously", ErrorCodes::AUTHENTICATION_FAILED);
    }

    if (spnego_challenge.empty()) // I.e., now using user name and password strings ("Basic").
    {
        if (!request_credentials)
            request_credentials = std::make_unique<BasicCredentials>();

        auto * basic_credentials = dynamic_cast<BasicCredentials *>(request_credentials.get());
        if (!basic_credentials)
            throw Exception("Invalid authentication: unexpected 'Basic' HTTP Authorization scheme", ErrorCodes::AUTHENTICATION_FAILED);

        basic_credentials->setUserName(user);
        basic_credentials->setPassword(password);
    }
    else
    {
        if (!request_credentials)
            request_credentials = request_context->makeGSSAcceptorContext();

        auto * gss_acceptor_context = dynamic_cast<GSSAcceptorContext *>(request_credentials.get());
        if (!gss_acceptor_context)
            throw Exception("Invalid authentication: unexpected 'Negotiate' HTTP Authorization scheme expected", ErrorCodes::AUTHENTICATION_FAILED);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunreachable-code"
        const auto spnego_response = base64Encode(gss_acceptor_context->processToken(base64Decode(spnego_challenge), log));
#pragma GCC diagnostic pop

        if (!spnego_response.empty())
            response.set("WWW-Authenticate", "Negotiate " + spnego_response);

        if (!gss_acceptor_context->isFailed() && !gss_acceptor_context->isReady())
        {
            if (spnego_response.empty())
                throw Exception("Invalid authentication: 'Negotiate' HTTP Authorization failure", ErrorCodes::AUTHENTICATION_FAILED);

            response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
            response.send();
            return false;
        }
    }

    /// Set client info. It will be used for quota accounting parameters in 'setUser' method.

    ClientInfo & client_info = context->getClientInfo();
    client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    client_info.interface = ClientInfo::Interface::HTTP;

    ClientInfo::HTTPMethod http_method = ClientInfo::HTTPMethod::UNKNOWN;
    if (request.getMethod() == HTTPServerRequest::HTTP_GET)
        http_method = ClientInfo::HTTPMethod::GET;
    else if (request.getMethod() == HTTPServerRequest::HTTP_POST)
        http_method = ClientInfo::HTTPMethod::POST;

    client_info.http_method = http_method;
    client_info.http_user_agent = request.get("User-Agent", "");
    client_info.http_referer = request.get("Referer", "");
    client_info.forwarded_for = request.get("X-Forwarded-For", "");

    try
    {
        context->setUser(*request_credentials, request.clientAddress());
    }
    catch (const Authentication::Require<BasicCredentials> & required_credentials)
    {
        request_credentials = std::make_unique<BasicCredentials>();

        if (required_credentials.getRealm().empty())
            response.set("WWW-Authenticate", "Basic");
        else
            response.set("WWW-Authenticate", "Basic realm=\"" + required_credentials.getRealm() + "\"");

        response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
        response.send();
        return false;
    }
    catch (const Authentication::Require<GSSAcceptorContext> & required_credentials)
    {
        request_credentials = request_context->makeGSSAcceptorContext();

        if (required_credentials.getRealm().empty())
            response.set("WWW-Authenticate", "Negotiate");
        else
            response.set("WWW-Authenticate", "Negotiate realm=\"" + required_credentials.getRealm() + "\"");

        response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
        response.send();
        return false;
    }

    request_credentials.reset();

    if (!quota_key.empty())
        context->setQuotaKey(quota_key);

    /// Query sent through HTTP interface is initial.
    client_info.initial_user = client_info.current_user;
    client_info.initial_address = client_info.current_address;

    return true;
}


void HTTPHandler::processQuery(
    ContextMutablePtr context,
    HTTPServerRequest & request,
    HTMLForm & params,
    HTTPServerResponse & response,
    Output & used_output,
    std::optional<CurrentThread::QueryScope> & query_scope)
{
    using namespace Poco::Net;

    LOG_TRACE(log, "Request URI: {}", request.getURI());

    if (!authenticateUser(context, request, params, response))
        return; // '401 Unauthorized' response with 'Negotiate' has been sent at this point.

    /// The user could specify session identifier and session timeout.
    /// It allows to modify settings, create temporary tables and reuse them in subsequent requests.

    std::shared_ptr<NamedSession> session;
    String session_id;
    std::chrono::steady_clock::duration session_timeout;
    bool session_is_set = params.has("session_id");
    const auto & config = server.config();

    if (session_is_set)
    {
        session_id = params.get("session_id");
        session_timeout = parseSessionTimeout(config, params);
        std::string session_check = params.get("session_check", "");

        session = context->acquireNamedSession(session_id, session_timeout, session_check == "1");

        context->copyFrom(session->context);  /// FIXME: maybe move this part to HandleRequest(), copyFrom() is used only here.
        context->setSessionContext(session->context);
    }

    SCOPE_EXIT({
        if (session)
            session->release();
    });

    // Parse the OpenTelemetry traceparent header.
    // Disable in Arcadia -- it interferes with the
    // test_clickhouse.TestTracing.test_tracing_via_http_proxy[traceparent] test.
#if !defined(ARCADIA_BUILD)
    if (request.has("traceparent"))
    {
        std::string opentelemetry_traceparent = request.get("traceparent");
        std::string error;
        if (!context->getClientInfo().client_trace_context.parseTraceparentHeader(
            opentelemetry_traceparent, error))
        {
            throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER,
                "Failed to parse OpenTelemetry traceparent header '{}': {}",
                opentelemetry_traceparent, error);
        }

        context->getClientInfo().client_trace_context.tracestate = request.get("tracestate", "");
    }
#endif

    // Set the query id supplied by the user, if any, and also update the OpenTelemetry fields.
    context->setCurrentQueryId(params.get("query_id", request.get("X-ClickHouse-Query-Id", "")));

    ClientInfo & client_info = context->getClientInfo();
    client_info.initial_query_id = client_info.current_query_id;

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
        else if (std::string::npos != http_response_compression_methods.find("xz"))
            http_response_compression_method = CompressionMethod::Xz;
        else if (std::string::npos != http_response_compression_methods.find("zstd"))
            http_response_compression_method = CompressionMethod::Zstd;
    }

    bool client_supports_http_compression = http_response_compression_method != CompressionMethod::None;

    /// Client can pass a 'compress' flag in the query string. In this case the query result is
    /// compressed using internal algorithm. This is not reflected in HTTP headers.
    bool internal_compression = params.getParsed<bool>("compress", false);

    /// At least, we should postpone sending of first buffer_size result bytes
    size_t buffer_size_total = std::max(
        params.getParsed<size_t>("buffer_size", DBMS_DEFAULT_BUFFER_SIZE), static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE));

    /// If it is specified, the whole result will be buffered.
    ///  First ~buffer_size bytes will be buffered in memory, the remaining bytes will be stored in temporary file.
    bool buffer_until_eof = params.getParsed<bool>("wait_end_of_query", false);

    size_t buffer_size_http = DBMS_DEFAULT_BUFFER_SIZE;
    size_t buffer_size_memory = (buffer_size_total > buffer_size_http) ? buffer_size_total : 0;

    unsigned keep_alive_timeout = config.getUInt("keep_alive_timeout", 10);

    used_output.out = std::make_shared<WriteBufferFromHTTPServerResponse>(
        response,
        request.getMethod() == HTTPRequest::HTTP_HEAD,
        keep_alive_timeout,
        client_supports_http_compression,
        http_response_compression_method);

    if (internal_compression)
        used_output.out_maybe_compressed = std::make_shared<CompressedWriteBuffer>(*used_output.out);
    else
        used_output.out_maybe_compressed = used_output.out;

    if (buffer_size_memory > 0 || buffer_until_eof)
    {
        CascadeWriteBuffer::WriteBufferPtrs cascade_buffer1;
        CascadeWriteBuffer::WriteBufferConstructors cascade_buffer2;

        if (buffer_size_memory > 0)
            cascade_buffer1.emplace_back(std::make_shared<MemoryWriteBuffer>(buffer_size_memory));

        if (buffer_until_eof)
        {
            const std::string tmp_path(context->getTemporaryVolume()->getDisk()->getPath());
            const std::string tmp_path_template(tmp_path + "http_buffers/");

            auto create_tmp_disk_buffer = [tmp_path_template] (const WriteBufferPtr &)
            {
                return WriteBufferFromTemporaryFile::create(tmp_path_template);
            };

            cascade_buffer2.emplace_back(std::move(create_tmp_disk_buffer));
        }
        else
        {
            auto push_memory_buffer_and_continue = [next_buffer = used_output.out_maybe_compressed] (const WriteBufferPtr & prev_buf)
            {
                auto * prev_memory_buffer = typeid_cast<MemoryWriteBuffer *>(prev_buf.get());
                if (!prev_memory_buffer)
                    throw Exception("Expected MemoryWriteBuffer", ErrorCodes::LOGICAL_ERROR);

                auto rdbuf = prev_memory_buffer->tryGetReadBuffer();
                copyData(*rdbuf , *next_buffer);

                return next_buffer;
            };

            cascade_buffer2.emplace_back(push_memory_buffer_and_continue);
        }

        used_output.out_maybe_delayed_and_compressed = std::make_shared<CascadeWriteBuffer>(
            std::move(cascade_buffer1), std::move(cascade_buffer2));
    }
    else
    {
        used_output.out_maybe_delayed_and_compressed = used_output.out_maybe_compressed;
    }

    /// Request body can be compressed using algorithm specified in the Content-Encoding header.
    String http_request_compression_method_str = request.get("Content-Encoding", "");
    auto in_post = wrapReadBufferWithCompressionMethod(
        wrapReadBufferReference(request.getStream()), chooseCompressionMethod({}, http_request_compression_method_str));

    /// The data can also be compressed using incompatible internal algorithm. This is indicated by
    /// 'decompress' query parameter.
    std::unique_ptr<ReadBuffer> in_post_maybe_compressed;
    bool in_post_compressed = false;
    if (params.getParsed<bool>("decompress", false))
    {
        in_post_maybe_compressed = std::make_unique<CompressedReadBuffer>(*in_post);
        in_post_compressed = true;
    }
    else
        in_post_maybe_compressed = std::move(in_post);

    std::unique_ptr<ReadBuffer> in;

    static const NameSet reserved_param_names{"compress", "decompress", "user", "password", "quota_key", "query_id", "stacktrace",
        "buffer_size", "wait_end_of_query", "session_id", "session_timeout", "session_check"};

    Names reserved_param_suffixes;

    auto param_could_be_skipped = [&] (const String & name)
    {
        /// Empty parameter appears when URL like ?&a=b or a=b&&c=d. Just skip them for user's convenience.
        if (name.empty())
            return true;

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
    const auto & settings = context->getSettingsRef();

    /// Only readonly queries are allowed for HTTP GET requests.
    if (request.getMethod() == HTTPServerRequest::HTTP_GET)
    {
        if (settings.readonly == 0)
            context->setSetting("readonly", 2);
    }

    bool has_external_data = startsWith(request.getContentType(), "multipart/form-data");

    if (has_external_data)
    {
        /// Skip unneeded parameters to avoid confusing them later with context settings or query parameters.
        reserved_param_suffixes.reserve(3);
        /// It is a bug and ambiguity with `date_time_input_format` and `low_cardinality_allow_in_native_format` formats/settings.
        reserved_param_suffixes.emplace_back("_format");
        reserved_param_suffixes.emplace_back("_types");
        reserved_param_suffixes.emplace_back("_structure");
    }

    std::string database = request.get("X-ClickHouse-Database", "");
    std::string default_format = request.get("X-ClickHouse-Format", "");

    SettingsChanges settings_changes;
    for (const auto & [key, value] : params)
    {
        if (key == "database")
        {
            if (database.empty())
                database = value;
        }
        else if (key == "default_format")
        {
            if (default_format.empty())
                default_format = value;
        }
        else if (param_could_be_skipped(key))
        {
        }
        else
        {
            /// Other than query parameters are treated as settings.
            if (!customizeQueryParam(context, key, value))
                settings_changes.push_back({key, value});
        }
    }

    if (!database.empty())
        context->setCurrentDatabase(database);

    if (!default_format.empty())
        context->setDefaultFormat(default_format);

    /// For external data we also want settings
    context->checkSettingsConstraints(settings_changes);
    context->applySettingsChanges(settings_changes);

    const auto & query = getQuery(request, params, context);
    std::unique_ptr<ReadBuffer> in_param = std::make_unique<ReadBufferFromString>(query);
    in = has_external_data ? std::move(in_param) : std::make_unique<ConcatReadBuffer>(*in_param, *in_post_maybe_compressed);

    /// HTTP response compression is turned on only if the client signalled that they support it
    /// (using Accept-Encoding header) and 'enable_http_compression' setting is turned on.
    used_output.out->setCompression(client_supports_http_compression && settings.enable_http_compression);
    if (client_supports_http_compression)
        used_output.out->setCompressionLevel(settings.http_zlib_compression_level);

    used_output.out->setSendProgressInterval(settings.http_headers_progress_interval_ms);

    /// If 'http_native_compression_disable_checksumming_on_decompress' setting is turned on,
    /// checksums of client data compressed with internal algorithm are not checked.
    if (in_post_compressed && settings.http_native_compression_disable_checksumming_on_decompress)
        static_cast<CompressedReadBuffer &>(*in_post_maybe_compressed).disableChecksumming();

    /// Add CORS header if 'add_http_cors_header' setting is turned on and the client passed
    /// Origin header.
    used_output.out->addHeaderCORS(settings.add_http_cors_header && !request.get("Origin", "").empty());

    auto append_callback = [context] (ProgressCallback callback)
    {
        auto prev = context->getProgressCallback();

        context->setProgressCallback([prev, callback] (const Progress & progress)
        {
            if (prev)
                prev(progress);

            callback(progress);
        });
    };

    /// While still no data has been sent, we will report about query execution progress by sending HTTP headers.
    if (settings.send_progress_in_http_headers)
        append_callback([&used_output] (const Progress & progress) { used_output.out->onProgress(progress); });

    if (settings.readonly > 0 && settings.cancel_http_readonly_queries_on_client_close)
    {
        append_callback([context, &request](const Progress &)
        {
            /// Assume that at the point this method is called no one is reading data from the socket any more:
            /// should be true for read-only queries.
            if (!request.checkPeerConnected())
                context->killCurrentQuery();
        });
    }

    customizeContext(request, context);

    query_scope.emplace(context);

    executeQuery(*in, *used_output.out_maybe_delayed_and_compressed, /* allow_into_outfile = */ false, context,
        [&response] (const String & current_query_id, const String & content_type, const String & format, const String & timezone)
        {
            response.setContentType(content_type);
            response.add("X-ClickHouse-Query-Id", current_query_id);
            response.add("X-ClickHouse-Format", format);
            response.add("X-ClickHouse-Timezone", timezone);
        }
    );

    if (used_output.hasDelayed())
    {
        /// TODO: set Content-Length if possible
        pushDelayedResults(used_output);
    }

    /// Send HTTP headers with code 200 if no exception happened and the data is still not sent to
    /// the client.
    used_output.out->finalize();
}

void HTTPHandler::trySendExceptionToClient(
    const std::string & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response, Output & used_output)
{
    try
    {
        response.set("X-ClickHouse-Exception-Code", toString<int>(exception_code));

        /// FIXME: make sure that no one else is reading from the same stream at the moment.

        /// If HTTP method is POST and Keep-Alive is turned on, we should read the whole request body
        /// to avoid reading part of the current request body in the next request.
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST
            && response.getKeepAlive()
            && exception_code != ErrorCodes::HTTP_LENGTH_REQUIRED
            && !request.getStream().eof())
        {
            request.getStream().ignoreAll();
        }

        if (exception_code == ErrorCodes::REQUIRED_PASSWORD)
        {
            response.requireAuthentication("ClickHouse server HTTP API");
        }
        else
        {
            response.setStatusAndReason(exceptionCodeToHTTPStatus(exception_code));
        }

        if (!response.sent() && !used_output.out_maybe_compressed)
        {
            /// If nothing was sent yet and we don't even know if we must compress the response.
            *response.send() << s << std::endl;
        }
        else if (used_output.out_maybe_compressed)
        {
            /// Destroy CascadeBuffer to actualize buffers' positions and reset extra references
            if (used_output.hasDelayed())
                used_output.out_maybe_delayed_and_compressed.reset();

            /// Send the error message into already used (and possibly compressed) stream.
            /// Note that the error message will possibly be sent after some data.
            /// Also HTTP code 200 could have already been sent.

            /// If buffer has data, and that data wasn't sent yet, then no need to send that data
            bool data_sent = used_output.out->count() != used_output.out->offset();

            if (!data_sent)
            {
                used_output.out_maybe_compressed->position() = used_output.out_maybe_compressed->buffer().begin();
                used_output.out->position() = used_output.out->buffer().begin();
            }

            writeString(s, *used_output.out_maybe_compressed);
            writeChar('\n', *used_output.out_maybe_compressed);

            used_output.out_maybe_compressed->next();
            used_output.out->finalize();
        }
        else
        {
            assert(false);
            __builtin_unreachable();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot send exception to client");
    }
}


void HTTPHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    setThreadName("HTTPHandler");
    ThreadStatus thread_status;

    SCOPE_EXIT({
        // If there is no request_credentials instance waiting for the next round, then the request is processed,
        // so no need to preserve request_context either.
        // Needs to be performed with respect to the other destructors in the scope though.
        if (!request_credentials)
            request_context.reset();
    });

    if (!request_context)
    {
        // Context should be initialized before anything, for correct memory accounting.
        request_context = Context::createCopy(server.context());
        request_credentials.reset();
    }

    /// Cannot be set here, since query_id is unknown.
    std::optional<CurrentThread::QueryScope> query_scope;
    Output used_output;

    /// In case of exception, send stack trace to client.
    bool with_stacktrace = false;

    try
    {
        response.setContentType("text/plain; charset=UTF-8");
        response.set("X-ClickHouse-Server-Display-Name", server_display_name);
        /// For keep-alive to work.
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        HTMLForm params(request_context->getSettingsRef(), request);
        with_stacktrace = params.getParsed<bool>("stacktrace", false);

        /// FIXME: maybe this check is already unnecessary.
        /// Workaround. Poco does not detect 411 Length Required case.
        if (request.getMethod() == HTTPRequest::HTTP_POST && !request.getChunkedTransferEncoding() && !request.hasContentLength())
        {
            throw Exception(
                "The Transfer-Encoding is not chunked and there is no Content-Length header for POST request",
                ErrorCodes::HTTP_LENGTH_REQUIRED);
        }

        processQuery(request_context, request, params, response, used_output, query_scope);
        LOG_DEBUG(log, (request_credentials ? "Authentication in progress..." : "Done processing query"));
    }
    catch (...)
    {
        SCOPE_EXIT({
            request_credentials.reset(); // ...so that the next requests on the connection have to always start afresh in case of exceptions.
        });

        tryLogCurrentException(log);

        /** If exception is received from remote server, then stack trace is embedded in message.
          * If exception is thrown on local server, then stack trace is in separate field.
          */
        std::string exception_message = getCurrentExceptionMessage(with_stacktrace, true);
        int exception_code = getCurrentExceptionCode();

        trySendExceptionToClient(exception_message, exception_code, request, response, used_output);
    }

    if (used_output.out)
        used_output.out->finalize();
}

DynamicQueryHandler::DynamicQueryHandler(IServer & server_, const std::string & param_name_)
    : HTTPHandler(server_, "DynamicQueryHandler"), param_name(param_name_)
{
}

bool DynamicQueryHandler::customizeQueryParam(ContextMutablePtr context, const std::string & key, const std::string & value)
{
    if (key == param_name)
        return true;    /// do nothing

    if (startsWith(key, "param_"))
    {
        /// Save name and values of substitution in dictionary.
        const String parameter_name = key.substr(strlen("param_"));
        context->setQueryParameter(parameter_name, value);
        return true;
    }

    return false;
}

std::string DynamicQueryHandler::getQuery(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context)
{
    if (likely(!startsWith(request.getContentType(), "multipart/form-data")))
    {
        /// Part of the query can be passed in the 'query' parameter and the rest in the request body
        /// (http method need not necessarily be POST). In this case the entire query consists of the
        /// contents of the 'query' parameter, a line break and the request body.
        std::string query_param = params.get(param_name, "");
        return query_param.empty() ? query_param : query_param + "\n";
    }

    /// Support for "external data for query processing".
    /// Used in case of POST request with form-data, but it isn't expected to be deleted after that scope.
    ExternalTablesHandler handler(context, params);
    params.load(request, request.getStream(), handler);

    std::string full_query;
    /// Params are of both form params POST and uri (GET params)
    for (const auto & it : params)
        if (it.first == param_name)
            full_query += it.second;

    return full_query;
}

PredefinedQueryHandler::PredefinedQueryHandler(
    IServer & server_,
    const NameSet & receive_params_,
    const std::string & predefined_query_,
    const CompiledRegexPtr & url_regex_,
    const std::unordered_map<String, CompiledRegexPtr> & header_name_with_regex_)
    : HTTPHandler(server_, "PredefinedQueryHandler")
    , receive_params(receive_params_)
    , predefined_query(predefined_query_)
    , url_regex(url_regex_)
    , header_name_with_capture_regex(header_name_with_regex_)
{
}

bool PredefinedQueryHandler::customizeQueryParam(ContextMutablePtr context, const std::string & key, const std::string & value)
{
    if (receive_params.count(key))
    {
        context->setQueryParameter(key, value);
        return true;
    }

    return false;
}

void PredefinedQueryHandler::customizeContext(HTTPServerRequest & request, ContextMutablePtr context)
{
    /// If in the configuration file, the handler's header is regex and contains named capture group
    /// We will extract regex named capture groups as query parameters

    const auto & set_query_params = [&](const char * begin, const char * end, const CompiledRegexPtr & compiled_regex)
    {
        int num_captures = compiled_regex->NumberOfCapturingGroups() + 1;

        re2::StringPiece matches[num_captures];
        re2::StringPiece input(begin, end - begin);
        if (compiled_regex->Match(input, 0, end - begin, re2::RE2::Anchor::ANCHOR_BOTH, matches, num_captures))
        {
            for (const auto & [capturing_name, capturing_index] : compiled_regex->NamedCapturingGroups())
            {
                const auto & capturing_value = matches[capturing_index];

                if (capturing_value.data())
                    context->setQueryParameter(capturing_name, String(capturing_value.data(), capturing_value.size()));
            }
        }
    };

    if (url_regex)
    {
        const auto & uri = request.getURI();
        set_query_params(uri.data(), find_first_symbols<'?'>(uri.data(), uri.data() + uri.size()), url_regex);
    }

    for (const auto & [header_name, regex] : header_name_with_capture_regex)
    {
        const auto & header_value = request.get(header_name);
        set_query_params(header_value.data(), header_value.data() + header_value.size(), regex);
    }
}

std::string PredefinedQueryHandler::getQuery(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context)
{
    if (unlikely(startsWith(request.getContentType(), "multipart/form-data")))
    {
        /// Support for "external data for query processing".
        ExternalTablesHandler handler(context, params);
        params.load(request, request.getStream(), handler);
    }

    return predefined_query;
}

HTTPRequestHandlerFactoryPtr createDynamicHandlerFactory(IServer & server, const std::string & config_prefix)
{
    const auto & query_param_name = server.config().getString(config_prefix + ".handler.query_param_name", "query");
    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<DynamicQueryHandler>>(server, std::move(query_param_name));

    factory->addFiltersFromConfig(server.config(), config_prefix);

    return factory;
}

static inline bool capturingNamedQueryParam(NameSet receive_params, const CompiledRegexPtr & compiled_regex)
{
    const auto & capturing_names = compiled_regex->NamedCapturingGroups();
    return std::count_if(capturing_names.begin(), capturing_names.end(), [&](const auto & iterator)
    {
        return std::count_if(receive_params.begin(), receive_params.end(),
            [&](const auto & param_name) { return param_name == iterator.first; });
    });
}

static inline CompiledRegexPtr getCompiledRegex(const std::string & expression)
{
    auto compiled_regex = std::make_shared<const re2::RE2>(expression);

    if (!compiled_regex->ok())
        throw Exception(
            "Cannot compile re2: " + expression + " for http handling rule, error: " + compiled_regex->error()
                + ". Look at https://github.com/google/re2/wiki/Syntax for reference.",
            ErrorCodes::CANNOT_COMPILE_REGEXP);

    return compiled_regex;
}

HTTPRequestHandlerFactoryPtr createPredefinedHandlerFactory(IServer & server, const std::string & config_prefix)
{
    Poco::Util::AbstractConfiguration & configuration = server.config();

    if (!configuration.has(config_prefix + ".handler.query"))
        throw Exception("There is no path '" + config_prefix + ".handler.query' in configuration file.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    std::string predefined_query = configuration.getString(config_prefix + ".handler.query");
    NameSet analyze_receive_params = analyzeReceiveQueryParams(predefined_query);

    std::unordered_map<String, CompiledRegexPtr> headers_name_with_regex;
    Poco::Util::AbstractConfiguration::Keys headers_name;
    configuration.keys(config_prefix + ".headers", headers_name);

    for (const auto & header_name : headers_name)
    {
        auto expression = configuration.getString(config_prefix + ".headers." + header_name);

        if (!startsWith(expression, "regex:"))
            continue;

        expression = expression.substr(6);
        auto regex = getCompiledRegex(expression);
        if (capturingNamedQueryParam(analyze_receive_params, regex))
            headers_name_with_regex.emplace(std::make_pair(header_name, regex));
    }

    std::shared_ptr<HandlingRuleHTTPHandlerFactory<PredefinedQueryHandler>> factory;

    if (configuration.has(config_prefix + ".url"))
    {
        auto url_expression = configuration.getString(config_prefix + ".url");

        if (startsWith(url_expression, "regex:"))
            url_expression = url_expression.substr(6);

        auto regex = getCompiledRegex(url_expression);
        if (capturingNamedQueryParam(analyze_receive_params, regex))
        {
            factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PredefinedQueryHandler>>(
                server,
                std::move(analyze_receive_params),
                std::move(predefined_query),
                std::move(regex),
                std::move(headers_name_with_regex));
            factory->addFiltersFromConfig(configuration, config_prefix);
            return factory;
        }
    }

    factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PredefinedQueryHandler>>(
        server, std::move(analyze_receive_params), std::move(predefined_query), CompiledRegexPtr{}, std::move(headers_name_with_regex));
    factory->addFiltersFromConfig(configuration, config_prefix);

    return factory;
}

}
