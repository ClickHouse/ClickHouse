#include <chrono>
#include <iomanip>

#include <Poco/File.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/NetException.h>

#include <ext/scope_guard.h>

#include <Common/ExternalTable.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>

#include <IO/ReadBufferFromIStream.h>
#include <IO/ZlibInflatingReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/CascadeWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/WriteBufferFromTemporaryFile.h>

#include <DataStreams/IProfilingBlockInputStream.h>

#include <Interpreters/executeQuery.h>
#include <Interpreters/Quota.h>
#include <Common/typeid_cast.h>

#include <Poco/Net/HTTPStream.h>

#include "HTTPHandler.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int READONLY;
    extern const int UNKNOWN_COMPRESSION_METHOD;

    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_OPEN_FILE;

    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
    extern const int TOO_DEEP_AST;
    extern const int TOO_BIG_AST;
    extern const int UNEXPECTED_AST_STRUCTURE;

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

    extern const int QUERY_IS_TOO_LARGE;

    extern const int NOT_IMPLEMENTED;
    extern const int SOCKET_TIMEOUT;

    extern const int UNKNOWN_USER;
    extern const int WRONG_PASSWORD;
    extern const int REQUIRED_PASSWORD;

    extern const int INVALID_SESSION_TIMEOUT;
    extern const int HTTP_LENGTH_REQUIRED;
}


static Poco::Net::HTTPResponse::HTTPStatus exceptionCodeToHTTPStatus(int exception_code)
{
    using namespace Poco::Net;

    if (exception_code == ErrorCodes::REQUIRED_PASSWORD)
        return HTTPResponse::HTTP_UNAUTHORIZED;
    else if (exception_code == ErrorCodes::CANNOT_PARSE_TEXT ||
             exception_code == ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE ||
             exception_code == ErrorCodes::CANNOT_PARSE_QUOTED_STRING ||
             exception_code == ErrorCodes::CANNOT_PARSE_DATE ||
             exception_code == ErrorCodes::CANNOT_PARSE_DATETIME ||
             exception_code == ErrorCodes::CANNOT_PARSE_NUMBER)
        return HTTPResponse::HTTP_BAD_REQUEST;
    else if (exception_code == ErrorCodes::UNKNOWN_ELEMENT_IN_AST ||
             exception_code == ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE ||
             exception_code == ErrorCodes::TOO_DEEP_AST ||
             exception_code == ErrorCodes::TOO_BIG_AST ||
             exception_code == ErrorCodes::UNEXPECTED_AST_STRUCTURE)
        return HTTPResponse::HTTP_BAD_REQUEST;
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
             exception_code == ErrorCodes::UNKNOWN_DATABASE_ENGINE)
        return HTTPResponse::HTTP_NOT_FOUND;
    else if (exception_code == ErrorCodes::UNKNOWN_TYPE_OF_QUERY)
        return HTTPResponse::HTTP_NOT_FOUND;
    else if (exception_code == ErrorCodes::QUERY_IS_TOO_LARGE)
        return HTTPResponse::HTTP_REQUESTENTITYTOOLARGE;
    else if (exception_code == ErrorCodes::NOT_IMPLEMENTED)
        return HTTPResponse::HTTP_NOT_IMPLEMENTED;
    else if (exception_code == ErrorCodes::SOCKET_TIMEOUT ||
             exception_code == ErrorCodes::CANNOT_OPEN_FILE)
        return HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
    else if (exception_code == ErrorCodes::HTTP_LENGTH_REQUIRED)
        return HTTPResponse::HTTP_LENGTH_REQUIRED;

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

    auto cascade_buffer = typeid_cast<CascadeWriteBuffer *>(used_output.out_maybe_delayed_and_compressed.get());
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

    ConcatReadBuffer concat_read_buffer(read_buffers_raw_ptr);
    copyData(concat_read_buffer, *used_output.out_maybe_compressed);
}


HTTPHandler::HTTPHandler(IServer & server_)
    : server(server_)
    , log(&Logger::get("HTTPHandler"))
{
}


void HTTPHandler::processQuery(
    Poco::Net::HTTPServerRequest & request,
    HTMLForm & params,
    Poco::Net::HTTPServerResponse & response,
    Output & used_output)
{
    LOG_TRACE(log, "Request URI: " << request.getURI());

    std::istream & istr = request.stream();

    /// Part of the query can be passed in the 'query' parameter and the rest in the request body
    /// (http method need not necessarily be POST). In this case the entire query consists of the
    /// contents of the 'query' parameter, a line break and the request body.
    std::string query_param = params.get("query", "");
    if (!query_param.empty())
        query_param += '\n';

    /// The user and password can be passed by headers (similar to X-Auth-*),
    /// which is used by load balancers to pass authentication information.
    std::string user = request.get("X-ClickHouse-User", "");
    std::string password = request.get("X-ClickHouse-Key", "");
    std::string quota_key = request.get("X-ClickHouse-Quota", "");

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

    const auto & config = server.config();

    Context context = server.context();
    context.setGlobalContext(server.context());

    context.setUser(user, password, request.clientAddress(), quota_key);
    context.setCurrentQueryId(query_id);

    /// The user could specify session identifier and session timeout.
    /// It allows to modify settings, create temporary tables and reuse them in subsequent requests.

    std::shared_ptr<Context> session;
    String session_id;
    std::chrono::steady_clock::duration session_timeout;
    bool session_is_set = params.has("session_id");

    if (session_is_set)
    {
        session_id = params.get("session_id");
        session_timeout = parseSessionTimeout(config, params);
        std::string session_check = params.get("session_check", "");

        session = context.acquireSession(session_id, session_timeout, session_check == "1");

        context = *session;
        context.setSessionContext(*session);
    }

    SCOPE_EXIT({
        if (session_is_set)
            session->releaseSession(session_id, session_timeout);
    });

    /// The client can pass a HTTP header indicating supported compression method (gzip or deflate).
    String http_response_compression_methods = request.get("Accept-Encoding", "");
    bool client_supports_http_compression = false;
    ZlibCompressionMethod http_response_compression_method {};

    if (!http_response_compression_methods.empty())
    {
        /// Both gzip and deflate are supported. If the client supports both, gzip is preferred.
        /// NOTE parsing of the list of methods is slightly incorrect.
        if (std::string::npos != http_response_compression_methods.find("gzip"))
        {
            client_supports_http_compression = true;
            http_response_compression_method = ZlibCompressionMethod::Gzip;
        }
        else if (std::string::npos != http_response_compression_methods.find("deflate"))
        {
            client_supports_http_compression = true;
            http_response_compression_method = ZlibCompressionMethod::Zlib;
        }
    }

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
        request, response, keep_alive_timeout,
        client_supports_http_compression, http_response_compression_method, buffer_size_http);
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
            std::string tmp_path_template = context.getTemporaryPath() + "http_buffers/";

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
                auto prev_memory_buffer = typeid_cast<MemoryWriteBuffer *>(prev_buf.get());
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

    std::unique_ptr<ReadBuffer> in_param = std::make_unique<ReadBufferFromString>(query_param);

    std::unique_ptr<ReadBuffer> in_post_raw = std::make_unique<ReadBufferFromIStream>(istr);

    /// Request body can be compressed using algorithm specified in the Content-Encoding header.
    std::unique_ptr<ReadBuffer> in_post;
    String http_request_compression_method_str = request.get("Content-Encoding", "");
    if (!http_request_compression_method_str.empty())
    {
        ZlibCompressionMethod method;
        if (http_request_compression_method_str == "gzip")
        {
            method = ZlibCompressionMethod::Gzip;
        }
        else if (http_request_compression_method_str == "deflate")
        {
            method = ZlibCompressionMethod::Zlib;
        }
        else
            throw Exception("Unknown Content-Encoding of HTTP request: " + http_request_compression_method_str,
                ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
        in_post = std::make_unique<ZlibInflatingReadBuffer>(*in_post_raw, method);
    }
    else
        in_post = std::move(in_post_raw);

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

    /// Support for "external data for query processing".
    if (startsWith(request.getContentType().data(), "multipart/form-data"))
    {
        in = std::move(in_param);
        ExternalTablesHandler handler(context, params);

        params.load(request, istr, handler);

        /// Erase unneeded parameters to avoid confusing them later with context settings or query
        /// parameters.
        for (const auto & it : handler.names)
        {
            params.erase(it + "_format");
            params.erase(it + "_types");
            params.erase(it + "_structure");
        }
    }
    else
        in = std::make_unique<ConcatReadBuffer>(*in_param, *in_post_maybe_compressed);

    /// Settings can be overridden in the query.
    /// Some parameters (database, default_format, everything used in the code above) do not
    /// belong to the Settings class.

    /// 'readonly' setting values mean:
    /// readonly = 0 - any query is allowed, client can change any setting.
    /// readonly = 1 - only readonly queries are allowed, client can't change settings.
    /// readonly = 2 - only readonly queries are allowed, client can change any setting except 'readonly'.

    /// In theory if initially readonly = 0, the client can change any setting and then set readonly
    /// to some other value.
    auto & limits = context.getSettingsRef().limits;

    /// Only readonly queries are allowed for HTTP GET requests.
    if (request.getMethod() == Poco::Net::HTTPServerRequest::HTTP_GET)
    {
        if (limits.readonly == 0)
            limits.readonly = 2;
    }

    auto readonly_before_query = limits.readonly;

    NameSet reserved_param_names{"query", "compress", "decompress", "user", "password", "quota_key", "query_id", "stacktrace",
        "buffer_size", "wait_end_of_query",
        "session_id", "session_timeout", "session_check"
    };

    const Settings & settings = context.getSettingsRef();

    for (auto it = params.begin(); it != params.end(); ++it)
    {
        if (it->first == "database")
        {
            context.setCurrentDatabase(it->second);
        }
        else if (it->first == "default_format")
        {
            context.setDefaultFormat(it->second);
        }
        else if (reserved_param_names.find(it->first) != reserved_param_names.end())
        {
        }
        else
        {
            /// All other query parameters are treated as settings.
            String value;
            /// Setting is skipped if value wasn't changed.
            if (!settings.tryGet(it->first, value) || it->second != value)
            {
                if (readonly_before_query == 1)
                    throw Exception("Cannot override setting (" + it->first + ") in readonly mode", ErrorCodes::READONLY);

                if (readonly_before_query && it->first == "readonly")
                    throw Exception("Setting 'readonly' cannot be overrided in readonly mode", ErrorCodes::READONLY);

                context.setSetting(it->first, it->second);
            }
        }
    }

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

    /// While still no data has been sent, we will report about query execution progress by sending HTTP headers.
    if (settings.send_progress_in_http_headers)
        context.setProgressCallback([&used_output] (const Progress & progress) { used_output.out->onProgress(progress); });

    executeQuery(*in, *used_output.out_maybe_delayed_and_compressed, /* allow_into_outfile = */ false, context,
        [&response] (const String & content_type) { response.setContentType(content_type); });

    if (used_output.hasDelayed())
    {
        /// TODO: set Content-Length if possible
        pushDelayedResults(used_output);
    }

    /// Send HTTP headers with code 200 if no exception happened and the data is still not sent to
    /// the client.
    used_output.out->finalize();
}

void HTTPHandler::trySendExceptionToClient(const std::string & s, int exception_code,
    Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response,
    Output & used_output)
{
    try
    {
        /// If HTTP method is POST and Keep-Alive is turned on, we should read the whole request body
        /// to avoid reading part of the current request body in the next request.
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST
            && response.getKeepAlive()
            && !request.stream().eof()
            && exception_code != ErrorCodes::HTTP_LENGTH_REQUIRED)
        {
            request.stream().ignore(std::numeric_limits<std::streamsize>::max());
        }

        bool auth_fail = exception_code == ErrorCodes::UNKNOWN_USER ||
                         exception_code == ErrorCodes::WRONG_PASSWORD ||
                         exception_code == ErrorCodes::REQUIRED_PASSWORD;

        if (auth_fail)
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
            response.send() << s << std::endl;
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
            used_output.out->next();
            used_output.out->finalize();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot send exception to client");
    }
}


void HTTPHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    Output used_output;

    /// In case of exception, send stack trace to client.
    bool with_stacktrace = false;

    try
    {
        response.setContentType("text/plain; charset=UTF-8");

        /// For keep-alive to work.
        if (request.getVersion() == Poco::Net::HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        HTMLForm params(request);
        with_stacktrace = params.getParsed<bool>("stacktrace", false);

        /// Workaround. Poco does not detect 411 Length Required case.
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST && !request.getChunkedTransferEncoding() &&
            !request.hasContentLength())
        {
            throw Exception("There is neither Transfer-Encoding header nor Content-Length header", ErrorCodes::HTTP_LENGTH_REQUIRED);
        }

        processQuery(request, params, response, used_output);
        LOG_INFO(log, "Done processing query");
    }
    catch (...)
    {
        tryLogCurrentException(log);

        /** If exception is received from remote server, then stack trace is embedded in message.
          * If exception is thrown on local server, then stack trace is in separate field.
          */
        std::string exception_message = getCurrentExceptionMessage(with_stacktrace, true);
        int exception_code = getCurrentExceptionCode();

        trySendExceptionToClient(exception_message, exception_code, request, response, used_output);
    }
}


}
