#include <Server/HTTPHandler.h>

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/ExternalTable.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Disks/StoragePolicy.h>
#include <IO/CascadeWriteBuffer.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Parsers/QueryParameterVisitor.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Session.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/HTTPHandlerRequestFilter.h>
#include <Server/IServer.h>
#include <Common/logger_useful.h>
#include <Common/SettingsChanges.h>
#include <Common/StringUtils.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>

#include <base/getFQDNOrHostName.h>
#include <base/isSharedPtrUnique.h>
#include <Server/HTTP/HTTPResponse.h>
#include <Server/HTTP/authenticateUserByHTTP.h>
#include <Server/HTTP/sendExceptionToHTTPClient.h>
#include <Server/HTTP/setReadOnlyIfHTTPMethodIdempotent.h>

#include <Poco/Net/HTTPMessage.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>


namespace DB
{
namespace Setting
{
    extern const SettingsBool add_http_cors_header;
    extern const SettingsBool cancel_http_readonly_queries_on_client_close;
    extern const SettingsBool enable_http_compression;
    extern const SettingsUInt64 http_headers_progress_interval_ms;
    extern const SettingsUInt64 http_max_request_param_data_size;
    extern const SettingsBool http_native_compression_disable_checksumming_on_decompress;
    extern const SettingsUInt64 http_response_buffer_size;
    extern const SettingsBool http_wait_end_of_query;
    extern const SettingsBool http_write_exception_in_output_format;
    extern const SettingsInt64 http_zlib_compression_level;
    extern const SettingsUInt64 readonly;
    extern const SettingsBool send_progress_in_http_headers;
    extern const SettingsInt64 zstd_window_log_max;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_COMPILE_REGEXP;

    extern const int NO_ELEMENTS_IN_CONFIG;

    extern const int INVALID_SESSION_TIMEOUT;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int HTTP_LENGTH_REQUIRED;
    extern const int SESSION_ID_EMPTY;
}

namespace
{
bool tryAddHTTPOptionHeadersFromConfig(HTTPServerResponse & response, const Poco::Util::LayeredConfiguration & config)
{
    if (config.has("http_options_response"))
    {
        Strings config_keys;
        config.keys("http_options_response", config_keys);
        for (const std::string & config_key : config_keys)
        {
            if (config_key == "header" || config_key.starts_with("header["))
            {
                /// If there is empty header name, it will not be processed and message about it will be in logs
                if (config.getString("http_options_response." + config_key + ".name", "").empty())
                    LOG_WARNING(getLogger("processOptionsRequest"), "Empty header was found in config. It will not be processed.");
                else
                    response.add(config.getString("http_options_response." + config_key + ".name", ""),
                                 config.getString("http_options_response." + config_key + ".value", ""));

            }
        }
        return true;
    }
    return false;
}

/// Process options request. Useful for CORS.
void processOptionsRequest(HTTPServerResponse & response, const Poco::Util::LayeredConfiguration & config)
{
    /// If can add some headers from config
    if (tryAddHTTPOptionHeadersFromConfig(response, config))
    {
        response.setKeepAlive(false);
        response.setStatusAndReason(HTTPResponse::HTTP_NO_CONTENT);
        response.send();
    }
}
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
            throw Exception(ErrorCodes::INVALID_SESSION_TIMEOUT, "Invalid session timeout: '{}'", session_timeout_str);

        if (session_timeout > max_session_timeout)
            throw Exception(ErrorCodes::INVALID_SESSION_TIMEOUT, "Session timeout '{}' is larger than max_session_timeout: {}. "
                "Maximum session timeout could be modified in configuration file.",
                session_timeout_str, max_session_timeout);
    }

    return std::chrono::seconds(session_timeout);
}

HTTPHandlerConnectionConfig::HTTPHandlerConnectionConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    if (config.has(config_prefix + ".handler.password"))
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "{}.handler.password will be ignored. Please remove it from config.", config_prefix);
    if (config.has(config_prefix + ".handler.user"))
        credentials.emplace(config.getString(config_prefix + ".handler.user", "default"));
}


HTTPHandler::HTTPHandler(IServer & server_, const HTTPHandlerConnectionConfig & connection_config_, const std::string & name, const HTTPResponseHeaderSetup & http_response_headers_override_)
    : server(server_)
    , log(getLogger(name))
    , default_settings(server.context()->getSettingsRef())
    , http_response_headers_override(http_response_headers_override_)
    , connection_config(connection_config_)
{
    server_display_name = server.config().getString("display_name", getFQDNOrHostName());
}


/// We need d-tor to be present in this translation unit to make it play well with some
/// forward decls in the header. Other than that, the default d-tor would be OK.
HTTPHandler::~HTTPHandler() = default;


bool HTTPHandler::authenticateUser(HTTPServerRequest & request, HTMLForm & params, HTTPServerResponse & response)
{
    return authenticateUserByHTTP(request, params, response, *session, request_credentials, connection_config, server.context(), log);
}


void HTTPHandler::processQuery(
    HTTPServerRequest & request,
    HTMLForm & params,
    HTTPServerResponse & response,
    Output & used_output,
    std::optional<CurrentThread::QueryScope> & query_scope,
    const ProfileEvents::Event & write_event)
{
    using namespace Poco::Net;

    LOG_TRACE(log, "Request URI: {}", request.getURI());

    if (!authenticateUser(request, params, response))
        return; // '401 Unauthorized' response with 'Negotiate' has been sent at this point.

    /// The user could specify session identifier and session timeout.
    /// It allows to modify settings, create temporary tables and reuse them in subsequent requests.

    String session_id;
    std::chrono::steady_clock::duration session_timeout;
    bool session_is_set = params.has("session_id");
    const auto & config = server.config();

    /// Close http session (if any) after processing the request
    bool close_session = false;
    if (params.getParsed<bool>("close_session", false) && server.config().getBool("enable_http_close_session", true))
        close_session = true;

    if (session_is_set)
    {
        session_id = params.get("session_id");
        if (session_id.empty())
            throw Exception(ErrorCodes::SESSION_ID_EMPTY, "Session id query parameter was provided, but it was empty");
        session_timeout = parseSessionTimeout(config, params);
        std::string session_check = params.get("session_check", "");
        session->makeSessionContext(session_id, session_timeout, session_check == "1");
    }
    else
    {
        session_id = "";
        /// We should create it even if we don't have a session_id
        session->makeSessionContext();
    }

    /// We need to have both releasing/closing a session here and below. The problem with having it only as a SCOPE_EXIT
    /// is that it will be invoked after finalizing the buffer in the end of processQuery, and that technically means that
    /// the client has received all the data, but the session is not released yet. And it can (and sometimes does) happen
    /// that we'll try to acquire the same session in another request before releasing the session here, and the session for
    /// the following request will be technically locked, while it shouldn't be.
    /// Also, SCOPE_EXIT is still needed to release a session in case of any exception. If the exception occurs at some point
    /// after releasing the session below, this whole call will be no-op (due to named_session being nullptr already inside a session).
    SCOPE_EXIT_SAFE({ releaseOrCloseSession(session_id, close_session); });

    auto context = session->makeQueryContext();

    auto roles = params.getAll("role");
    if (!roles.empty())
        context->setCurrentRoles(roles);

    std::string database = request.get("X-ClickHouse-Database", params.get("database", ""));
    if (!database.empty())
        context->setCurrentDatabase(database);

    std::string default_format = request.get("X-ClickHouse-Format", params.get("default_format", ""));
    if (!default_format.empty())
        context->setDefaultFormat(default_format);

    /// Anything else beside HTTP POST should be readonly queries.
    setReadOnlyIfHTTPMethodIdempotent(context, request.getMethod());

    /// Set the query id supplied by the user, if any, and also update the OpenTelemetry fields.
    context->setCurrentQueryId(params.get("query_id", request.get("X-ClickHouse-Query-Id", "")));

    bool has_external_data = startsWith(request.getContentType(), "multipart/form-data");

    auto param_could_be_skipped = [&] (const String & name)
    {
        /// Empty parameter appears when URL like ?&a=b or a=b&&c=d. Just skip them for user's convenience.
        if (name.empty())
            return true;

        /// Some parameters (database, default_format, everything used in the code above) do not
        /// belong to the Settings class.
        static const NameSet reserved_param_names{"compress", "decompress", "user", "password", "quota_key", "query_id", "stacktrace", "role",
            "buffer_size", "wait_end_of_query", "session_id", "session_timeout", "session_check", "client_protocol_version", "close_session",
            "database", "default_format"};

        if (reserved_param_names.contains(name))
            return true;

        /// For external data we also want settings.
        if (has_external_data)
        {
            /// Skip unneeded parameters to avoid confusing them later with context settings or query parameters.
            /// It is a bug and ambiguity with `date_time_input_format` and `low_cardinality_allow_in_native_format` formats/settings.
            static const Names reserved_param_suffixes = {"_format", "_types", "_structure"};
            for (const String & suffix : reserved_param_suffixes)
            {
                if (endsWith(name, suffix))
                    return true;
            }
        }

        return false;
    };

    /// Settings can be overridden in the query.
    SettingsChanges settings_changes;
    for (const auto & [key, value] : params)
    {
        if (!param_could_be_skipped(key))
        {
            /// Other than query parameters are treated as settings.
            if (!customizeQueryParam(context, key, value))
                settings_changes.setSetting(key, value);
        }
    }

    context->checkSettingsConstraints(settings_changes, SettingSource::QUERY);
    context->applySettingsChanges(settings_changes);

    /// Initialize query scope, once query_id is initialized.
    /// (To track as much allocations as possible)
    query_scope.emplace(context);

    const auto & settings = context->getSettingsRef();

    /// This parameter is used to tune the behavior of output formats (such as Native) for compatibility.
    if (params.has("client_protocol_version"))
    {
        UInt64 version_param = parse<UInt64>(params.get("client_protocol_version"));
        context->setClientProtocolVersion(version_param);
    }

    /// The client can pass a HTTP header indicating supported compression method (gzip or deflate).
    String http_response_compression_methods = request.get("Accept-Encoding", "");
    CompressionMethod http_response_compression_method = CompressionMethod::None;

    if (!http_response_compression_methods.empty())
        http_response_compression_method = chooseHTTPCompressionMethod(http_response_compression_methods);

    bool client_supports_http_compression = http_response_compression_method != CompressionMethod::None;

    /// Client can pass a 'compress' flag in the query string. In this case the query result is
    /// compressed using internal algorithm. This is not reflected in HTTP headers.
    bool internal_compression = params.getParsedLast<bool>("compress", false);

    /// If wait_end_of_query is specified, the whole result will be buffered.
    ///  First ~buffer_size bytes will be buffered in memory, the remaining bytes will be stored in temporary file.
    auto buffer_size_http = settings[Setting::http_response_buffer_size];
    /// setting overrides deprecated buffer_size parameter
    if (!params.has("http_response_buffer_size"))
        buffer_size_http = params.getParsedLast<size_t>("buffer_size", buffer_size_http);

    bool wait_end_of_query = settings[Setting::http_wait_end_of_query];
    /// setting overrides deprecated wait_end_of_query parameter
    if (!params.has("http_wait_end_of_query"))
        wait_end_of_query = params.getParsedLast<bool>("wait_end_of_query", wait_end_of_query);


    bool enable_http_compression = params.getParsedLast<bool>("enable_http_compression", settings[Setting::enable_http_compression]);
    Int64 http_zlib_compression_level
        = params.getParsed<Int64>("http_zlib_compression_level", settings[Setting::http_zlib_compression_level]);

    used_output.out_holder =
        std::make_shared<WriteBufferFromHTTPServerResponse>(
            response,
            request.getMethod() == HTTPRequest::HTTP_HEAD,
            write_event);
    used_output.out_maybe_compressed = used_output.out_holder;
    used_output.out = used_output.out_holder;

    if (client_supports_http_compression && enable_http_compression)
    {
        used_output.out_holder->setCompressionMethodHeader(http_response_compression_method);
        used_output.wrap_compressed_holder = wrapWriteBufferWithCompressionMethod(
            used_output.out.get(),
            http_response_compression_method,
            static_cast<int>(http_zlib_compression_level),
            0,
            DBMS_DEFAULT_BUFFER_SIZE,
            nullptr,
            0,
            false);
        used_output.out_maybe_compressed = used_output.wrap_compressed_holder;
        used_output.out = used_output.wrap_compressed_holder;
    }

    if (internal_compression)
    {
        used_output.out_compressed_holder = std::make_shared<CompressedWriteBuffer>(*used_output.out);
        used_output.out_maybe_compressed = used_output.out_compressed_holder;
        used_output.out = used_output.out_compressed_holder;
    }

    if (buffer_size_http > 0 || wait_end_of_query)
    {
        CascadeWriteBuffer::WriteBufferPtrs cascade_buffers;
        CascadeWriteBuffer::WriteBufferConstructors cascade_buffers_lazy;

        if (buffer_size_http > 0)
            cascade_buffers.emplace_back(std::make_shared<MemoryWriteBuffer>(buffer_size_http));

        if (wait_end_of_query)
        {
            auto tmp_data = server.context()->getTempDataOnDisk();
            cascade_buffers_lazy.emplace_back([tmp_data](const WriteBufferPtr &) -> WriteBufferPtr
            {
                return std::make_unique<TemporaryDataBuffer>(tmp_data.get());
            });
        }
        else
        {
            auto push_memory_buffer_and_continue = [next_buffer = used_output.out] (const WriteBufferPtr & prev_buf)
            {
                auto * prev_memory_buffer = typeid_cast<MemoryWriteBuffer *>(prev_buf.get());
                if (!prev_memory_buffer)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected MemoryWriteBuffer");

                auto rdbuf = prev_memory_buffer->tryGetReadBuffer();
                copyData(*rdbuf, *next_buffer);

                return next_buffer;
            };

            cascade_buffers_lazy.emplace_back(push_memory_buffer_and_continue);
        }

        used_output.out_delayed_and_compressed_holder = std::make_unique<CascadeWriteBuffer>(std::move(cascade_buffers), std::move(cascade_buffers_lazy));
        used_output.out_maybe_delayed_and_compressed = used_output.out_delayed_and_compressed_holder;
    }
    else
    {
        used_output.out_maybe_delayed_and_compressed = used_output.out_maybe_compressed;
    }

    /// Request body can be compressed using algorithm specified in the Content-Encoding header.
    String http_request_compression_method_str = request.get("Content-Encoding", "");
    int zstd_window_log_max = static_cast<int>(context->getSettingsRef()[Setting::zstd_window_log_max]);
    auto in_post = wrapReadBufferWithCompressionMethod(
        wrapReadBufferReference(request.getStream()),
        chooseCompressionMethod({}, http_request_compression_method_str),
        zstd_window_log_max);

    /// The data can also be compressed using incompatible internal algorithm. This is indicated by
    /// 'decompress' query parameter.
    std::unique_ptr<ReadBuffer> in_post_maybe_compressed;
    bool is_in_post_compressed = false;
    if (params.getParsedLast<bool>("decompress", false))
    {
        in_post_maybe_compressed = std::make_unique<CompressedReadBuffer>(*in_post, /* allow_different_codecs_ = */ false, /* external_data_ = */ true);
        is_in_post_compressed = true;
    }
    else
        in_post_maybe_compressed = std::move(in_post);

    /// NOTE: this may create pretty huge allocations that will not be accounted in trace_log,
    /// because memory_profiler_sample_probability/memory_profiler_step are not applied yet,
    /// they will be applied in ProcessList::insert() from executeQuery() itself.
    const auto & query = getQuery(request, params, context);
    std::unique_ptr<ReadBuffer> in_param = std::make_unique<ReadBufferFromString>(query);

    used_output.out_holder->setSendProgress(settings[Setting::send_progress_in_http_headers]);
    used_output.out_holder->setSendProgressInterval(settings[Setting::http_headers_progress_interval_ms]);

    /// If 'http_native_compression_disable_checksumming_on_decompress' setting is turned on,
    /// checksums of client data compressed with internal algorithm are not checked.
    if (is_in_post_compressed && settings[Setting::http_native_compression_disable_checksumming_on_decompress])
        static_cast<CompressedReadBuffer &>(*in_post_maybe_compressed).disableChecksumming();

    /// Add CORS header if 'add_http_cors_header' setting is turned on send * in Access-Control-Allow-Origin
    /// Note that whether the header is added is determined by the settings, and we can only get the user settings after authentication.
    /// Once the authentication fails, the header can't be added.
    if (settings[Setting::add_http_cors_header] && !request.get("Origin", "").empty() && !config.has("http_options_response"))
        used_output.out_holder->addHeaderCORS(true);

    auto append_callback = [my_context = context] (ProgressCallback callback)
    {
        auto prev = my_context->getProgressCallback();

        my_context->setProgressCallback([prev, callback] (const Progress & progress)
        {
            if (prev)
                prev(progress);

            callback(progress);
        });
    };

    /// While still no data has been sent, we will report about query execution progress by sending HTTP headers.
    /// Note that we add it unconditionally so the progress is available for `X-ClickHouse-Summary`
    append_callback([&used_output](const Progress & progress)
    {
        used_output.out_holder->onProgress(progress);
    });

    if (settings[Setting::readonly] > 0 && settings[Setting::cancel_http_readonly_queries_on_client_close])
    {
        append_callback([&context, &request](const Progress &)
        {
            /// Assume that at the point this method is called no one is reading data from the socket any more:
            /// should be true for read-only queries.
            if (!request.checkPeerConnected())
                context->killCurrentQuery();
        });
    }

    customizeContext(request, context, *in_post_maybe_compressed);
    std::unique_ptr<ReadBuffer> in = has_external_data ? std::move(in_param) : std::make_unique<ConcatReadBuffer>(*in_param, *in_post_maybe_compressed);

    applyHTTPResponseHeaders(response, http_response_headers_override);

    auto set_query_result = [&response, this] (const QueryResultDetails & details)
    {
        response.add("X-ClickHouse-Query-Id", details.query_id);

        if (!(http_response_headers_override && http_response_headers_override->contains(Poco::Net::HTTPMessage::CONTENT_TYPE))
            && details.content_type)
            response.setContentType(*details.content_type);

        if (details.format)
            response.add("X-ClickHouse-Format", *details.format);

        if (details.timezone)
            response.add("X-ClickHouse-Timezone", *details.timezone);

        for (const auto & [name, value] : details.additional_headers)
            response.set(name, value);
    };

    auto handle_exception_in_output_format = [&, session_id, close_session](IOutputFormat & current_output_format,
                                                 const String & format_name,
                                                 const ContextPtr & context_,
                                                 const std::optional<FormatSettings> & format_settings)
    {
        if (settings[Setting::http_write_exception_in_output_format] && current_output_format.supportsWritingException())
        {
            /// If wait_end_of_query=true in case of an exception all data written to output format during query execution will be
            /// ignored, so we cannot write exception message in current output format as it will be also ignored.
            /// Instead, we create exception_writer function that will write exception in required format
            /// and will use it later in trySendExceptionToClient when all buffers will be prepared.
            if (wait_end_of_query)
            {
                auto header = current_output_format.getPort(IOutputFormat::PortKind::Main).getHeader();
                used_output.exception_writer = [&, format_name, header, context_, format_settings, session_id, close_session](WriteBuffer & buf, int code, const String & message)
                {
                    if (used_output.out_holder->isCanceled())
                    {
                        chassert(buf.isCanceled());
                        return;
                    }

                    drainRequestIfNeeded(request, response);
                    used_output.out_holder->setExceptionCode(code);

                    auto output_format = FormatFactory::instance().getOutputFormat(format_name, buf, header, context_, format_settings);
                    output_format->setException(message);
                    output_format->finalize();
                    releaseOrCloseSession(session_id, close_session);
                    used_output.finalize();
                    used_output.exception_is_written = true;
                };
            }
            else
            {
                if (used_output.out_holder->isCanceled())
                    return;

                bool with_stacktrace = (params.getParsed<bool>("stacktrace", false) && server.config().getBool("enable_http_stacktrace", true));
                ExecutionStatus status = ExecutionStatus::fromCurrentException("", with_stacktrace);

                drainRequestIfNeeded(request, response);
                used_output.out_holder->setExceptionCode(status.code);
                current_output_format.setException(status.message);
                current_output_format.finalize();
                releaseOrCloseSession(session_id, close_session);
                used_output.finalize();

                used_output.exception_is_written = true;
            }
        }
    };

    auto query_finish_callback = [&]()
    {
        releaseOrCloseSession(session_id, close_session);

        /// Flush all the data from one buffer to another, to track
        /// NetworkSendElapsedMicroseconds/NetworkSendBytes from the query
        /// context
        used_output.finalize();
    };

    executeQuery(
        *in,
        *used_output.out_maybe_delayed_and_compressed,
        /* allow_into_outfile = */ false,
        context,
        set_query_result,
        QueryFlags{},
        {},
        handle_exception_in_output_format,
        query_finish_callback);
}

bool HTTPHandler::trySendExceptionToClient(
    int exception_code, const std::string & message, HTTPServerRequest & request, HTTPServerResponse & response, Output & used_output)
try
{
    if (!used_output.out_holder && !used_output.exception_is_written)
    {
        /// If nothing was sent yet and we don't even know if we must compress the response.
        auto wb = WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD);
        return wb.cancelWithException(request, exception_code, message, nullptr);
    }

    chassert(used_output.out_maybe_compressed);

    /// Destroy CascadeBuffer to actualize buffers' positions and reset extra references
    if (used_output.hasDelayed())
    {
        /// do not call finalize here for CascadeWriteBuffer used_output.out_maybe_delayed_and_compressed,
        /// exception is written into used_output.out_maybe_compressed later
        auto write_buffers = used_output.out_delayed_and_compressed_holder->getResultBuffers();
        /// cancel the rest unused buffers
        for (auto & wb : write_buffers)
            if (isSharedPtrUnique(wb))
                wb->cancel();

        used_output.out_maybe_delayed_and_compressed = used_output.out_maybe_compressed;
        used_output.out_delayed_and_compressed_holder.reset();
    }

    if (used_output.exception_is_written)
    {
        LOG_TRACE(log, "Exception has already been written to output format by current output formatter, nothing to do");
        /// everything has been held by output format write
        return true;
    }

    /// We might have special formatter for exception message.
    if (used_output.exception_writer)
    {
        used_output.exception_writer(*used_output.out_maybe_compressed, exception_code, message);
        return true;
    }

    /// This is the worst case scenario
    /// There is no support from output_format.
    /// output_format either did not faced the exception or did not support the exception writing

    /// Send the error message into already used (and possibly compressed) stream.
    /// Note that the error message will possibly be sent after some data.
    /// Also HTTP code 200 could have already been sent.
    return used_output.out_holder->cancelWithException(request, exception_code, message, used_output.out_maybe_compressed.get());
}
catch (...)
{
    /// The message could be not sent due to error on allocations
    /// or due to exception in used_output.exception_writer
    /// or because the socket is broken
    tryLogCurrentException(log, "Cannot send exception to client");
    return false;
}

void HTTPHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event)
{
    setThreadName("HTTPHandler");

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::HTTP, request.isSecure());
    SCOPE_EXIT({ session.reset(); });
    std::optional<CurrentThread::QueryScope> query_scope;

    Output used_output;

    /// In case of exception, send stack trace to client.
    bool with_stacktrace = false;

    OpenTelemetry::TracingContextHolderPtr thread_trace_context;
    SCOPE_EXIT({
        // make sure the response status is recorded
        if (thread_trace_context)
            thread_trace_context->root_span.addAttribute("clickhouse.http_status", response.getStatus());
    });

    try
    {
        if (request.getMethod() == HTTPServerRequest::HTTP_OPTIONS)
        {
            processOptionsRequest(response, server.config());
            return;
        }

        // Parse the OpenTelemetry traceparent header.
        auto & client_trace_context = session->getClientTraceContext();
        if (request.has("traceparent"))
        {
            std::string opentelemetry_traceparent = request.get("traceparent");
            std::string error;
            if (!client_trace_context.parseTraceparentHeader(opentelemetry_traceparent, error))
            {
                LOG_DEBUG(log, "Failed to parse OpenTelemetry traceparent header '{}': {}", opentelemetry_traceparent, error);
            }
            client_trace_context.tracestate = request.get("tracestate", "");
        }

        // Setup tracing context for this thread
        auto context = session->sessionOrGlobalContext();
        thread_trace_context = std::make_unique<OpenTelemetry::TracingContextHolder>("HTTPHandler",
            client_trace_context,
            context->getSettingsRef(),
            context->getOpenTelemetrySpanLog());
        thread_trace_context->root_span.kind = OpenTelemetry::SpanKind::SERVER;
        thread_trace_context->root_span.addAttribute("clickhouse.uri", request.getURI());
        thread_trace_context->root_span.addAttribute("http.referer", request.get("Referer", ""));
        thread_trace_context->root_span.addAttribute("http.user.agent", request.get("User-Agent", ""));
        thread_trace_context->root_span.addAttribute("http.method", request.getMethod());

        response.setContentType("text/plain; charset=UTF-8");
        response.add("Access-Control-Expose-Headers", "X-ClickHouse-Query-Id,X-ClickHouse-Summary,X-ClickHouse-Server-Display-Name,X-ClickHouse-Format,X-ClickHouse-Timezone,X-ClickHouse-Exception-Code");
        response.set("X-ClickHouse-Server-Display-Name", server_display_name);

        if (!request.get("Origin", "").empty())
            tryAddHTTPOptionHeadersFromConfig(response, server.config());

        /// For keep-alive to work.
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        HTMLForm params(default_settings, request);

        if (params.getParsed<bool>("stacktrace", false) && server.config().getBool("enable_http_stacktrace", true))
            with_stacktrace = true;

        /// FIXME: maybe this check is already unnecessary.
        /// Workaround. Poco does not detect 411 Length Required case.
        if (request.getMethod() == HTTPRequest::HTTP_POST && !request.getChunkedTransferEncoding() && !request.hasContentLength())
        {
            throw Exception(ErrorCodes::HTTP_LENGTH_REQUIRED,
                            "The Transfer-Encoding is not chunked and there "
                            "is no Content-Length header for POST request");
        }

        processQuery(request, params, response, used_output, query_scope, write_event);
        if (request_credentials)
            LOG_DEBUG(log, "Authentication in progress...");
        else
            LOG_DEBUG(log, "Done processing query");
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
        ExecutionStatus status = ExecutionStatus::fromCurrentException("", with_stacktrace);
        auto error_sent = trySendExceptionToClient(status.code, status.message, request, response, used_output);

        used_output.cancel();

        if (!error_sent && thread_trace_context)
                thread_trace_context->root_span.addAttribute("clickhouse.exception", "Cannot flush data to client");

        if (thread_trace_context)
            thread_trace_context->root_span.addAttribute(status);
    }
}

void HTTPHandler::releaseOrCloseSession(const String & session_id, bool close_session)
{
    if (!session_id.empty())
    {
        if (close_session)
            session->closeSession(session_id);
        else
            session->releaseSessionID();
    }
}

DynamicQueryHandler::DynamicQueryHandler(
    IServer & server_,
    const HTTPHandlerConnectionConfig & connection_config,
    const std::string & param_name_,
    const HTTPResponseHeaderSetup & http_response_headers_override_)
    : HTTPHandler(server_, connection_config, "DynamicQueryHandler", http_response_headers_override_)
    , param_name(param_name_)
{
}

bool DynamicQueryHandler::customizeQueryParam(ContextMutablePtr context, const std::string & key, const std::string & value)
{
    if (key == param_name)
        return true;    /// do nothing

    if (startsWith(key, QUERY_PARAMETER_NAME_PREFIX))
    {
        /// Save name and values of substitution in dictionary.
        const String parameter_name = key.substr(strlen(QUERY_PARAMETER_NAME_PREFIX));

        if (!context->getQueryParameters().contains(parameter_name))
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
    {
        if (it.first == param_name)
        {
            full_query += it.second;
        }
        else
        {
            customizeQueryParam(context, it.first, it.second);
        }
    }

    return full_query;
}

PredefinedQueryHandler::PredefinedQueryHandler(
    IServer & server_,
    const HTTPHandlerConnectionConfig & connection_config,
    const NameSet & receive_params_,
    const std::string & predefined_query_,
    const CompiledRegexPtr & url_regex_,
    const std::unordered_map<String, CompiledRegexPtr> & header_name_with_regex_,
    const HTTPResponseHeaderSetup & http_response_headers_override_)
    : HTTPHandler(server_, connection_config, "PredefinedQueryHandler", http_response_headers_override_)
    , receive_params(receive_params_)
    , predefined_query(predefined_query_)
    , url_regex(url_regex_)
    , header_name_with_capture_regex(header_name_with_regex_)
{
}

bool PredefinedQueryHandler::customizeQueryParam(ContextMutablePtr context, const std::string & key, const std::string & value)
{
    if (receive_params.contains(key))
    {
        context->setQueryParameter(key, value);
        return true;
    }

    if (startsWith(key, QUERY_PARAMETER_NAME_PREFIX))
    {
        /// Save name and values of substitution in dictionary.
        const String parameter_name = key.substr(strlen(QUERY_PARAMETER_NAME_PREFIX));

        if (receive_params.contains(parameter_name))
            context->setQueryParameter(parameter_name, value);
        return true;
    }

    return false;
}

void PredefinedQueryHandler::customizeContext(HTTPServerRequest & request, ContextMutablePtr context, ReadBuffer & body)
{
    /// If in the configuration file, the handler's header is regex and contains named capture group
    /// We will extract regex named capture groups as query parameters

    const auto & set_query_params = [&](const char * begin, const char * end, const CompiledRegexPtr & compiled_regex)
    {
        int num_captures = compiled_regex->NumberOfCapturingGroups() + 1;

        std::vector<std::string_view> matches(num_captures);
        std::string_view input(begin, end - begin);
        if (compiled_regex->Match(input, 0, end - begin, re2::RE2::Anchor::ANCHOR_BOTH, matches.data(), num_captures))
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

    if (unlikely(receive_params.contains("_request_body") && !context->getQueryParameters().contains("_request_body")))
    {
        WriteBufferFromOwnString value;
        const auto & settings = context->getSettingsRef();

        copyDataMaxBytes(body, value, settings[Setting::http_max_request_param_data_size]);
        context->setQueryParameter("_request_body", value.str());
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

HTTPRequestHandlerFactoryPtr createDynamicHandlerFactory(IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    std::unordered_map<String, String> & common_headers)
{
    auto query_param_name = config.getString(config_prefix + ".handler.query_param_name", "query");

    HTTPHandlerConnectionConfig connection_config(config, config_prefix);
    HTTPResponseHeaderSetup http_response_headers_override = parseHTTPResponseHeaders(config, config_prefix);
    if (http_response_headers_override.has_value())
        http_response_headers_override.value().insert(common_headers.begin(), common_headers.end());

    auto creator = [&server, query_param_name, http_response_headers_override, connection_config]() -> std::unique_ptr<DynamicQueryHandler>
    { return std::make_unique<DynamicQueryHandler>(server, connection_config, query_param_name, http_response_headers_override); };

    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<DynamicQueryHandler>>(std::move(creator));
    factory->addFiltersFromConfig(config, config_prefix);
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
        throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP, "Cannot compile re2: {} for http handling rule, error: {}. "
            "Look at https://github.com/google/re2/wiki/Syntax for reference.", expression, compiled_regex->error());

    return compiled_regex;
}

HTTPRequestHandlerFactoryPtr createPredefinedHandlerFactory(IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    std::unordered_map<String, String> & common_headers)
{
    if (!config.has(config_prefix + ".handler.query"))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "There is no path '{}.handler.query' in configuration file.", config_prefix);

    std::string predefined_query = config.getString(config_prefix + ".handler.query");
    NameSet analyze_receive_params = analyzeReceiveQueryParams(predefined_query);

    std::unordered_map<String, CompiledRegexPtr> headers_name_with_regex;
    Poco::Util::AbstractConfiguration::Keys headers_name;
    config.keys(config_prefix + ".headers", headers_name);

    HTTPHandlerConnectionConfig connection_config(config, config_prefix);

    for (const auto & header_name : headers_name)
    {
        auto expression = config.getString(config_prefix + ".headers." + header_name);

        if (!startsWith(expression, "regex:"))
            continue;

        expression = expression.substr(6);
        auto regex = getCompiledRegex(expression);
        if (capturingNamedQueryParam(analyze_receive_params, regex))
            headers_name_with_regex.emplace(std::make_pair(header_name, regex));
    }

    HTTPResponseHeaderSetup http_response_headers_override = parseHTTPResponseHeaders(config, config_prefix);
    if (http_response_headers_override.has_value())
        http_response_headers_override.value().insert(common_headers.begin(), common_headers.end());

    std::shared_ptr<HandlingRuleHTTPHandlerFactory<PredefinedQueryHandler>> factory;

    if (config.has(config_prefix + ".url"))
    {
        auto url_expression = config.getString(config_prefix + ".url");

        if (startsWith(url_expression, "regex:"))
            url_expression = url_expression.substr(6);

        auto regex = getCompiledRegex(url_expression);
        if (capturingNamedQueryParam(analyze_receive_params, regex))
        {
            auto creator = [
                &server,
                analyze_receive_params,
                predefined_query,
                regex,
                headers_name_with_regex,
                http_response_headers_override,
                connection_config]
                -> std::unique_ptr<PredefinedQueryHandler>
            {
                return std::make_unique<PredefinedQueryHandler>(
                    server,
                    connection_config,
                    analyze_receive_params,
                    predefined_query,
                    regex,
                    headers_name_with_regex,
                    http_response_headers_override);
            };
            factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PredefinedQueryHandler>>(std::move(creator));
            factory->addFiltersFromConfig(config, config_prefix);
            return factory;
        }
    }

    auto creator = [
        &server,
        analyze_receive_params,
        predefined_query,
        headers_name_with_regex,
        http_response_headers_override,
        connection_config]
        -> std::unique_ptr<PredefinedQueryHandler>
    {
        return std::make_unique<PredefinedQueryHandler>(
            server,
            connection_config,
            analyze_receive_params,
            predefined_query,
            CompiledRegexPtr{},
            headers_name_with_regex,
            http_response_headers_override);
    };
    factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PredefinedQueryHandler>>(std::move(creator));
    factory->addFiltersFromConfig(config, config_prefix);
    return factory;
}

void HTTPHandler::Output::pushDelayedResults() const
{
    auto * cascade_buffer = typeid_cast<CascadeWriteBuffer *>(out_maybe_delayed_and_compressed.get());
    if (!cascade_buffer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected CascadeWriteBuffer");

    cascade_buffer->finalize();
    auto write_buffers = cascade_buffer->getResultBuffers();

    if (write_buffers.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "At least one buffer is expected to overwrite result into HTTP response");

    ConcatReadBuffer::Buffers read_buffers;
    for (auto & write_buf : write_buffers)
    {
        if (auto * write_buf_concrete = dynamic_cast<TemporaryDataBuffer *>(write_buf.get()))
        {
            if (auto reread_buf = write_buf_concrete->read())
                read_buffers.emplace_back(std::move(reread_buf));
        }

        if (auto * write_buf_concrete = dynamic_cast<IReadableWriteBuffer *>(write_buf.get()))
        {
            if (auto reread_buf = write_buf_concrete->tryGetReadBuffer())
                read_buffers.emplace_back(std::move(reread_buf));
        }
    }

    if (!read_buffers.empty())
    {
        ConcatReadBuffer concat_read_buffer(std::move(read_buffers));
        copyData(concat_read_buffer, *out_maybe_compressed);
    }
}

void HTTPHandler::Output::finalize()
{
    if (finalized)
        return;
    finalized = true;

    if (hasDelayed())
        pushDelayedResults();

    if (out_delayed_and_compressed_holder)
        out_delayed_and_compressed_holder->finalize();
    if (out_compressed_holder)
        out_compressed_holder->finalize();
    if (wrap_compressed_holder)
        wrap_compressed_holder->finalize();
    if (out_holder)
        out_holder->finalize();
}

void HTTPHandler::Output::cancel()
{
    if (canceled)
        return;
    canceled = true;

    if (out_delayed_and_compressed_holder)
        out_delayed_and_compressed_holder->cancel();
    if (out_compressed_holder)
        out_compressed_holder->cancel();
    if (wrap_compressed_holder)
        wrap_compressed_holder->cancel();
    if (out_holder)
        out_holder->cancel();
}
}
