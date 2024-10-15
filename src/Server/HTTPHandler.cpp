#include <Server/HTTPHandler.h>

#include <Access/Credentials.h>
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
#include <Formats/FormatFactory.h>

#include <base/getFQDNOrHostName.h>
#include <base/scope_guard.h>
#include <Server/HTTP/HTTPResponse.h>
#include <Server/HTTP/authenticateUserByHTTP.h>
#include <Server/HTTP/sendExceptionToHTTPClient.h>
#include <Server/HTTP/setReadOnlyIfHTTPMethodIdempotent.h>
#include <boost/container/flat_set.hpp>

#include <Poco/Net/HTTPMessage.h>

#include "config.h"

#include <algorithm>
#include <chrono>
#include <memory>
#include <optional>
#include <sstream>
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
    extern const int HTTP_LENGTH_REQUIRED;
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


void HTTPHandler::pushDelayedResults(Output & used_output)
{
    std::vector<WriteBufferPtr> write_buffers;
    ConcatReadBuffer::Buffers read_buffers;

    auto * cascade_buffer = typeid_cast<CascadeWriteBuffer *>(used_output.out_maybe_delayed_and_compressed);
    if (!cascade_buffer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected CascadeWriteBuffer");

    cascade_buffer->getResultBuffers(write_buffers);

    if (write_buffers.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "At least one buffer is expected to overwrite result into HTTP response");

    for (auto & write_buf : write_buffers)
    {
        if (!write_buf)
            continue;

        IReadableWriteBuffer * write_buf_concrete = dynamic_cast<IReadableWriteBuffer *>(write_buf.get());
        if (write_buf_concrete)
        {
            ReadBufferPtr reread_buf = write_buf_concrete->tryGetReadBuffer();
            if (reread_buf)
                read_buffers.emplace_back(wrapReadBufferPointer(reread_buf));
        }
    }

    if (!read_buffers.empty())
    {
        ConcatReadBuffer concat_read_buffer(std::move(read_buffers));
        copyData(concat_read_buffer, *used_output.out_maybe_compressed);
    }
}


HTTPHandler::HTTPHandler(IServer & server_, const std::string & name, const HTTPResponseHeaderSetup & http_response_headers_override_)
    : server(server_)
    , log(getLogger(name))
    , default_settings(server.context()->getSettingsRef())
    , http_response_headers_override(http_response_headers_override_)
{
    server_display_name = server.config().getString("display_name", getFQDNOrHostName());
}


/// We need d-tor to be present in this translation unit to make it play well with some
/// forward decls in the header. Other than that, the default d-tor would be OK.
HTTPHandler::~HTTPHandler() = default;


bool HTTPHandler::authenticateUser(HTTPServerRequest & request, HTMLForm & params, HTTPServerResponse & response)
{
    return authenticateUserByHTTP(request, params, response, *session, request_credentials, server.context(), log);
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

    SCOPE_EXIT({ session->releaseSessionID(); });

    String session_id;
    std::chrono::steady_clock::duration session_timeout;
    bool session_is_set = params.has("session_id");
    const auto & config = server.config();

    if (session_is_set)
    {
        session_id = params.get("session_id");
        session_timeout = parseSessionTimeout(config, params);
        std::string session_check = params.get("session_check", "");
        session->makeSessionContext(session_id, session_timeout, session_check == "1");
    }
    else
    {
        /// We should create it even if we don't have a session_id
        session->makeSessionContext();
    }

    auto context = session->makeQueryContext();

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
    bool internal_compression = params.getParsed<bool>("compress", false);

    /// At least, we should postpone sending of first buffer_size result bytes
    size_t buffer_size_total = std::max(
        params.getParsed<size_t>("buffer_size", context->getSettingsRef()[Setting::http_response_buffer_size]),
        static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE));

    /// If it is specified, the whole result will be buffered.
    ///  First ~buffer_size bytes will be buffered in memory, the remaining bytes will be stored in temporary file.
    bool buffer_until_eof = params.getParsed<bool>("wait_end_of_query", context->getSettingsRef()[Setting::http_wait_end_of_query]);

    size_t buffer_size_http = DBMS_DEFAULT_BUFFER_SIZE;
    size_t buffer_size_memory = (buffer_size_total > buffer_size_http) ? buffer_size_total : 0;

    bool enable_http_compression = params.getParsed<bool>("enable_http_compression", context->getSettingsRef()[Setting::enable_http_compression]);
    Int64 http_zlib_compression_level
        = params.getParsed<Int64>("http_zlib_compression_level", context->getSettingsRef()[Setting::http_zlib_compression_level]);

    used_output.out_holder =
        std::make_shared<WriteBufferFromHTTPServerResponse>(
            response,
            request.getMethod() == HTTPRequest::HTTP_HEAD,
            write_event);
    used_output.out = used_output.out_holder;
    used_output.out_maybe_compressed = used_output.out_holder;

    if (client_supports_http_compression && enable_http_compression)
    {
        used_output.out_holder->setCompressionMethodHeader(http_response_compression_method);
        used_output.wrap_compressed_holder = wrapWriteBufferWithCompressionMethod(
            used_output.out_holder.get(),
            http_response_compression_method,
            static_cast<int>(http_zlib_compression_level),
            0,
            DBMS_DEFAULT_BUFFER_SIZE,
            nullptr,
            0,
            false);
        used_output.out = used_output.wrap_compressed_holder;
    }

    if (internal_compression)
    {
        used_output.out_compressed_holder = std::make_shared<CompressedWriteBuffer>(*used_output.out);
        used_output.out_maybe_compressed = used_output.out_compressed_holder;
    }
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
            auto tmp_data = std::make_shared<TemporaryDataOnDisk>(server.context()->getTempDataOnDisk());

            auto create_tmp_disk_buffer = [tmp_data] (const WriteBufferPtr &) -> WriteBufferPtr {
                return tmp_data->createRawStream();
            };

            cascade_buffer2.emplace_back(std::move(create_tmp_disk_buffer));
        }
        else
        {
            auto push_memory_buffer_and_continue = [next_buffer = used_output.out_maybe_compressed] (const WriteBufferPtr & prev_buf)
            {
                auto * prev_memory_buffer = typeid_cast<MemoryWriteBuffer *>(prev_buf.get());
                if (!prev_memory_buffer)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected MemoryWriteBuffer");

                auto rdbuf = prev_memory_buffer->tryGetReadBuffer();
                copyData(*rdbuf, *next_buffer);

                return next_buffer;
            };

            cascade_buffer2.emplace_back(push_memory_buffer_and_continue);
        }

        used_output.out_delayed_and_compressed_holder = std::make_unique<CascadeWriteBuffer>(std::move(cascade_buffer1), std::move(cascade_buffer2));
        used_output.out_maybe_delayed_and_compressed = used_output.out_delayed_and_compressed_holder.get();
    }
    else
    {
        used_output.out_maybe_delayed_and_compressed = used_output.out_maybe_compressed.get();
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
    if (params.getParsed<bool>("decompress", false))
    {
        in_post_maybe_compressed = std::make_unique<CompressedReadBuffer>(*in_post, /* allow_different_codecs_ = */ false, /* external_data_ = */ true);
        is_in_post_compressed = true;
    }
    else
        in_post_maybe_compressed = std::move(in_post);

    std::unique_ptr<ReadBuffer> in;

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
                settings_changes.push_back({key, value});
        }
    }

    context->checkSettingsConstraints(settings_changes, SettingSource::QUERY);
    context->applySettingsChanges(settings_changes);
    const auto & settings = context->getSettingsRef();

    /// Set the query id supplied by the user, if any, and also update the OpenTelemetry fields.
    context->setCurrentQueryId(params.get("query_id", request.get("X-ClickHouse-Query-Id", "")));

    /// Initialize query scope, once query_id is initialized.
    /// (To track as much allocations as possible)
    query_scope.emplace(context);

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
    in = has_external_data ? std::move(in_param) : std::make_unique<ConcatReadBuffer>(*in_param, *in_post_maybe_compressed);

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
    };

    auto handle_exception_in_output_format = [&](IOutputFormat & current_output_format,
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
            if (buffer_until_eof)
            {
                auto header = current_output_format.getPort(IOutputFormat::PortKind::Main).getHeader();
                used_output.exception_writer = [format_name, header, context_, format_settings](WriteBuffer & buf, const String & message)
                {
                    auto output_format = FormatFactory::instance().getOutputFormat(format_name, buf, header, context_, format_settings);
                    output_format->setException(message);
                    output_format->finalize();
                };
            }
            else
            {
                bool with_stacktrace = (params.getParsed<bool>("stacktrace", false) && server.config().getBool("enable_http_stacktrace", true));
                ExecutionStatus status = ExecutionStatus::fromCurrentException("", with_stacktrace);
                setHTTPResponseStatusAndHeadersForException(status.code, request, response, used_output.out_holder.get(), log);
                current_output_format.setException(status.message);
                current_output_format.finalize();
                used_output.exception_is_written = true;
            }
        }
    };

    executeQuery(
        *in,
        *used_output.out_maybe_delayed_and_compressed,
        /* allow_into_outfile = */ false,
        context,
        set_query_result,
        QueryFlags{},
        {},
        handle_exception_in_output_format);

    session->releaseSessionID();

    if (used_output.hasDelayed())
    {
        /// TODO: set Content-Length if possible
        pushDelayedResults(used_output);
    }

    /// Send HTTP headers with code 200 if no exception happened and the data is still not sent to the client.
    used_output.finalize();
}

void HTTPHandler::trySendExceptionToClient(
    const std::string & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response, Output & used_output)
try
{
    setHTTPResponseStatusAndHeadersForException(exception_code, request, response, used_output.out_holder.get(), log);

    if (!used_output.out_holder && !used_output.exception_is_written)
    {
        /// If nothing was sent yet and we don't even know if we must compress the response.
        WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD).writeln(s);
    }
    else if (used_output.out_maybe_compressed)
    {
        /// Destroy CascadeBuffer to actualize buffers' positions and reset extra references
        if (used_output.hasDelayed())
        {
            /// do not call finalize here for CascadeWriteBuffer used_output.out_maybe_delayed_and_compressed,
            /// exception is written into used_output.out_maybe_compressed later
            /// HTTPHandler::trySendExceptionToClient is called with exception context, it is Ok to destroy buffers
            used_output.out_delayed_and_compressed_holder.reset();
            used_output.out_maybe_delayed_and_compressed = nullptr;
        }

        if (!used_output.exception_is_written)
        {
            /// Send the error message into already used (and possibly compressed) stream.
            /// Note that the error message will possibly be sent after some data.
            /// Also HTTP code 200 could have already been sent.

            /// If buffer has data, and that data wasn't sent yet, then no need to send that data
            bool data_sent = used_output.out_holder->count() != used_output.out_holder->offset();

            if (!data_sent)
            {
                used_output.out_maybe_compressed->position() = used_output.out_maybe_compressed->buffer().begin();
                used_output.out_holder->position() = used_output.out_holder->buffer().begin();
            }

            /// We might have special formatter for exception message.
            if (used_output.exception_writer)
            {
                used_output.exception_writer(*used_output.out_maybe_compressed, s);
            }
            else
            {
                writeString(s, *used_output.out_maybe_compressed);
                writeChar('\n', *used_output.out_maybe_compressed);
            }
        }

        used_output.out_maybe_compressed->next();
    }
    else
    {
        UNREACHABLE();
    }

    used_output.finalize();
}
catch (...)
{
    tryLogCurrentException(log, "Cannot send exception to client");

    used_output.cancel();
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
    /// Close http session (if any) after processing the request
    bool close_session = false;
    String session_id;

    SCOPE_EXIT_SAFE({
        if (close_session && !session_id.empty())
            session->closeSession(session_id);
    });

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

        response.setContentType("text/plain; charset=UTF-8");
        response.set("X-ClickHouse-Server-Display-Name", server_display_name);

        if (!request.get("Origin", "").empty())
            tryAddHTTPOptionHeadersFromConfig(response, server.config());

        /// For keep-alive to work.
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        HTMLForm params(default_settings, request);

        if (params.getParsed<bool>("stacktrace", false) && server.config().getBool("enable_http_stacktrace", true))
            with_stacktrace = true;

        if (params.getParsed<bool>("close_session", false) && server.config().getBool("enable_http_close_session", true))
            close_session = true;

        if (close_session)
            session_id = params.get("session_id");

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

        /// Check if exception was thrown in used_output.finalize().
        /// In this case used_output can be in invalid state and we
        /// cannot write in it anymore. So, just log this exception.
        if (used_output.isFinalized() || used_output.isCanceled())
        {
            if (thread_trace_context)
                thread_trace_context->root_span.addAttribute("clickhouse.exception", "Cannot flush data to client");

            tryLogCurrentException(log, "Cannot flush data to client");
            return;
        }

        tryLogCurrentException(log);

        /** If exception is received from remote server, then stack trace is embedded in message.
          * If exception is thrown on local server, then stack trace is in separate field.
          */
        ExecutionStatus status = ExecutionStatus::fromCurrentException("", with_stacktrace);
        trySendExceptionToClient(status.message, status.code, request, response, used_output);

        if (thread_trace_context)
            thread_trace_context->root_span.addAttribute(status);

        return;
    }

    used_output.finalize();
}

DynamicQueryHandler::DynamicQueryHandler(
    IServer & server_, const std::string & param_name_, const HTTPResponseHeaderSetup & http_response_headers_override_)
    : HTTPHandler(server_, "DynamicQueryHandler", http_response_headers_override_), param_name(param_name_)
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
    const NameSet & receive_params_,
    const std::string & predefined_query_,
    const CompiledRegexPtr & url_regex_,
    const std::unordered_map<String, CompiledRegexPtr> & header_name_with_regex_,
    const HTTPResponseHeaderSetup & http_response_headers_override_)
    : HTTPHandler(server_, "PredefinedQueryHandler", http_response_headers_override_)
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

        std::string_view matches[num_captures];
        std::string_view input(begin, end - begin);
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
    const std::string & config_prefix)
{
    auto query_param_name = config.getString(config_prefix + ".handler.query_param_name", "query");

    HTTPResponseHeaderSetup http_response_headers_override = parseHTTPResponseHeaders(config, config_prefix);

    auto creator = [&server, query_param_name, http_response_headers_override]() -> std::unique_ptr<DynamicQueryHandler>
    { return std::make_unique<DynamicQueryHandler>(server, query_param_name, http_response_headers_override); };

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
    const std::string & config_prefix)
{
    if (!config.has(config_prefix + ".handler.query"))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "There is no path '{}.handler.query' in configuration file.", config_prefix);

    std::string predefined_query = config.getString(config_prefix + ".handler.query");
    NameSet analyze_receive_params = analyzeReceiveQueryParams(predefined_query);

    std::unordered_map<String, CompiledRegexPtr> headers_name_with_regex;
    Poco::Util::AbstractConfiguration::Keys headers_name;
    config.keys(config_prefix + ".headers", headers_name);

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
                http_response_headers_override]
                -> std::unique_ptr<PredefinedQueryHandler>
            {
                return std::make_unique<PredefinedQueryHandler>(
                    server, analyze_receive_params, predefined_query, regex,
                    headers_name_with_regex, http_response_headers_override);
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
        http_response_headers_override]
        -> std::unique_ptr<PredefinedQueryHandler>
    {
        return std::make_unique<PredefinedQueryHandler>(
            server, analyze_receive_params, predefined_query, CompiledRegexPtr{},
            headers_name_with_regex, http_response_headers_override);
    };

    factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PredefinedQueryHandler>>(std::move(creator));

    factory->addFiltersFromConfig(config, config_prefix);

    return factory;
}

}
