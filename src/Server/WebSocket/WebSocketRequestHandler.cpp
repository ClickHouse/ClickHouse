#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/SettingsChanges.h>
#include <Compression/CompressedReadBuffer.h>
#include <Server/WebSocket/WebSocketRequestHandler.h>

namespace DB
{


void WebSocketRequestHandler::processQuery(
    Poco::JSON::Object & request,
    WriteBuffer & output,
    std::optional<CurrentThread::QueryScope> & query_scope
)
{
    // TODO: make appropriate changes to make this thing work
//    using namespace Poco::Net;

//    /// The user could specify session identifier and session timeout.
//    /// It allows to modify settings, create temporary tables and reuse them in subsequent requests.
//     const auto & config = server.config();
//

    ///temporary solution should be changed cause
    /// The user could hold one session in one webSocket connection and maybe between them.
    /// It allows to modify settings, create temporary tables and reuse them in subsequent requests.
      session->makeSessionContext();
      auto client_info = session->getClientInfo();
      auto context = session->makeQueryContext(std::move(client_info));

      query_scope.emplace(context);
//    }
//
//    auto client_info = session->getClientInfo();
//    auto context = session->makeQueryContext(std::move(client_info));
//
//    /// This parameter is used to tune the behavior of output formats (such as Native) for compatibility.
//    if (params.has("client_protocol_version"))
//    {
//        UInt64 version_param = parse<UInt64>(params.get("client_protocol_version"));
//        context->setClientProtocolVersion(version_param);
//    }
//
//    /// The client can pass a HTTP header indicating supported compression method (gzip or deflate).
//    String http_response_compression_methods = request.get("Accept-Encoding", "");
//    CompressionMethod http_response_compression_method = CompressionMethod::None;
//
//    if (!http_response_compression_methods.empty())
//        http_response_compression_method = chooseHTTPCompressionMethod(http_response_compression_methods);
/////TODO: Remove This Crutch while all parameters will be implemented
//#pragma clang diagnostic ignored "-Wunused-variable"
//
//    bool client_supports_http_compression = http_response_compression_method != CompressionMethod::None;
//
//    /// Client can pass a 'compress' flag in the query string. In this case the query result is
//    /// compressed using internal algorithm. This is not reflected in HTTP headers.
//    bool internal_compression = params.getParsed<bool>("compress", false);
//
//    /// At least, we should postpone sending of first buffer_size result bytes
//    size_t buffer_size_total = std::max(
//        params.getParsed<size_t>("buffer_size", context->getSettingsRef().http_response_buffer_size),
//        static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE));
//
//    /// If it is specified, the whole result will be buffered.
//    ///  First ~buffer_size bytes will be buffered in memory, the remaining bytes will be stored in temporary file.
//    bool buffer_until_eof = params.getParsed<bool>("wait_end_of_query", context->getSettingsRef().http_wait_end_of_query);
//
//    size_t buffer_size_http = DBMS_DEFAULT_BUFFER_SIZE;
//    size_t buffer_size_memory = (buffer_size_total > buffer_size_http) ? buffer_size_total : 0;
//
//    unsigned keep_alive_timeout = config.getUInt("keep_alive_timeout", 10);
//
//    ///TODO: compression support
//
//    ///TODO:buffering support
//
//    /// Request body can be compressed using algorithm specified in the Content-Encoding header.
//    String http_request_compression_method_str = request.get("Content-Encoding", "");
//    int zstd_window_log_max = static_cast<int>(context->getSettingsRef().zstd_window_log_max);
//    auto in_post = wrapReadBufferWithCompressionMethod(
//        wrapReadBufferReference(request.getStream()),
//        chooseCompressionMethod({}, http_request_compression_method_str), zstd_window_log_max);
//
//    /// The data can also be compressed using incompatible internal algorithm. This is indicated by
//    /// 'decompress' query parameter.
//    std::unique_ptr<ReadBuffer> in_post_maybe_compressed;
//    bool in_post_compressed = false;
//    if (params.getParsed<bool>("decompress", false))
//    {
//        in_post_maybe_compressed = std::make_unique<CompressedReadBuffer>(*in_post);
//        in_post_compressed = true;
//    }
//    else
//        in_post_maybe_compressed = std::move(in_post);
//
//    std::unique_ptr<ReadBuffer> in;
//
//    static const NameSet reserved_param_names{"compress", "decompress", "user", "password", "quota_key", "query_id", "stacktrace",
//                                              "buffer_size", "wait_end_of_query", "session_id", "session_timeout", "session_check", "client_protocol_version", "close_session"};
//
//    Names reserved_param_suffixes;
//
//    auto param_could_be_skipped = [&] (const String & name)
//    {
//        /// Empty parameter appears when URL like ?&a=b or a=b&&c=d. Just skip them for user's convenience.
//        if (name.empty())
//            return true;
//
//        if (reserved_param_names.contains(name))
//            return true;
//
//        for (const String & suffix : reserved_param_suffixes)
//        {
//            if (endsWith(name, suffix))
//                return true;
//        }
//
//        return false;
//    };
//
//    /// Settings can be overridden in the query.
//    /// Some parameters (database, default_format, everything used in the code above) do not
//    /// belong to the Settings class.
//
//    /// 'readonly' setting values mean:
//    /// readonly = 0 - any query is allowed, client can change any setting.
//    /// readonly = 1 - only readonly queries are allowed, client can't change settings.
//    /// readonly = 2 - only readonly queries are allowed, client can change any setting except 'readonly'.
//
//    /// In theory if initially readonly = 0, the client can change any setting and then set readonly
//    /// to some other value.
//    const auto & settings = context->getSettingsRef();
//
//    /// Only readonly queries are allowed for HTTP GET requests.
//    if (request.getMethod() == HTTPServerRequest::HTTP_GET)
//    {
//        if (settings.readonly == 0)
//            context->setSetting("readonly", 2);
//    }
//
//    bool has_external_data = false;//startsWith(request.getContentType(), "multipart/form-data");
//
//    if (has_external_data)
//    {
//        /// Skip unneeded parameters to avoid confusing them later with context settings or query parameters.
//        reserved_param_suffixes.reserve(3);
//        /// It is a bug and ambiguity with `date_time_input_format` and `low_cardinality_allow_in_native_format` formats/settings.
//        reserved_param_suffixes.emplace_back("_format");
//        reserved_param_suffixes.emplace_back("_types");
//        reserved_param_suffixes.emplace_back("_structure");
//    }
//
//    std::string database = request.get("X-ClickHouse-Database", "");
//    std::string default_format = request.get("X-ClickHouse-Format", "");
//
//    SettingsChanges settings_changes;
//    for (const auto & [key, value] : params)
//    {
//        if (key == "database")
//        {
//            if (database.empty())
//                database = value;
//        }
//        else if (key == "default_format")
//        {
//            if (default_format.empty())
//                default_format = value;
//        }
//        else if (param_could_be_skipped(key))
//        {
//        }
//        else
//        {
//            /// Other than query parameters are treated as settings.
////            if (!customizeQueryParam(context, key, value))
////                settings_changes.push_back({key, value});
//        }
//    }
//
//    if (!database.empty())
//        context->setCurrentDatabase(database);
//
//    if (!default_format.empty())
//        context->setDefaultFormat(default_format);
//
//    /// For external data we also want settings
//    context->checkSettingsConstraints(settings_changes);
//    context->applySettingsChanges(settings_changes);
//
//    /// Set the query id supplied by the user, if any, and also update the OpenTelemetry fields.
//    context->setCurrentQueryId(params.get("query_id", request.get("X-ClickHouse-Query-Id", "")));
//
//    /// Initialize query scope, once query_id is initialized.
//    /// (To track as much allocations as possible)
//    query_scope.emplace(context);
//
//    /// NOTE: this may create pretty huge allocations that will not be accounted in trace_log,
//    /// because memory_profiler_sample_probability/memory_profiler_step are not applied yet,
//    /// they will be applied in ProcessList::insert() from executeQuery() itself.
////    const auto & query = getQuery(request, params, context);
////    std::unique_ptr<ReadBuffer> in_param = std::make_unique<ReadBufferFromString>(query);
//
//
//    /// If 'http_native_compression_disable_checksumming_on_decompress' setting is turned on,
//    /// checksums of client data compressed with internal algorithm are not checked.
//    if (in_post_compressed && settings.http_native_compression_disable_checksumming_on_decompress)
//        static_cast<CompressedReadBuffer &>(*in_post_maybe_compressed).disableChecksumming();
//
//
//    auto append_callback = [context = context] (ProgressCallback callback)
//    {
//        auto prev = context->getProgressCallback();
//
//        context->setProgressCallback([prev, callback] (const Progress & progress)
//                                     {
//                                         if (prev)
//                                             prev(progress);
//
//                                         callback(progress);
//                                     });
//    };
//
//    /// While still no data has been sent, we will report about query execution progress by sending HTTP headers.
//    /// Note that we add it unconditionally so the progress is available for `X-ClickHouse-Summary`
//
//    if (settings.readonly > 0 && settings.cancel_http_readonly_queries_on_client_close)
//    {
//        append_callback([&context, &request](const Progress &)
//                        {
//                            /// Assume that at the point this method is called no one is reading data from the socket any more:
//                            /// should be true for read-only queries.
//                            if (!request.checkPeerConnected())
//                                context->killCurrentQuery();
//                        });
//    }
//
////    customizeContext(request, context, *in_post_maybe_compressed);
////    in = has_external_data ? std::move(in_param) : std::make_unique<ConcatReadBuffer>(*in_param, web_socket_input);
//
//    ///TODO: add some result details callback
//    /// TODO change web_socket_input to in


    ReadBufferFromOwnString input(request.get("data").toString());

    executeQuery(input, output, /* allow_into_outfile = */ false, context, nullptr);
}

void WebSocketRequestHandler::handleRequest(Poco::JSON::Object & request, DB::WebSocket & webSocket)
{
    auto data = request.get("data").extract<std::string>();
    WriteBufferFromWebSocket output(webSocket, true);

    std::optional<CurrentThread::QueryScope> query_scope;


    processQuery(request, output, query_scope);
    //webSocket.sendFrame(data.c_str(), std::min(std::max(0,static_cast<int>(data.size())), 10000000), 0x81);

}

}
