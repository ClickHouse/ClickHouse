#include <Server/PrometheusRequestHandler.h>

#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTP/sendExceptionToHTTPClient.h>
#include <Server/HTTPHandler.h>
#include <Server/IServer.h>
#include <Server/PrometheusMetricsWriter.h>
#include <base/scope_guard.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/String.h>
#include <Poco/URI.h>
#include <Common/logger_useful.h>
#include <Common/maskSensitiveQueryParameters.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>
#include "config.h"

#include <Access/Credentials.h>
#include <Common/CurrentThread.h>
#include <Common/StringUtils.h>
#include <Common/QueryScope.h>
#include <IO/SnappyBasicReadBuffer.h>
#include <IO/SnappyBasicWriteBuffer.h>
#include <IO/ZstdInflatingReadBuffer.h>
#include <IO/Protobuf/ProtobufZeroCopyInputStreamFromReadBuffer.h>
#include <IO/Protobuf/ProtobufZeroCopyOutputStreamFromWriteBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Session.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/authenticateUserByHTTP.h>
#include <Server/HTTP/checkHTTPHeader.h>
#include <Server/HTTP/setReadOnlyIfHTTPMethodIdempotent.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>
#include <Storages/TimeSeries/PrometheusRemoteReadProtocol.h>
#include <Storages/TimeSeries/PrometheusRemoteWriteProtocol.h>
#include <Storages/TimeSeries/PrometheusHTTPProtocolAPI.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 http_response_buffer_size;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_WRITE_TO_OSTREAM;
    extern const int INCOMPATIBLE_SCHEMA;
    extern const int SUPPORT_IS_DISABLED;
    extern const int NOT_IMPLEMENTED;
    extern const int SNAPPY_UNCOMPRESS_FAILED;
    extern const int UNSUPPORTED_MEDIA_TYPE;
    extern const int ZSTD_DECODER_FAILED;
}

/// Base implementation of a prometheus protocol.
class PrometheusRequestHandler::Impl
{
public:
    explicit Impl(PrometheusRequestHandler & parent) : parent_ref(parent) {}
    virtual ~Impl() = default;
    virtual void beforeHandlingRequest(HTTPServerRequest & /* request */) {}
    virtual bool isSettingLikeParameter(const String & /* name */) { return false; }
    virtual void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) = 0;
    virtual void onException() {}

protected:
    PrometheusRequestHandler & parent() { return parent_ref; }
    IServer & server() { return parent().server; }
    const PrometheusRequestHandlerConfig & config() { return parent().config; }
    PrometheusMetricsWriter & metrics_writer() { return *parent().metrics_writer; }
    LoggerPtr log() { return parent().log; }
    WriteBuffer & getOutputStream(HTTPServerResponse & response) { return parent().getOutputStream(response); }

private:
    PrometheusRequestHandler & parent_ref;
};


/// Implementation of the exposing metrics protocol.
class PrometheusRequestHandler::MetricsImpl : public Impl
{
public:
    explicit MetricsImpl(PrometheusRequestHandler & parent) : Impl(parent) {}

    void beforeHandlingRequest(HTTPServerRequest & request) override
    {
        LOG_INFO(log(), "Handling metrics request from {}", request.get("User-Agent"));
        chassert(config().type == PrometheusRequestHandlerConfig::Type::Metrics);
    }

    void handleRequest(HTTPServerRequest & /* request */, HTTPServerResponse & response) override
    {
        response.setContentType("text/plain; version=0.0.4; charset=UTF-8");
        auto & out = getOutputStream(response);

        if (config().expose_info)
            metrics_writer().writeInfo(out);

        if (config().expose_events)
            metrics_writer().writeEvents(out);

        if (config().expose_metrics)
            metrics_writer().writeMetrics(out);

        if (config().expose_asynchronous_metrics)
            metrics_writer().writeAsynchronousMetrics(out, parent().async_metrics);

        if (config().expose_errors)
            metrics_writer().writeErrors(out);

        if (config().expose_histograms)
            metrics_writer().writeHistogramMetrics(out);

        if (config().expose_dimensional_metrics)
            metrics_writer().writeDimensionalMetrics(out);
    }
};


/// Base implementation of a protocol with Context and authentication.
class PrometheusRequestHandler::ImplWithContext : public Impl
{
public:
    explicit ImplWithContext(PrometheusRequestHandler & parent) : Impl(parent), default_settings(server().context()->getSettingsRef()) { }

    virtual void handlingRequestWithContext(HTTPServerRequest & request, HTTPServerResponse & response) = 0;

    /// When true, `handleRequest` parses `application/x-www-form-urlencoded` (and multipart) bodies for POST/PUT.
    /// Must stay false for Write/Read so the raw body stream stays available for protobuf.
    virtual bool shouldParseFormFromRequestBody(const HTTPServerRequest & /* request */) const { return false; }

protected:
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override
    {
        SCOPE_EXIT({
            request_credentials.reset();
            context.reset();
            session.reset();
            params.reset();
        });

        const auto & method = request.getMethod();
        if (shouldParseFormFromRequestBody(request)
            && (method == Poco::Net::HTTPRequest::HTTP_POST || method == Poco::Net::HTTPRequest::HTTP_PUT))
            params = std::make_unique<HTMLForm>(default_settings, request, *request.getStream());
        else
            params = std::make_unique<HTMLForm>(default_settings, request);
        parent().send_stacktrace = config().is_stacktrace_enabled && params->getParsed<bool>("stacktrace", false);

        if (!authenticateUserAndMakeContext(request, response))
            return; /// The user is not authenticated yet, and the HTTP_UNAUTHORIZED response is sent with the "WWW-Authenticate" header,
                    /// and `request_credentials` must be preserved until the next request or until any exception.

        /// Apply `http_response_buffer_size` for the output buffer (0 means use the default).
        auto buffer_size = context->getSettingsRef()[Setting::http_response_buffer_size].value;
        if (buffer_size == 0)
            buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
        parent().http_response_buffer_size = buffer_size;

        /// Initialize query scope.
        QueryScope query_scope;
        if (context)
            query_scope = QueryScope::create(context);

        handlingRequestWithContext(request, response);
    }

    bool authenticateUserAndMakeContext(HTTPServerRequest & request, HTTPServerResponse & response)
    {
        session = std::make_unique<Session>(server().context(), ClientInfo::Interface::PROMETHEUS, request.isSecure());

        if (!authenticateUser(request, response))
            return false;

        makeContext(request);
        return true;
    }

    bool authenticateUser(HTTPServerRequest & request, HTTPServerResponse & response)
    {
        return authenticateUserByHTTP(request, *params, response, *session, request_credentials, config().connection_config, server().context(), log());
    }

    bool isSettingLikeParameter(const String & name) override
    {
        /// Empty parameter appears when URL like ?&a=b or a=b&&c=d. Just skip them for user's convenience.
        if (name.empty())
            return false;

        /// Some parameters (default_format, everything used in the code above) do not belong to the
        /// Settings class.
        static const NameSet reserved_param_names{"user", "password", "quota_key", "stacktrace", "role", "query_id", "database", "table"};
        return !reserved_param_names.contains(name);
    }

    void makeContext(HTTPServerRequest & request)
    {
        context = session->makeQueryContext();

        /// Anything else beside HTTP POST should be readonly queries.
        setReadOnlyIfHTTPMethodIdempotent(context, request.getMethod());

        auto roles = params->getAll("role");
        if (!roles.empty())
            context->setCurrentRoles(roles);

        SettingsChanges settings_changes;
        for (const auto & [key, value] : *params)
        {
            if (isSettingLikeParameter(key))
            {
                /// This query parameter should be considered as a ClickHouse setting.
                settings_changes.push_back({key, value});
            }
        }

        context->checkSettingsConstraints(settings_changes, SettingSource::QUERY);
        context->applySettingsChanges(settings_changes);

        /// Set the query id supplied by the user, if any, and also update the OpenTelemetry fields.
        String query_id = params->get("query_id", request.get("X-ClickHouse-Query-Id", ""));

        /// Sanitize query_id: remove ASCII control characters to prevent CRLF injection
        /// into HTTP response headers (the query_id is reflected in X-ClickHouse-Query-Id).
        std::erase_if(query_id, [](unsigned char c) { return isControlASCII(c) || c == 0x7F; });

        context->setCurrentQueryId(query_id);
    }

    /// Resolves the time series table for the current request. Each of the database and table names comes
    /// either from the configuration or from the URL query parameter 'database' and 'table'.
    /// A query parameter can't override a value set in the configuration.
    /// If the database isn't set, the table name is treated as a possibly-qualified  `database.table` name,
    /// and if the table name is not a qualified name then the database name falls back to "default".
    StorageID getTimeSeriesTableID()
    {
        QualifiedTableName full_name;
        full_name.database = config().time_series_table_name.database;
        full_name.table = config().time_series_table_name.table;

        if (params->has("database"))
        {
            if (!full_name.database.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "The database is set in the configuration of this prometheus handler and cannot be overridden by the 'database' query parameter");
            full_name.database = params->get("database");
        }

        if (params->has("table"))
        {
            if (!full_name.table.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "The table is set in the configuration of this prometheus handler and cannot be overridden by the 'table' query parameter");
            full_name.table = params->get("table");
        }

        if (full_name.table.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The time series table name is not set; specify it in the configuration or in the 'table' query parameter");

        if (full_name.database.empty())
        {
            full_name = QualifiedTableName::parseFromString(full_name.table);
            if (full_name.database.empty())
                full_name.database = "default";
        }

        return StorageID{full_name};
    }

    void onException() override
    {
        // So that the next requests on the connection have to always start afresh in case of exceptions.
        request_credentials.reset();
    }

    const Settings & default_settings;
    std::unique_ptr<HTMLForm> params;
    std::unique_ptr<Session> session;
    std::unique_ptr<Credentials> request_credentials;
    ContextMutablePtr context;
};


/// Implementation of the remote-write protocol.
class PrometheusRequestHandler::WriteImpl : public ImplWithContext
{
public:
    using ImplWithContext::ImplWithContext;

    void beforeHandlingRequest(HTTPServerRequest & request) override
    {
        LOG_INFO(log(), "Handling remote write request from {}", request.get("User-Agent", ""));
        chassert(config().type == PrometheusRequestHandlerConfig::Type::Write
            || config().type == PrometheusRequestHandlerConfig::Type::APIv1);
    }

    void handlingRequestWithContext([[maybe_unused]] HTTPServerRequest & request, [[maybe_unused]] HTTPServerResponse & response) override
    {
#if USE_PROMETHEUS_PROTOBUFS
        /// Unsupported content types and encodings get 415 Unsupported Media Type.
        const String content_type = request.get("Content-Type", "");
        if (content_type != "application/x-protobuf")
            throw Exception(ErrorCodes::UNSUPPORTED_MEDIA_TYPE,
                "HTTP header Content-Type has unsupported value '{}' (must be 'application/x-protobuf')", content_type);

        /// The remote-write 1.0 spec mandates snappy, but some senders can also compress with zstd.
        const String content_encoding = request.get("Content-Encoding", "");
        std::unique_ptr<ReadBuffer> decompressing_buf;
        if (content_encoding == "snappy")
            decompressing_buf = std::make_unique<SnappyBasicReadBuffer>(wrapReadBufferPointer(request.getStream()));
        else if (content_encoding == "zstd")
            decompressing_buf = std::make_unique<ZstdInflatingReadBuffer>(wrapReadBufferPointer(request.getStream()));
        else
            throw Exception(ErrorCodes::UNSUPPORTED_MEDIA_TYPE,
                "HTTP header Content-Encoding has unsupported value '{}' (must be 'snappy' or 'zstd')", content_encoding);

        auto table = DatabaseCatalog::instance().getTable(getTimeSeriesTableID(), context);
        PrometheusRemoteWriteProtocol protocol{table, context};

        prometheus::WriteRequest write_request;

        {
            ProtobufZeroCopyInputStreamFromReadBuffer zero_copy_input_stream{std::move(decompressing_buf)};

            try
            {
                if (!write_request.ParsePartialFromZeroCopyStream(&zero_copy_input_stream))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse WriteRequest");
            }
            catch (const Exception & e)
            {
                /// A body this server cannot decode is the sender's to fix, and a sender resends a 5xx for ever.
                /// The decoders' own codes also cover reading stored files, so only this request retags them.
                if (e.code() == ErrorCodes::SNAPPY_UNCOMPRESS_FAILED || e.code() == ErrorCodes::ZSTD_DECODER_FAILED)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot decode the request body: {}", e.message());
                throw;
            }
        }

        protocol.write(write_request.timeseries(), write_request.metadata());

        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTPStatus::HTTP_NO_CONTENT, Poco::Net::HTTPResponse::HTTP_REASON_NO_CONTENT);
        response.setChunkedTransferEncoding(false);

#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Prometheus remote write protocol is disabled");
#endif
    }
};

/// Implementation of the remote-read protocol.
class PrometheusRequestHandler::ReadImpl : public ImplWithContext
{
public:
    using ImplWithContext::ImplWithContext;

    void beforeHandlingRequest(HTTPServerRequest & request) override
    {
        LOG_INFO(log(), "Handling remote read request from {}", request.get("User-Agent", ""));
        chassert(config().type == PrometheusRequestHandlerConfig::Type::Read
            || config().type == PrometheusRequestHandlerConfig::Type::APIv1);
    }

    void handlingRequestWithContext([[maybe_unused]] HTTPServerRequest & request, [[maybe_unused]] HTTPServerResponse & response) override
    {
#if USE_PROMETHEUS_PROTOBUFS
        checkHTTPHeader(request, "Content-Type", "application/x-protobuf");
        checkHTTPHeader(request, "Content-Encoding", "snappy");

        auto table = DatabaseCatalog::instance().getTable(getTimeSeriesTableID(), context);
        PrometheusRemoteReadProtocol protocol{table, context};

        prometheus::ReadRequest read_request;

        {
            ProtobufZeroCopyInputStreamFromReadBuffer zero_copy_input_stream{
                std::make_unique<SnappyBasicReadBuffer>(wrapReadBufferPointer(request.getStream()))};

            try
            {
                if (!read_request.ParseFromZeroCopyStream(&zero_copy_input_stream))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse ReadRequest");
            }
            catch (const Exception & e)
            {
                if (e.code() == ErrorCodes::SNAPPY_UNCOMPRESS_FAILED)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot decode the request body: {}", e.message());
                throw;
            }
        }

        /// Prometheus remote-read uses raw snappy block compression (not the snappy framing format
        /// used by `SnappyFramedWriteBuffer` for HTTP `Content-Encoding`). Serialize the response
        /// straight into the compression buffer, then drop the `prometheus::ReadResponse` object
        /// tree before finalizing `SnappyBasicWriteBuffer`, so only the accumulated serialized data
        /// (not the object tree) is held while it is compressed into a single raw snappy block.
        response.setContentType("application/x-protobuf");
        response.set("Content-Encoding", "snappy");

        auto & out = getOutputStream(response);
        SnappyBasicWriteBuffer snappy_out(&out);
        {
            prometheus::ReadResponse read_response;

            size_t num_queries = read_request.queries_size();
            for (size_t i = 0; i != num_queries; ++i)
            {
                const auto & query = read_request.queries(static_cast<int>(i));
                auto & new_query_result = *read_response.add_results();
                protocol.readTimeSeries(
                    *new_query_result.mutable_timeseries(),
                    query.start_timestamp_ms(),
                    query.end_timestamp_ms(),
                    query.matchers(),
                    query.hints());
            }

#    if 0
            LOG_DEBUG(log, "ReadResponse = {}", read_response.DebugString());
#    endif

            /// The zero-copy stream is intentionally not finalized here: finalizing it would flush
            /// and compress `snappy_out` while the object tree is still alive. Serialization leaves
            /// all bytes buffered in `snappy_out`; compression happens in `snappy_out.finalize()`
            /// below, after the object tree has been released.
            ProtobufZeroCopyOutputStreamFromWriteBuffer zero_copy_output_stream{snappy_out};
            if (!read_response.SerializeToZeroCopyStream(&zero_copy_output_stream))
                throw Exception(ErrorCodes::CANNOT_WRITE_TO_OSTREAM, "Failed to serialize the Prometheus ReadResponse");
        }
        snappy_out.finalize();

#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Prometheus remote read protocol is disabled");
#endif
    }
};

/// Handles the read-only query and metadata endpoints of the Prometheus HTTP API (/api/v1/query, query_range, series, labels,
/// label/<name>/values, metadata) and the always-empty rules, alerts and query_exemplars endpoints.
class PrometheusRequestHandler::QueryImpl : public ImplWithContext
{
public:
    using ImplWithContext::ImplWithContext;

    bool shouldParseFormFromRequestBody(const HTTPServerRequest & /* request */) const override { return true; }

    void beforeHandlingRequest(HTTPServerRequest & request) override
    {
        LOG_INFO(log(), "Handling Prometheus HTTP API query request from {}", request.get("User-Agent", ""));
        chassert(config().type == PrometheusRequestHandlerConfig::Type::Query
            || config().type == PrometheusRequestHandlerConfig::Type::APIv1);
        empty_result = getEmptyResult(Poco::URI(request.getURI()).getPath());
    }

    bool isSettingLikeParameter(const String & name) override
    {
        /// Empty parameter appears when URL like ?&a=b or a=b&&c=d. Just skip them for user's convenience.
        if (name.empty())
            return false;

        /// The endpoints with an empty result run no query, so their parameters are not settings.
        if (empty_result)
            return false;

        /// Some parameters (default_format, everything used in the code above) do not belong to the
        /// Settings class. `limit` is defined by Prometheus on these endpoints, so it must not fall through to the ClickHouse setting.
        static const NameSet reserved_param_names{"user", "password", "query", "time", "start", "end", "step", "match[]", "limit", "limit_per_metric", "metric", "lookback_delta", "database", "table"};
        return !reserved_param_names.contains(name);
    }

    /// Parses the optional `limit` parameter of the metadata endpoints: the maximum number of returned items,
    /// with 0 (the default) meaning no limit.
    UInt64 getLimitParam() const
    {
        String limit_param = params->get("limit", "");
        if (limit_param.empty())
            return 0;

        Int64 parsed_limit = 0;
        if (!tryParse(parsed_limit, limit_param.data(), limit_param.size()) || (parsed_limit < 0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Invalid value of the 'limit' parameter: '{}', expected a non-negative integer",
                            limit_param);
        return static_cast<UInt64>(parsed_limit);
    }

    void handlingRequestWithContext(HTTPServerRequest & request, HTTPServerResponse & response) override
    {
        const String & uri = request.getURI();
        /// This endpoint accepts user/password (and other secrets) as query-string parameters via
        /// authenticateUserByHTTP, so the URI must be masked before it reaches the logs.
        LOG_DEBUG(log(), "Processing Prometheus HTTP API query request: method={}, uri={}", request.getMethod(), maskSensitiveQueryParametersInURI(uri));

        response.setContentType("application/json");

        try
        {
            /// Dispatch by the trailing path segment only (e.g. "/query_range", "/query"), so the same
            /// endpoint works both bare ("/api/v1/query") and behind a configured prefix ("/prefix/api/v1/query").
            /// Use the decoded path without the query string (matching APIv1Impl::getImpl) so a
            /// percent-encoded label name in ".../label/<name>/values" is read correctly.
            const String uri_path = Poco::URI(uri).getPath();

            if (uri_path.ends_with("/format_query"))
            {
                /// The format_query endpoint only parses and reformats the given PromQL expression,
                /// so it doesn't need the TimeSeries table.
                formatQuery(getOutputStream(response), params->get("query", ""));
                return;
            }

            if (empty_result)
            {
                /// Like Prometheus, query_exemplars answers 204 No Content to an expression without a selector.
                if (!validateEmptyResultRequest(uri_path))
                {
                    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NO_CONTENT, Poco::Net::HTTPResponse::HTTP_REASON_NO_CONTENT);
                    response.setChunkedTransferEncoding(false);
                    return;
                }
                writeString(*empty_result, getOutputStream(response));
                return;
            }

            auto table = DatabaseCatalog::instance().getTable(getTimeSeriesTableID(), context);
            PrometheusHTTPProtocolAPI protocol{table, context};

            auto query_finish_callback = [&]()
            {
                getOutputStream(response).finalize();
            };

            if (uri_path.ends_with("/query_range"))
            {
                String query = params->get("query", "");
                String start = params->get("start", "");
                String end = params->get("end", "");
                String step = params->get("step", "");
                String lookback_delta = params->get("lookback_delta", "");

                /// TODO: Support the following **optional** query parameters:
                /// - timeout=<duration>: Evaluation timeout
                /// - limit=<number>: Maximum number of returned series

                PrometheusHTTPProtocolAPI::Params params
                {
                    .type = PrometheusHTTPProtocolAPI::Type::Range,
                    .promql_query = query,
                    .time_param = "",
                    .start_param = start,
                    .end_param = end,
                    .step_param = step,
                    .lookback_delta_param = lookback_delta,
                };

                protocol.executePromQLQuery(getOutputStream(response), params, query_finish_callback);
            }
            else if (uri_path.ends_with("/query"))
            {
                String query = params->get("query", "");
                String time = params->get("time", "");
                String lookback_delta = params->get("lookback_delta", "");

                /// TODO: Support optional parameters same as for the range query.

                PrometheusHTTPProtocolAPI::Params params
                {
                    .type = PrometheusHTTPProtocolAPI::Type::Instant,
                    .promql_query = query,
                    .time_param = time,
                    .start_param = "",
                    .end_param = "",
                    .step_param = "",
                    .lookback_delta_param = lookback_delta,
                };

                protocol.executePromQLQuery(getOutputStream(response), params, query_finish_callback);
            }
            else if (uri_path.ends_with("/parse_query"))
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The parse_query endpoint is not implemented");
            }
            else if (uri_path.ends_with("/series"))
            {
                Strings match = params->getAll("match[]");
                String start = params->get("start", "");
                String end = params->get("end", "");
                UInt64 limit = getLimitParam();

                protocol.getSeries(getOutputStream(response), match, start, end, limit, query_finish_callback);
            }
            else if (uri_path.ends_with("/metadata"))
            {
                String metric = params->get("metric", "");
                /// Both limit parameters are optional; negative values are accepted and mean "no limit", like in Prometheus.
                Int64 limit = getMetadataLimitParam("limit");
                Int64 limit_per_metric = getMetadataLimitParam("limit_per_metric");

                protocol.getMetadata(getOutputStream(response), metric, limit, limit_per_metric, query_finish_callback);
            }
            else if (uri_path.ends_with("/labels"))
            {
                Strings match = params->getAll("match[]");
                String start = params->get("start", "");
                String end = params->get("end", "");
                UInt64 limit = getLimitParam();

                protocol.getLabels(getOutputStream(response), match, start, end, limit, query_finish_callback);
            }
            else if (auto label_name = extractLabelValuesName(uri_path))
            {
                Strings match = params->getAll("match[]");
                String start = params->get("start", "");
                String end = params->get("end", "");
                UInt64 limit = getLimitParam();

                protocol.getLabelValues(getOutputStream(response), *label_name, match, start, end, limit, query_finish_callback);
            }
            else
            {
                LOG_ERROR(log(), "No matching endpoint found for URI: {}, method: {}", maskSensitiveQueryParametersInURI(uri), request.getMethod());
                response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
                writeString(R"({"status":"error","errorType":"not_found","error":"API endpoint not found"})", getOutputStream(response));
            }
        }
        catch (const Exception & e)
        {
            /// Once the response header has been sent we can no longer produce
            /// a well-formed Prometheus error response. So we let the outer handler
            /// abort the chunked stream via cancelWithException() instead.
            if (response.sent())
                throw;

            /// Drop any partial success body still sitting in the output buffer
            /// before writing the error response.
            getOutputStream(response).rejectBufferedDataSave();

            /// A schema-version rejection (see TimeSeriesVersion.h) is a problem with the server or the table,
            /// not with the query: report it as an internal error so that clients don't attribute it
            /// to the PromQL expression.
            bool server_side_error = (e.code() == ErrorCodes::INCOMPATIBLE_SCHEMA);
            response.setStatusAndReason(
                server_side_error ? Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR : Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
            String error_str;
            WriteBufferFromString error_buf(error_str);
            writeString(server_side_error ? R"({"status":"error","errorType":"internal","error":)"
                                          : R"({"status":"error","errorType":"bad_data","error":)", error_buf);
            writeJSONString(e.message(), error_buf, FormatSettings{});
            writeString("}", error_buf);
            error_buf.finalize();
            writeString(error_str, getOutputStream(response));

            LOG_ERROR(log(), "Error executing query: {}", e.displayText());
        }
    }

private:
    /// ClickHouse has no rules, alerts or exemplars, so these endpoints always return an empty result.
    static std::optional<std::string_view> getEmptyResult(std::string_view uri_path)
    {
        if (uri_path.ends_with("/rules"))
            return R"({"status":"success","data":{"groups":[]}})";
        if (uri_path.ends_with("/alerts"))
            return R"({"status":"success","data":{"alerts":[]}})";
        if (uri_path.ends_with("/query_exemplars"))
            return R"({"status":"success","data":[]})";
        return std::nullopt;
    }

    std::optional<std::string_view> empty_result;

    /// Checks a query_exemplars expression like the Prometheus parser does. Returns false if it has no selector.
    static bool validateExemplarsQuery(const String & query_text)
    {
        using ResultType = PrometheusQueryResultType;
        /// Prometheus functions that can't be converted to SQL yet, with their argument types.
        static const std::unordered_map<std::string_view, std::vector<ResultType>> unsupported_functions = {
            {"sort", {ResultType::INSTANT_VECTOR}},
            {"sort_desc", {ResultType::INSTANT_VECTOR}},
            {"histogram_count", {ResultType::INSTANT_VECTOR}},
            {"histogram_sum", {ResultType::INSTANT_VECTOR}},
            {"histogram_fraction", {ResultType::SCALAR, ResultType::SCALAR, ResultType::INSTANT_VECTOR}},
        };

        PrometheusQueryTree query{query_text};
        std::vector<std::unique_ptr<PrometheusQueryTree::Node>> nodes;
        const auto * root = query.getRoot()->clone(nodes);
        std::vector<std::unique_ptr<PrometheusQueryTree::Node>> constants;
        bool has_selector = false;
        /// `clone` puts every node after its children, so the arguments of a node are rewritten before the node itself.
        for (auto & node : nodes)
        {
            has_selector |= (node->node_type == PrometheusQueryTree::NodeType::InstantSelector);
            replaceParameterWithConstant(*node, constants, query.getTimeScale());
            if (node->node_type != PrometheusQueryTree::NodeType::Function)
                continue;
            auto & function = typeid_cast<PrometheusQueryTree::Function &>(*node);
            auto it = unsupported_functions.find(function.function_name);
            if (it == unsupported_functions.end())
                continue;

            const auto & argument_types = it->second;
            if (function.children.size() != argument_types.size())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function '{}' expects {} arguments, but was called with {} arguments",
                                function.function_name, argument_types.size(), function.children.size());
            for (size_t i = 0; i != argument_types.size(); ++i)
            {
                const auto * argument = function.children[i];
                if (argument->result_type != argument_types[i])
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function '{}' expects an argument of type {}, but expression {} has type {}",
                                    function.function_name, argument_types[i], argument->toString(query), argument->result_type);
            }

            /// Call a convertible function with the same argument types, so the conversion still checks the arguments.
            if (function.function_name == "histogram_fraction")
            {
                function.function_name = "clamp";
                function.children = {function.children[2], function.children[0], function.children[1]};
            }
            else
            {
                function.function_name = "abs";
            }
        }

        checkConversionToSQL(*root, query.getTimeScale());
        return has_selector;
    }

    /// The parser doesn't check the argument types and counts of functions, the conversion to SQL does.
    /// The SQL is never run, so it may name a table that doesn't exist.
    static void checkConversionToSQL(const PrometheusQueryTree::Node & root, UInt32 time_scale)
    {
        std::vector<std::unique_ptr<PrometheusQueryTree::Node>> nodes;
        const auto * new_root = root.clone(nodes);
        PrometheusQueryEvaluationSettings evaluation_settings;
        evaluation_settings.time_series_storage_id = StorageID{"default", "query_exemplars"};
        evaluation_settings.table_timestamp_type = std::make_shared<DataTypeDateTime64>(3);
        evaluation_settings.use_current_time = true;
        auto checked_query = std::make_shared<PrometheusQueryTree>(std::move(nodes), new_root, time_scale);
        PrometheusQueryToSQL::Converter{checked_query, evaluation_settings}.getSQL();
    }

    /// The conversion to SQL supports only some forms of phi of `histogram_quantile` and `quantile` and of the level of
    /// `quantile_over_time`, e.g. not `histogram_quantile(scalar(up), up)`, but Prometheus only checks that they are scalars.
    /// So such a parameter is checked as an expression of its own and replaced with a constant.
    /// A literal is kept, so that error messages show the expression as it was written.
    static void replaceParameterWithConstant(
        PrometheusQueryTree::Node & node, std::vector<std::unique_ptr<PrometheusQueryTree::Node>> & constants, UInt32 time_scale)
    {
        bool has_parameter = false;
        if (node.node_type == PrometheusQueryTree::NodeType::Function)
        {
            const auto & function_name = typeid_cast<const PrometheusQueryTree::Function &>(node).function_name;
            has_parameter = (function_name == "histogram_quantile") || (function_name == "quantile_over_time");
        }
        else if (node.node_type == PrometheusQueryTree::NodeType::AggregationOperator)
        {
            /// The parameter of `quantile`, `topk`, `bottomk`, `limitk` and `count_values` is the first of two arguments.
            has_parameter = (node.children.size() == 2);
        }

        if (!has_parameter || node.children.empty())
            return;

        auto & parameter = node.children[0];
        if ((parameter->result_type != PrometheusQueryResultType::SCALAR) || (parameter->node_type == PrometheusQueryTree::NodeType::Scalar))
            return;

        checkConversionToSQL(*parameter, time_scale);
        auto constant = std::make_unique<PrometheusQueryTree::Scalar>();
        constant->parent = &node;
        parameter = constants.emplace_back(std::move(constant)).get();
    }

    /// Rejects the malformed requests that Prometheus also rejects on the endpoints with an empty result.
    /// Returns false for a query_exemplars expression without a selector, which has no result at all.
    bool validateEmptyResultRequest(std::string_view uri_path) const
    {
        if (uri_path.ends_with("/query_exemplars"))
        {
            String start = params->get("start", "");
            String end = params->get("end", "");
            Int64 start_time = start.empty() ? std::numeric_limits<Int64>::min() : parseTimeSeriesTimestamp(start, 3).value;
            Int64 end_time = end.empty() ? std::numeric_limits<Int64>::max() : parseTimeSeriesTimestamp(end, 3).value;
            if (end_time < start_time)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'end' timestamp must not be before the 'start' timestamp");

            return validateExemplarsQuery(params->get("query", ""));
        }
        else if (uri_path.ends_with("/rules"))
        {
            String type = Poco::toLower(params->get("type", ""));
            if (!type.empty() && type != "alert" && type != "record")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid value of the 'type' parameter: '{}', expected 'alert' or 'record'", type);

            static const NameSet bool_values{"", "1", "t", "true", "0", "f", "false"};
            String exclude_alerts = params->get("exclude_alerts", "");
            if (!bool_values.contains(Poco::toLower(exclude_alerts)))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid value of the 'exclude_alerts' parameter: '{}', expected a boolean", exclude_alerts);

            for (const auto & match : params->getAll("match[]"))
            {
                PrometheusQueryTree selector;
                selector.parse(match);
                const auto * root = selector.getRoot();
                if (!root || (root->node_type != PrometheusQueryTree::NodeType::InstantSelector))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value {} of the 'match[]' parameter is not an instant selector", quoteString(match));
            }

            String group_limit = params->get("group_limit", "");
            String group_next_token = params->get("group_next_token", "");
            Int64 parsed_group_limit = 0;
            if (!group_limit.empty() && (!tryParse(parsed_group_limit, group_limit.data(), group_limit.size()) || parsed_group_limit <= 0))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Invalid value of the 'group_limit' parameter: '{}', expected a positive integer", group_limit);

            /// There are no rule groups, so no token is valid.
            if (!group_next_token.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid value of the 'group_next_token' parameter: '{}'", group_next_token);
        }
        return true;
    }

    /// Handles the format_query endpoint: parses the PromQL expression given in the 'query' parameter
    /// and writes it back serialized from the parsed tree, i.e. with the whitespace normalized,
    /// the comments removed, and the redundant parentheses dropped.
    static void formatQuery(WriteBuffer & out, const String & query)
    {
        PrometheusQueryTree promql_tree;
        promql_tree.parse(query);

        writeString(R"({"status":"success","data":)", out);
        writeJSONString(promql_tree.toString(), out, FormatSettings{});
        writeChar('}', out);
    }

    /// Parses an optional integer parameter of the metadata endpoint; an absent parameter defaults to -1 (no limit).
    Int64 getMetadataLimitParam(const String & name) const
    {
        String value = params->get(name, "");
        if (value.empty())
            return -1;

        Int64 result = 0;
        if (!tryParse(result, value.data(), value.size()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Invalid value of the '{}' parameter: '{}', expected an integer", name, value);
        return result;
    }

    /// Extracts the label name from a label-values endpoint path ".../label/<name>/values".
    /// Returns std::nullopt when `uri_path` isn't a valid label-values endpoint.
    static std::optional<String> extractLabelValuesName(std::string_view uri_path)
    {
        static constexpr std::string_view values_suffix = "/values";
        static constexpr std::string_view label_segment = "/label";

        if (!uri_path.ends_with(values_suffix))
            return std::nullopt;

        /// Strip the "/values" suffix, leaving "<prefix>/label/<name>".
        std::string_view without_values = uri_path.substr(0, uri_path.size() - values_suffix.size());

        /// A label name never contains '/', so it is the last path segment.
        size_t name_slash = without_values.rfind('/');
        if (name_slash == std::string_view::npos)
            return std::nullopt;

        std::string_view label_name = without_values.substr(name_slash + 1);
        if (label_name.empty())
            return std::nullopt;

        /// The segment before the name must be "/label".
        if (!without_values.substr(0, name_slash).ends_with(label_segment))
            return std::nullopt;

        return String{label_name};
    }
};


/// Handles all Prometheus "/api/v1" protocols, dispatching each request to the
/// Write, Read, or Query implementation based on its path.
class PrometheusRequestHandler::APIv1Impl : public Impl
{
public:
    explicit APIv1Impl(PrometheusRequestHandler & parent)
        : Impl(parent)
        , write_impl(parent)
        , read_impl(parent)
        , query_impl(parent)
    {
    }

    void beforeHandlingRequest(HTTPServerRequest & request) override
    {
        chassert(config().type == PrometheusRequestHandlerConfig::Type::APIv1);
        current_impl = &getImpl(request);
        current_impl->beforeHandlingRequest(request);
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override
    {
        /// `current_impl` was selected in beforeHandlingRequest().
        /// Forward the whole request to it so its own authentication, context setup,
        /// and endpoint dispatch run exactly as for a dedicated single-protocol handler.
        current_impl->handleRequest(request, response);
    }

    void onException() override
    {
        if (current_impl)
            current_impl->onException();
    }

private:
    /// Selects the implementation for a request based on the trailing segment of its path,
    /// so the same endpoint works both bare ("/api/v1/write") and behind a configured prefix
    /// ("/prefix/api/v1/write").
    Impl & getImpl(const HTTPServerRequest & request)
    {
        /// Get the decoded URL path (without the query string).
        const String path = Poco::URI(request.getURI()).getPath();

        if (path.ends_with("/write"))
            return write_impl;
        if (path.ends_with("/read"))
            return read_impl;

        /// All other /api/v1/* endpoints (query, query_range, series, labels, label/<name>/values, metadata, rules, alerts,
        /// query_exemplars) are served by the Query implementation, which itself returns 404 for unknown paths.
        return query_impl;
    }

    WriteImpl write_impl;
    ReadImpl read_impl;
    QueryImpl query_impl;
    Impl * current_impl = nullptr;
};


PrometheusRequestHandler::PrometheusRequestHandler(
    IServer & server_,
    const PrometheusRequestHandlerConfig & config_,
    const AsynchronousMetrics & async_metrics_,
    std::shared_ptr<PrometheusMetricsWriter> metrics_writer_,
    std::unordered_map<String, String> response_headers_)
    : server(server_)
    , config(config_)
    , async_metrics(async_metrics_)
    , metrics_writer(metrics_writer_)
    , log(getLogger("PrometheusRequestHandler"))
{
    response_headers = response_headers_;
    createImpl();
}

PrometheusRequestHandler::~PrometheusRequestHandler() = default;

void PrometheusRequestHandler::createImpl()
{
    switch (config.type)
    {
        case PrometheusRequestHandlerConfig::Type::Metrics:
        {
            impl = std::make_unique<MetricsImpl>(*this);
            return;
        }
        case PrometheusRequestHandlerConfig::Type::Write:
        {
            impl = std::make_unique<WriteImpl>(*this);
            return;
        }
        case PrometheusRequestHandlerConfig::Type::Read:
        {
            impl = std::make_unique<ReadImpl>(*this);
            return;
        }
        case PrometheusRequestHandlerConfig::Type::Query:
        {
            impl = std::make_unique<QueryImpl>(*this);
            return;
        }
        case PrometheusRequestHandlerConfig::Type::APIv1:
        {
            impl = std::make_unique<APIv1Impl>(*this);
            return;
        }
    }
    UNREACHABLE();
}

void PrometheusRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event_)
{
    DB::setThreadName(ThreadName::PROMETHEUS_HANDLER);
    applyHTTPResponseHeaders(response, response_headers);

    try
    {
        write_event = write_event_;
        http_method = request.getMethod();
        chassert(!write_buffer_from_response); /// Nothing is written to the response yet.

        /// Make keep-alive works.
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        setResponseDefaultHeaders(response);

        impl->beforeHandlingRequest(request);
        impl->handleRequest(request, response);

        getOutputStream(response).finalize();
    }
    catch (...)
    {
        tryLogCurrentException(log);

        ExecutionStatus status = ExecutionStatus::fromCurrentException("", send_stacktrace);
        getOutputStream(response).cancelWithException(request, status.code, status.message, nullptr);

        tryCallOnException();
    }
}

WriteBufferFromHTTPServerResponse & PrometheusRequestHandler::getOutputStream(HTTPServerResponse & response)
{
    if (write_buffer_from_response)
        return *write_buffer_from_response;

    write_buffer_from_response = std::make_unique<WriteBufferFromHTTPServerResponse>(
        response, http_method == HTTPRequest::HTTP_HEAD, write_event, http_response_buffer_size);

    return *write_buffer_from_response;
}

void PrometheusRequestHandler::tryCallOnException()
{
    try
    {
        if (impl)
            impl->onException();
    }
    catch (...)
    {
        tryLogCurrentException(log, "onException");
    }
}

}
