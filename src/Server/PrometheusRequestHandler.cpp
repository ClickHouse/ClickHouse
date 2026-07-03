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
#include <Poco/URI.h>
#include <Common/logger_useful.h>
#include <Common/maskSensitiveQueryParameters.h>
#include <Common/setThreadName.h>
#include "config.h"

#include <Access/Credentials.h>
#include <Common/CurrentThread.h>
#include <Common/StringUtils.h>
#include <Common/QueryScope.h>
#include <IO/SnappyReadBuffer.h>
#include <IO/SnappyWriteBuffer.h>
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
    extern const int SUPPORT_IS_DISABLED;
    extern const int NOT_IMPLEMENTED;
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
        checkHTTPHeader(request, "Content-Type", "application/x-protobuf");
        checkHTTPHeader(request, "Content-Encoding", "snappy");

        auto table = DatabaseCatalog::instance().getTable(getTimeSeriesTableID(), context);
        PrometheusRemoteWriteProtocol protocol{table, context};

        prometheus::WriteRequest write_request;

        {
            ProtobufZeroCopyInputStreamFromReadBuffer zero_copy_input_stream{
                std::make_unique<SnappyReadBuffer>(wrapReadBufferPointer(request.getStream()))};

            if (!write_request.ParsePartialFromZeroCopyStream(&zero_copy_input_stream))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse WriteRequest");
        }

        if (write_request.timeseries_size())
            protocol.writeTimeSeries(write_request.timeseries());

        if (write_request.metadata_size())
            protocol.writeMetricsMetadata(write_request.metadata());

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
                std::make_unique<SnappyReadBuffer>(wrapReadBufferPointer(request.getStream()))};

            if (!read_request.ParseFromZeroCopyStream(&zero_copy_input_stream))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse ReadRequest");
        }

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

        response.setContentType("application/x-protobuf");
        response.set("Content-Encoding", "snappy");

        ProtobufZeroCopyOutputStreamFromWriteBuffer zero_copy_output_stream{std::make_unique<SnappyWriteBuffer>(getOutputStream(response))};
        read_response.SerializeToZeroCopyStream(&zero_copy_output_stream);
        zero_copy_output_stream.finalize();

#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Prometheus remote read protocol is disabled");
#endif
    }
};

/// Handles the read-only query and metadata endpoints of the Prometheus HTTP API
/// (/api/v1/query, /api/v1/query_range, /api/v1/series, /api/v1/labels, /api/v1/label/<name>/values).
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
    }

    bool isSettingLikeParameter(const String & name) override
    {
        /// Empty parameter appears when URL like ?&a=b or a=b&&c=d. Just skip them for user's convenience.
        if (name.empty())
            return false;

        /// Some parameters (default_format, everything used in the code above) do not belong to the
        /// Settings class.
        static const NameSet reserved_param_names{"user", "password", "query", "time", "start", "end", "step", "database", "table"};
        return !reserved_param_names.contains(name);
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
            auto table = DatabaseCatalog::instance().getTable(getTimeSeriesTableID(), context);
            PrometheusHTTPProtocolAPI protocol{table, context};

            auto query_finish_callback = [&]()
            {
                getOutputStream(response).finalize();
            };

            /// Dispatch by the trailing path segment only (e.g. "/query_range", "/query"), so the same
            /// endpoint works both bare ("/api/v1/query") and behind a configured prefix ("/prefix/api/v1/query").
            /// Use the decoded path without the query string (matching APIv1Impl::getImpl) so a
            /// percent-encoded label name in ".../label/<name>/values" is read correctly.
            const String uri_path = Poco::URI(uri).getPath();

            if (uri_path.ends_with("/query_range"))
            {
                String query = params->get("query", "");
                String start = params->get("start", "");
                String end = params->get("end", "");
                String step = params->get("step", "");

                /// TODO: Support the following **optional** query parameters:
                /// - timeout=<duration>: Evaluation timeout
                /// - limit=<number>: Maximum number of returned series
                /// - lookback_delta=<number>: Override for the lookback period for this query.

                PrometheusHTTPProtocolAPI::Params params
                {
                    .type = PrometheusHTTPProtocolAPI::Type::Range,
                    .promql_query = query,
                    .time_param = "",
                    .start_param = start,
                    .end_param = end,
                    .step_param = step,
                };

                protocol.executePromQLQuery(getOutputStream(response), params, query_finish_callback);
            }
            else if (uri_path.ends_with("/query"))
            {
                String query = params->get("query", "");
                String time = params->get("time", "");

                /// TODO: Support optional parameters same as for the range query.

                PrometheusHTTPProtocolAPI::Params params
                {
                    .type = PrometheusHTTPProtocolAPI::Type::Instant,
                    .promql_query = query,
                    .time_param = time,
                    .start_param = "",
                    .end_param = "",
                    .step_param = "",
                };

                protocol.executePromQLQuery(getOutputStream(response), params, query_finish_callback);
            }
            else if (uri_path.ends_with("/format_query"))
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The format_query endpoint is not implemented");
            }
            else if (uri_path.ends_with("/parse_query"))
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The parse_query endpoint is not implemented");
            }
            else if (uri_path.ends_with("/series"))
            {
                String match = params->get("match[]", "");
                String start = params->get("start", "");
                String end = params->get("end", "");

                /// TODO: Support limit=<number> optional parameter

                protocol.getSeries(getOutputStream(response), match, start, end);
            }
            else if (uri_path.ends_with("/labels"))
            {
                String match = params->get("match[]", "");
                String start = params->get("start", "");
                String end = params->get("end", "");

                protocol.getLabels(getOutputStream(response), match, start, end);
            }
            else if (auto label_name = extractLabelValuesName(uri_path))
            {
                String match = params->get("match[]", "");
                String start = params->get("start", "");
                String end = params->get("end", "");

                protocol.getLabelValues(getOutputStream(response), *label_name, match, start, end);
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

            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
            String error_str;
            WriteBufferFromString error_buf(error_str);
            writeString(R"({"status":"error","errorType":"bad_data","error":)", error_buf);
            writeJSONString(e.message(), error_buf, FormatSettings{});
            writeString("}", error_buf);
            error_buf.finalize();
            writeString(error_str, getOutputStream(response));

            LOG_ERROR(log(), "Error executing query: {}", e.displayText());
        }
    }

private:
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

        /// All other /api/v1/* endpoints (query, query_range, series, labels, label/<name>/values)
        /// are served by the Query implementation, which itself returns 404 for unknown paths.
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
