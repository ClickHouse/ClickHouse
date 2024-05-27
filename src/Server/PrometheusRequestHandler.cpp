#include <Server/PrometheusRequestHandler.h>

#include <Access/Credentials.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <IO/SnappyReadBuffer.h>
#include <IO/Protobuf/ProtobufZeroCopyInputStreamFromReadBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Session.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/authenticateUserByHTTP.h>
#include <Server/HTTP/checkHTTPHeader.h>
#include <Server/HTTP/setReadOnlyIfHTTPMethodIdempotent.h>
#include <Server/IServer.h>
#include <Server/PrometheusRequestHandlerConfig.h>
#include <Storages/TimeSeries/PrometheusRemoteWriteProtocol.h>
#include "config.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}


PrometheusRequestHandler::PrometheusRequestHandler(IServer & server_, const PrometheusRequestHandlerConfigPtr & config_, const AsynchronousMetrics & async_metrics_)
    : PrometheusMetricsOnlyRequestHandler(server_, config_, async_metrics_)
    , default_settings(server_.context()->getSettingsRef())
{
}

PrometheusRequestHandler::~PrometheusRequestHandler() = default;


void PrometheusRequestHandler::handleMetrics(HTTPServerRequest & request, HTTPServerResponse & response)
{
    wrapHandler(request, response, /* authenticate= */ false, [&] { PrometheusMetricsOnlyRequestHandler::handleMetrics(request, response); });
}

void PrometheusRequestHandler::handleRemoteWrite(HTTPServerRequest & request, HTTPServerResponse & response)
{
    wrapHandler(request, response, /* authenticate= */ true, [&] { handleRemoteWriteImpl(request, response); });
}


void PrometheusRequestHandler::wrapHandler(HTTPServerRequest & request, HTTPServerResponse & response, bool authenticate, std::function<void()> && func)
{
    SCOPE_EXIT({
        request_credentials.reset();
        context.reset();
        session.reset();
        params.reset();
    });

    params = std::make_unique<HTMLForm>(default_settings, request);
    send_stacktrace = config->is_stacktrace_enabled && params->getParsed<bool>("stacktrace", false);

    if (authenticate)
    {
        if (!authenticateUserAndMakeContext(request, response))
            return; /// The user is not authenticated yet, and the HTTP_UNAUTHORIZED response is sent with the "WWW-Authenticate" header,
                    /// and `request_credentials` must be preserved until the next request or until any exception.
    }

    /// Initialize query scope.
    std::optional<CurrentThread::QueryScope> query_scope;
    if (context)
        query_scope.emplace(context);

    std::move(func)();
}


void PrometheusRequestHandler::handleRemoteWriteImpl(HTTPServerRequest & request, [[maybe_unused]] HTTPServerResponse & response)
{
    LOG_INFO(log, "Handling remote write request from {}", request.get("User-Agent", ""));

#if USE_PROMETHEUS_PROTOBUFS
    chassert(config->remote_write);
    const auto & remote_write_config = *config->remote_write;

    checkHTTPHeader(request, "Content-Type", "application/x-protobuf");
    checkHTTPHeader(request, "Content-Encoding", "snappy");

    ProtobufZeroCopyInputStreamFromReadBuffer zero_copy_input_stream{std::make_unique<SnappyReadBuffer>(wrapReadBufferReference(request.getStream()))};

    prometheus::WriteRequest write_request;
    if (!write_request.ParsePartialFromZeroCopyStream(&zero_copy_input_stream))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse WriteRequest");

    auto table = DatabaseCatalog::instance().getTable(StorageID{remote_write_config.table_name}, context);
    PrometheusRemoteWriteProtocol protocol{table, context};

    if (write_request.timeseries_size())
        protocol.writeTimeSeries(write_request.timeseries());

    if (write_request.metadata_size())
        protocol.writeMetricsMetadata(write_request.metadata());

    response.setContentType("text/plain; charset=UTF-8");
    response.send();

#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Prometheus remote write protocol is disabled");
#endif
}


bool PrometheusRequestHandler::authenticateUserAndMakeContext(HTTPServerRequest & request, HTTPServerResponse & response)
{
    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::PROMETHEUS, request.isSecure());

    if (!authenticateUser(request, response))
        return false;

    makeContext(request);
    return true;
}


bool PrometheusRequestHandler::authenticateUser(HTTPServerRequest & request, HTTPServerResponse & response)
{
    return authenticateUserByHTTP(request, *params, response, *session, request_credentials, server.context(), log);
}


void PrometheusRequestHandler::makeContext(HTTPServerRequest & request)
{
    context = session->makeQueryContext();

    /// Anything else beside HTTP POST should be readonly queries.
    setReadOnlyIfHTTPMethodIdempotent(context, request.getMethod());

    auto roles = params->getAll("role");
    if (!roles.empty())
        context->setCurrentRoles(roles);

    auto param_could_be_skipped = [&] (const String & name)
    {
        /// Empty parameter appears when URL like ?&a=b or a=b&&c=d. Just skip them for user's convenience.
        if (name.empty())
            return true;

        /// Some parameters (database, default_format, everything used in the code above) do not
        /// belong to the Settings class.
        static const NameSet reserved_param_names{"user", "password", "quota_key", "stacktrace", "role", "query_id"};
        return reserved_param_names.contains(name);
    };

    /// Settings can be overridden in the query.
    SettingsChanges settings_changes;
    for (const auto & [key, value] : *params)
    {
        if (!param_could_be_skipped(key))
        {
            /// Other than query parameters are treated as settings.
            settings_changes.push_back({key, value});
        }
    }

    context->checkSettingsConstraints(settings_changes, SettingSource::QUERY);
    context->applySettingsChanges(settings_changes);

    /// Set the query id supplied by the user, if any, and also update the OpenTelemetry fields.
    context->setCurrentQueryId(params->get("query_id", request.get("X-ClickHouse-Query-Id", "")));
}


void PrometheusRequestHandler::onException()
{
    // So that the next requests on the connection have to always start afresh in case of exceptions.
    request_credentials.reset();
}

}
