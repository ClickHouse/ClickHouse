#include <Server/StatelessWorker/StatelessWorkerClient.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterserverCredentials.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/HTTPHeaderEntries.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Core/ProtocolDefines.h>
#include <base/types.h>
#include <fmt/format.h>

namespace DB
{

namespace
{

/// W3C `traceparent`/`tracestate` headers carrying the current trace context to the worker, so the
/// task runs inside the initiator's trace. Empty when tracing is off. Headers (not a task field) keep
/// the task wire format unchanged: workers that do not know them ignore them.
HTTPHeaderEntries getTraceContextHeaders()
{
    HTTPHeaderEntries headers;
    const auto & current_trace_context = OpenTelemetry::CurrentContext();
    if (current_trace_context.isTraceEnabled())
    {
        headers.emplace_back("traceparent", current_trace_context.composeTraceparentHeader());
        if (!current_trace_context.tracestate.empty())
            headers.emplace_back("tracestate", current_trace_context.tracestate);
    }
    return headers;
}

String doSendTask(const String & endpoint_uri, const String & task_id, std::function<void(WriteBuffer&)> task_serializer, const String & unique_temp_file_path, const ContextPtr & context)
{
    auto credentials = context->getInterserverCredentials();
    Poco::Net::HTTPBasicCredentials creds{};
    if (!credentials->getUser().empty())
    {
        creds.setUsername(credentials->getUser());
        creds.setPassword(credentials->getPassword());
    }

    ConnectionTimeouts timeouts;
    timeouts.connection_timeout = Poco::Timespan(100 * 1000);
    timeouts.send_timeout = Poco::Timespan(100 * 1000 * 1000);
    timeouts.receive_timeout = Poco::Timespan(100 * 1000 * 1000);
    ReadSettings read_settings;
    /// Not safe to retry: worker would schedule a duplicate task.
    read_settings.http_settings.max_tries = 1;
    read_settings.http_settings.retry_initial_backoff_ms = 500;
    read_settings.http_settings.retry_max_backoff_ms = 1000;

    Poco::URI uri(endpoint_uri);
    uri.addQueryParameter("operation",   "start");
    uri.addQueryParameter("compress",    "false");
    uri.addQueryParameter("task_id",     task_id);
    uri.addQueryParameter("temp_path",   unique_temp_file_path);

    auto write_body_callback = [&task_serializer] (std::ostream & os)
    {
        WriteBufferFromOStream buf(os);
        task_serializer(buf);
        buf.finalize();
    };

    auto in = BuilderRWBufferFromHTTP(uri)
        .withConnectionGroup(HTTPConnectionGroupType::HTTP)
        .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
        .withTimeouts(timeouts)
        .withSettings(read_settings)
        .withHeaders(getTraceContextHeaders())
        .withOutCallback(write_body_callback)
        .withDelayInit(false)
        .create(creds);

    std::string s;
    readStringUntilEOF(s, *in);

    return s;
}

}

void serializeTask(const DistributedQueryTaskDescription & task_description, WriteBuffer & out);


String sendTask(const String & endpoint_uri, const String & unique_task_id, const DistributedQueryTaskDescription & task_description, const String & unique_temp_file_path, const ContextPtr & context)
{
    /// The dispatch request is the trace hop to the worker: the `traceparent` header sent inside this
    /// scope names this span, so the worker's spans hang under it. The span covers only the start
    /// request; the task itself runs asynchronously on the worker and is polled by the initiator.
    OpenTelemetry::SpanHolder span("StatelessWorkerClient::sendTask", OpenTelemetry::SpanKind::CLIENT);
    if (span.isTraceEnabled())
    {
        Poco::URI uri(endpoint_uri);
        span.addAttribute("clickhouse.distributed.task_id", task_description.task.task_id);
        span.addAttribute("clickhouse.initial_query_id", task_description.initial_query_id);
        span.addAttribute("clickhouse.target_host", fmt::format("{}:{}", uri.getHost(), uri.getPort()));
    }

    auto task_serializer = [task_description] (WriteBuffer & buf)
    {
        serializeTask(task_description, buf);
    };

    return doSendTask(endpoint_uri, unique_task_id, task_serializer, unique_temp_file_path, context);
}

/// Get task status by its id.
/// If wait_for_ms is set, the function will wait for the task to finish for the specified amount of time.
DistributedQueryTaskStatus getTaskStatus(const String & endpoint_uri, const String & task_id, UInt32 wait_for_ms, const ContextPtr & context, bool for_cleanup)
{
    auto credentials = context->getInterserverCredentials();
    Poco::Net::HTTPBasicCredentials creds{};
    if (!credentials->getUser().empty())
    {
        creds.setUsername(credentials->getUser());
        creds.setPassword(credentials->getPassword());
    }

    ConnectionTimeouts timeouts;
    timeouts.connection_timeout = Poco::Timespan(100 * 1000);
    ReadSettings read_settings;
    if (for_cleanup)
    {
        /// Bound a best-effort cleanup poll: the receive timeout only needs to cover the worker's
        /// server-side wait plus a small network margin, and a failed poll must not retry.
        timeouts.send_timeout = Poco::Timespan(2 * 1000 * 1000);
        timeouts.receive_timeout = Poco::Timespan((wait_for_ms + 2000) * 1000);
        read_settings.http_settings.max_tries = 1;
    }
    else
    {
        timeouts.send_timeout = Poco::Timespan(100 * 1000 * 1000);
        timeouts.receive_timeout = Poco::Timespan(100 * 1000 * 1000);
        /// Safe to retry: read-only.
        read_settings.http_settings.max_tries = 3;
        read_settings.http_settings.retry_initial_backoff_ms = 200;
        read_settings.http_settings.retry_max_backoff_ms = 1000;
    }

    Poco::URI uri(endpoint_uri);
    uri.addQueryParameter("operation",   "get_status");
    uri.addQueryParameter("compress",    "false");
    uri.addQueryParameter("task_id",     task_id);
    uri.addQueryParameter("wait_for_ms", std::to_string(wait_for_ms));

    auto in = BuilderRWBufferFromHTTP(uri)
        .withConnectionGroup(HTTPConnectionGroupType::HTTP)
        .withMethod(Poco::Net::HTTPRequest::HTTP_GET)
        .withTimeouts(timeouts)
        .withSettings(read_settings)
        .withDelayInit(false)
        .create(creds);

    DistributedQueryTaskStatus result;
    result.read(*in, DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS);
    in->eof();

    return result;
}

void cancelTask(const String & endpoint_uri, const String & task_id, const ContextPtr & context)
{
    auto credentials = context->getInterserverCredentials();
    Poco::Net::HTTPBasicCredentials creds{};
    if (!credentials->getUser().empty())
    {
        creds.setUsername(credentials->getUser());
        creds.setPassword(credentials->getPassword());
    }

    /// Short timeouts for cancel — it's best-effort. Workers will
    /// eventually clean up orphaned tasks on their own.
    ConnectionTimeouts timeouts;
    timeouts.connection_timeout = Poco::Timespan(100 * 1000);
    timeouts.send_timeout = Poco::Timespan(5 * 1000 * 1000);
    timeouts.receive_timeout = Poco::Timespan(5 * 1000 * 1000);
    ReadSettings read_settings;
    /// Safe to retry: idempotent.
    read_settings.http_settings.max_tries = 3;
    read_settings.http_settings.retry_initial_backoff_ms = 200;
    read_settings.http_settings.retry_max_backoff_ms = 1000;

    Poco::URI uri(endpoint_uri);
    uri.addQueryParameter("operation",   "cancel");
    uri.addQueryParameter("compress",    "false");
    uri.addQueryParameter("task_id",     task_id);

    auto in = BuilderRWBufferFromHTTP(uri)
        .withConnectionGroup(HTTPConnectionGroupType::HTTP)
        .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
        .withTimeouts(timeouts)
        .withSettings(read_settings)
        .withHeaders(getTraceContextHeaders())
        .withDelayInit(false)
        .create(creds);

    std::string s;
    readStringUntilEOF(s, *in);
}

void forgetTask(const String & endpoint_uri, const String & task_id, const ContextPtr & context)
{
    auto credentials = context->getInterserverCredentials();
    Poco::Net::HTTPBasicCredentials creds{};
    if (!credentials->getUser().empty())
    {
        creds.setUsername(credentials->getUser());
        creds.setPassword(credentials->getPassword());
    }

    ConnectionTimeouts timeouts;
    timeouts.connection_timeout = Poco::Timespan(100 * 1000);
    timeouts.send_timeout = Poco::Timespan(100 * 1000 * 1000);
    timeouts.receive_timeout = Poco::Timespan(100 * 1000 * 1000);
    ReadSettings read_settings;
    /// Safe to retry: idempotent.
    read_settings.http_settings.max_tries = 3;
    read_settings.http_settings.retry_initial_backoff_ms = 200;
    read_settings.http_settings.retry_max_backoff_ms = 1000;

    Poco::URI uri(endpoint_uri);
    uri.addQueryParameter("operation",   "forget");
    uri.addQueryParameter("compress",    "false");
    uri.addQueryParameter("task_id",     task_id);

    auto in = BuilderRWBufferFromHTTP(uri)
        .withConnectionGroup(HTTPConnectionGroupType::HTTP)
        .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
        .withTimeouts(timeouts)
        .withSettings(read_settings)
        .withHeaders(getTraceContextHeaders())
        .withDelayInit(false)
        .create(creds);

    std::string s;
    readStringUntilEOF(s, *in);
}

}
