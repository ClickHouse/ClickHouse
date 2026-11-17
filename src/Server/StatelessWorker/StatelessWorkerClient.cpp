#include <Server/StatelessWorker/StatelessWorkerClient.h>
#include <Server/StatelessWorker/StatelessWorkerTaskSerialization.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterserverCredentials.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/HTTPHeaderEntries.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Core/ProtocolDefines.h>
#include <base/types.h>
#include <fmt/format.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

/// W3C `traceparent`/`tracestate` headers carrying the current trace context to the worker. Sent with the start request only
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

/// Sends `operation` with `id_name` = `id`; the body is written by `task_serializer` while it is sent.
String doSendTask(const String & endpoint_uri, const String & operation, const String & id_name, const String & id,
    std::function<void(WriteBuffer&)> task_serializer, const String & unique_temp_file_path, const TaskCollectors & collectors, const ContextPtr & context)
{
    auto credentials = context->getInterserverCredentials();
    Poco::Net::HTTPBasicCredentials creds{};
    if (!credentials->getUser().empty())
    {
        creds.setUsername(credentials->getUser());
        creds.setPassword(String(credentials->getPassword()));
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
    uri.addQueryParameter("operation",   operation);
    uri.addQueryParameter("compress",    "false");
    uri.addQueryParameter(id_name,       id);
    uri.addQueryParameter("temp_path",   unique_temp_file_path);
    /// Absent when nothing is collected, so an older worker sees the request it has always seen.
    if (collectors.any())
        uri.addQueryParameter("collect", collectors.toString());

    auto write_body_callback = [&task_serializer] (WriteBuffer & out)
    {
        task_serializer(out);
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

String sendTaskBatch(const String & endpoint_uri, const String & unique_query_id, const DistributedQueryTaskBatch & batch,
    const String & unique_temp_file_path, const TaskCollectors & collectors, const ContextPtr & context)
{
    size_t task_count = 0;
    for (const auto & stage : batch.stages)
        task_count += stage.tasks.size();

    /// The dispatch request is the trace hop to the worker, so the worker's spans hang under it.
    const Poco::URI uri(endpoint_uri);
    OpenTelemetry::SpanHolder span(
        "StatelessWorkerClient::sendTaskBatch",
        OpenTelemetry::SpanKind::CLIENT,
        {
            {"clickhouse.distributed.query_id", unique_query_id},
            {"clickhouse.distributed.task_count", std::to_string(task_count)},
            {"clickhouse.distributed.stage_count", std::to_string(batch.stages.size())},
            {"clickhouse.initial_query_id", batch.initial_query_id},
            {"clickhouse.target_host", fmt::format("{}:{}", uri.getHost(), uri.getPort())},
        });

    /// By reference: `doSendTask` writes the body before it returns.
    auto batch_serializer = [&] (WriteBuffer & buf)
    {
        serializeTaskBatch(batch, buf);
    };

    try
    {
        return doSendTask(endpoint_uri, "start_tasks", "query_id", unique_query_id, batch_serializer, unique_temp_file_path, collectors, context);
    }
    catch (...)
    {
        span.addAttribute(std::current_exception());
        throw;
    }
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
        creds.setPassword(String(credentials->getPassword()));
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
        /// Retrying is fine: a retry can lose drained log lines, which is accepted.
        read_settings.http_settings.max_tries = 3;
        read_settings.http_settings.retry_initial_backoff_ms = 200;
        read_settings.http_settings.retry_max_backoff_ms = 1000;
    }

    Poco::URI uri(endpoint_uri);
    uri.addQueryParameter("operation",   "get_status");
    uri.addQueryParameter("compress",    "false");
    uri.addQueryParameter("task_id",     task_id);
    uri.addQueryParameter("wait_for_ms", std::to_string(wait_for_ms));

    auto builder = BuilderRWBufferFromHTTP(uri);
    builder.withConnectionGroup(HTTPConnectionGroupType::HTTP)
        .withMethod(Poco::Net::HTTPRequest::HTTP_GET)
        .withTimeouts(timeouts)
        .withSettings(read_settings)
        .withDelayInit(false);
    /// A cleanup poll belongs to the teardown of a query that can already be killed.
    if (for_cleanup)
        builder.withoutQueryCancellationCheck();
    auto in = builder.create(creds);

    DistributedQueryTaskStatus result;
    result.read(*in, DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS);
    /// Nothing may follow the end tag.
    if (!in->eof())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Unexpected trailing data in stateless worker task status response for task {}", task_id);

    return result;
}

void cancelTask(const String & endpoint_uri, const String & task_id, const ContextPtr & context)
{
    auto credentials = context->getInterserverCredentials();
    Poco::Net::HTTPBasicCredentials creds{};
    if (!credentials->getUser().empty())
    {
        creds.setUsername(credentials->getUser());
        creds.setPassword(String(credentials->getPassword()));
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

    /// Sent although the query can already be killed: the worker must still stop the task.
    auto in = BuilderRWBufferFromHTTP(uri)
        .withConnectionGroup(HTTPConnectionGroupType::HTTP)
        .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
        .withTimeouts(timeouts)
        .withSettings(read_settings)
        .withDelayInit(false)
        .withoutQueryCancellationCheck()
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
        creds.setPassword(String(credentials->getPassword()));
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
        .withDelayInit(false)
        .withoutQueryCancellationCheck()
        .create(creds);

    std::string s;
    readStringUntilEOF(s, *in);
}

}
