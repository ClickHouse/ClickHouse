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

namespace
{

/// Sends `operation` with the task ids in the body. A teardown request is sent although the query can already be killed.
std::unique_ptr<ReadWriteBufferFromHTTP> sendTaskIds(const String & endpoint_uri, const String & operation, const Strings & task_ids,
    std::optional<UInt32> wait_for_ms, const ConnectionTimeouts & timeouts, const ReadSettings & read_settings, bool for_teardown,
    const ContextPtr & context)
{
    auto credentials = context->getInterserverCredentials();
    Poco::Net::HTTPBasicCredentials creds{};
    if (!credentials->getUser().empty())
    {
        creds.setUsername(credentials->getUser());
        creds.setPassword(credentials->getPassword());
    }

    Poco::URI uri(endpoint_uri);
    uri.addQueryParameter("operation", operation);
    uri.addQueryParameter("compress", "false");
    if (wait_for_ms)
        uri.addQueryParameter("wait_for_ms", std::to_string(*wait_for_ms));

    auto builder = BuilderRWBufferFromHTTP(uri);
    builder.withConnectionGroup(HTTPConnectionGroupType::HTTP)
        .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
        .withTimeouts(timeouts)
        .withSettings(read_settings)
        .withOutCallback([&task_ids](WriteBuffer & out) { writeTaskIds(task_ids, out); })
        .withDelayInit(false);
    if (for_teardown)
        builder.withoutQueryCancellationCheck();
    return builder.create(creds);
}

}

std::vector<DistributedQueryTaskStatusEntry> getTaskStatuses(const String & endpoint_uri, const Strings & task_ids, UInt32 wait_for_ms,
    const ContextPtr & context, bool for_cleanup)
{
    ConnectionTimeouts timeouts;
    timeouts.connection_timeout = Poco::Timespan(100 * 1000);
    ReadSettings read_settings;
    if (for_cleanup)
    {
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

    auto in = sendTaskIds(endpoint_uri, "get_statuses", task_ids, wait_for_ms, timeouts, read_settings, for_cleanup, context);
    auto entries = readTaskStatuses(*in);
    if (!in->eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected trailing data in a stateless worker statuses response");
    if (entries.size() != task_ids.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "A stateless worker returned {} statuses for {} tasks", entries.size(), task_ids.size());
    return entries;
}

void cancelTasks(const String & endpoint_uri, const Strings & task_ids, const ContextPtr & context)
{
    ConnectionTimeouts timeouts;
    timeouts.connection_timeout = Poco::Timespan(100 * 1000);
    timeouts.send_timeout = Poco::Timespan(5 * 1000 * 1000);
    timeouts.receive_timeout = Poco::Timespan(5 * 1000 * 1000);
    ReadSettings read_settings;
    /// Safe to retry: idempotent.
    read_settings.http_settings.max_tries = 3;
    read_settings.http_settings.retry_initial_backoff_ms = 200;
    read_settings.http_settings.retry_max_backoff_ms = 1000;

    auto in = sendTaskIds(endpoint_uri, "cancel_tasks", task_ids, {}, timeouts, read_settings, /*for_teardown=*/ true, context);
    in->ignoreAll();
}

void forgetTasks(const String & endpoint_uri, const Strings & task_ids, const ContextPtr & context)
{
    ConnectionTimeouts timeouts;
    timeouts.connection_timeout = Poco::Timespan(100 * 1000);
    timeouts.send_timeout = Poco::Timespan(100 * 1000 * 1000);
    timeouts.receive_timeout = Poco::Timespan(100 * 1000 * 1000);
    ReadSettings read_settings;
    /// Safe to retry: idempotent.
    read_settings.http_settings.max_tries = 3;
    read_settings.http_settings.retry_initial_backoff_ms = 200;
    read_settings.http_settings.retry_max_backoff_ms = 1000;

    auto in = sendTaskIds(endpoint_uri, "forget_tasks", task_ids, {}, timeouts, read_settings, /*for_teardown=*/ true, context);
    in->ignoreAll();
}

}
