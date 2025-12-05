#pragma once

#include <Common/OpenTelemetryTraceContext.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/thread_local_rng.h>
#include <Coordination/KeeperCommon.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <unordered_map>
#include <string>

namespace Coordination
{
    struct ZooKeeperRequest;
}

namespace DB
{

struct KeeperSpans
{
    OpenTelemetry::Span receive_request{
        .span_id = thread_local_rng(),
        .operation_name = "keeper.receive_request",
        .kind = OpenTelemetry::SpanKind::SERVER,
    };

    OpenTelemetry::Span process_request{
        .span_id = thread_local_rng(),
        .operation_name = "keeper.process_request",
        .kind = OpenTelemetry::SpanKind::SERVER,
    };

    OpenTelemetry::Span dispatcher_responses_queue{
        .span_id = thread_local_rng(),
        .operation_name = "keeper.dispatcher.responses_queue",
        .kind = OpenTelemetry::SpanKind::INTERNAL,
    };

    OpenTelemetry::Span send_response{
        .span_id = thread_local_rng(),
        .operation_name = "keeper.send_response",
        .kind = OpenTelemetry::SpanKind::SERVER,
    };

    OpenTelemetry::Span read_wait_for_write{
        .span_id = thread_local_rng(),
        .operation_name = "keeper.read.wait_for_write",
        .kind = OpenTelemetry::SpanKind::INTERNAL,
    };

    OpenTelemetry::Span read_process{
        .span_id = thread_local_rng(),
        .operation_name = "keeper.read.process",
        .kind = OpenTelemetry::SpanKind::INTERNAL,
    };

    OpenTelemetry::Span write_pre_commit{
        .span_id = thread_local_rng(),
        .operation_name = "keeper.write.pre_commit",
        .kind = OpenTelemetry::SpanKind::INTERNAL,
    };

    OpenTelemetry::Span write_commit{
        .span_id = thread_local_rng(),
        .operation_name = "keeper.write.commit",
        .kind = OpenTelemetry::SpanKind::INTERNAL,
    };

    static void log(
        const OpenTelemetry::TracingContext & parent_context,
        OpenTelemetry::Span & span,
        OpenTelemetry::SpanStatus status = OpenTelemetry::SpanStatus::OK,
        const String & error_message = {},
        std::unordered_map<std::string, std::string> && extra_attributes = {});
};

}
