#pragma once

#include <Common/OpenTelemetryTraceContext.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/thread_local_rng.h>
#include <Core/Types.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <unordered_map>
#include <string>
#include <chrono>

namespace Coordination
{
    struct ZooKeeperRequest;
}

namespace DB
{

struct KeeperSpans
{
    OpenTelemetry::Span receive_request{
        .operation_name = "keeper.receive_request",
        .kind = OpenTelemetry::SpanKind::SERVER,
    };

    OpenTelemetry::Span process_request{
        .operation_name = "keeper.process_request",
        .kind = OpenTelemetry::SpanKind::SERVER,
    };

    OpenTelemetry::Span dispatcher_responses_queue{
        .operation_name = "keeper.dispatcher.responses_queue",
        .kind = OpenTelemetry::SpanKind::INTERNAL,
    };

    OpenTelemetry::Span send_response{
        .operation_name = "keeper.send_response",
        .kind = OpenTelemetry::SpanKind::SERVER,
    };

    OpenTelemetry::Span read_wait_for_write{
        .operation_name = "keeper.read.wait_for_write",
        .kind = OpenTelemetry::SpanKind::INTERNAL,
    };

    OpenTelemetry::Span read_process{
        .operation_name = "keeper.read.process",
        .kind = OpenTelemetry::SpanKind::INTERNAL,
    };

    OpenTelemetry::Span write_pre_commit{
        .operation_name = "keeper.write.pre_commit",
        .kind = OpenTelemetry::SpanKind::INTERNAL,
    };

    OpenTelemetry::Span write_commit{
        .operation_name = "keeper.write.commit",
        .kind = OpenTelemetry::SpanKind::INTERNAL,
    };

    static UInt64 now()
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    static void initialize(
        const OpenTelemetry::TracingContext & parent_context,
        OpenTelemetry::Span & span,
        UInt64 start_time_us = now());

    static void finalize(
        OpenTelemetry::Span & span,
        OpenTelemetry::SpanStatus status = OpenTelemetry::SpanStatus::OK,
        const String & error_message = {},
        std::unordered_map<std::string, std::string> && extra_attributes = {},
        UInt64 finish_time_us = now());
};

}
