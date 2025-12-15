#pragma once

#include <Common/OpenTelemetryTraceContext.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/thread_local_rng.h>
#include <Core/Types.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <unordered_map>
#include <string>
#include <chrono>
#include <optional>

namespace Coordination
{
    struct ZooKeeperRequest;
}

namespace DB
{

struct MaybeSpan
{
    const char * operation_name;
    const OpenTelemetry::SpanKind kind;
    std::optional<OpenTelemetry::Span> span;

    MaybeSpan(const char * name, OpenTelemetry::SpanKind k)
        : operation_name(name), kind(k) {}
};

struct ZooKeeperOpentelemetrySpans
{
    // Keeper client spans
    MaybeSpan client_requests_queue{"zookeeper.client.requests_queue", OpenTelemetry::SpanKind::INTERNAL};

    // Keeper server spans
    MaybeSpan receive_request{"keeper.receive_request", OpenTelemetry::SpanKind::SERVER};
    MaybeSpan dispatcher_requests_queue{"keeper.dispatcher.requests_queue", OpenTelemetry::SpanKind::INTERNAL};
    MaybeSpan dispatcher_responses_queue{"keeper.dispatcher.responses_queue", OpenTelemetry::SpanKind::INTERNAL};
    MaybeSpan send_response{"keeper.send_response", OpenTelemetry::SpanKind::SERVER};
    MaybeSpan read_wait_for_write{"keeper.read.wait_for_write", OpenTelemetry::SpanKind::INTERNAL};
    MaybeSpan read_process{"keeper.read.process", OpenTelemetry::SpanKind::INTERNAL};
    MaybeSpan pre_commit{"keeper.write.pre_commit", OpenTelemetry::SpanKind::INTERNAL};
    MaybeSpan commit{"keeper.write.commit", OpenTelemetry::SpanKind::INTERNAL};

    static UInt64 now()
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    static void maybeInitialize(
        MaybeSpan & maybe_span,
        const std::optional<OpenTelemetry::TracingContext> & parent_context,
        UInt64 start_time_us = now());

    static void maybeFinalize(
        MaybeSpan & maybe_span,
        std::unordered_map<std::string, std::string> && extra_attributes = {},
        OpenTelemetry::SpanStatus status = OpenTelemetry::SpanStatus::OK,
        const String & error_message = {},
        UInt64 finish_time_us = now());
};

}
