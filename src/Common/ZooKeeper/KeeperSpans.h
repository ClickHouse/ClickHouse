#pragma once

#include <Common/HistogramMetrics.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/thread_local_rng.h>
#include <Core/Types.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <unordered_map>
#include <string>
#include <chrono>
#include <optional>
#include <memory>

namespace HistogramMetrics
{
    extern Metric & KeeperClientQueueDuration;
    extern Metric & KeeperReceiveRequestTime;
    extern Metric & KeeperDispatcherRequestsQueueTime;
    extern Metric & KeeperWritePreCommitTime;
    extern Metric & KeeperWriteCommitTime;
    extern Metric & KeeperDispatcherResponsesQueueTime;
    extern Metric & KeeperSendResponseTime;
    extern Metric & KeeperReadWaitForWriteTime;
    extern Metric & KeeperReadProcessTime;
}

namespace Coordination
{
    struct ZooKeeperRequest;
}

namespace DB
{

struct MaybeSpan
{
    const std::string_view operation_name;
    const OpenTelemetry::SpanKind kind;
    HistogramMetrics::Metric & histogram;
    std::unique_ptr<OpenTelemetry::Span> span;
    UInt64 start_time_us = 0;

    MaybeSpan(const std::string_view operation_name_, OpenTelemetry::SpanKind kind_, HistogramMetrics::Metric & histogram_)
        : operation_name(operation_name_), kind(kind_), histogram(histogram_) {}

    MaybeSpan(const MaybeSpan & other)
        : operation_name(other.operation_name)
        , kind(other.kind)
        , histogram(other.histogram)
        , span(other.span ? std::make_unique<OpenTelemetry::Span>(*other.span) : nullptr)
        , start_time_us(other.start_time_us)
    {
    }

    MaybeSpan & operator=(const MaybeSpan & other)
    {
        if (this == &other)
            return *this;

        chassert(operation_name == other.operation_name);
        chassert(kind == other.kind);
        chassert(&histogram == &other.histogram);

        start_time_us = other.start_time_us;
        if (other.span)
            span = std::make_unique<OpenTelemetry::Span>(*other.span);
        else
            span.reset();

        return *this;
    }

    MaybeSpan(MaybeSpan &&) noexcept = default;

    MaybeSpan & operator=(MaybeSpan && other) noexcept
    {
        if (this == &other)
            return *this;

        chassert(operation_name == other.operation_name);
        chassert(kind == other.kind);
        chassert(&histogram == &other.histogram);

        start_time_us = other.start_time_us;
        span = std::move(other.span);

        return *this;
    }
};

struct ZooKeeperOpentelemetrySpans
{
    // Keeper client spans
    MaybeSpan client_requests_queue{"zookeeper.client.requests_queue", OpenTelemetry::SpanKind::INTERNAL, HistogramMetrics::KeeperClientQueueDuration};

    // Keeper server spans
    MaybeSpan receive_request{"keeper.receive_request", OpenTelemetry::SpanKind::SERVER, HistogramMetrics::KeeperReceiveRequestTime};
    MaybeSpan dispatcher_requests_queue{"keeper.dispatcher.requests_queue", OpenTelemetry::SpanKind::INTERNAL, HistogramMetrics::KeeperDispatcherRequestsQueueTime};
    MaybeSpan dispatcher_responses_queue{"keeper.dispatcher.responses_queue", OpenTelemetry::SpanKind::INTERNAL, HistogramMetrics::KeeperDispatcherResponsesQueueTime};
    MaybeSpan send_response{"keeper.send_response", OpenTelemetry::SpanKind::SERVER, HistogramMetrics::KeeperSendResponseTime};
    MaybeSpan read_wait_for_write{"keeper.read.wait_for_write", OpenTelemetry::SpanKind::INTERNAL, HistogramMetrics::KeeperReadWaitForWriteTime};
    MaybeSpan read_process{"keeper.read.process", OpenTelemetry::SpanKind::INTERNAL, HistogramMetrics::KeeperReadProcessTime};
    MaybeSpan pre_commit{"keeper.write.pre_commit", OpenTelemetry::SpanKind::INTERNAL, HistogramMetrics::KeeperWritePreCommitTime};
    MaybeSpan commit{"keeper.write.commit", OpenTelemetry::SpanKind::INTERNAL, HistogramMetrics::KeeperWriteCommitTime};

    static UInt64 now()
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    static void maybeInitialize(
        MaybeSpan & maybe_span,
        const std::optional<OpenTelemetry::TracingContext> & parent_context,
        UInt64 start_time_us = now());

    template <typename MakeAttributes>
    static void maybeFinalize(
        MaybeSpan & maybe_span,
        MakeAttributes && make_attributes,
        OpenTelemetry::SpanStatus status = OpenTelemetry::SpanStatus::OK,
        const String & error_message = {},
        UInt64 finish_time_us = now())
    {
        if (!maybe_span.span)
        {
            chassert(maybe_span.start_time_us != 0);
            maybe_span.histogram.observe((finish_time_us - maybe_span.start_time_us) / 1000);
            return;
        }

        maybeFinalizeImpl(maybe_span, make_attributes(), status, error_message, finish_time_us);
    }

    static void maybeFinalizeImpl(
        MaybeSpan & maybe_span,
        std::vector<OpenTelemetry::SpanAttribute> attributes,
        OpenTelemetry::SpanStatus status,
        const String & error_message,
        UInt64 finish_time_us);
};

}
