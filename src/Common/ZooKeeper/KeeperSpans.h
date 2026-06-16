#pragma once

#include <Common/HistogramMetrics.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/thread_local_rng.h>
#include <Core/Types.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <array>
#include <chrono>
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

namespace DB
{

#define APPLY_FOR_KEEPER_SPANS(M) \
    M(ClientRequestsQueue,      "zookeeper.client.requests_queue",    INTERNAL, KeeperClientQueueDuration) \
    M(ReceiveRequest,           "keeper.receive_request",             SERVER,   KeeperReceiveRequestTime) \
    M(DispatcherRequestsQueue,  "keeper.dispatcher.requests_queue",   INTERNAL, KeeperDispatcherRequestsQueueTime) \
    M(DispatcherResponsesQueue, "keeper.dispatcher.responses_queue",  INTERNAL, KeeperDispatcherResponsesQueueTime) \
    M(SendResponse,             "keeper.send_response",               SERVER,   KeeperSendResponseTime) \
    M(ReadWaitForWrite,         "keeper.read.wait_for_write",         INTERNAL, KeeperReadWaitForWriteTime) \
    M(ReadProcess,              "keeper.read.process",                INTERNAL, KeeperReadProcessTime) \
    M(PreCommit,                "keeper.write.pre_commit",            INTERNAL, KeeperWritePreCommitTime) \
    M(Commit,                   "keeper.write.commit",                INTERNAL, KeeperWriteCommitTime) \

namespace KeeperSpan
{
    enum Operation : size_t
    {
    #define M(NAME, ...) NAME,
        APPLY_FOR_KEEPER_SPANS(M)
    #undef M
        Count
    };
}

struct SpanDescriptor
{
    std::string_view operation_name;
    OpenTelemetry::SpanKind kind;
    HistogramMetrics::Metric & histogram;
};

struct MaybeSpan
{
    std::unique_ptr<OpenTelemetry::Span> span;
    UInt64 start_time_us = 0;
};

class ZooKeeperOpentelemetrySpans
{
public:
    ZooKeeperOpentelemetrySpans() = default;
    ZooKeeperOpentelemetrySpans(const ZooKeeperOpentelemetrySpans &) : ZooKeeperOpentelemetrySpans() {}
    ZooKeeperOpentelemetrySpans & operator=(const ZooKeeperOpentelemetrySpans &) { return *this; } // NOLINT(cert-oop54-cpp)
    ZooKeeperOpentelemetrySpans(ZooKeeperOpentelemetrySpans &&) = default;
    ZooKeeperOpentelemetrySpans & operator=(ZooKeeperOpentelemetrySpans &&) = default;

    static UInt64 now()
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    void maybeInitialize(
        KeeperSpan::Operation operation,
        const OpenTelemetry::TracingContext * parent_context,
        UInt64 start_time_us = now());

    template <typename MakeAttributes>
    void maybeFinalize(
        KeeperSpan::Operation operation,
        MakeAttributes && make_attributes,
        OpenTelemetry::SpanStatus status = OpenTelemetry::SpanStatus::OK,
        const String & error_message = {},
        UInt64 finish_time_us = now())
    {
        auto & maybe_span = maybe_spans[operation];

        chassert(maybe_span.start_time_us != 0);
        const auto latency_ms = static_cast<HistogramMetrics::Value>(finish_time_us - maybe_span.start_time_us) / 1000.0;
        getSpanDescriptor(operation).histogram.observe(latency_ms);

        if (!maybe_span.span)
            return;

        maybeFinalizeImpl(maybe_span, make_attributes(), status, error_message, finish_time_us);
    }

private:
    static const SpanDescriptor & getSpanDescriptor(KeeperSpan::Operation operation);

    static void maybeFinalizeImpl(
        MaybeSpan & maybe_span,
        std::vector<OpenTelemetry::SpanAttribute> attributes = {},
        OpenTelemetry::SpanStatus status = OpenTelemetry::SpanStatus::OK,
        const String & error_message = {},
        UInt64 finish_time_us = now());

    std::array<MaybeSpan, KeeperSpan::Count> maybe_spans = {};
};

}
