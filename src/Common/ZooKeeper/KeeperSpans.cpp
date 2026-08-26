#include <Common/ZooKeeper/KeeperSpans.h>
#include <Coordination/KeeperDispatcher.h>
#include <Interpreters/Context.h>
#include <optional>

namespace DB
{

namespace
{
    std::shared_ptr<OpenTelemetrySpanLog> getSpanLog()
    {
        static std::shared_ptr<OpenTelemetrySpanLog> span_log;

        if (auto maybe_span_log = std::atomic_load_explicit(&span_log, std::memory_order_relaxed))
        {
            return maybe_span_log;
        }

        if (const auto maybe_global_context = Context::getGlobalContextInstance())
        {
            if (auto maybe_span_log = maybe_global_context->getOpenTelemetrySpanLog())
            {
                std::atomic_store_explicit(&span_log, maybe_span_log, std::memory_order_relaxed);
                return maybe_span_log;
            }
        }

        return nullptr;
    }

#if USE_NURAFT
    std::shared_ptr<KeeperDispatcher> getKeeperDispatcher()
    {
        if (const auto global_context = Context::getGlobalContextInstance())
            return global_context->tryGetKeeperDispatcher();
        return nullptr;
    }
#endif
}

void ZooKeeperOpentelemetrySpans::maybeInitialize(
    MaybeSpan & maybe_span,
    const std::optional<OpenTelemetry::TracingContext> & parent_context,
    UInt64 start_time_us)
{
    chassert(maybe_span.start_time_us == 0);
    chassert(maybe_span.span == std::nullopt);

    maybe_span.start_time_us = start_time_us;

    if (!parent_context)
        return;

    maybe_span.span.emplace(OpenTelemetry::Span{
        .trace_id = parent_context->trace_id,
        .span_id = thread_local_rng(),
        .parent_span_id = parent_context->span_id,
        .operation_name = String(maybe_span.operation_name),
        .start_time_us = start_time_us,
        .kind = maybe_span.kind,
    });
}

void ZooKeeperOpentelemetrySpans::maybeFinalizeImpl(
    MaybeSpan & maybe_span,
    std::vector<OpenTelemetry::SpanAttribute> attributes,
    OpenTelemetry::SpanStatus status,
    const String & error_message,
    UInt64 finish_time_us)
{
    chassert(maybe_span.start_time_us != 0);

    maybe_span.histogram.observe((finish_time_us - maybe_span.start_time_us) / 1000);

    if (!maybe_span.span)
        return;

    chassert(maybe_span.span->start_time_us != 0);
    chassert(maybe_span.span->span_id != 0);
    chassert(maybe_span.span->trace_id != UUID());

    const auto span_log = getSpanLog();
    if (!span_log)
    {
        maybe_span.span.reset();
        return;
    }

    maybe_span.span->finish_time_us = finish_time_us;
    maybe_span.span->status_code = status;
    maybe_span.span->status_message = error_message;

#if USE_NURAFT
    static const auto keeper_dispatcher = getKeeperDispatcher();
    if (keeper_dispatcher)
        attributes.emplace_back("raft.role", keeper_dispatcher->getRoleString());
#endif

    maybe_span.span->attributes = std::move(attributes);

    span_log->add(OpenTelemetrySpanLogElement(std::move(*maybe_span.span)));
    maybe_span.span.reset();
}

}
