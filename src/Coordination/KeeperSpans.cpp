#include <Coordination/KeeperSpans.h>
#include <Interpreters/Context.h>
#include <mutex>

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
}

void KeeperSpans::initialize(
    const OpenTelemetry::TracingContext & parent_context,
    OpenTelemetry::Span & span,
    UInt64 start_time_us)
{
    span.span_id = thread_local_rng();
    span.trace_id = parent_context.trace_id;
    span.parent_span_id = parent_context.span_id;
    span.start_time_us = start_time_us;
}

void KeeperSpans::finalize(
    OpenTelemetry::Span & span,
    OpenTelemetry::SpanStatus status,
    const String & error_message,
    std::unordered_map<std::string, std::string> && extra_attributes,
    UInt64 finish_time_us)
{
    chassert(span.start_time_us != 0);
    chassert(span.span_id != 0);
    chassert(span.trace_id != UUID());

    const auto span_log = getSpanLog();
    if (!span_log)
        return;

    span.finish_time_us = finish_time_us;
    span.status_code = status;
    span.status_message = error_message;
    span.attributes.merge(extra_attributes);

    span_log->add(OpenTelemetrySpanLogElement(std::move(span)));
}

}
