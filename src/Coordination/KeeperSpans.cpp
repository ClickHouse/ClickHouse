#include <Coordination/KeeperSpans.h>
#include <Interpreters/Context.h>
#include <mutex>

namespace DB
{

namespace
{
    std::shared_ptr<OpenTelemetrySpanLog> getSpanLog()
    {
        static std::shared_ptr<OpenTelemetrySpanLog> cached_span_log;
        static std::once_flag init_flag;

        std::call_once(init_flag, []() {
            if (const auto global_context = Context::getGlobalContextInstance())
            {
                if (auto maybe_span_log = global_context->getOpenTelemetrySpanLog())
                {
                    cached_span_log = maybe_span_log;
                }
            }
        });

        return cached_span_log;
    }
}

void KeeperSpans::log(
    const OpenTelemetry::TracingContext & parent_context,
    OpenTelemetry::Span & span,
    OpenTelemetry::SpanStatus status,
    const String & error_message,
    std::unordered_map<std::string, std::string> && extra_attributes)
{
    chassert(span.start_time_us != 0);
    chassert(span.finish_time_us != 0);
    chassert(span.span_id != 0);

    const auto span_log = getSpanLog();
    if (!span_log)
        return;

    span.trace_id = parent_context.trace_id;
    span.parent_span_id = parent_context.span_id;
    span.status_code = status;

    span.attributes.merge(extra_attributes);

    if (!error_message.empty())
        span.status_message = error_message;

    span_log->add(OpenTelemetrySpanLogElement(std::move(span)));
}

}
