#pragma once

#include <Interpreters/OpenTelemetrySpanLog.h>
#include "Interpreters/Context.h"
#include <atomic>
#include <memory>

namespace DB
{

class WithSpanLog
{
protected:
    std::shared_ptr<OpenTelemetrySpanLog> getSpanLog()
    {
        if (auto maybe_span_log = std::atomic_load_explicit(&span_log, std::memory_order_relaxed))
            return maybe_span_log;

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

private:
    std::shared_ptr<OpenTelemetrySpanLog> span_log;
};

}
