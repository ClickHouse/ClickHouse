#pragma once

#include <Common/DistributedTracing.h>

namespace DB::opentracing
{

    std::shared_ptr<Span> getCurrentSpan();
    std::shared_ptr<IDistributedTracer> getCurrentTracer();

    std::shared_ptr<Span> switchToChildSpan(const String& operation_name);
    void switchToParentSpan(const std::shared_ptr<Span>& parent_span);


    class SpanGuard
    {
    public:
        SpanGuard();
        SpanGuard(const String& operation_name);
        ~SpanGuard();

//    private:
//        std::shared_ptr<DB::opentracing::Span> parent_span;
    };

}
