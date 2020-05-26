#include "DistributedTracing.h"

#include <Interpreters/Context.h>

#include <IO/ReadHelpers.h>

//#include <Common/CurrentThread.h>


namespace DB::opentracing
{

std::shared_ptr<Span> getCurrentSpan()
{
    if (CurrentThread::getGroup())
    {
        if (auto context = CurrentThread::getGroup()->query_context)
        {
            if (context->getSettings().enable_distributed_tracing)
            {
                if (auto span = CurrentThread::getSpan())
                {
                    return span;
                }

                if (auto span = context->getSpan())
                {
                    return span;
                }
            }
        }
    }

    return nullptr;
}


std::shared_ptr<IDistributedTracer> getCurrentTracer()
{
    if (CurrentThread::getGroup())
    {
        if (auto context = CurrentThread::getGroup()->query_context)
        {
            if (context->getSettings().enable_distributed_tracing)
            {
                return context->getDistributedTracer();
            }
        }
    }
    return std::make_shared<NoopTracer>();
}

std::shared_ptr<Span> switchToChildSpan(const String& operation_name)
{
    if (!CurrentThread::getGroup())
    {
        if (!!CurrentThread::getSpan())
            abort();

        return nullptr;
    }

    std::shared_ptr<Span> parent_span_ret = nullptr;
    if (auto context = CurrentThread::getGroup()->query_context)
    {
        if (context->getSettings().enable_distributed_tracing)
        {
            auto parent_thread_span = CurrentThread::getSpan();

            SpanReferenceList parent_reference;

            if (!!parent_thread_span)
            {
                parent_reference.emplace_back(
                        DB::opentracing::SpanReferenceType::ChildOfRef,
                        std::make_shared<DB::opentracing::SpanContext>(parent_thread_span->getSpanContext()));
                parent_span_ret = parent_thread_span;
            } else if (const auto& parent_span = context->getSpan())
            {
                parent_reference.emplace_back(
                        DB::opentracing::SpanReferenceType::ChildOfRef,
                        std::make_shared<DB::opentracing::SpanContext>(parent_span->getSpanContext()));
            }

            if (!parent_reference.empty())
            {
                CurrentThread::setSpan(context->getDistributedTracer()->CreateSpan(
                        operation_name,
                        parent_reference,
                        std::nullopt));

                // CHECKME
                if (!CurrentThread::getSpan())
                    abort();
            }
        } else
        {
            if (!!CurrentThread::getSpan())
                abort();
        }
    } else
    {
        if (!!CurrentThread::getSpan())
            abort();
    }

    return parent_span_ret;
}


bool operator != (const SpanContext& lhs, const SpanContext& rhs)
{
    return !(lhs.trace_id == rhs.trace_id && lhs.span_id == rhs.span_id && lhs.isInitialized == rhs.isInitialized);
}


void switchToParentSpan(const std::shared_ptr<Span>& parent_span)
{
    // Here parent_span may be nullptr.

    auto span = CurrentThread::getSpan();

    if (!span)
    {
        // FIXME
//        if (!!parent_span)
//            abort();
        return;
    }

    if (!!parent_span && *(span->getReferences()[0].second) != parent_span->getSpanContext())
        abort();

    span->finishSpan();
    CurrentThread::setSpan(parent_span);
}


SpanGuard::SpanGuard()
//        : parent_span(nullptr)
{ }

SpanGuard::SpanGuard(const String& /*operation_name*/)
//        : parent_span(switchToChildSpan(operation_name))
{ }

SpanGuard::~SpanGuard()
{
//    switchToParentSpan(std::move(parent_span));
}

}
