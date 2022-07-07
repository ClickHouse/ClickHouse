#pragma once

#include <Core/Field.h>

namespace DB
{

struct OpenTelemetrySpan
{
    UUID trace_id;
    UInt64 span_id;
    UInt64 parent_span_id;
    std::string operation_name;
    UInt64 start_time_us;
    UInt64 finish_time_us;
    Map attributes;

    void addAttribute(const std::string& name, UInt64 value);
    void addAttributeIfNotZero(const std::string& name, UInt64 value)
    {
        if (value != 0)
            addAttribute(name, value);
    }

    void addAttribute(const std::string& name, const std::string& value);
    void addAttribute(const Exception & e);
    void addAttribute(std::exception_ptr e);

    bool isTraceEnabled() const
    {
        return trace_id != UUID();
    }
};

struct OpenTelemetrySpanHolder;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class OpenTelemetrySpanLog;

// The runtime info we need to create new OpenTelemetry spans.
struct OpenTelemetryTraceContext
{
    UUID trace_id{};
    UInt64 span_id = 0;
    // The incoming tracestate header and the trace flags, we just pass them
    // downstream. See https://www.w3.org/TR/trace-context/
    String tracestate;
    UInt8 trace_flags = 0;

    // Parse/compose OpenTelemetry traceparent header.
    bool parseTraceparentHeader(const std::string & traceparent, std::string & error);
    std::string composeTraceparentHeader() const;

    bool isTraceEnabled() const
    {
        return trace_id != UUID();
    }
};

// tracing context kept on thread local
struct OpenTelemetryThreadTraceContext : OpenTelemetryTraceContext
{
    OpenTelemetryThreadTraceContext& operator =(const OpenTelemetryTraceContext& context)
    {
        *(static_cast<OpenTelemetryTraceContext*>(this)) = context;
        return *this;
    }

    void reset();

    static const OpenTelemetryThreadTraceContext& current();

    std::weak_ptr<OpenTelemetrySpanLog> span_log;
};

struct OpenTelemetryThreadTraceContextScope
{
    // forbidden copy ctor and assignment to make the destructor safe
    OpenTelemetryThreadTraceContextScope(const OpenTelemetryThreadTraceContextScope& scope) = delete;
    OpenTelemetryThreadTraceContextScope& operator =(const OpenTelemetryThreadTraceContextScope& scope) = delete;

    OpenTelemetryThreadTraceContextScope(const std::string& _operation_name,
                                         const OpenTelemetryTraceContext& _parent_trace_context,
                                         const std::weak_ptr<OpenTelemetrySpanLog>& _log);

    OpenTelemetryThreadTraceContextScope(const std::string& _operation_name,
                                         const OpenTelemetryThreadTraceContext& _parent_thread_trace_context)
        : OpenTelemetryThreadTraceContextScope(_operation_name,
                                               _parent_thread_trace_context,
                                               _parent_thread_trace_context.span_log)
    {
    }

    ~OpenTelemetryThreadTraceContextScope();



    OpenTelemetrySpan root_span;
};

using OpenTelemetryThreadTraceContextScopePtr = std::unique_ptr<OpenTelemetryThreadTraceContextScope>;

}
