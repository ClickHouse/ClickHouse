#pragma once

#include <Core/Field.h>

namespace DB
{

struct Settings;

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
    void addAttribute(const std::string& name, std::function<std::string()> value_supplier);
    void addAttribute(const Exception & e);
    void addAttribute(std::exception_ptr e);

    bool isTraceEnabled() const
    {
        return trace_id != UUID();
    }
};

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

/// tracing context kept on thread local
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

/// A scoped tracing context, is used to hold the tracing context at the beginning of each thread execution and clear the context automatically when the scope exists.
/// It should be the root of all span logs for one tracing.
///
/// It's SAFE to construct this object multiple times on one same thread,
/// but it's not encourage to do so because this is only a protection in case of code changes.
struct OpenTelemetryThreadTraceContextScope
{
    // forbidden copy ctor and assignment to make the destructor safe
    OpenTelemetryThreadTraceContextScope(const OpenTelemetryThreadTraceContextScope& scope) = delete;
    OpenTelemetryThreadTraceContextScope& operator =(const OpenTelemetryThreadTraceContextScope& scope) = delete;

    OpenTelemetryThreadTraceContextScope(const std::string& _operation_name,
                                         const OpenTelemetryTraceContext& _parent_trace_context,
                                         const std::weak_ptr<OpenTelemetrySpanLog>& _log)
        : OpenTelemetryThreadTraceContextScope(_operation_name,
                                               _parent_trace_context,
                                               nullptr,
                                               _log)
    {
    }

    /// Initialize a tracing context on a child thread based on the context from the parent thread
    OpenTelemetryThreadTraceContextScope(const std::string& _operation_name,
                                         const OpenTelemetryThreadTraceContext& _parent_thread_trace_context)
        : OpenTelemetryThreadTraceContextScope(_operation_name,
                                               _parent_thread_trace_context,
                                               nullptr,
                                               _parent_thread_trace_context.span_log)
    {
    }

    /// For Servers like HTTP/TCP/GRPC to initialize tracing context on thread that process requests from clients
    OpenTelemetryThreadTraceContextScope(const std::string& _operation_name,
                                         OpenTelemetryTraceContext _parent_trace_context,
                                         const Settings& _settings,
                                         const std::weak_ptr<OpenTelemetrySpanLog>& _log)
        : OpenTelemetryThreadTraceContextScope(_operation_name,
                                               _parent_trace_context,
                                               &_settings,
                                               _log)
    {
    }

    OpenTelemetryThreadTraceContextScope(const std::string& _operation_name,
                                         OpenTelemetryTraceContext _parent_trace_context,
                                         const Settings* settings_ptr,
                                         const std::weak_ptr<OpenTelemetrySpanLog>& _log);

    ~OpenTelemetryThreadTraceContextScope();

    OpenTelemetrySpan root_span;

private:
    bool is_context_owner = true;
};

using OpenTelemetryThreadTraceContextScopePtr = std::unique_ptr<OpenTelemetryThreadTraceContextScope>;

/// A span holder is usually used in a function scope
struct OpenTelemetrySpanHolder : public OpenTelemetrySpan
{
    OpenTelemetrySpanHolder(const std::string & _operation_name);
    ~OpenTelemetrySpanHolder();

    void finish();
};

}
