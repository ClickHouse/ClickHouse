#pragma once

#include <Core/Field.h>

namespace DB
{

struct Settings;

struct OpenTelemetrySpan
{
    UUID trace_id{};
    UInt64 span_id = 0;
    UInt64 parent_span_id = 0;
    String operation_name;
    UInt64 start_time_us = 0;
    UInt64 finish_time_us = 0;
    Map attributes;

    void addAttribute(std::string_view name, UInt64 value);
    void addAttributeIfNotZero(std::string_view name, UInt64 value);
    void addAttribute(std::string_view name, std::string_view value);
    void addAttribute(std::string_view name, std::function<String()> value_supplier);

    /// Following two methods are declared as noexcept to make sure they're exception safe
    /// This is because they're usually called in exception handler
    void addAttribute(const Exception & e) noexcept;
    void addAttribute(std::exception_ptr e) noexcept;

    bool isTraceEnabled() const
    {
        return trace_id != UUID();
    }
};

class OpenTelemetrySpanLog;

/// See https://www.w3.org/TR/trace-context/ for trace_flags definition
enum TraceFlags : UInt8
{
    TRACE_FLAG_NONE = 0,
    TRACE_FLAG_SAMPLED = 1,
};

// The runtime info we need to create new OpenTelemetry spans.
struct OpenTelemetryTraceContext
{
    UUID trace_id{};
    UInt64 span_id = 0;
    // The incoming tracestate header and the trace flags, we just pass them
    // downstream. See https://www.w3.org/TR/trace-context/
    String tracestate;
    UInt8 trace_flags = TRACE_FLAG_NONE;

    // Parse/compose OpenTelemetry traceparent header.
    bool parseTraceparentHeader(std::string_view traceparent, String & error);
    String composeTraceparentHeader() const;

    bool isTraceEnabled() const
    {
        return trace_id != UUID();
    }
};

/// Tracing context kept on thread local
struct OpenTelemetryThreadTraceContext : OpenTelemetryTraceContext
{
    OpenTelemetryThreadTraceContext& operator =(const OpenTelemetryTraceContext& context)
    {
        *(static_cast<OpenTelemetryTraceContext*>(this)) = context;
        return *this;
    }

    void reset();

    static const OpenTelemetryThreadTraceContext& current();

    /// Use weak_ptr instead of shared_ptr to hold a reference to the underlying system.opentelemetry_span_log table
    /// Since this object is kept on threads and passed across threads, a weak_ptr is more safe to prevent potential leak
    std::weak_ptr<OpenTelemetrySpanLog> span_log;
};

/// A scoped tracing context, is used to hold the tracing context at the beginning of each thread execution and clear the context automatically when the scope exists.
/// It should be the root of all span logs for one tracing.
///
/// It's SAFE to construct this object multiple times on one same thread,
/// but it's not encourage to do so because this is only a protection in case of code changes.
struct OpenTelemetryThreadTraceContextScope
{
    /// Forbidden copy ctor and assignment to make the destructor safe
    OpenTelemetryThreadTraceContextScope(const OpenTelemetryThreadTraceContextScope& scope) = delete;
    OpenTelemetryThreadTraceContextScope& operator =(const OpenTelemetryThreadTraceContextScope& scope) = delete;

    OpenTelemetryThreadTraceContextScope(std::string_view _operation_name,
                                         const OpenTelemetryTraceContext& _parent_trace_context,
                                         const std::weak_ptr<OpenTelemetrySpanLog>& _log)
        : OpenTelemetryThreadTraceContextScope(_operation_name,
                                               _parent_trace_context,
                                               nullptr,
                                               _log)
    {
    }

    /// Initialize a tracing context on a child thread based on the context from the parent thread
    OpenTelemetryThreadTraceContextScope(std::string_view _operation_name,
                                         const OpenTelemetryThreadTraceContext& _parent_thread_trace_context)
        : OpenTelemetryThreadTraceContextScope(_operation_name,
                                               _parent_thread_trace_context,
                                               nullptr,
                                               _parent_thread_trace_context.span_log)
    {
    }

    /// For servers like HTTP/TCP/GRPC to initialize tracing context on thread that process requests from clients
    OpenTelemetryThreadTraceContextScope(std::string_view _operation_name,
                                         OpenTelemetryTraceContext _parent_trace_context,
                                         const Settings& _settings,
                                         const std::weak_ptr<OpenTelemetrySpanLog>& _log)
        : OpenTelemetryThreadTraceContextScope(_operation_name,
                                               _parent_trace_context,
                                               &_settings,
                                               _log)
    {
    }

    OpenTelemetryThreadTraceContextScope(std::string_view _operation_name,
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
    OpenTelemetrySpanHolder(std::string_view);
    ~OpenTelemetrySpanHolder();

    void finish();
};

}
