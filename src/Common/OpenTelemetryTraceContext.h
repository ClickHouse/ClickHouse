#pragma once

#include <Core/Field.h>

namespace DB
{

struct Settings;
class OpenTelemetrySpanLog;

namespace OpenTelemetry
{

struct Span
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
    void addAttributeIfNotEmpty(std::string_view name, std::string_view value);
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

/// See https://www.w3.org/TR/trace-context/ for trace_flags definition
enum TraceFlags : UInt8
{
    TRACE_FLAG_NONE = 0,
    TRACE_FLAG_SAMPLED = 1,
};

/// The runtime info we need to create new OpenTelemetry spans.
struct TracingContext
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

/// Tracing context kept on each thread
struct TracingContextOnThread : TracingContext
{
    TracingContextOnThread& operator =(const TracingContext& context)
    {
        *(static_cast<TracingContext*>(this)) = context;
        return *this;
    }

    void reset() noexcept;

    /// Use weak_ptr instead of shared_ptr to hold a reference to the underlying system.opentelemetry_span_log table
    /// Since this object is kept on threads and passed across threads, a weak_ptr is more safe to prevent potential leak
    std::weak_ptr<OpenTelemetrySpanLog> span_log;
};

/// Get tracing context on current thread
const TracingContextOnThread& CurrentContext();

/// Holder of tracing context.
/// It should be initialized at the beginning of each thread execution.
/// And once it's destructed, it clears the context automatically.
///
/// It's also the root of all spans on current thread execution.
///
/// Although it's SAFE to construct this object multiple times on one same thread, it should be created at the beginning of one thread execution.
struct TracingContextHolder
{
    /// Forbidden copy ctor and assignment to make the destructor safe
    TracingContextHolder(const TracingContextHolder& scope) = delete;
    TracingContextHolder& operator =(const TracingContextHolder& scope) = delete;

    TracingContextHolder(std::string_view _operation_name,
        const TracingContext& _parent_trace_context,
        const std::weak_ptr<OpenTelemetrySpanLog>& _log)
        : TracingContextHolder(_operation_name,
            _parent_trace_context,
            nullptr,
            _log)
    {
    }

    /// Initialize a tracing context on a child thread based on the context from the parent thread
    TracingContextHolder(std::string_view _operation_name, const TracingContextOnThread & _parent_thread_trace_context)
        : TracingContextHolder(_operation_name,
            _parent_thread_trace_context,
            nullptr,
            _parent_thread_trace_context.span_log)
    {
    }

    /// For servers like HTTP/TCP/GRPC to initialize tracing context on thread that process requests from clients
    TracingContextHolder(std::string_view _operation_name,
        TracingContext _parent_trace_context,
        const Settings & _settings,
        const std::weak_ptr<OpenTelemetrySpanLog> & _log)
        : TracingContextHolder(_operation_name,
            _parent_trace_context,
            &_settings,
            _log)
    {
    }

    TracingContextHolder(std::string_view _operation_name,
        TracingContext _parent_trace_context,
        const Settings* settings_ptr,
        const std::weak_ptr<OpenTelemetrySpanLog> & _log);

    ~TracingContextHolder();

    Span root_span;

private:
    bool is_context_owner = true;
};

using TracingContextHolderPtr = std::unique_ptr<TracingContextHolder>;

/// A span holder that creates span automatically in a (function) scope if tracing is enabled.
/// Once it's created or destructed, it automatically maitains the tracing context on the thread that it lives.
struct SpanHolder : public Span
{
    SpanHolder(std::string_view);
    ~SpanHolder();

    /// Finish a span explicitly if needed.
    /// It's safe to call it multiple times
    void finish() noexcept;
};

}

}

