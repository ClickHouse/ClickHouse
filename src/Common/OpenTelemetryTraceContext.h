#pragma once

#include <Core/Field.h>

namespace DB
{

struct Settings;
class OpenTelemetrySpanLog;
class WriteBuffer;
class ReadBuffer;
struct ExecutionStatus;

namespace OpenTelemetry
{

/// See https://opentelemetry.io/docs/reference/specification/trace/api/#spankind
enum class SpanKind : uint8_t
{
    /// Default value. Indicates that the span represents an internal operation within an application,
    /// as opposed to an operations with remote parents or children.
    INTERNAL = 0,

    /// Indicates that the span covers server-side handling of a synchronous RPC or other remote request.
    /// This span is often the child of a remote CLIENT span that was expected to wait for a response.
    SERVER   = 1,

    /// Indicates that the span describes a request to some remote service.
    /// This span is usually the parent of a remote SERVER span and does not end until the response is received.
    CLIENT   = 2,

    /// Indicates that the span describes the initiators of an asynchronous request. This parent span will often end before the corresponding child CONSUMER span, possibly even before the child span starts.
    /// In messaging scenarios with batching, tracing individual messages requires a new PRODUCER span per message to be created.
    PRODUCER = 3,

    /// Indicates that the span describes a child of an asynchronous PRODUCER request
    CONSUMER = 4
};

struct Span
{
    UUID trace_id;
    UInt64 span_id = 0;
    UInt64 parent_span_id = 0;
    String operation_name;
    UInt64 start_time_us = 0;
    UInt64 finish_time_us = 0;
    SpanKind kind = SpanKind::INTERNAL;
    Map attributes;

    /// Following methods are declared as noexcept to make sure they're exception safe.
    /// This is because sometimes they will be called in exception handlers/dtor.
    /// Returns true if attribute is successfully added and false otherwise.
    bool addAttribute(std::string_view name, UInt64 value) noexcept;
    bool addAttributeIfNotZero(std::string_view name, UInt64 value) noexcept;
    bool addAttribute(std::string_view name, std::string_view value) noexcept;
    bool addAttributeIfNotEmpty(std::string_view name, std::string_view value) noexcept;
    bool addAttribute(std::string_view name, std::function<String()> value_supplier) noexcept;
    bool addAttribute(const Exception & e) noexcept;
    bool addAttribute(std::exception_ptr e) noexcept;
    bool addAttribute(const ExecutionStatus & e) noexcept;

    bool isTraceEnabled() const
    {
        return trace_id != UUID();
    }

private:
    bool addAttributeImpl(std::string_view name, std::string_view value) noexcept;
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
    UUID trace_id;
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

    void deserialize(ReadBuffer & buf);
    void serialize(WriteBuffer & buf) const;
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
    explicit SpanHolder(std::string_view, SpanKind _kind = SpanKind::INTERNAL);
    ~SpanHolder();

    /// Finish a span explicitly if needed.
    /// It's safe to call it multiple times
    void finish() noexcept;
};

}

inline WriteBuffer & operator<<(WriteBuffer & buf, const OpenTelemetry::TracingContext & context)
{
    context.serialize(buf);
    return buf;
}

inline ReadBuffer & operator>> (ReadBuffer & buf, OpenTelemetry::TracingContext & context)
{
    context.deserialize(buf);
    return buf;
}

}
