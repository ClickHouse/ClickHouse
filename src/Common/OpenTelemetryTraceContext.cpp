#include "Interpreters/OpenTelemetrySpanLog.h"

#include <random>
#include <base/getThreadId.h>
#include <Common/Exception.h>
#include <Common/hex.h>
#include <Core/Settings.h>

namespace DB
{

thread_local OpenTelemetryThreadTraceContext current_thread_trace_context;

void OpenTelemetrySpan::addAttribute(const std::string & name, UInt64 value)
{
    if (trace_id == UUID() || name.empty())
        return;

    this->attributes.push_back(Tuple{name, std::to_string(value)});
}

void OpenTelemetrySpan::addAttribute(const std::string & name, const std::string & value)
{
    if (trace_id == UUID() || name.empty() || value.empty())
        return;

    this->attributes.push_back(Tuple{name, value});
}

void OpenTelemetrySpan::addAttribute(const std::string & name, std::function<std::string()> value_supplier)
{
    if (!this->isTraceEnabled() || !value_supplier)
        return;

    std::string value = value_supplier();
    if (value.empty())
        return;

    this->attributes.push_back(Tuple{name, value});
}

void OpenTelemetrySpan::addAttribute(const Exception & e)
{
    if (trace_id == UUID())
        return;

    this->attributes.push_back(Tuple{"clickhouse.exception", getExceptionMessage(e, false)});
}

void OpenTelemetrySpan::addAttribute(std::exception_ptr e)
{
    if (trace_id == UUID() || e == nullptr)
        return;

    this->attributes.push_back(Tuple{"clickhouse.exception", getExceptionMessage(e, false)});
}

OpenTelemetrySpanHolder::OpenTelemetrySpanHolder(const std::string & _operation_name)
{
    if (current_thread_trace_context.trace_id == UUID())
    {
        this->trace_id = 0;
        this->span_id = 0;
        this->parent_span_id = 0;
    }
    else
    {
        this->trace_id = current_thread_trace_context.trace_id;
        this->parent_span_id = current_thread_trace_context.span_id;
        this->span_id = thread_local_rng(); // create a new id for this span
        this->operation_name = _operation_name;
        this->start_time_us
            = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        // set current span id to this
        current_thread_trace_context.span_id = this->span_id;
    }
}

void OpenTelemetrySpanHolder::finish()
{
    if (trace_id == UUID())
        return;

    // First of all, return old value of current span.
    assert(current_thread_trace_context.span_id == span_id);
    current_thread_trace_context.span_id = parent_span_id;

    try
    {
        auto log = current_thread_trace_context.span_log.lock();
        if (!log)
        {
            // The log might be disabled.
            return;
        }

        this->finish_time_us
            = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        log->add(OpenTelemetrySpanLogElement(*this));
    }
    catch (...)
    {
        tryLogCurrentException(__FUNCTION__);
    }

    trace_id = UUID();
}

OpenTelemetrySpanHolder::~OpenTelemetrySpanHolder()
{
    finish();
}


bool OpenTelemetryTraceContext::parseTraceparentHeader(const std::string & traceparent, std::string & error)
{
    trace_id = 0;

    // Version 00, which is the only one we can parse, is fixed width. Use this
    // fact for an additional sanity check.
    const int expected_length = strlen("xx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-xxxxxxxxxxxxxxxx-xx");
    if (traceparent.length() != expected_length)
    {
        error = fmt::format("unexpected length {}, expected {}", traceparent.length(), expected_length);
        return false;
    }

    const char * data = traceparent.data();

    uint8_t version = unhex2(data);
    data += 2;

    if (version != 0)
    {
        error = fmt::format("unexpected version {}, expected 00", version);
        return false;
    }

    if (*data != '-')
    {
        error = fmt::format("Malformed traceparant header: {}", traceparent);
        return false;
    }

    ++data;
    UInt64 trace_id_higher_64 = unhexUInt<UInt64>(data);
    UInt64 trace_id_lower_64 = unhexUInt<UInt64>(data + 16);
    data += 32;

    if (*data != '-')
    {
        error = fmt::format("Malformed traceparant header: {}", traceparent);
        return false;
    }

    ++data;
    UInt64 span_id_64 = unhexUInt<UInt64>(data);
    data += 16;

    if (*data != '-')
    {
        error = fmt::format("Malformed traceparant header: {}", traceparent);
        return false;
    }

    ++data;
    this->trace_flags = unhex2(data);
    this->trace_id.toUnderType().items[0] = trace_id_higher_64;
    this->trace_id.toUnderType().items[1] = trace_id_lower_64;
    this->span_id = span_id_64;
    return true;
}


std::string OpenTelemetryTraceContext::composeTraceparentHeader() const
{
    // This span is a parent for its children, so we specify this span_id as a
    // parent id.
    return fmt::format(
        "00-{:016x}{:016x}-{:016x}-{:02x}",
        trace_id.toUnderType().items[0],
        trace_id.toUnderType().items[1],
        span_id,
        // This cast is needed because fmt is being weird and complaining that
        // "mixing character types is not allowed".
        static_cast<uint8_t>(trace_flags));
}

const OpenTelemetryThreadTraceContext & OpenTelemetryThreadTraceContext::current()
{
    return current_thread_trace_context;
}

void OpenTelemetryThreadTraceContext::reset()
{
    this->trace_id = UUID();
    this->span_id = 0;
    this->trace_flags = 0;
    this->tracestate = "";
    this->span_log.reset();
}

OpenTelemetryThreadTraceContextScope::OpenTelemetryThreadTraceContextScope(
    const std::string & _operation_name,
    OpenTelemetryTraceContext _parent_trace_context,
    const Settings * settings_ptr,
    const std::weak_ptr<OpenTelemetrySpanLog> & _span_log)
{
    if (current_thread_trace_context.isTraceEnabled())
    {
        ///
        /// This is not the normal case,
        /// it means that construction of current object is not at the start of current thread.
        /// Usually this is due to:
        ///    1. bad design
        ///    2. right design but code changes so that original point where this object is constructing is not the new start execution of current thread
        ///
        /// In such case, we should use current context as parent of this new constructing object,
        /// So this branch ensures this class can be instantiated multiple times on one same thread safely.
        ///
        this->is_context_owner = false;
        this->root_span.trace_id = current_thread_trace_context.trace_id;
        this->root_span.parent_span_id = current_thread_trace_context.span_id;
        this->root_span.span_id = thread_local_rng();
        this->root_span.operation_name = _operation_name;
        this->root_span.start_time_us
            = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        current_thread_trace_context.span_id = this->root_span.span_id;
        return;
    }

    this->root_span.trace_id = UUID();
    this->root_span.span_id = 0;

    if (!_parent_trace_context.isTraceEnabled())
    {
        if (settings_ptr == nullptr)
            /// skip tracing context initialization on current thread
            return;

        // start the trace ourselves, with some configurable probability.
        std::bernoulli_distribution should_start_trace{settings_ptr->opentelemetry_start_trace_probability};
        if (!should_start_trace(thread_local_rng))
            /// skip tracing context initialization on current thread
            return;

        while (_parent_trace_context.trace_id == UUID())
        {
            // make sure the random generated trace_id is not 0 which is an invalid id
            _parent_trace_context.trace_id.toUnderType().items[0] = thread_local_rng(); //-V656
            _parent_trace_context.trace_id.toUnderType().items[1] = thread_local_rng(); //-V656
        }
        _parent_trace_context.span_id = 0;
    }

    this->root_span.trace_id = _parent_trace_context.trace_id;
    this->root_span.parent_span_id = _parent_trace_context.span_id;
    this->root_span.span_id = thread_local_rng();
    this->root_span.operation_name = _operation_name;
    this->root_span.start_time_us
        = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    /// set up trace context on current thread
    current_thread_trace_context = _parent_trace_context;
    current_thread_trace_context.span_id = this->root_span.span_id;
    current_thread_trace_context.trace_flags = 1;
    current_thread_trace_context.span_log = _span_log;
}

OpenTelemetryThreadTraceContextScope::~OpenTelemetryThreadTraceContextScope()
{
    if (this->root_span.isTraceEnabled())
    {
        auto shared_span_log = current_thread_trace_context.span_log.lock();
        if (shared_span_log)
        {
            this->root_span.addAttribute("clickhouse.thread_id", getThreadId());
            this->root_span.finish_time_us
                = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

            shared_span_log->add(OpenTelemetrySpanLogElement(this->root_span));
        }

        this->root_span.trace_id = UUID();
    }

    if (this->is_context_owner)
    {
        // clear the context on current thread
        current_thread_trace_context.reset();
    }
    else
    {
        current_thread_trace_context.span_id = this->root_span.parent_span_id;
    }
}


}
