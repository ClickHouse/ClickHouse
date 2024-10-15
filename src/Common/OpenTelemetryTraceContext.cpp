#include "Interpreters/OpenTelemetrySpanLog.h"

#include <random>
#include <base/getThreadId.h>
#include <Common/thread_local_rng.h>
#include <Common/Exception.h>
#include <base/hex.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/AsyncTaskExecutor.h>

namespace DB
{

namespace Setting
{
    extern const SettingsFloat opentelemetry_start_trace_probability;
}

namespace OpenTelemetry
{

/// This code can be executed inside fibers, we should use fiber local tracing context.
thread_local FiberLocal<TracingContextOnThread> current_trace_context;

bool Span::addAttribute(std::string_view name, UInt64 value) noexcept
{
    if (!this->isTraceEnabled() || name.empty())
        return false;

    return addAttributeImpl(name, toString(value));
}

bool Span::addAttributeIfNotZero(std::string_view name, UInt64 value) noexcept
{
    if (!this->isTraceEnabled() || name.empty() || value == 0)
        return false;

    return addAttributeImpl(name, toString(value));
}

bool Span::addAttribute(std::string_view name, std::string_view value) noexcept
{
    if (!this->isTraceEnabled() || name.empty())
        return false;

    return addAttributeImpl(name, value);
}

bool Span::addAttributeIfNotEmpty(std::string_view name, std::string_view value) noexcept
{
    if (!this->isTraceEnabled() || name.empty() || value.empty())
        return false;

    return addAttributeImpl(name, value);
}

bool Span::addAttribute(std::string_view name, std::function<String()> value_supplier) noexcept
{
    if (!this->isTraceEnabled() || name.empty() || !value_supplier)
        return false;

    try
    {
        auto value = value_supplier();
        return value.empty() ? false : addAttributeImpl(name, value);
    }
    catch (...)
    {
        /// Ignore exception raised by value_supplier
        return false;
    }
}

bool Span::addAttribute(const Exception & e) noexcept
{
    if (!this->isTraceEnabled())
        return false;

    return addAttributeImpl("clickhouse.exception", getExceptionMessage(e, false))
        && addAttributeImpl("clickhouse.exception_code", toString(e.code()));
}

bool Span::addAttribute(std::exception_ptr e) noexcept
{
    if (!this->isTraceEnabled() || e == nullptr)
        return false;

    return addAttributeImpl("clickhouse.exception", getExceptionMessage(e, false));
}

bool Span::addAttribute(const ExecutionStatus & e) noexcept
{
    if (!this->isTraceEnabled())
        return false;

    return addAttributeImpl("clickhouse.exception", e.message)
        && addAttributeImpl("clickhouse.exception_code", toString(e.code));
}

bool Span::addAttributeImpl(std::string_view name, std::string_view value) noexcept
{
    try
    {
        this->attributes.push_back(Tuple{name, value});
    }
    catch (...)
    {
        return false;
    }
    return true;
}

SpanHolder::SpanHolder(std::string_view _operation_name, SpanKind _kind)
{
    if (!current_trace_context->isTraceEnabled())
    {
        return;
    }

    /// Use try-catch to make sure the ctor is exception safe.
    try
    {
        this->trace_id = current_trace_context->trace_id;
        this->parent_span_id = current_trace_context->span_id;
        this->span_id = thread_local_rng(); // create a new id for this span
        this->operation_name = _operation_name;
        this->kind = _kind;
        this->start_time_us
            = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        this->addAttribute("clickhouse.thread_id", getThreadId());
    }
    catch (...)
    {
        tryLogCurrentException(__FUNCTION__);

        /// Clear related fields to make sure the span won't be recorded.
        this->trace_id = UUID();
        return;
    }

    /// Set current span as parent of other spans created later on this thread.
    current_trace_context->span_id = this->span_id;
}

void SpanHolder::finish() noexcept
{
    if (!this->isTraceEnabled())
        return;

    // First of all, restore old value of current span.
    assert(current_trace_context->span_id == span_id);
    current_trace_context->span_id = parent_span_id;

    try
    {
        auto log = current_trace_context->span_log.lock();

        /// The log might be disabled, check it before use
        if (log)
        {
            this->finish_time_us
                = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

            log->add(OpenTelemetrySpanLogElement(*this));
        }
    }
    catch (...)
    {
        tryLogCurrentException(__FUNCTION__);
    }

    trace_id = UUID();
}

SpanHolder::~SpanHolder()
{
    finish();
}

bool TracingContext::parseTraceparentHeader(std::string_view traceparent, String & error)
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
    UUIDHelpers::getHighBytes(this->trace_id) = trace_id_higher_64;
    UUIDHelpers::getLowBytes(this->trace_id) = trace_id_lower_64;
    this->span_id = span_id_64;
    return true;
}

String TracingContext::composeTraceparentHeader() const
{
    // This span is a parent for its children, so we specify this span_id as a
    // parent id.
    return fmt::format(
        "00-{:016x}{:016x}-{:016x}-{:02x}",
        UUIDHelpers::getHighBytes(trace_id),
        UUIDHelpers::getLowBytes(trace_id),
        span_id,
        // This cast is needed because fmt is being weird and complaining that
        // "mixing character types is not allowed".
        static_cast<uint8_t>(trace_flags));
}

void TracingContext::deserialize(ReadBuffer & buf)
{
    readUUIDText(trace_id, buf);
    assertChar('\n', buf);
    readIntText(span_id, buf);
    assertChar('\n', buf);
    readEscapedString(tracestate, buf);
    assertChar('\n', buf);
    readIntText(trace_flags, buf);
    assertChar('\n', buf);
}

void TracingContext::serialize(WriteBuffer & buf) const
{
    writeUUIDText(trace_id, buf);
    writeChar('\n', buf);
    writeIntText(span_id, buf);
    writeChar('\n', buf);
    writeEscapedString(tracestate, buf);
    writeChar('\n', buf);
    writeIntText(trace_flags, buf);
    writeChar('\n', buf);
}

const TracingContextOnThread & CurrentContext()
{
    return *current_trace_context;
}

void TracingContextOnThread::reset() noexcept
{
    this->trace_id = UUID();
    this->span_id = 0;
    this->trace_flags = TRACE_FLAG_NONE;
    this->tracestate = "";
    this->span_log.reset();
}

TracingContextHolder::TracingContextHolder(
    std::string_view _operation_name,
    TracingContext _parent_trace_context,
    const Settings * settings_ptr,
    const std::weak_ptr<OpenTelemetrySpanLog> & _span_log)
{
    /// Use try-catch to make sure the ctor is exception safe.
    /// If any exception is raised during the construction, the tracing is not enabled on current thread.
    try
    {
        if (current_trace_context->isTraceEnabled())
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
            this->root_span.trace_id = current_trace_context->trace_id;
            this->root_span.parent_span_id = current_trace_context->span_id;
            this->root_span.span_id = thread_local_rng();
            this->root_span.operation_name = _operation_name;
            this->root_span.start_time_us
                = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

            /// Set the root span as parent of other spans created on current thread
            current_trace_context->span_id = this->root_span.span_id;
            return;
        }

        if (!_parent_trace_context.isTraceEnabled())
        {
            if (settings_ptr == nullptr)
                /// Skip tracing context initialization on current thread
                return;

            // Start the trace with some configurable probability.
            std::bernoulli_distribution should_start_trace{(*settings_ptr)[Setting::opentelemetry_start_trace_probability]};
            if (!should_start_trace(thread_local_rng))
                /// skip tracing context initialization on current thread
                return;

            while (_parent_trace_context.trace_id == UUID())
            {
                // Make sure the random generated trace_id is not 0 which is an invalid id.
                UUIDHelpers::getHighBytes(_parent_trace_context.trace_id) = thread_local_rng();
                UUIDHelpers::getLowBytes(_parent_trace_context.trace_id) = thread_local_rng();
            }
            _parent_trace_context.span_id = 0;
        }

        this->root_span.trace_id = _parent_trace_context.trace_id;
        this->root_span.parent_span_id = _parent_trace_context.span_id;
        this->root_span.span_id = thread_local_rng();
        this->root_span.operation_name = _operation_name;
        this->root_span.start_time_us
            = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        /// Add new initialization here
    }
    catch (...)
    {
        tryLogCurrentException(__FUNCTION__);

        /// Clear related fields to make sure the tracing is not enabled.
        this->root_span.trace_id = UUID();
        return;
    }

    /// Set up trace context on current thread only when the root span is successfully initialized.
    *current_trace_context = _parent_trace_context;
    current_trace_context->span_id = this->root_span.span_id;
    current_trace_context->trace_flags = TRACE_FLAG_SAMPLED;
    current_trace_context->span_log = _span_log;
}

TracingContextHolder::~TracingContextHolder()
{
    if (!this->root_span.isTraceEnabled())
    {
        return;
    }

    try
    {
        auto shared_span_log = current_trace_context->span_log.lock();
        if (shared_span_log)
        {
            try
            {
                /// This object is created to initialize tracing context on a new thread,
                /// it's helpful to record the thread_id so that we know the thread switching from the span log
                this->root_span.addAttribute("clickhouse.thread_id", getThreadId());
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
                /// It's acceptable that the attribute is not recorded in case of any exception,
                /// so the exception is ignored to try to log the span.
            }

            this->root_span.finish_time_us
                = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

            shared_span_log->add(OpenTelemetrySpanLogElement(this->root_span));
        }
    }
    catch (...)
    {
        tryLogCurrentException(__FUNCTION__);
    }

    this->root_span.trace_id = UUID();

    if (this->is_context_owner)
    {
        /// Clear the context on current thread
        current_trace_context->reset();
    }
    else
    {
        current_trace_context->span_id = this->root_span.parent_span_id;
    }
}

}
}
