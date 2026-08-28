#include <utility>

#include <Common/AsyncTaskExecutor.h>
#include <base/scope_guard.h>
#include <fmt/format.h>


namespace DB
{

AsyncTaskExecutor::AsyncTaskExecutor(
    std::unique_ptr<AsyncTask> task_,
    String operation_name_,
    OpenTelemetry::SpanAttributes initial_span_attributes_,
    UInt64 initial_span_start_time_us_,
    UInt64 initial_span_id_)
    : task(std::move(task_))
    , operation_name(std::move(operation_name_))
    , parent_trace_context(OpenTelemetry::CurrentContext())
    , span_attributes(std::move(initial_span_attributes_))
    , initial_span_start_time_us(initial_span_start_time_us_)
    , initial_span_id(initial_span_id_)
{
}

void AsyncTaskExecutor::addSpanAttribute(OpenTelemetry::SpanAttribute attribute)
{
    std::lock_guard guard(span_attributes_mutex);
    span_attributes.push_back(std::move(attribute));
}

void AsyncTaskExecutor::flushSpanAttributes(OpenTelemetry::Span & span) noexcept
{
    if (!span.isTraceEnabled())
        return;
    /// Span::addAttribute never throws, attributes are best-effort
    std::lock_guard guard(span_attributes_mutex);
    for (const auto & attribute : span_attributes)
        span.addAttribute(attribute);
}

void AsyncTaskExecutor::resume()
{
    if (routine_is_finished)
        return;

    /// Create coroutine lazily on first resume() call.
    if (!coroutine)
        createCoroutine();

    if (!checkBeforeTaskResume())
        return;

    {
        std::lock_guard guard(coroutine_lock);
        if (is_cancelled)
            return;

        resumeUnlocked();

        /// Destroy coroutine when it's finished.
        if (routine_is_finished)
            destroyCoroutine();

        if (exception)
            processException(exception);
    }

    afterTaskResume();
}

void AsyncTaskExecutor::resumeUnlocked()
{
    coroutine.resume();
}

void AsyncTaskExecutor::cancel()
{
    std::lock_guard guard(coroutine_lock);
    is_cancelled = true;
    {
        SCOPE_EXIT({ destroyCoroutine(); });
        cancelBefore();
    }
    cancelAfter();
}

void AsyncTaskExecutor::restart()
{
    std::lock_guard guard(coroutine_lock);
    if (!routine_is_finished)
        destroyCoroutine();
    routine_is_finished = false;
}

struct AsyncTaskExecutor::Routine
{
    AsyncTaskExecutor & executor;

    struct AsyncCallback
    {
        AsyncTaskExecutor & executor;
        SuspendCallback suspend_callback;

        void operator()(int fd, Poco::Timespan timeout, AsyncEventTimeoutType type, const std::string & desc, uint32_t events)
        {
            executor.processAsyncEvent(fd, timeout, type, desc, events);
            suspend_callback();
            executor.clearAsyncEvent();
        }
    };

    void operator()(SuspendCallback suspend_callback)
    {
        /// Stores the fiber-local tracing context from the thread that created the executor and open one span per task execution.
        /// A non-zero initial span id continues a span opened before the executor existed: spans created
        /// during the synchronous send already reference it as their parent, so the fiber span must keep it.
        OpenTelemetry::TracingContextHolder trace_context_holder(
            executor.operation_name,
            executor.parent_trace_context,
            std::exchange(executor.initial_span_id, 0ULL));

        /// A non-zero initial start time hands over a span opened before the executor existed
        /// Otherwise keep the current time set by the holder. The exchange makes the handover one-shot
        if (trace_context_holder.root_span.isTraceEnabled())
        {
            if (UInt64 initial_start_time_us = std::exchange(executor.initial_span_start_time_us, 0ULL))
                trace_context_holder.root_span.start_time_us = initial_start_time_us;
        }

        /// Copy the buffered attributes onto the span right before it is finished
        SCOPE_EXIT({ executor.flushSpanAttributes(trace_context_holder.root_span); });

        auto async_callback = AsyncCallback{executor, suspend_callback};
        try
        {
            executor.task->run(async_callback, suspend_callback);
        }
        catch (const boost::context::detail::forced_unwind &)
        {
            /// This exception is thrown by coroutine implementation in case if coroutine is being deleted but hasn't exited
            /// It should not be caught or it will segfault.
            /// Other exceptions must be caught
            throw;
        }
        catch (...)
        {
            executor.exception = std::current_exception();
        }

        executor.routine_is_finished = true;
    }
};

void AsyncTaskExecutor::createCoroutine()
{
    coroutine = StackfulCoroutine(coroutine_stack, Routine{*this});
}

void AsyncTaskExecutor::destroyCoroutine()
{
    StackfulCoroutine to_destroy = std::move(coroutine);
}

String getSocketTimeoutExceededMessageByTimeoutType(AsyncEventTimeoutType type, Poco::Timespan timeout, const String & socket_description)
{
    switch (type)
    {
        case AsyncEventTimeoutType::CONNECT:
            return fmt::format("Timeout exceeded while connecting to socket ({}, connection timeout {} ms)", socket_description, timeout.totalMilliseconds());
        case AsyncEventTimeoutType::RECEIVE:
            return fmt::format("Timeout exceeded while reading from socket ({}, receive timeout {} ms)", socket_description, timeout.totalMilliseconds());
        case AsyncEventTimeoutType::SEND:
            return fmt::format("Timeout exceeded while writing to socket ({}, send timeout {} ms)", socket_description, timeout.totalMilliseconds());
        default:
            return fmt::format("Timeout exceeded while working with socket ({}, {} ms)", socket_description, timeout.totalMilliseconds());
    }
}

}

