#include <Common/AsyncTaskExecutor.h>
#include <base/scope_guard.h>


namespace DB
{

AsyncTaskExecutor::AsyncTaskExecutor(std::unique_ptr<AsyncTask> task_) : task(std::move(task_))
{
}

void AsyncTaskExecutor::resume()
{
    if (routine_is_finished)
        return;

    /// Create fiber lazily on first resume() call.
    if (!fiber)
        createFiber();

    if (!checkBeforeTaskResume())
        return;

    {
        std::lock_guard guard(fiber_lock);
        if (is_cancelled)
            return;

        resumeUnlocked();

        /// Destroy fiber when it's finished.
        if (routine_is_finished)
            destroyFiber();

        if (exception)
            processException(exception);
    }

    afterTaskResume();
}

void AsyncTaskExecutor::resumeUnlocked()
{
    fiber.resume();
}

void AsyncTaskExecutor::cancel()
{
    std::lock_guard guard(fiber_lock);
    is_cancelled = true;
    {
        SCOPE_EXIT({ destroyFiber(); });
        cancelBefore();
    }
    cancelAfter();
}

void AsyncTaskExecutor::restart()
{
    std::lock_guard guard(fiber_lock);
    if (!routine_is_finished)
        destroyFiber();
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
        auto async_callback = AsyncCallback{executor, suspend_callback};
        try
        {
            executor.task->run(async_callback, suspend_callback);
        }
        catch (const boost::context::detail::forced_unwind &)
        {
            /// This exception is thrown by fiber implementation in case if fiber is being deleted but hasn't exited
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

void AsyncTaskExecutor::createFiber()
{
    fiber = Fiber(fiber_stack, Routine{*this});
}

void AsyncTaskExecutor::destroyFiber()
{
    Fiber to_destroy = std::move(fiber);
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

