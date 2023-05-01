#include <Common/AsyncTaskExecutor.h>

namespace DB
{

AsyncTaskExecutor::AsyncTaskExecutor(std::unique_ptr<AsyncTask> task_) : task(std::move(task_))
{
    createFiber();
}

void AsyncTaskExecutor::resume()
{
    if (routine_is_finished)
        return;

    if (!checkBeforeTaskResume())
        return;

    {
        std::lock_guard guard(fiber_lock);
        if (is_cancelled)
            return;

        resumeUnlocked();
        if (exception)
            processException(exception);
    }

    afterTaskResume();
}

void AsyncTaskExecutor::resumeUnlocked()
{
    fiber = std::move(fiber).resume();
}

void AsyncTaskExecutor::cancel()
{
    std::lock_guard guard(fiber_lock);
    is_cancelled = true;
    cancelBefore();
    destroyFiber();
    cancelAfter();
}

void AsyncTaskExecutor::restart()
{
    std::lock_guard guard(fiber_lock);
    if (fiber)
        destroyFiber();
    createFiber();
    routine_is_finished = false;
}

struct AsyncTaskExecutor::Routine
{
    AsyncTaskExecutor & executor;

    struct AsyncCallback
    {
        AsyncTaskExecutor & executor;
        Fiber & fiber;

        void operator()(int fd, Poco::Timespan timeout, AsyncEventTimeoutType type, const std::string & desc, uint32_t events)
        {
            executor.processAsyncEvent(fd, timeout, type, desc, events);
            fiber = std::move(fiber).resume();
            executor.clearAsyncEvent();
        }
    };

    struct ResumeCallback
    {
        Fiber & fiber;

        void operator()()
        {
            fiber = std::move(fiber).resume();
        }
    };

    Fiber operator()(Fiber && sink)
    {
        auto async_callback = AsyncCallback{executor, sink};
        auto suspend_callback = ResumeCallback{sink};
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
        return std::move(sink);
    }
};

void AsyncTaskExecutor::createFiber()
{
    fiber = boost::context::fiber(std::allocator_arg_t(), fiber_stack, Routine{*this});
}

void AsyncTaskExecutor::destroyFiber()
{
    boost::context::fiber to_destroy = std::move(fiber);
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

