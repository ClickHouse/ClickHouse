#pragma once

#include <atomic>
#include <exception>
#include <functional>

namespace DB
{

/// Simple task that can be executed and waited by several threads.
class DeserializationTask
{
public:

    explicit DeserializationTask(std::function<void()> deserialize_) : deserialize(std::move(deserialize_))
    {
    }

    /// Try to execute, if another thread already started execution, just return.
    void tryExecute()
    {
        bool expected = false;
        if (!started.compare_exchange_strong(expected, true))
            return;

        try
        {
            deserialize();
        }
        catch (...)
        {
            exception = std::current_exception();
        }

        finished = true;
        finished.notify_all();
    }

    /// Wait until task is executed and return an exception if any.
    std::exception_ptr wait()
    {
        finished.wait(false);
        return exception;
    }

private:
    std::function<void()> deserialize;
    std::atomic_bool started = false;
    std::atomic_bool finished = false;
    std::exception_ptr exception;
};

}
