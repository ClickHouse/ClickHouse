#pragma once

#include <future>
#include <functional>

namespace DB
{

/// Interface to run task asynchronously with possibility to wait for execution.
class Executor
{
public:
    virtual ~Executor() = default;
    virtual std::future<void> execute(std::function<void()> task) = 0;
};

/// Executes task synchronously in case when disk doesn't support async operations.
class SyncExecutor : public Executor
{
public:
    SyncExecutor() = default;
    std::future<void> execute(std::function<void()> task) override
    {
        auto promise = std::make_shared<std::promise<void>>();
        try
        {
            task();
            promise->set_value();
        }
        catch (...)
        {
            try
            {
                promise->set_exception(std::current_exception());
            }
            catch (...) { }
        }
        return promise->get_future();
    }
};

}
