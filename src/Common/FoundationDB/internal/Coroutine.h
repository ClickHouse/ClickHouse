#pragma once
#include <atomic>
#include <coroutine>
#include <exception>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>
#include <foundationdb/fdb_c.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/FoundationDB/FoundationDBHelpers.h>
#include <Common/FoundationDB/internal/AsyncTrxTracker.h>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace DB::FoundationDB::Coroutine
{
class FDBFutureAwaiter
{
public:
    explicit FDBFutureAwaiter(FDBFuture * future_, AsyncTrxCancelToken * cancel_token_ = nullptr)
        : future(future_), cancel_token(cancel_token_)
    {
    }

    // NOLINTBEGIN(readability-identifier-naming)
    bool await_ready() const noexcept { return fdb_future_is_ready(future); }
    void await_suspend(std::coroutine_handle<> h)
    {
        if (cancel_token)
        {
            cancel_token->assertNotCanceled();
            cancel_token->setCancelPoint(future);
        }
        throwIfFDBError(fdb_future_set_callback(future, callback, new std::coroutine_handle<>(h)));
    }
    void await_resume()
    {
        if (cancel_token)
            cancel_token->setCancelPoint(nullptr);
        throwIfFDBError(fdb_future_get_error(future));
    }
    // NOLINTEND(readability-identifier-naming)

private:
    static void callback(FDBFuture *, void * payload)
    {
        auto * handle = reinterpret_cast<std::coroutine_handle<> *>(payload);
        handle->resume();
        delete handle;
    }
    FDBFuture * future;
    AsyncTrxCancelToken * cancel_token;
};

/// FDBFuturePtrAwaiter will take the ownership of the FDBFuture
class FDBFuturePtrAwaiter : public FDBFutureAwaiter
{
public:
    explicit FDBFuturePtrAwaiter(FDBFuture * future_, AsyncTrxCancelToken * cancel_token_ = nullptr)
        : FDBFutureAwaiter(future_, cancel_token_), future(fdb_manage_object(future_))
    {
    }
    std::shared_ptr<FDBFuture> await_resume()
    {
        FDBFutureAwaiter::await_resume();
        return future;
    }

private:
    std::shared_ptr<FDBFuture> future;
};

template <class T>
class Task;

template <typename TTask>
struct TaskReturn;

template <typename T>
struct TaskReturn<Task<T>>
{
    using type = T;
};

template <typename TTask>
using TaskReturnType = TaskReturn<TTask>::type;

template <class T>
struct TaskAwaiter;

template <typename T>
concept Awaitable = requires(T t) { t.operator co_await(); };

struct TaskPromiseTypeBase
{
    TaskPromiseTypeBase() : log(&Poco::Logger::get("FDBTask")) { }

    // NOLINTBEGIN(readability-identifier-naming)
    std::suspend_always initial_suspend() noexcept { return {}; }

    void unhandled_exception() { eptr = std::current_exception(); }

    FDBFutureAwaiter await_transform(FDBFuture & f) const { return FDBFutureAwaiter(&f, cancel_token.get()); }

    /// co_await (FDBFuture *) will take the ownership of the FDBFuture
    FDBFuturePtrAwaiter await_transform(FDBFuture * f) const { return FDBFuturePtrAwaiter(f, cancel_token.get()); }

    template <Awaitable T>
    auto await_transform(T && t) const
    {
        return t.operator co_await();
    }
    // NOLINTEND(readability-identifier-naming)

    std::exception_ptr eptr;
    std::atomic<bool> started = false;
    std::unique_ptr<AsyncTrxCancelToken> cancel_token;

    Poco::Logger * log;
};

template <class TTask>
struct TaskPromiseType;

template <class TTask>
requires(!std::is_void_v<TaskReturnType<TTask>>)
struct TaskPromiseType<TTask> : public TaskPromiseTypeBase
{
    using T = TaskReturnType<TTask>;

    // NOLINTBEGIN(readability-identifier-naming)
    TTask get_return_object() { return TTask(this); }

    T value;
    std::function<void(const T &, std::exception_ptr)> callback;

    using TaskPromiseTypeBase::await_transform;

    template <typename From>
    void return_value(From && value_)
    {
        value = std::forward<From>(value_);
    }

    std::suspend_never final_suspend() noexcept
    {
        if (callback)
        {
            try
            {
                callback(value, this->eptr);
            }
            catch (...)
            {
                tryLogCurrentException(this->log, "Unexpected exception in callback");
            }
        }
        return {};
    }
    // NOLINTEND(readability-identifier-naming)
};

template <class TTask>
requires std::is_void_v<TaskReturnType<TTask>>
struct TaskPromiseType<TTask> : public TaskPromiseTypeBase
{
    // NOLINTBEGIN(readability-identifier-naming)
    TTask get_return_object() { return TTask(this); }

    std::function<void(std::exception_ptr)> callback;

    using TaskPromiseTypeBase::await_transform;

    void return_void() { }

    std::suspend_never final_suspend() noexcept
    {
        if (callback)
        {
            try
            {
                callback(this->eptr);
            }
            catch (...)
            {
                tryLogCurrentException(this->log, "Unexpected exception in callback");
            }
        }
        return {};
    }
    // NOLINTEND(readability-identifier-naming)
};

template <class T>
struct TaskAwaiter
{
    Task<T>::promise_type * promise;
    TaskPromiseTypeBase * waiting_promise = nullptr;

    explicit TaskAwaiter(decltype(promise) p) : promise(p) { }

    // NOLINTBEGIN(readability-identifier-naming)
    constexpr bool await_ready() const noexcept { return false; }

    template <class current_promise_type>
    requires std::is_base_of_v<TaskPromiseTypeBase, current_promise_type>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<current_promise_type> current_handle)
    {
        if (promise->started.exchange(true))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot start task twice");

        waiting_promise = &current_handle.promise();

        promise->cancel_token = std::move(waiting_promise->cancel_token);

        if constexpr (std::is_void_v<T>)
            promise->callback = [current_handle](std::exception_ptr) { current_handle.resume(); };
        else
            promise->callback = [current_handle](const T &, std::exception_ptr) { current_handle.resume(); };

        return std::coroutine_handle<decltype(*promise)>::from_promise(*promise);
    }
    T await_resume()
    {
        waiting_promise->cancel_token = std::move(promise->cancel_token);

        if (promise->eptr)
            std::rethrow_exception(promise->eptr);
        else if constexpr (!std::is_void_v<T>)
            return std::move(promise->value);
    }
    // NOLINTEND(readability-identifier-naming)
};

template <class T>
class Task
{
public:
    using promise_type = TaskPromiseType<Task>;
    using handler_t = std::coroutine_handle<promise_type>;

    explicit Task(promise_type * p) : promise(p) { }

    void start(decltype(promise_type::callback) cb, decltype(promise_type::cancel_token) cancel_token = nullptr)
    {
        if (promise->started.exchange(true))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot start task twice");

        promise->callback = cb;
        promise->cancel_token = std::move(cancel_token);

        auto handler = handler_t::from_promise(*promise);
        handler.resume();
    }

    TaskAwaiter<T> operator co_await() const noexcept { return TaskAwaiter<T>(promise); }

private:
    promise_type * promise;
    template <class TTask>
    friend struct TaskPromiseType;
};

struct ForkCancelTokenTag
{
    struct Awaiter
    {
        // NOLINTBEGIN(readability-identifier-naming)
        constexpr bool await_ready() const noexcept { return false; }

        template <class current_promise_type>
        requires std::is_base_of_v<TaskPromiseTypeBase, current_promise_type>
        bool await_suspend(std::coroutine_handle<current_promise_type> current_handle)
        {
            auto & ctk = current_handle.promise().cancel_token;
            if (ctk)
                token = ctk->splitToken();

            return false;
        }
        auto await_resume() { return std::move(token); }
        // NOLINTEND(readability-identifier-naming)

        std::unique_ptr<AsyncTrxCancelToken> token;
    };

    auto operator co_await() const noexcept { return Awaiter{}; }
};
}
