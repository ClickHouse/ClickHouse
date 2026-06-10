#pragma once

#include <Common/ThreadStackSize.h>

#if defined(OS_DARWIN)

#include <exception>
#include <functional>
#include <memory>
#include <system_error>
#include <type_traits>
#include <utility>

#include <pthread.h>

namespace DB
{

/// A minimal drop-in replacement for `std::thread` that creates the underlying thread with an
/// explicit stack size (`DEFAULT_THREAD_STACK_SIZE`). It exists only because `std::thread` offers
/// no way to set the stack size, and macOS' 512 KiB default is too small for us (see
/// `ThreadStackSize.h`). Only the subset of the `std::thread` interface used by `ThreadPoolImpl`
/// is implemented.
class ThreadWithStackSize
{
public:
    ThreadWithStackSize() noexcept = default;

    template <typename Function, typename... Args,
              typename = std::enable_if_t<!std::is_same_v<std::decay_t<Function>, ThreadWithStackSize>>>
    explicit ThreadWithStackSize(Function && f, Args &&... args)
    {
        /// Decay-copy the callable and its arguments into a heap closure, like `std::thread` does.
        auto closure = std::make_unique<std::function<void()>>(
            std::bind(std::forward<Function>(f), std::forward<Args>(args)...));

        pthread_attr_t attr;
        if (int err = pthread_attr_init(&attr); err != 0)
            throw std::system_error(err, std::generic_category(), "pthread_attr_init");

        if (int err = pthread_attr_setstacksize(&attr, DEFAULT_THREAD_STACK_SIZE); err != 0)
        {
            pthread_attr_destroy(&attr);
            throw std::system_error(err, std::generic_category(), "pthread_attr_setstacksize");
        }

        int err = pthread_create(&handle, &attr, &ThreadWithStackSize::run, closure.get());
        pthread_attr_destroy(&attr);
        if (err != 0)
            throw std::system_error(err, std::generic_category(), "pthread_create");

        /// The new thread owns the closure now.
        closure.release();
        joined_or_detached = false;
    }

    ThreadWithStackSize(ThreadWithStackSize && other) noexcept
        : handle(other.handle), joined_or_detached(other.joined_or_detached)
    {
        other.joined_or_detached = true;
    }

    ThreadWithStackSize & operator=(ThreadWithStackSize && other) noexcept
    {
        if (this != &other)
        {
            /// Matches `std::thread`: assigning over a joinable thread is a hard error.
            if (joinable())
                std::terminate();
            handle = other.handle;
            joined_or_detached = other.joined_or_detached;
            other.joined_or_detached = true;
        }
        return *this;
    }

    ThreadWithStackSize(const ThreadWithStackSize &) = delete;
    ThreadWithStackSize & operator=(const ThreadWithStackSize &) = delete;

    ~ThreadWithStackSize()
    {
        if (joinable())
            std::terminate();
    }

    bool joinable() const noexcept { return !joined_or_detached; }

    void join()
    {
        if (!joinable())
            throw std::system_error(std::make_error_code(std::errc::invalid_argument), "join");
        if (int err = pthread_join(handle, nullptr); err != 0)
            throw std::system_error(err, std::generic_category(), "pthread_join");
        joined_or_detached = true;
    }

    void detach()
    {
        if (!joinable())
            throw std::system_error(std::make_error_code(std::errc::invalid_argument), "detach");
        if (int err = pthread_detach(handle); err != 0)
            throw std::system_error(err, std::generic_category(), "pthread_detach");
        joined_or_detached = true;
    }

private:
    static void * run(void * arg)
    {
        std::unique_ptr<std::function<void()>> closure(static_cast<std::function<void()> *>(arg));
        (*closure)();
        return nullptr;
    }

    pthread_t handle{};
    bool joined_or_detached = true;
};

}

#endif
