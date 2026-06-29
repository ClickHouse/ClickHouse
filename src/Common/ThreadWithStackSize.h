#pragma once

#include <Common/ThreadStackSize.h>

#if defined(OS_DARWIN)

#include <cerrno>
#include <exception>
#include <functional>
#include <memory>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>

#include <pthread.h>

namespace DB
{

/// A minimal drop-in replacement for `std::thread` that creates the underlying thread with an
/// explicit stack size (`DEFAULT_THREAD_STACK_SIZE`). It exists only because `std::thread` offers
/// no way to set the stack size, and macOS' 512 KiB default is too small for us (see
/// `ThreadStackSize.h`).
///
/// The behavior is meant to match `std::thread` exactly (so it can stand in for it in
/// `ThreadPoolImpl`); each member below mirrors libc++'s implementation, referenced inline. See
/// `class thread` in contrib/llvm-project/libcxx/include/__thread/thread.h and
/// contrib/llvm-project/libcxx/src/thread.cpp. The single intentional difference is the
/// `pthread_attr_setstacksize` call in the constructor.
///
/// As in libc++, the joinable state is tracked by the thread handle itself (null vs non-null), not
/// a separate flag; on Darwin `pthread_t` is a pointer, so a value-initialized handle is null,
/// matching `__libcpp_thread_isnull` (`get_id() == 0`).
class ThreadWithStackSize
{
public:
    /// libc++: `thread() noexcept : __t_(_LIBCPP_NULL_THREAD) {}`
    ThreadWithStackSize() noexcept = default;

    /// libc++: `template <class _Fp, class... _Args, ...> explicit thread(_Fp&&, _Args&&...)`.
    template <typename Function, typename... Args,
              std::enable_if_t<!std::is_same_v<std::decay_t<Function>, ThreadWithStackSize>, int> = 0>
    explicit ThreadWithStackSize(Function && f, Args &&... args)
    {
        static_assert(std::is_constructible_v<std::decay_t<Function>, Function>);
        static_assert((std::is_constructible_v<std::decay_t<Args>, Args> && ...));
        static_assert(std::is_invocable_v<std::decay_t<Function>, std::decay_t<Args>...>);

        /// Decay-copy the callable and its arguments into a heap closure, like libc++'s
        /// `__thread_proxy`/`__thread_execute` (which store them in a tuple and `__invoke` them).
        /// We omit libc++'s leading `unique_ptr<__thread_struct>` tuple element: that is internal
        /// libc++ thread-local bookkeeping we do not rely on for these threads.
        using Closure = std::tuple<std::decay_t<Function>, std::decay_t<Args>...>;
        auto closure = std::make_unique<Closure>(std::forward<Function>(f), std::forward<Args>(args)...);

        pthread_attr_t attr;
        if (int err = pthread_attr_init(&attr); err != 0)
            throw std::system_error(err, std::generic_category(), "pthread_attr_init");

        /// The whole reason this class exists; `std::thread` cannot do this.
        if (int err = pthread_attr_setstacksize(&attr, DEFAULT_THREAD_STACK_SIZE); err != 0)
        {
            pthread_attr_destroy(&attr);
            throw std::system_error(err, std::generic_category(), "pthread_attr_setstacksize");
        }

        /// libc++: `__libcpp_thread_create` -> `pthread_create(__t, nullptr, __func, __arg)`; on
        /// success release the closure to the new thread, otherwise `__throw_system_error`.
        int err = pthread_create(&handle, &attr, &ThreadWithStackSize::run<Closure>, closure.get());
        pthread_attr_destroy(&attr);
        if (err != 0)
            throw std::system_error(err, std::generic_category(), "thread constructor failed");

        closure.release();
    }

    ThreadWithStackSize(const ThreadWithStackSize &) = delete;
    ThreadWithStackSize & operator=(const ThreadWithStackSize &) = delete;

    /// libc++: `thread(thread&& __t) noexcept : __t_(__t.__t_) { __t.__t_ = _LIBCPP_NULL_THREAD; }`
    ThreadWithStackSize(ThreadWithStackSize && other) noexcept
        : handle(other.handle)
    {
        other.handle = pthread_t{};
    }

    /// libc++: `thread& operator=(thread&&)` - `terminate()` if still joinable, then steal.
    ThreadWithStackSize & operator=(ThreadWithStackSize && other) noexcept
    {
        if (joinable())
            std::terminate();
        handle = other.handle;
        other.handle = pthread_t{};
        return *this;
    }

    /// libc++ `thread::~thread()`: `if (!__libcpp_thread_isnull(&__t_)) terminate();`
    ~ThreadWithStackSize()
    {
        if (joinable())
            std::terminate();
    }

    /// libc++: `joinable() const noexcept { return !__libcpp_thread_isnull(&__t_); }`
    bool joinable() const noexcept { return handle != pthread_t{}; }

    /// libc++ `thread::join()` (thread.cpp): EINVAL if not joinable, else `pthread_join` and null
    /// the handle on success, throwing `system_error` with this exact message on any error.
    void join()
    {
        int ec = EINVAL;
        if (joinable())
        {
            ec = pthread_join(handle, nullptr);
            if (ec == 0)
                handle = pthread_t{};
        }
        if (ec)
            throw std::system_error(ec, std::generic_category(), "thread::join failed");
    }

    /// libc++ `thread::detach()` (thread.cpp): mirrors `join()` with `pthread_detach`.
    void detach()
    {
        int ec = EINVAL;
        if (joinable())
        {
            ec = pthread_detach(handle);
            if (ec == 0)
                handle = pthread_t{};
        }
        if (ec)
            throw std::system_error(ec, std::generic_category(), "thread::detach failed");
    }

private:
    /// Mirrors libc++'s `__thread_proxy<_Gp>` + `__thread_execute`: a distinct C entry point per
    /// closure type that `INVOKE`s the moved-from tuple elements. Deliberately no try/catch, just
    /// like libc++: an exception escaping the thread function finds no handler, so the Itanium ABI
    /// calls `std::terminate` (during the phase-1 search, before unwinding into the C pthread
    /// frames) - exactly `std::thread`'s contract for an uncaught exception.
    template <typename Closure>
    static void * run(void * arg)
    {
        std::unique_ptr<Closure> closure(static_cast<Closure *>(arg));
        std::apply([](auto &&... a) { std::invoke(std::forward<decltype(a)>(a)...); }, std::move(*closure));
        return nullptr;
    }

    pthread_t handle{};
};

}

#endif
