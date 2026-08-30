#pragma once
/// BOOST_USE_ASAN, BOOST_USE_MSAN, BOOST_USE_TSAN and BOOST_USE_UCONTEXT are defined via CMake for sanitizer builds.
#include <base/defines.h>
#include <boost/context/fiber.hpp>

#include <Common/Exception.h>
#include <Common/FiberLocal.h>
#include <Common/SilkFiberScheduler.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Class wrapper for boost::context::fiber.
/// It tracks current executing coroutine for thread and
/// supports storing coroutine-specific data
/// that will be destroyed on coroutine destructor.
class StackfulCoroutine
{
private:
    using Impl = boost::context::fiber;
    using CoroutinePtr = StackfulCoroutine *;

public:
    template <typename StackAlloc, typename Fn>
    StackfulCoroutine(StackAlloc && salloc, Fn && fn)
        : impl(std::allocator_arg_t(), std::forward<StackAlloc>(salloc), RoutineImpl<Fn>(std::forward<Fn>(fn)))
        , coroutine_locals(FiberLocalStorage::create())
    {
        if (Silk::isInsideFiber())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Stackful coroutines cannot be created inside silk fibers");
    }

    StackfulCoroutine() = default;

    ~StackfulCoroutine()
    {
        unwind();
    }

    StackfulCoroutine(StackfulCoroutine && other) = default;

    StackfulCoroutine & operator=(StackfulCoroutine && other) noexcept
    {
        if (this != &other)
        {
            unwind();
            impl = std::move(other.impl);
            coroutine_locals = std::move(other.coroutine_locals);
        }
        return *this;
    }

    StackfulCoroutine(const StackfulCoroutine &) = delete;
    StackfulCoroutine & operator =(const StackfulCoroutine &) = delete;

    explicit operator bool() const
    {
        return impl.operator bool();
    }

    void resume()
    {
        /// Update information about current executing coroutine.
        CoroutinePtr & current_coroutine = getCurrentCoroutine();
        CoroutinePtr parent_coroutine = current_coroutine;
        current_coroutine = this;
        FiberLocalStorage::swapCoroutineLocal(*coroutine_locals);
        impl = std::move(impl).resume();
        FiberLocalStorage::swapCoroutineLocal(*coroutine_locals);
        /// Restore parent coroutine.
        current_coroutine = parent_coroutine;
    }

    /// Defined in `StackfulCoroutine.cpp`: a static local in a header-defined function gives every
    /// shared object its own copy.
    static CoroutinePtr & getCurrentCoroutine();

private:
    template <typename Fn>
    struct RoutineImpl
    {
        struct SuspendCallback
        {
            Impl & impl;

            void operator()()
            {
                impl = std::move(impl).resume();
            }
        };

        explicit RoutineImpl(Fn && fn_) : fn(std::move(fn_))
        {
        }

        Impl operator()(Impl && sink)
        {
            SuspendCallback suspend_callback{sink};
            fn(suspend_callback);
            return std::move(sink);
        }

        Fn fn;
    };

    Impl && release()
    {
        return std::move(impl);
    }

    /// Destroying a coroutine that is suspended unwinds its stack: Called from the destructor body, while coroutine_locals is still alive.
    void unwind() noexcept
    {
        if (!impl)
            return;

        CoroutinePtr & current_coroutine = getCurrentCoroutine();
        CoroutinePtr parent_coroutine = current_coroutine;
        current_coroutine = this;
        FiberLocalStorage::swapCoroutineLocal(*coroutine_locals);
        {
            Impl to_destroy = std::move(impl);
        }
        FiberLocalStorage::swapCoroutineLocal(*coroutine_locals);
        current_coroutine = parent_coroutine;
    }

    Impl impl;
    FiberLocalStorage::Holder coroutine_locals;
};
