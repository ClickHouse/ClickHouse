#pragma once
/// BOOST_USE_ASAN, BOOST_USE_MSAN, BOOST_USE_TSAN and BOOST_USE_UCONTEXT are defined via CMake for sanitizer builds.
#include <base/defines.h>
#include <boost/context/fiber.hpp>
#include <map>

/// Class wrapper for boost::context::fiber.
/// It tracks current executing fiber for thread and
/// supports storing fiber-specific data
/// that will be destroyed on fiber destructor.
class Fiber
{
private:
    using Impl = boost::context::fiber;
    using FiberPtr = Fiber *;
    template <typename T> friend class FiberLocal;

public:
    template <typename StackAlloc, typename Fn>
    Fiber(StackAlloc && salloc, Fn && fn) : impl(std::allocator_arg_t(), std::forward<StackAlloc>(salloc), RoutineImpl<Fn>(std::forward<Fn>(fn)))
    {
    }

    Fiber() = default;

    ~Fiber()
    {
        unwind();
    }

    Fiber(Fiber && other) = default;

    Fiber & operator=(Fiber && other) noexcept
    {
        if (this != &other)
        {
            unwind();
            impl = std::move(other.impl);
            local_data = std::move(other.local_data);
        }
        return *this;
    }

    Fiber(const Fiber &) = delete;
    Fiber & operator =(const Fiber &) = delete;

    explicit operator bool() const
    {
        return impl.operator bool();
    }

    void resume()
    {
        /// Update information about current executing fiber.
        FiberPtr & current_fiber = getCurrentFiber();
        FiberPtr parent_fiber = current_fiber;
        current_fiber = this;
        impl = std::move(impl).resume();
        /// Restore parent fiber.
        current_fiber = parent_fiber;
    }

    /// Defined in `Fiber.cpp`: a static local in a header-defined function gives every shared
    /// object its own copy.
    static FiberPtr & getCurrentFiber();

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

    /// Special wrapper to store data in uniquer_ptr.
    struct DataWrapper
    {
        virtual ~DataWrapper() = default;
    };

    using DataPtr = std::unique_ptr<DataWrapper>;

    /// Get reference to fiber-specific data by key.
    DataPtr & getLocalData(const void * key)
    {
        return local_data[key];
    }

    Impl && release()
    {
        return std::move(impl);
    }

    /// Destroying a fiber that is suspended unwinds its stack: Called from the destructor body, while local_data is still alive.
    void unwind() noexcept
    {
        if (!impl)
            return;

        FiberPtr & current_fiber = getCurrentFiber();
        FiberPtr parent_fiber = current_fiber;
        current_fiber = this;
        {
            Impl to_destroy = std::move(impl);
        }
        current_fiber = parent_fiber;
    }

    Impl impl;
    std::map<const void *, DataPtr> local_data;
};

/// Implementation for fiber local variable.
/// If we are in fiber, it returns fiber local data,
/// otherwise it returns a thread local fallback.
/// Fiber local data is destroyed in Fiber destructor.
/// Implementation is similar to boost::fiber::fiber_specific_ptr
/// (we cannot use it because we don't use boost::fiber API.
///
/// There is exactly one `FiberLocal` object per `T`, obtained via `instance` (the constructor is
/// private, so a second one cannot be created).
template <typename T>
class FiberLocal
{
public:
    static FiberLocal & instance()
    {
        static FiberLocal fiber_local;
        return fiber_local;
    }

    T & operator*()
    {
        return get();
    }

    T * operator->()
    {
        return &get();
    }

private:
    FiberLocal() = default;

    struct DataWrapperImpl : public Fiber::DataWrapper
    {
        T impl;
    };

    T & get()
    {
        Fiber * current_fiber = Fiber::getCurrentFiber();
        if (!current_fiber)
            return main_instance;

        Fiber::DataPtr & ptr = current_fiber->getLocalData(this);
        /// Initialize instance on first request.
        if (!ptr)
            ptr = std::make_unique<DataWrapperImpl>();

        return dynamic_cast<DataWrapperImpl *>(ptr.get())->impl;
    }

    static inline thread_local T main_instance;
};
