#pragma once
/// defines.h should be included before fiber.hpp
/// BOOST_USE_ASAN, BOOST_USE_TSAN and BOOST_USE_UCONTEXT should be correctly defined for sanitizers.
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
    Fiber(StackAlloc && salloc, Fn && fn) : impl(std::allocator_arg_t(), std::forward<StackAlloc>(salloc), RoutineImpl(std::forward<Fn>(fn)))
    {
    }

    Fiber() = default;

    Fiber(Fiber && other) = default;
    Fiber & operator=(Fiber && other) = default;

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

    static FiberPtr & getCurrentFiber()
    {
        thread_local static FiberPtr current_fiber;
        return current_fiber;
    }

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

    /// Get reference to fiber-specific data by key
    /// (the pointer to the structure that uses this data).
    DataPtr & getLocalData(void * key)
    {
        return local_data[key];
    }

    Impl && release()
    {
        return std::move(impl);
    }

    Impl impl;
    std::map<void *, DataPtr> local_data;
};

/// Implementation for fiber local variable.
/// If we are in fiber, it returns fiber local data,
/// otherwise it returns it's single field.
/// Fiber local data is destroyed in Fiber destructor.
/// Implementation is similar to boost::fiber::fiber_specific_ptr
/// (we cannot use it because we don't use boost::fiber API.
template <typename T>
class FiberLocal
{
public:
    T & operator*()
    {
        return get();
    }

    T * operator->()
    {
        return &get();
    }

private:
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

    T main_instance;
};
