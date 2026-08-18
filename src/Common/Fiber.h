#pragma once
/// BOOST_USE_ASAN, BOOST_USE_MSAN, BOOST_USE_TSAN and BOOST_USE_UCONTEXT are defined via CMake for sanitizer builds.
#include <base/defines.h>
#include <boost/context/fiber.hpp>
#include <map>
#include <string_view>
#include <typeinfo>

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

    /// Get reference to fiber-specific data by key and it must not depend on the thread
    DataPtr & getLocalData(std::string_view key)
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
    std::map<std::string_view, DataPtr> local_data;
};

/// Implementation for fiber local variable.
/// If we are in fiber, it returns fiber local data,
/// otherwise it returns it's single field.
/// Fiber local data is destroyed in Fiber destructor.
/// Implementation is similar to boost::fiber::fiber_specific_ptr
/// (we cannot use it because we don't use boost::fiber API.
///
/// The per-fiber slot uses type`T` alone, so every `FiberLocal<T>` object with the same `T` aliases the same slot.
/// This is intentional for `thread_local` instances of one logical variable: a fiber suspended on one thread and
/// resumed on another goes through different `FiberLocal` objects but must find the same data.
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

        /// typeid names have static storage duration and identical content in every copy
        /// of the code, so the same fiber data is found from any thread and shared object.
        Fiber::DataPtr & ptr = current_fiber->getLocalData(typeid(T).name());
        /// Initialize instance on first request.
        if (!ptr)
            ptr = std::make_unique<DataWrapperImpl>();

        return dynamic_cast<DataWrapperImpl *>(ptr.get())->impl;
    }

    T main_instance;
};
