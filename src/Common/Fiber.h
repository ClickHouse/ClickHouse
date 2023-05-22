#pragma once
/// defines.h should be included before fiber.hpp
/// BOOST_USE_ASAN, BOOST_USE_TSAN and BOOST_USE_UCONTEXT should be correctly defined for sanitizers.
#include <base/defines.h>
#include <boost/context/fiber.hpp>

/// Class wrapper for boost::context::fiber.
/// It tracks current executing fiber for thread and
/// supports storing fiber-specific data
/// that will be destroyed on fiber destructor.
class Fiber
{
public:
    struct CleanUpFn
    {
        virtual void operator()(void *) = 0;
        virtual ~CleanUpFn() = default;
    };

    using Impl = boost::context::fiber;

    template< typename StackAlloc, typename Fn>
    Fiber(StackAlloc && salloc, Fn && fn) : impl(std::allocator_arg_t(), std::forward<StackAlloc>(salloc), std::forward<Fn>(fn))
    {
    }

    Fiber() = default;

    Fiber(Fiber && other) = default;
    Fiber & operator=(Fiber && other) = default;

    Fiber(const Fiber &) = delete;
    Fiber & operator =(const Fiber &) = delete;

    ~Fiber()
    {
        for (auto & [_, data] : local_data)
            (*data.cleanup_fn)(data.ptr);
    }

    explicit operator bool() const
    {
        return impl.operator bool();
    }

    void resume()
    {
        /// Update information about current executing fiber.
        auto & current_fiber_info = getCurrentFiberInfo();
        auto parent_fiber_info = current_fiber_info;
        current_fiber_info = FiberInfo{this, &parent_fiber_info};
        impl = std::move(impl).resume();
        /// Restore current fiber info.
        current_fiber_info = parent_fiber_info;
    }

    /// Set pointer to fiber-specific data, it will be stored in hash-map
    /// using provided key and cleaned on fiber destructor using provided
    /// cleanup function.
    void setLocalData(void * key, void * ptr, CleanUpFn * cleanup_fn)
    {
        local_data[key] = FiberLocalData{ptr, cleanup_fn};
    }

    /// Get pointer to fiber-specific data by key.
    /// If no data was stored by this key, return nullptr.
    void * getLocalData(void * key)
    {
        return local_data[key].ptr;
    }

    struct FiberInfo
    {
        Fiber * fiber = nullptr;
        FiberInfo * parent_info = nullptr;
    };

    static FiberInfo & getCurrentFiberInfo()
    {
        thread_local static FiberInfo current_fiber_info;
        return current_fiber_info;
    }

private:
    struct FiberLocalData
    {
        void * ptr;
        CleanUpFn * cleanup_fn;
    };

    Impl impl;
    std::unordered_map<void *, FiberLocalData> local_data;
};

/// Implementation for fiber local variable.
/// If we are not in fiber, it returns thread local data.
/// If we are in fiber, it returns fiber local data.
/// Fiber local data is destroyed in Fiber destructor.
/// On first request, fiber local data is copied from parent
/// fiber data or from current thread data if there is no parent fiber.
/// Implementation is similar to boost::fiber::fiber_specific_ptr
/// (we cannot use it because we don't use boost::fiber API.
template <typename T>
class FiberLocal
{
public:
    struct DefaultCleanUpFn : public Fiber::CleanUpFn
    {
        void operator()(void * data) override
        {
            delete static_cast<T *>(data);
        }
    };

    struct CustomCleanUpFn : public Fiber::CleanUpFn
    {
        explicit CustomCleanUpFn(void (*fn_)(T*)) : fn(fn_)
        {
        }

        void operator()(void * data) override
        {
            if (likely(fn != nullptr))
                fn(static_cast<T *>(data));
        }

        void (*fn)(T*);
    };

    FiberLocal() : cleanup_fn(std::make_unique<DefaultCleanUpFn>())
    {
    }

    explicit FiberLocal(void (*fn)(T*)) : cleanup_fn(std::make_unique<CustomCleanUpFn>(fn))
    {
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
    friend Fiber;

    T & get()
    {
        return getInstanceForFiber(Fiber::getCurrentFiberInfo());
    }

    T & getInstanceForFiber(const Fiber::FiberInfo & fiber_info)
    {
        /// If it's not a fiber, return thread local instance.
        if (!fiber_info.fiber)
            return getThreadLocalInstance();

        T * ptr = static_cast<T *>(fiber_info.fiber->getLocalData(this));
        /// If it's the first request, we need to initialize instance for the fiber
        /// using instance from parent fiber or main thread that executes this fiber.
        if (!ptr)
        {
            auto parent_instance = getInstanceForFiber(*fiber_info.parent_info);
            /// Crete new object and store pointer inside Fiber, so it will be destroyed in Fiber destructor.
            ptr = new T(parent_instance);
            fiber_info.fiber->setLocalData(this, ptr, cleanup_fn.get());
        }

        return *ptr;
    }

    T & getThreadLocalInstance()
    {
        static thread_local T thread_local_instance;
        return thread_local_instance;
    }

    std::unique_ptr<Fiber::CleanUpFn> cleanup_fn;
};

