#pragma once

#if defined(OS_LINUX)

#include <base/defines.h>

#include <silk/fibers/fiber.h>

#include <cstddef>
#include <type_traits>

namespace Silk
{


enum FiberLocalSlot : size_t
{
    CurrentThreadSlot = 0,
};

/// Silk::FiberLocal implements the thread-local storage abstraction for both plain threads and silk fibers.
///
/// Silk library provides FiberScheduler::getLocalStorage().
/// It returns a small (one cacheline) buffer on thread-local storage that fibers copy to/from on context switch,
/// implementing a fiber-local storage abstraction.
///
/// This class is a stateless wrapper for storing variables on that buffer.
/// It can be used by both fibers and plain threads.
/// Instances of this class don't have to be thread local themselves.
template <typename T, size_t slot>
class FiberLocal
{
    static_assert(std::is_pointer_v<T>, "FiberLocal supports pointer types only");
    static_assert(slot < silk::CACHELINE_SIZE / sizeof(T), "FiberLocal slot index out of range");

public:
    constexpr FiberLocal() noexcept = default;

    FiberLocal(const FiberLocal &) = delete;
    FiberLocal & operator=(const FiberLocal &) = delete;

    ALWAYS_INLINE T get() const noexcept { return ref(); }
    ALWAYS_INLINE void set(T value) noexcept { ref() = value; }

    /// NOLINTNEXTLINE(google-explicit-constructor)
    ALWAYS_INLINE operator T() const noexcept { return ref(); }
    ALWAYS_INLINE T operator->() const noexcept { return ref(); }
    ALWAYS_INLINE FiberLocal & operator=(T value) noexcept
    {
        ref() = value;
        return *this;
    }

private:
    ALWAYS_INLINE static T & ref() noexcept { return static_cast<T *>(silk::FiberScheduler::getLocalStorage())[slot]; }
};

}

#endif
