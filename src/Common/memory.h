#pragma once

#include <cstdlib>
#include <new>
#include <base/defines.h>

#include <Common/AllocationInterceptors.h>
#include <Common/Concepts.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/MemoryTrackerDebugBlockerInThread.h>

#if defined(OS_LINUX)
#    include <malloc.h>
#elif defined(OS_DARWIN)
#    include <malloc/malloc.h>
#endif

/// Guard pages interface.
///
/// Uses MADV_GUARD_INSTALL/MADV_GUARD_REMOVE (since Linux 6.13+) which does
/// not splits VMA (unlike mprotect()), or fallback to mprotect()
///
/// Uses MADV_GUARD_INSTALL if available, or mprotect() if not
void memoryGuardInstall(void *addr, size_t len);
/// Uses MADV_GUARD_REMOVE if available, or mprotect() if not
void memoryGuardRemove(void *addr, size_t len);

namespace Memory
{

inline ALWAYS_INLINE size_t alignToSizeT(std::align_val_t align) noexcept
{
    return static_cast<size_t>(align);
}

inline ALWAYS_INLINE size_t alignUp(size_t size, size_t align) noexcept
{
    return (size + align - 1) / align * align;
}

inline ALWAYS_INLINE void * newNoExcept(std::size_t size) noexcept
{
    return __real_malloc(size);
}

inline ALWAYS_INLINE void * newNoExcept(std::size_t size, std::align_val_t align) noexcept
{
    return __real_aligned_alloc(static_cast<size_t>(align), alignUp(size, static_cast<size_t>(align)));
}

inline ALWAYS_INLINE void deleteImpl(void * ptr) noexcept
{
    __real_free(ptr);
}

#if USE_JEMALLOC

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size, TAlign... align) noexcept
{
    if (ptr == nullptr) [[unlikely]]
        return;

    if constexpr (sizeof...(TAlign) == 1)
        je_sdallocx(ptr, size, MALLOCX_ALIGN(alignToSizeT(align...)));
    else
        je_sdallocx(ptr, size, 0);
}

#else

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size [[maybe_unused]], TAlign... /* align */) noexcept
{
    __real_free(ptr);
}

#endif

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE size_t getActualAllocationSize(size_t size, TAlign... align [[maybe_unused]])
{
    size_t actual_size = size;

#if USE_JEMALLOC
    /// The nallocx() function allocates no memory, but it performs the same size computation as the mallocx() function
    /// @note je_mallocx() != je_malloc(). It's expected they don't differ much in allocation logic.
    size_t size_for_nallocx = size;
    if (size_for_nallocx == 0) [[unlikely]]
        size_for_nallocx = 1;

    if constexpr (sizeof...(TAlign) == 1)
    {
        actual_size = je_nallocx(size_for_nallocx, MALLOCX_ALIGN(alignToSizeT(align...)));
    }
    else
    {
        actual_size = je_nallocx(size_for_nallocx, 0);
    }
#endif

    return actual_size;
}

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE size_t trackMemory(std::size_t size, AllocationTrace & trace, TAlign... align)
{
    std::size_t actual_size = getActualAllocationSize(size, align...);
    trace = CurrentMemoryTracker::allocNoThrow(actual_size);
    return actual_size;
}

/// We cannot throw from C API
template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE size_t trackMemoryFromC(std::size_t size, AllocationTrace & trace, TAlign... align)
{
    [[maybe_unused]] MemoryTrackerDebugBlockerInThread blocker;
    std::size_t actual_size = getActualAllocationSize(size, align...);
    trace = CurrentMemoryTracker::allocNoThrow(actual_size);
    return actual_size;
}

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE size_t untrackMemory(void * ptr [[maybe_unused]], AllocationTrace & trace, std::size_t size [[maybe_unused]] = 0, TAlign... align [[maybe_unused]]) noexcept
{
    std::size_t actual_size = 0;

    try
    {
#if USE_JEMALLOC

        /// @note It's also possible to use je_malloc_usable_size() here.
        if (ptr != nullptr) [[likely]]
        {
            if constexpr (sizeof...(TAlign) == 1)
                actual_size = je_sallocx(ptr, MALLOCX_ALIGN(alignToSizeT(align...)));
            else
                actual_size = je_sallocx(ptr, 0);
        }
#else
        if (size)
            actual_size = size;
#    if defined(_GNU_SOURCE)
        /// It's innaccurate resource free for sanitizers. malloc_usable_size() result is greater or equal to allocated size.
        else
            actual_size = malloc_usable_size(ptr);
#    elif defined(OS_DARWIN)
        else
            actual_size = malloc_size(ptr);
#    endif
#endif
        trace = CurrentMemoryTracker::free(actual_size);
    }
    catch (...) // NOLINT(bugprone-empty-catch) Ok: operator delete must not throw
    {
    }

    return actual_size;
}

}
