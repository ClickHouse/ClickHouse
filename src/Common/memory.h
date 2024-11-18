#pragma once

#include <new>
#include <base/defines.h>

#include <Common/Concepts.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/GWPAsan.h>
#include "config.h"

#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>
#endif

#if !USE_JEMALLOC
#    include <cstdlib>
#endif

namespace ProfileEvents
{
    extern const Event GWPAsanAllocateSuccess;
    extern const Event GWPAsanAllocateFailed;
    extern const Event GWPAsanFree;
}

namespace Memory
{

inline ALWAYS_INLINE size_t alignToSizeT(std::align_val_t align) noexcept
{
    return static_cast<size_t>(align);
}

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE void * newImpl(std::size_t size, TAlign... align)
{
#if USE_GWP_ASAN
    if (unlikely(GWPAsan::shouldSample()))
    {
        if constexpr (sizeof...(TAlign) == 1)
        {
            if (void * ptr = GWPAsan::GuardedAlloc.allocate(size, alignToSizeT(align...)))
            {
                ProfileEvents::increment(ProfileEvents::GWPAsanAllocateSuccess);
                return ptr;
            }

            ProfileEvents::increment(ProfileEvents::GWPAsanAllocateFailed);
        }
        else
        {
            if (void * ptr = GWPAsan::GuardedAlloc.allocate(size))
            {
                ProfileEvents::increment(ProfileEvents::GWPAsanAllocateSuccess);
                return ptr;
            }

            ProfileEvents::increment(ProfileEvents::GWPAsanAllocateFailed);
        }
    }
#endif

    void * ptr = nullptr;
    if constexpr (sizeof...(TAlign) == 1)
        ptr = aligned_alloc(alignToSizeT(align...), size);
    else
        ptr = malloc(size);

    if (likely(ptr != nullptr))
        return ptr;

    /// @note no std::get_new_handler logic implemented
    throw std::bad_alloc{};
}

inline ALWAYS_INLINE void * newNoExcept(std::size_t size) noexcept
{
#if USE_GWP_ASAN
    if (unlikely(GWPAsan::shouldSample()))
    {
        if (void * ptr = GWPAsan::GuardedAlloc.allocate(size))
        {
            ProfileEvents::increment(ProfileEvents::GWPAsanAllocateSuccess);
            return ptr;
        }

        ProfileEvents::increment(ProfileEvents::GWPAsanAllocateFailed);
    }
#endif
    return malloc(size);
}

inline ALWAYS_INLINE void * newNoExcept(std::size_t size, std::align_val_t align) noexcept
{
#if USE_GWP_ASAN
    if (unlikely(GWPAsan::shouldSample()))
    {
        if (void * ptr = GWPAsan::GuardedAlloc.allocate(size, alignToSizeT(align)))
        {
            ProfileEvents::increment(ProfileEvents::GWPAsanAllocateSuccess);
            return ptr;
        }

        ProfileEvents::increment(ProfileEvents::GWPAsanAllocateFailed);
    }
#endif
    return aligned_alloc(static_cast<size_t>(align), size);
}

inline ALWAYS_INLINE void deleteImpl(void * ptr) noexcept
{
#if USE_GWP_ASAN
    if (unlikely(GWPAsan::GuardedAlloc.pointerIsMine(ptr)))
    {
        ProfileEvents::increment(ProfileEvents::GWPAsanFree);
        GWPAsan::GuardedAlloc.deallocate(ptr);
        return;
    }
#endif
    free(ptr);
}

#if USE_JEMALLOC

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size, TAlign... align) noexcept
{
    if (unlikely(ptr == nullptr))
        return;

#if USE_GWP_ASAN
    if (unlikely(GWPAsan::GuardedAlloc.pointerIsMine(ptr)))
    {
        ProfileEvents::increment(ProfileEvents::GWPAsanFree);
        GWPAsan::GuardedAlloc.deallocate(ptr);
        return;
    }
#endif

    if constexpr (sizeof...(TAlign) == 1)
        sdallocx(ptr, size, MALLOCX_ALIGN(alignToSizeT(align...)));
    else
        sdallocx(ptr, size, 0);
}

#else

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size [[maybe_unused]], TAlign... /* align */) noexcept
{
#if USE_GWP_ASAN
    if (unlikely(GWPAsan::GuardedAlloc.pointerIsMine(ptr)))
    {
        ProfileEvents::increment(ProfileEvents::GWPAsanFree);
        GWPAsan::GuardedAlloc.deallocate(ptr);
        return;
    }
#endif
    free(ptr);
}

#endif

#if defined(OS_LINUX)
#    include <malloc.h>
#elif defined(OS_DARWIN)
#    include <malloc/malloc.h>
#endif

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE size_t getActualAllocationSize(size_t size, TAlign... align [[maybe_unused]])
{
    size_t actual_size = size;

#if USE_JEMALLOC
    /// The nallocx() function allocates no memory, but it performs the same size computation as the mallocx() function
    /// @note je_mallocx() != je_malloc(). It's expected they don't differ much in allocation logic.
    if (likely(size != 0))
    {
        if constexpr (sizeof...(TAlign) == 1)
            actual_size = nallocx(size, MALLOCX_ALIGN(alignToSizeT(align...)));
        else
            actual_size = nallocx(size, 0);
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

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE size_t untrackMemory(void * ptr [[maybe_unused]], AllocationTrace & trace, std::size_t size [[maybe_unused]] = 0, TAlign... align [[maybe_unused]]) noexcept
{
    std::size_t actual_size = 0;

#if USE_GWP_ASAN
    if (unlikely(GWPAsan::GuardedAlloc.pointerIsMine(ptr)))
    {
        if (!size)
            size = GWPAsan::GuardedAlloc.getSize(ptr);
        trace = CurrentMemoryTracker::free(size);
        return size;
    }
#endif

    try
    {
#if USE_JEMALLOC

        /// @note It's also possible to use je_malloc_usable_size() here.
        if (likely(ptr != nullptr))
        {
            if constexpr (sizeof...(TAlign) == 1)
                actual_size = sallocx(ptr, MALLOCX_ALIGN(alignToSizeT(align...)));
            else
                actual_size = sallocx(ptr, 0);
        }
#else
        if (size)
            actual_size = size;
#    if defined(_GNU_SOURCE)
        /// It's innaccurate resource free for sanitizers. malloc_usable_size() result is greater or equal to allocated size.
        else
            actual_size = malloc_usable_size(ptr);
#    endif
#endif
        trace = CurrentMemoryTracker::free(actual_size);
    }
    catch (...) /// NOLINT(bugprone-empty-catch)
    {
    }

    return actual_size;
}

}
