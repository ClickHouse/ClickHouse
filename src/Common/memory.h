#pragma once

#include <new>
#include <base/defines.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/config.h>

#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>
#endif

#if !USE_JEMALLOC
#    include <cstdlib>
#endif


namespace Memory
{

inline ALWAYS_INLINE void * newImpl(std::size_t size)
{
    auto * ptr = malloc(size);
    if (likely(ptr != nullptr))
        return ptr;

    /// @note no std::get_new_handler logic implemented
    throw std::bad_alloc{};
}

inline ALWAYS_INLINE void * newNoExept(std::size_t size) noexcept
{
    return malloc(size);
}

inline ALWAYS_INLINE void deleteImpl(void * ptr) noexcept
{
    free(ptr);
}

#if USE_JEMALLOC

inline ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size) noexcept
{
    if (unlikely(ptr == nullptr))
        return;

    sdallocx(ptr, size, 0);
}

#else

inline ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size [[maybe_unused]]) noexcept
{
    free(ptr);
}

#endif

#if defined(OS_LINUX)
#   include <malloc.h>
#elif defined(OS_DARWIN)
#   include <malloc/malloc.h>
#endif


inline ALWAYS_INLINE size_t getActualAllocationSize(size_t size)
{
    size_t actual_size = size;

#if USE_JEMALLOC
    /// The nallocx() function allocates no memory, but it performs the same size computation as the mallocx() function
    /// @note je_mallocx() != je_malloc(). It's expected they don't differ much in allocation logic.
    if (likely(size != 0))
        actual_size = nallocx(size, 0);
#endif

    return actual_size;
}

inline ALWAYS_INLINE void trackMemory(std::size_t size)
{
    std::size_t actual_size = getActualAllocationSize(size);
    CurrentMemoryTracker::allocNoThrow(actual_size);
}

inline ALWAYS_INLINE void untrackMemory(void * ptr [[maybe_unused]], std::size_t size [[maybe_unused]] = 0) noexcept
{
    try
    {
#if USE_JEMALLOC
        /// @note It's also possible to use je_malloc_usable_size() here.
        if (likely(ptr != nullptr))
            CurrentMemoryTracker::free(sallocx(ptr, 0));
#else
        if (size)
            CurrentMemoryTracker::free(size);
#    if defined(_GNU_SOURCE)
        /// It's innaccurate resource free for sanitizers. malloc_usable_size() result is greater or equal to allocated size.
        else
            CurrentMemoryTracker::free(malloc_usable_size(ptr));
#    endif
#endif
    }
    catch (...)
    {}
}

}
