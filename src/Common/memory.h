#pragma once
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"

#include <new>
#include <base/defines.h>

#include <Common/Concepts.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/config.h>

#if defined(USE_JEMALLOC) && USE_JEMALLOC
# pragma message "Use jemalloc"
#    include <jemalloc/jemalloc.h>
#endif

#if defined(USE_MIMALLOC) && USE_MIMALLOC
# pragma message "Use mimalloc"
#    include <mimalloc-override.h>
#endif

#if !defined(USE_JEMALLOC) && !defined(USE_MIMALLOC)
# pragma message "Use default allocator"
#    include <cstdlib>
#endif

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
    void * ptr = nullptr;
// #if USE_MIMALLOC
//     if constexpr (sizeof...(TAlign) == 1)
//         ptr = mi_aligned_alloc(alignToSizeT(align...), size);
//     else
//         ptr = mi_malloc(size);
// #else
    if constexpr (sizeof...(TAlign) == 1)
        ptr = aligned_alloc(alignToSizeT(align...), size);
    else
        ptr = malloc(size);
// #endif

    if (likely(ptr != nullptr))
        return ptr;

    /// @note no std::get_new_handler logic implemented
    throw std::bad_alloc{};
}

inline ALWAYS_INLINE void * newNoExept(std::size_t size) noexcept
{
// #if USE_MIMALLOC
//     return mi_malloc(size);
// #else
    return malloc(size);
// #endif
}

inline ALWAYS_INLINE void * newNoExept(std::size_t size, std::align_val_t align) noexcept
{
// #if USE_MIMALLOC
//     return mi_aligned_alloc(static_cast<size_t>(align), size);
// #else
    return aligned_alloc(static_cast<size_t>(align), size);
// #endif
}

inline ALWAYS_INLINE void deleteImpl(void * ptr) noexcept
{
// #if USE_MIMALLOC
//     mi_free(ptr);
// #else
    free(ptr);
// #endif
}

#if USE_JEMALLOC

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size, TAlign... align) noexcept
{
    if (unlikely(ptr == nullptr))
        return;

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
// #if USE_MIMALLOC
//     mi_free(ptr);
// #else
    free(ptr);
// #endif
}

#endif

// #if defined(OS_LINUX)
// #    include <malloc.h>
// #elif defined(OS_DARWIN)
// #    include <malloc/malloc.h>
// #endif

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
inline ALWAYS_INLINE void trackMemory(std::size_t size, TAlign... align)
{
    std::size_t actual_size = getActualAllocationSize(size, align...);
    CurrentMemoryTracker::allocNoThrow(actual_size);
}

template <std::same_as<std::align_val_t>... TAlign>
requires DB::OptionalArgument<TAlign...>
inline ALWAYS_INLINE void untrackMemory(void * ptr [[maybe_unused]], std::size_t size [[maybe_unused]] = 0, TAlign... align [[maybe_unused]]) noexcept
{
    try
    {
#if USE_JEMALLOC

        /// @note It's also possible to use je_malloc_usable_size() here.
        if (likely(ptr != nullptr))
        {
            if constexpr (sizeof...(TAlign) == 1)
                CurrentMemoryTracker::free_memory(sallocx(ptr, MALLOCX_ALIGN(alignToSizeT(align...))));
            else
                CurrentMemoryTracker::free_memory(sallocx(ptr, 0));
        }
#else
        if (size)
            CurrentMemoryTracker::free_memory(size);
#    if defined(_GNU_SOURCE) || USE_MIMALLOC
#    pragma message "defined(_GNU_SOURCE) || USE_MIMALLOC == true"
        /// It's innaccurate resource free for sanitizers. malloc_usable_size() result is greater or equal to allocated size.
        /// If we use mimalloc, malloc_usable_size will be expanded to mi_malloc_usable_size
        else
            CurrentMemoryTracker::free_memory(malloc_usable_size(ptr));
// #    elif USE_MIMALLOC
//         else
//             CurrentMemoryTracker::free_memory(mi_malloc_usable_size(ptr));
#    else
#    pragma message "defined(_GNU_SOURCE) || USE_MIMALLOC == false"
#    endif
#endif
    }
    catch (...)
    {
    }
}

}

#pragma GCC diagnostic pop
