#pragma once

#include <new>
#include "defines.h"

#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>
#endif

#if !USE_JEMALLOC || JEMALLOC_VERSION_MAJOR < 4
#    include <cstdlib>
#endif

#if USE_TCMALLOC_CPP
#    include <tcmalloc/tcmalloc.h>
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

#if USE_JEMALLOC && JEMALLOC_VERSION_MAJOR >= 4

inline ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size) noexcept
{
    if (unlikely(ptr == nullptr))
        return;

    sdallocx(ptr, size, 0);
}

#elif USE_TCMALLOC_CPP

inline ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size) noexcept
{
    if (unlikely(ptr == nullptr))
        return;

    TCMallocInternalDeleteSized(ptr, size);
}

#else

inline ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size [[maybe_unused]]) noexcept
{
    free(ptr);
}

#endif

}
