#pragma once

#if __has_include(<common/config_common.h>)
#include <common/config_common.h>
#endif

#if USE_JEMALLOC

#include <new>
#include <jemalloc/jemalloc.h>
#include <common/likely.h>

#if defined(_MSC_VER)
    #define ALWAYS_INLINE __forceinline
    #define NO_INLINE static __declspec(noinline)
#else
    #define ALWAYS_INLINE __attribute__((__always_inline__))
    #define NO_INLINE __attribute__((__noinline__))
#endif

namespace JeMalloc
{

void * handleOOM(std::size_t size, bool nothrow);

ALWAYS_INLINE inline void * newImpl(std::size_t size)
{
    void * ptr = je_malloc(size);
    if (likely(ptr != nullptr))
        return ptr;

    return handleOOM(size, false);
}

ALWAYS_INLINE inline void * newNoExept(std::size_t size) noexcept
{
    void * ptr = je_malloc(size);
    if (likely(ptr != nullptr))
        return ptr;

    return handleOOM(size, true);
}

ALWAYS_INLINE inline void deleteImpl(void * ptr) noexcept
{
    je_free(ptr);
}

ALWAYS_INLINE inline void deleteSized(void * ptr, std::size_t size) noexcept
{
    if (unlikely(ptr == nullptr))
        return;

    je_sdallocx(ptr, size, 0);
}

}

namespace Memory
{
    using namespace JeMalloc;
}

#else

namespace Memory
{

ALWAYS_INLINE inline void * newImpl(std::size_t size)
{
    auto * ptr = malloc(size);
    if (likely(ptr != nullptr))
        return ptr;

    /// @note no std::get_new_handler logic implemented
    std::__throw_bad_alloc();
}

ALWAYS_INLINE inline void * newNoExept(std::size_t size) noexcept
{
    return malloc(size);
}

ALWAYS_INLINE inline void deleteImpl(void * ptr) noexcept
{
    free(ptr);
}

ALWAYS_INLINE inline void deleteSized(void * ptr, std::size_t size) noexcept
{
    free(ptr);
}

}

#endif
