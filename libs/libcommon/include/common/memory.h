#pragma once

#include <new>
#include <common/likely.h>

#if __has_include(<common/config_common.h>)
#include <common/config_common.h>
#endif

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>

#if JEMALLOC_VERSION_MAJOR < 4
    #undef USE_JEMALLOC
    #define USE_JEMALLOC 0
    #include <cstdlib>
#endif
#endif

#if defined(_MSC_VER)
    #define ALWAYS_INLINE __forceinline
    #define NO_INLINE static __declspec(noinline)
#else
    #define ALWAYS_INLINE inline __attribute__((__always_inline__))
    #define NO_INLINE __attribute__((__noinline__))
#endif

#if USE_JEMALLOC

namespace JeMalloc
{

void * handleOOM(std::size_t size, bool nothrow);

ALWAYS_INLINE void * newImpl(std::size_t size)
{
    void * ptr = malloc(size);
    if (likely(ptr != nullptr))
        return ptr;

    return handleOOM(size, false);
}

ALWAYS_INLINE void * newNoExept(std::size_t size) noexcept
{
    void * ptr = malloc(size);
    if (likely(ptr != nullptr))
        return ptr;

    return handleOOM(size, true);
}

ALWAYS_INLINE void deleteImpl(void * ptr) noexcept
{
    free(ptr);
}

ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size) noexcept
{
    if (unlikely(ptr == nullptr))
        return;

    sdallocx(ptr, size, 0);
}

}

namespace Memory
{
    using namespace JeMalloc;
}

#else

namespace Memory
{

ALWAYS_INLINE void * newImpl(std::size_t size)
{
    auto * ptr = malloc(size);
    if (likely(ptr != nullptr))
        return ptr;

    /// @note no std::get_new_handler logic implemented
    throw std::bad_alloc{};
}

ALWAYS_INLINE void * newNoExept(std::size_t size) noexcept
{
    return malloc(size);
}

ALWAYS_INLINE void deleteImpl(void * ptr) noexcept
{
    free(ptr);
}

ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size [[maybe_unused]]) noexcept
{
    free(ptr);
}

}

#endif
