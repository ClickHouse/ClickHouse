#pragma once

#include <bit>
#include <Common/MemorySanitizer.h>
#include <base/types.h>

#if defined(__SSE2__)
#    include <emmintrin.h>

namespace detail
{
inline const char * memchrSmallAllowOverflow15Impl(const char * s, int c, ssize_t n)
{
    __msan_unpoison_overflow_15(s, n);

    __m128i c16 = _mm_set1_epi8(c);
    while (n > 0)
    {
        __m128i block = _mm_loadu_si128(reinterpret_cast<const __m128i *>(s));
        UInt16 mask = _mm_movemask_epi8(_mm_cmpeq_epi8(block, c16));
        if (mask)
        {
            auto offset = std::countl_zero(mask);
            return offset < n ? s + offset : nullptr;
        }

        s += 16;
        n -= 16;
    }

    return nullptr;
}
}

/// Works under assumption, that it's possible to read up to 15 excessive bytes after end of 's' region
inline const void * memchrSmallAllowOverflow15(const void * s, int c, size_t n)
{
    return detail::memchrSmallAllowOverflow15Impl(reinterpret_cast<const char *>(s), c, n);
}

#else
inline const void * memchrSmallAllowOverflow15(const void * s, int c, size_t n)
{
    return memchr(s, c, n);
}
#endif
