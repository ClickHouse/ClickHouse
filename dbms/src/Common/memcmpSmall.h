#pragma once

#include <cstdint>
#include <algorithm>

#ifdef __SSE2__
#include <emmintrin.h>


namespace detail
{

template <typename T>
inline int cmp(T a, T b)
{
    if (a < b)
        return -1;
    if (a > b)
        return 1;
    return 0;
}

}

/** All functions works under the following assumptions:
  * - it's possible to read up to 15 excessive bytes after end of 'a' and 'b' region;
  * - memory regions are relatively small and extra loop unrolling is not worth to do.
  */

/** Variant when memory regions may have different sizes.
  */
template <typename Char>
inline int memcmpSmallAllowOverflow15(const Char * a, size_t a_size, const Char * b, size_t b_size)
{
    size_t min_size = std::min(a_size, b_size);

    for (size_t offset = 0; offset < min_size; offset += 16)
    {
        uint16_t mask = _mm_movemask_epi8(_mm_cmpeq_epi8(
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(a + offset)),
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(b + offset))));
        mask = ~mask;

        if (mask)
        {
            offset += __builtin_ctz(mask);

            if (offset >= min_size)
                break;

            return detail::cmp(a[offset], b[offset]);
        }
    }

    return detail::cmp(a_size, b_size);
}


/** Variant when memory regions have same size.
  * TODO Check if the compiler can optimize previous function when the caller pass identical sizes.
  */
template <typename Char>
inline int memcmpSmallAllowOverflow15(const Char * a, const Char * b, size_t size)
{
    for (size_t offset = 0; offset < size; offset += 16)
    {
        uint16_t mask = _mm_movemask_epi8(_mm_cmpeq_epi8(
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(a + offset)),
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(b + offset))));
        mask = ~mask;

        if (mask)
        {
            offset += __builtin_ctz(mask);

            if (offset >= size)
                return 0;

            return detail::cmp(a[offset], b[offset]);
        }
    }

    return 0;
}


/** Compare memory regions for equality.
  */
template <typename Char>
inline bool memequalSmallAllowOverflow15(const Char * a, size_t a_size, const Char * b, size_t b_size)
{
    if (a_size != b_size)
        return false;

    for (size_t offset = 0; offset < a_size; offset += 16)
    {
        uint16_t mask = _mm_movemask_epi8(_mm_cmpeq_epi8(
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(a + offset)),
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(b + offset))));
        mask = ~mask;

        if (mask)
        {
            offset += __builtin_ctz(mask);
            return offset >= a_size;
        }
    }

    return true;
}


/** Variant when the caller know in advance that the size is a multiple of 16.
  */
template <typename Char>
inline int memcmpSmallMultipleOf16(const Char * a, const Char * b, size_t size)
{
    for (size_t offset = 0; offset < size; offset += 16)
    {
        uint16_t mask = _mm_movemask_epi8(_mm_cmpeq_epi8(
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(a + offset)),
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(b + offset))));
        mask = ~mask;

        if (mask)
        {
            offset += __builtin_ctz(mask);
            return detail::cmp(a[offset], b[offset]);
        }
    }

    return 0;
}


/** Variant when the size is 16 exactly.
  */
template <typename Char>
inline int memcmp16(const Char * a, const Char * b)
{
    uint16_t mask = _mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(a)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(b))));
    mask = ~mask;

    if (mask)
    {
        auto offset = __builtin_ctz(mask);
        return detail::cmp(a[offset], b[offset]);
    }

    return 0;
}


/** Variant when the size is 16 exactly.
  */
inline bool memequal16(const void * a, const void * b)
{
    return 0xFFFF == _mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(a)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(b))));
}


/** Compare memory region to zero */
inline bool memoryIsZeroSmallAllowOverflow15(const void * data, size_t size)
{
    const __m128i zero16 = _mm_setzero_si128();

    for (size_t offset = 0; offset < size; offset += 16)
    {
        uint16_t mask = _mm_movemask_epi8(_mm_cmpeq_epi8(zero16,
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(reinterpret_cast<const char *>(data) + offset))));
        mask = ~mask;

        if (mask)
        {
            offset += __builtin_ctz(mask);
            return offset >= size;
        }
    }

    return true;
}


#else

#include <cstring>

template <typename Char>
inline int memcmpSmallAllowOverflow15(const Char * a, size_t a_size, const Char * b, size_t b_size)
{
    return memcmp(a, b, std::min(a_size, b_size));
}

template <typename Char>
inline int memcmpSmallAllowOverflow15(const Char * a, const Char * b, size_t size)
{
    return memcmp(a, b, size);
}

template <typename Char>
inline bool memequalSmallAllowOverflow15(const Char * a, size_t a_size, const Char * b, size_t b_size)
{
    return a_size == b_size && 0 == memcmp(a, b, a_size);
}

template <typename Char>
inline int memcmpSmallMultipleOf16(const Char * a, const Char * b, size_t size)
{
    return memcmp(a, b, size);
}

template <typename Char>
inline int memcmp16(const Char * a, const Char * b)
{
    return memcmp(a, b, 16);
}

inline bool memequal16(const void * a, const void * b)
{
    return 0 == memcmp(a, b, 16);
}

inline bool memoryIsZeroSmallAllowOverflow15(const void * data, size_t size)
{
    const char * pos = reinterpret_cast<const char *>(data);
    const char * end = pos + size;

    for (; pos < end; ++pos)
        if (*pos)
            return false;

    return true;
}

#endif
