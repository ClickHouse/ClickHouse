#pragma once

#include <cstdint>

#if __SSE2__
    #include <emmintrin.h>
#endif
#if __SSE4_2__
    #include <nmmintrin.h>
#endif


/** Allow to search for next character from the set of 'symbols...' in a string.
  * It is similar to 'strpbrk', 'strcspn' (and 'strchr', 'memchr' in the case of one symbol and '\0'),
  * but with the following differencies:
  * - works with any memory ranges, including containing zero bytes;
  * - doesn't require terminating zero byte: end of memory range is passed explicitly;
  * - if not found, returns pointer to end instead of NULL;
  * - maximum number of symbols to search is 16.
  *
  * Uses SSE 2 in case of small number of symbols for search and SSE 4.2 in the case of large number of symbols,
  *  that have more than 2x performance advantage over trivial loop
  *  in the case of parsing tab-separated dump with (probably escaped) string fields.
  * In the case of parsing tab separated dump with short strings, there is no performance degradation over trivial loop.
  *
  * Note: the optimal threshold to choose between SSE 2 and SSE 4.2 may depend on CPU model.
  */

namespace detail
{

template <char s0>
inline bool is_in(char x)
{
    return x == s0;
}

template <char s0, char s1, char... tail>
inline bool is_in(char x)
{
    return x == s0 || is_in<s1, tail...>(x);
}

#if __SSE2__
template <char s0>
inline __m128i mm_is_in(__m128i bytes)
{
    __m128i eq0 = _mm_cmpeq_epi8(bytes, _mm_set1_epi8(s0));
    return eq0;
}

template <char s0, char s1, char... tail>
inline __m128i mm_is_in(__m128i bytes)
{
    __m128i eq0 = _mm_cmpeq_epi8(bytes, _mm_set1_epi8(s0));
    __m128i eq = mm_is_in<s1, tail...>(bytes);
    return _mm_or_si128(eq0, eq);
}
#endif


template <char... symbols>
inline const char * find_first_symbols_sse2(const char * begin, const char * end)
{
#if __SSE2__
    for (; begin + 15 < end; begin += 16)
    {
        __m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(begin));

        __m128i eq = mm_is_in<symbols...>(bytes);

        uint16_t bit_mask = _mm_movemask_epi8(eq);
        if (bit_mask)
            return begin + __builtin_ctz(bit_mask);
    }
#endif

    for (; begin < end; ++begin)
        if (is_in<symbols...>(*begin))
            return begin;
    return end;
}


template <size_t num_chars,
    char c01,     char c02 = 0, char c03 = 0, char c04 = 0,
    char c05 = 0, char c06 = 0, char c07 = 0, char c08 = 0,
    char c09 = 0, char c10 = 0, char c11 = 0, char c12 = 0,
    char c13 = 0, char c14 = 0, char c15 = 0, char c16 = 0>
inline const char * find_first_symbols_sse42_impl(const char * begin, const char * end)
{
#if __SSE4_2__
#define MODE (_SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_ANY | _SIDD_LEAST_SIGNIFICANT)
    __m128i set = _mm_setr_epi8(c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16);

    for (; begin + 15 < end; begin += 16)
    {
        __m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(begin));

        if (_mm_cmpestrc(set, num_chars, bytes, 16, MODE))
            return begin + _mm_cmpestri(set, num_chars, bytes, 16, MODE);
    }
#undef MODE
#endif

    for (; begin < end; ++begin)
        if (   (num_chars >= 1 && *begin == c01)
            || (num_chars >= 2 && *begin == c02)
            || (num_chars >= 3 && *begin == c03)
            || (num_chars >= 4 && *begin == c04)
            || (num_chars >= 5 && *begin == c05)
            || (num_chars >= 6 && *begin == c06)
            || (num_chars >= 7 && *begin == c07)
            || (num_chars >= 8 && *begin == c08)
            || (num_chars >= 9 && *begin == c09)
            || (num_chars >= 10 && *begin == c10)
            || (num_chars >= 11 && *begin == c11)
            || (num_chars >= 12 && *begin == c12)
            || (num_chars >= 13 && *begin == c13)
            || (num_chars >= 14 && *begin == c14)
            || (num_chars >= 15 && *begin == c15)
            || (num_chars >= 16 && *begin == c16))
            return begin;
    return end;
}


template <char... symbols>
inline const char * find_first_symbols_sse42(const char * begin, const char * end)
{
    return find_first_symbols_sse42_impl<sizeof...(symbols), symbols...>(begin, end);
}

}


template <char... symbols>
inline const char * find_first_symbols(const char * begin, const char * end)
{
#if __SSE4_2__
    if (sizeof...(symbols) >= 5)
        return detail::find_first_symbols_sse42<symbols...>(begin, end);
    else
#endif
        return detail::find_first_symbols_sse2<symbols...>(begin, end);
}
