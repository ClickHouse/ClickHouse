#pragma once

#include <Common/UTF8Helpers.h>
#include <ext/range.h>
#include <Poco/UTF8Encoding.h>
#include <Poco/Unicode.h>
#include <stdint.h>
#include <string.h>

#ifdef __SSE2__
    #include <emmintrin.h>
#endif

#ifdef __SSE4_1__
    #include <smmintrin.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_PARAMETER;
}


/** Variants for searching a substring in a string.
  * In most cases, performance is less than Volnitsky (see Volnitsky.h).
  */


struct StringSearcherBase
{
#ifdef __SSE2__
    static constexpr auto n = sizeof(__m128i);
    const int page_size = getpagesize();

    bool pageSafe(const void * const ptr) const
    {
        return ((page_size - 1) & reinterpret_cast<std::uintptr_t>(ptr)) <= page_size - n;
    }
#endif
};


/// Performs case-sensitive and case-insensitive search of UTF-8 strings
template <bool CaseSensitive, bool ASCII> class StringSearcher;

/// Case-insensitive UTF-8 searcher
template <>
class StringSearcher<false, false> : private StringSearcherBase
{
private:
    using UTF8SequenceBuffer = UInt8[6];

    /// substring to be searched for
    const UInt8 * const needle;
    const size_t needle_size;
    const UInt8 * const needle_end = needle + needle_size;
    /// lower and uppercase variants of the first octet of the first character in `needle`
    bool first_needle_symbol_is_ascii{};
    UInt8 l{};
    UInt8 u{};

#ifdef __SSE4_1__
    /// vectors filled with `l` and `u`, for determining leftmost position of the first symbol
    __m128i patl, patu;
    /// lower and uppercase vectors of first 16 characters of `needle`
    __m128i cachel = _mm_setzero_si128(), cacheu = _mm_setzero_si128();
    int cachemask{};
    size_t cache_valid_len{};
    size_t cache_actual_len{};
#endif

public:
    StringSearcher(const char * const needle_, const size_t needle_size)
        : needle{reinterpret_cast<const UInt8 *>(needle_)}, needle_size{needle_size}
    {
        if (0 == needle_size)
            return;

        static const Poco::UTF8Encoding utf8;
        UTF8SequenceBuffer l_seq, u_seq;

        if (*needle < 0x80u)
        {
            first_needle_symbol_is_ascii = true;
            l = std::tolower(*needle);
            u = std::toupper(*needle);
        }
        else
        {
            const auto first_u32 = utf8.convert(needle);
            const auto first_l_u32 = Poco::Unicode::toLower(first_u32);
            const auto first_u_u32 = Poco::Unicode::toUpper(first_u32);

            /// lower and uppercase variants of the first octet of the first character in `needle`
            utf8.convert(first_l_u32, l_seq, sizeof(l_seq));
            l = l_seq[0];
            utf8.convert(first_u_u32, u_seq, sizeof(u_seq));
            u = u_seq[0];
        }

#ifdef __SSE4_1__
        /// for detecting leftmost position of the first symbol
        patl = _mm_set1_epi8(l);
        patu = _mm_set1_epi8(u);
        /// lower and uppercase vectors of first 16 octets of `needle`

        auto needle_pos = needle;

        for (size_t i = 0; i < n;)
        {
            if (needle_pos == needle_end)
            {
                cachel = _mm_srli_si128(cachel, 1);
                cacheu = _mm_srli_si128(cacheu, 1);
                ++i;

                continue;
            }

            const auto src_len = UTF8::seqLength(*needle_pos);
            const auto c_u32 = utf8.convert(needle_pos);

            const auto c_l_u32 = Poco::Unicode::toLower(c_u32);
            const auto c_u_u32 = Poco::Unicode::toUpper(c_u32);

            const auto dst_l_len = static_cast<UInt8>(utf8.convert(c_l_u32, l_seq, sizeof(l_seq)));
            const auto dst_u_len = static_cast<UInt8>(utf8.convert(c_u_u32, u_seq, sizeof(u_seq)));

            /// @note Unicode standard states it is a rare but possible occasion
            if (!(dst_l_len == dst_u_len && dst_u_len == src_len))
                throw Exception{"UTF8 sequences with different lowercase and uppercase lengths are not supported", ErrorCodes::UNSUPPORTED_PARAMETER};

            cache_actual_len += src_len;
            if (cache_actual_len < n)
                cache_valid_len += src_len;

            for (size_t j = 0; j < src_len && i < n; ++j, ++i)
            {
                cachel = _mm_srli_si128(cachel, 1);
                cacheu = _mm_srli_si128(cacheu, 1);

                if (needle_pos != needle_end)
                {
                    cachel = _mm_insert_epi8(cachel, l_seq[j], n - 1);
                    cacheu = _mm_insert_epi8(cacheu, u_seq[j], n - 1);

                    cachemask |= 1 << i;
                    ++needle_pos;
                }
            }
        }
#endif
    }

    ALWAYS_INLINE bool compare(const UInt8 * pos) const
    {
        static const Poco::UTF8Encoding utf8;

#ifdef __SSE4_1__
        if (pageSafe(pos))
        {
            const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos));
            const auto v_against_l = _mm_cmpeq_epi8(v_haystack, cachel);
            const auto v_against_u = _mm_cmpeq_epi8(v_haystack, cacheu);
            const auto v_against_l_or_u = _mm_or_si128(v_against_l, v_against_u);
            const auto mask = _mm_movemask_epi8(v_against_l_or_u);

            if (0xffff == cachemask)
            {
                if (mask == cachemask)
                {
                    pos += cache_valid_len;
                    auto needle_pos = needle + cache_valid_len;

                    while (needle_pos < needle_end &&
                           Poco::Unicode::toLower(utf8.convert(pos)) ==
                           Poco::Unicode::toLower(utf8.convert(needle_pos)))
                    {
                        /// @note assuming sequences for lowercase and uppercase have exact same length
                        const auto len = UTF8::seqLength(*pos);
                        pos += len;
                        needle_pos += len;
                    }

                    if (needle_pos == needle_end)
                        return true;
                }
            }
            else if ((mask & cachemask) == cachemask)
                return true;

            return false;
        }
#endif

        if (*pos == l || *pos == u)
        {
            pos += first_needle_symbol_is_ascii;
            auto needle_pos = needle + first_needle_symbol_is_ascii;

            while (needle_pos < needle_end &&
                   Poco::Unicode::toLower(utf8.convert(pos)) ==
                   Poco::Unicode::toLower(utf8.convert(needle_pos)))
            {
                const auto len = UTF8::seqLength(*pos);
                pos += len;
                needle_pos += len;
            }

            if (needle_pos == needle_end)
                return true;
        }

        return false;
    }

    const UInt8 * search(const UInt8 * haystack, const UInt8 * const haystack_end) const
    {
        if (0 == needle_size)
            return haystack;

        static const Poco::UTF8Encoding utf8;

        while (haystack < haystack_end)
        {
#ifdef __SSE4_1__
            if (haystack + n <= haystack_end && pageSafe(haystack))
            {
                const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
                const auto v_against_l = _mm_cmpeq_epi8(v_haystack, patl);
                const auto v_against_u = _mm_cmpeq_epi8(v_haystack, patu);
                const auto v_against_l_or_u = _mm_or_si128(v_against_l, v_against_u);

                const auto mask = _mm_movemask_epi8(v_against_l_or_u);

                if (mask == 0)
                {
                    haystack += n;
                    UTF8::syncForward(haystack, haystack_end);
                    continue;
                }

                const auto offset = __builtin_ctz(mask);
                haystack += offset;

                if (haystack < haystack_end && haystack + n <= haystack_end && pageSafe(haystack))
                {
                    const auto v_haystack_offset = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
                    const auto v_against_l_offset = _mm_cmpeq_epi8(v_haystack_offset, cachel);
                    const auto v_against_u_offset = _mm_cmpeq_epi8(v_haystack_offset, cacheu);
                    const auto v_against_l_or_u_offset = _mm_or_si128(v_against_l_offset, v_against_u_offset);
                    const auto mask_offset = _mm_movemask_epi8(v_against_l_or_u_offset);

                    if (0xffff == cachemask)
                    {
                        if (mask_offset == cachemask)
                        {
                            auto haystack_pos = haystack + cache_valid_len;
                            auto needle_pos = needle + cache_valid_len;

                            while (haystack_pos < haystack_end && needle_pos < needle_end &&
                                   Poco::Unicode::toLower(utf8.convert(haystack_pos)) ==
                                   Poco::Unicode::toLower(utf8.convert(needle_pos)))
                            {
                                /// @note assuming sequences for lowercase and uppercase have exact same length
                                const auto len = UTF8::seqLength(*haystack_pos);
                                haystack_pos += len;
                                needle_pos += len;
                            }

                            if (needle_pos == needle_end)
                                return haystack;
                        }
                    }
                    else if ((mask_offset & cachemask) == cachemask)
                        return haystack;

                    /// first octet was ok, but not the first 16, move to start of next sequence and reapply
                    haystack += UTF8::seqLength(*haystack);
                    continue;
                }
            }
#endif

            if (haystack == haystack_end)
                return haystack_end;

            if (*haystack == l || *haystack == u)
            {
                auto haystack_pos = haystack + first_needle_symbol_is_ascii;
                auto needle_pos = needle + first_needle_symbol_is_ascii;

                while (haystack_pos < haystack_end && needle_pos < needle_end &&
                       Poco::Unicode::toLower(utf8.convert(haystack_pos)) ==
                       Poco::Unicode::toLower(utf8.convert(needle_pos)))
                {
                    const auto len = UTF8::seqLength(*haystack_pos);
                    haystack_pos += len;
                    needle_pos += len;
                }

                if (needle_pos == needle_end)
                    return haystack;
            }

            /// advance to the start of the next sequence
            haystack += UTF8::seqLength(*haystack);
        }

        return haystack_end;
    }

    const UInt8 * search(const UInt8 * haystack, const size_t haystack_size) const
    {
        return search(haystack, haystack + haystack_size);
    }
};


/// Case-insensitive ASCII searcher
template <>
class StringSearcher<false, true> : private StringSearcherBase
{
private:
    /// string to be searched for
    const UInt8 * const needle;
    const size_t needle_size;
    const UInt8 * const needle_end = needle + needle_size;
    /// lower and uppercase variants of the first character in `needle`
    UInt8 l{};
    UInt8 u{};

#ifdef __SSE4_1__
    /// vectors filled with `l` and `u`, for determining leftmost position of the first symbol
    __m128i patl, patu;
    /// lower and uppercase vectors of first 16 characters of `needle`
    __m128i cachel = _mm_setzero_si128(), cacheu = _mm_setzero_si128();
    int cachemask{};
#endif

public:
    StringSearcher(const char * const needle_, const size_t needle_size)
        : needle{reinterpret_cast<const UInt8 *>(needle_)}, needle_size{needle_size}
    {
        if (0 == needle_size)
            return;

        l = static_cast<UInt8>(std::tolower(*needle));
        u = static_cast<UInt8>(std::toupper(*needle));

#ifdef __SSE4_1__
        patl = _mm_set1_epi8(l);
        patu = _mm_set1_epi8(u);

        auto needle_pos = needle;

        for (const auto i : ext::range(0, n))
        {
            cachel = _mm_srli_si128(cachel, 1);
            cacheu = _mm_srli_si128(cacheu, 1);

            if (needle_pos != needle_end)
            {
                cachel = _mm_insert_epi8(cachel, std::tolower(*needle_pos), n - 1);
                cacheu = _mm_insert_epi8(cacheu, std::toupper(*needle_pos), n - 1);
                cachemask |= 1 << i;
                ++needle_pos;
            }
        }
#endif
    }

    ALWAYS_INLINE bool compare(const UInt8 * pos) const
    {
#ifdef __SSE4_1__
        if (pageSafe(pos))
        {
            const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos));
            const auto v_against_l = _mm_cmpeq_epi8(v_haystack, cachel);
            const auto v_against_u = _mm_cmpeq_epi8(v_haystack, cacheu);
            const auto v_against_l_or_u = _mm_or_si128(v_against_l, v_against_u);
            const auto mask = _mm_movemask_epi8(v_against_l_or_u);

            if (0xffff == cachemask)
            {
                if (mask == cachemask)
                {
                    pos += n;
                    auto needle_pos = needle + n;

                    while (needle_pos < needle_end && std::tolower(*pos) == std::tolower(*needle_pos))
                    {
                        ++pos;
                        ++needle_pos;
                    }

                    if (needle_pos == needle_end)
                        return true;
                }
            }
            else if ((mask & cachemask) == cachemask)
                return true;

            return false;
        }
#endif

        if (*pos == l || *pos == u)
        {
            ++pos;
            auto needle_pos = needle + 1;

            while (needle_pos < needle_end && std::tolower(*pos) == std::tolower(*needle_pos))
            {
                ++pos;
                ++needle_pos;
            }

            if (needle_pos == needle_end)
                return true;
        }

        return false;
    }

    const UInt8 * search(const UInt8 * haystack, const UInt8 * const haystack_end) const
    {
        if (0 == needle_size)
            return haystack;

        while (haystack < haystack_end)
        {
#ifdef __SSE4_1__
            if (haystack + n <= haystack_end && pageSafe(haystack))
            {
                const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
                const auto v_against_l = _mm_cmpeq_epi8(v_haystack, patl);
                const auto v_against_u = _mm_cmpeq_epi8(v_haystack, patu);
                const auto v_against_l_or_u = _mm_or_si128(v_against_l, v_against_u);

                const auto mask = _mm_movemask_epi8(v_against_l_or_u);

                if (mask == 0)
                {
                    haystack += n;
                    continue;
                }

                const auto offset = __builtin_ctz(mask);
                haystack += offset;

                if (haystack < haystack_end && haystack + n <= haystack_end && pageSafe(haystack))
                {
                    const auto v_haystack_offset = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
                    const auto v_against_l_offset = _mm_cmpeq_epi8(v_haystack_offset, cachel);
                    const auto v_against_u_offset = _mm_cmpeq_epi8(v_haystack_offset, cacheu);
                    const auto v_against_l_or_u_offset = _mm_or_si128(v_against_l_offset, v_against_u_offset);
                    const auto mask_offset = _mm_movemask_epi8(v_against_l_or_u_offset);

                    if (0xffff == cachemask)
                    {
                        if (mask_offset == cachemask)
                        {
                            auto haystack_pos = haystack + n;
                            auto needle_pos = needle + n;

                            while (haystack_pos < haystack_end && needle_pos < needle_end &&
                                   std::tolower(*haystack_pos) == std::tolower(*needle_pos))
                            {
                                ++haystack_pos;
                                ++needle_pos;
                            }

                            if (needle_pos == needle_end)
                                return haystack;
                        }
                    }
                    else if ((mask_offset & cachemask) == cachemask)
                        return haystack;

                    ++haystack;
                    continue;
                }
            }
#endif

            if (haystack == haystack_end)
                return haystack_end;

            if (*haystack == l || *haystack == u)
            {
                auto haystack_pos = haystack + 1;
                auto needle_pos = needle + 1;

                while (haystack_pos < haystack_end && needle_pos < needle_end &&
                       std::tolower(*haystack_pos) == std::tolower(*needle_pos))
                {
                    ++haystack_pos;
                    ++needle_pos;
                }

                if (needle_pos == needle_end)
                    return haystack;
            }

            ++haystack;
        }

        return haystack_end;
    }

    const UInt8 * search(const UInt8 * haystack, const size_t haystack_size) const
    {
        return search(haystack, haystack + haystack_size);
    }
};


/// Case-sensitive searcher (both ASCII and UTF-8)
template <bool ASCII>
class StringSearcher<true, ASCII> : private StringSearcherBase
{
private:
    /// string to be searched for
    const UInt8 * const needle;
    const size_t needle_size;
    const UInt8 * const needle_end = needle + needle_size;
    /// first character in `needle`
    UInt8 first{};

#ifdef __SSE4_1__
    /// vector filled `first` for determining leftmost position of the first symbol
    __m128i pattern;
    /// vector of first 16 characters of `needle`
    __m128i cache = _mm_setzero_si128();
    int cachemask{};
#endif

public:
    StringSearcher(const char * const needle_, const size_t needle_size)
        : needle{reinterpret_cast<const UInt8 *>(needle_)}, needle_size{needle_size}
    {
        if (0 == needle_size)
            return;

        first = *needle;

#ifdef __SSE4_1__
        pattern = _mm_set1_epi8(first);

        auto needle_pos = needle;

        for (const auto i : ext::range(0, n))
        {
            cache = _mm_srli_si128(cache, 1);

            if (needle_pos != needle_end)
            {
                cache = _mm_insert_epi8(cache, *needle_pos, n - 1);
                cachemask |= 1 << i;
                ++needle_pos;
            }
        }
#endif
    }

    ALWAYS_INLINE bool compare(const UInt8 * pos) const
    {
#ifdef __SSE4_1__
        if (pageSafe(pos))
        {
            const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos));
            const auto v_against_cache = _mm_cmpeq_epi8(v_haystack, cache);
            const auto mask = _mm_movemask_epi8(v_against_cache);

            if (0xffff == cachemask)
            {
                if (mask == cachemask)
                {
                    pos += n;
                    auto needle_pos = needle + n;

                    while (needle_pos < needle_end && *pos == *needle_pos)
                        ++pos, ++needle_pos;

                    if (needle_pos == needle_end)
                        return true;
                }
            }
            else if ((mask & cachemask) == cachemask)
                return true;

            return false;
        }
#endif

        if (*pos == first)
        {
            ++pos;
            auto needle_pos = needle + 1;

            while (needle_pos < needle_end && *pos == *needle_pos)
                ++pos, ++needle_pos;

            if (needle_pos == needle_end)
                return true;
        }

        return false;
    }

    const UInt8 * search(const UInt8 * haystack, const UInt8 * const haystack_end) const
    {
        if (0 == needle_size)
            return haystack;

        while (haystack < haystack_end)
        {
#ifdef __SSE4_1__
            if (haystack + n <= haystack_end && pageSafe(haystack))
            {
                /// find first character
                const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
                const auto v_against_pattern = _mm_cmpeq_epi8(v_haystack, pattern);

                const auto mask = _mm_movemask_epi8(v_against_pattern);

                /// first character not present in 16 octets starting at `haystack`
                if (mask == 0)
                {
                    haystack += n;
                    continue;
                }

                const auto offset = __builtin_ctz(mask);
                haystack += offset;

                if (haystack < haystack_end && haystack + n <= haystack_end && pageSafe(haystack))
                {
                    /// check for first 16 octets
                    const auto v_haystack_offset = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
                    const auto v_against_cache = _mm_cmpeq_epi8(v_haystack_offset, cache);
                    const auto mask_offset = _mm_movemask_epi8(v_against_cache);

                    if (0xffff == cachemask)
                    {
                        if (mask_offset == cachemask)
                        {
                            auto haystack_pos = haystack + n;
                            auto needle_pos = needle + n;

                            while (haystack_pos < haystack_end && needle_pos < needle_end &&
                                   *haystack_pos == *needle_pos)
                                ++haystack_pos, ++needle_pos;

                            if (needle_pos == needle_end)
                                return haystack;
                        }
                    }
                    else if ((mask_offset & cachemask) == cachemask)
                        return haystack;

                    ++haystack;
                    continue;
                }
            }
#endif

            if (haystack == haystack_end)
                return haystack_end;

            if (*haystack == first)
            {
                auto haystack_pos = haystack + 1;
                auto needle_pos = needle + 1;

                while (haystack_pos < haystack_end && needle_pos < needle_end &&
                       *haystack_pos == *needle_pos)
                    ++haystack_pos, ++needle_pos;

                if (needle_pos == needle_end)
                    return haystack;
            }

            ++haystack;
        }

        return haystack_end;
    }

    const UInt8 * search(const UInt8 * haystack, const size_t haystack_size) const
    {
        return search(haystack, haystack + haystack_size);
    }
};


using ASCIICaseSensitiveStringSearcher = StringSearcher<true, true>;
using ASCIICaseInsensitiveStringSearcher = StringSearcher<false, true>;
using UTF8CaseSensitiveStringSearcher = StringSearcher<true, false>;
using UTF8CaseInsensitiveStringSearcher = StringSearcher<false, false>;


/** Uses functions from libc.
  * It makes sense to use only with short haystacks when cheap initialization is required.
  * There is no option for case-insensitive search for UTF-8 strings.
  * It is required that strings are zero-terminated.
  */

struct LibCASCIICaseSensitiveStringSearcher
{
    const char * const needle;
    const size_t needle_size;

    LibCASCIICaseSensitiveStringSearcher(const char * const needle, const size_t needle_size)
        : needle(needle), needle_size(needle_size) {}

    const UInt8 * search(const UInt8 * haystack, const UInt8 * const haystack_end) const
    {
        auto res = strstr(reinterpret_cast<const char *>(haystack), reinterpret_cast<const char *>(needle));
        if (!res)
            return haystack_end;
        return reinterpret_cast<const UInt8 *>(res);
    }

    const UInt8 * search(const UInt8 * haystack, const size_t haystack_size) const
    {
        return search(haystack, haystack + haystack_size);
    }
};

struct LibCASCIICaseInsensitiveStringSearcher
{
    const char * const needle;
    const size_t needle_size;

    LibCASCIICaseInsensitiveStringSearcher(const char * const needle, const size_t needle_size)
        : needle(needle), needle_size(needle_size) {}

    const UInt8 * search(const UInt8 * haystack, const UInt8 * const haystack_end) const
    {
        auto res = strcasestr(reinterpret_cast<const char *>(haystack), reinterpret_cast<const char *>(needle));
        if (!res)
            return haystack_end;
        return reinterpret_cast<const UInt8 *>(res);
    }

    const UInt8 * search(const UInt8 * haystack, const size_t haystack_size) const
    {
        return search(haystack, haystack + haystack_size);
    }
};


}
