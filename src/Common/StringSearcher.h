#pragma once

#include <base/getPageSize.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Core/Defines.h>
#include <Poco/Unicode.h>
#include <cstdint>
#include <cstring>

#ifdef __SSE2__
    #include <emmintrin.h>
#endif

#ifdef __SSE4_1__
    #include <smmintrin.h>
#endif


namespace DB
{

/** Variants for searching a substring in a string.
  * In most cases, performance is less than Volnitsky (see Volnitsky.h).
  */

namespace impl
{

class StringSearcherBase
{
public:
    bool force_fallback = false;

#ifdef __SSE2__
protected:
    static constexpr size_t N = sizeof(__m128i);

    bool isPageSafe(const void * const ptr) const
    {
        return ((page_size - 1) & reinterpret_cast<std::uintptr_t>(ptr)) <= page_size - N;
    }

private:
    const Int64 page_size = ::getPageSize();
#endif
};


/// Performs case-sensitive or case-insensitive search of ASCII or UTF-8 strings
template <bool CaseSensitive, bool ASCII> class StringSearcher;


/// Case-sensitive ASCII and UTF8 searcher
template <bool ASCII>
class StringSearcher<true, ASCII> : public StringSearcherBase
{
private:
    /// string to be searched for
    const uint8_t * const needle;
    const uint8_t * const needle_end;
    /// first character in `needle`
    uint8_t first_needle_character = 0;

#ifdef __SSE4_1__
    /// second character of "needle" (if its length is > 1)
    uint8_t second_needle_character = 0;
    /// first/second needle character broadcast into a 16 bytes vector
    __m128i first_needle_character_vec;
    __m128i second_needle_character_vec;
    /// vector of first 16 characters of `needle`
    __m128i cache = _mm_setzero_si128();
    uint16_t cachemask = 0;
#endif

public:
    template <typename CharT>
    requires (sizeof(CharT) == 1)
    StringSearcher(const CharT * needle_, size_t needle_size)
        : needle(reinterpret_cast<const uint8_t *>(needle_))
        , needle_end(needle + needle_size)
    {
        if (needle_size == 0)
            return;

        first_needle_character = *needle;

#ifdef __SSE4_1__
        first_needle_character_vec = _mm_set1_epi8(first_needle_character);
        if (needle_size > 1)
        {
            second_needle_character = *(needle + 1);
            second_needle_character_vec = _mm_set1_epi8(second_needle_character);
        }
        const auto * needle_pos = needle;

        for (uint8_t i = 0; i < N; ++i)
        {
            cache = _mm_srli_si128(cache, 1);

            if (needle_pos != needle_end)
            {
                cache = _mm_insert_epi8(cache, *needle_pos, N - 1);
                cachemask |= 1 << i;
                ++needle_pos;
            }
        }
#endif
    }

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    ALWAYS_INLINE bool compare(const CharT * /*haystack*/, const CharT * /*haystack_end*/, const CharT * pos) const
    {
#ifdef __SSE4_1__
        if (isPageSafe(pos))
        {
            const __m128i haystack_characters = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos));
            const __m128i comparison_result = _mm_cmpeq_epi8(haystack_characters, cache);
            const uint16_t comparison_result_mask = _mm_movemask_epi8(comparison_result);

            if (0xffff == cachemask)
            {
                if (comparison_result_mask == cachemask)
                {
                    pos += N;
                    const auto * needle_pos = needle + N;

                    while (needle_pos < needle_end && *pos == *needle_pos)
                        ++pos, ++needle_pos;

                    if (needle_pos == needle_end)
                        return true;
                }
            }
            else if ((comparison_result_mask & cachemask) == cachemask)
                return true;

            return false;
        }
#endif

        if (*pos == first_needle_character)
        {
            ++pos;
            const auto * needle_pos = needle + 1;

            while (needle_pos < needle_end && *pos == *needle_pos)
                ++pos, ++needle_pos;

            if (needle_pos == needle_end)
                return true;
        }

        return false;
    }

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    const CharT * search(const CharT * haystack, const CharT * const haystack_end) const
    {
        const auto needle_size = needle_end - needle;

        if (needle == needle_end)
            return haystack;

#ifdef __SSE4_1__
        /// Fast path for single-character needles. Compare 16 characters of the haystack against the needle character at once.
        if (needle_size == 1)
        {
            while (haystack < haystack_end)
            {
                if (haystack + N <= haystack_end && isPageSafe(haystack))
                {
                    const __m128i haystack_characters = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
                    const __m128i comparison_result = _mm_cmpeq_epi8(haystack_characters, first_needle_character_vec);
                    const uint16_t comparison_result_mask = _mm_movemask_epi8(comparison_result);
                    if (comparison_result_mask == 0)
                    {
                        haystack += N;
                        continue;
                    }

                    const int offset = std::countr_zero(comparison_result_mask);
                    haystack += offset;

                    return haystack;
                }

                if (haystack == haystack_end)
                    return haystack_end;

                if (*haystack == first_needle_character)
                    return haystack;

                ++haystack;
            }

            return haystack_end;
        }
#endif

        while (haystack < haystack_end && haystack_end - haystack >= needle_size)
        {
#ifdef __SSE4_1__
            /// Compare the [0:15] bytes from haystack and broadcast 16 bytes vector from first character of needle.
            /// Compare the [1:16] bytes from haystack and broadcast 16 bytes vector from second character of needle.
            /// Bit AND the results of above two comparisons and get the mask.
            if ((haystack + 1 + N) <= haystack_end && isPageSafe(haystack + 1))
            {
                const __m128i haystack_characters_from_1st = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
                const __m128i haystack_characters_from_2nd = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack + 1));
                const __m128i comparison_result_1st = _mm_cmpeq_epi8(haystack_characters_from_1st, first_needle_character_vec);
                const __m128i comparison_result_2nd = _mm_cmpeq_epi8(haystack_characters_from_2nd, second_needle_character_vec);
                const __m128i comparison_result_combined = _mm_and_si128(comparison_result_1st, comparison_result_2nd);
                const uint16_t comparison_result_mask = _mm_movemask_epi8(comparison_result_combined);
                /// If the mask = 0, then first two characters [0:1] from needle are not in the [0:17] bytes of haystack.
                if (comparison_result_mask == 0)
                {
                    haystack += N;
                    continue;
                }

                const int offset = std::countr_zero(comparison_result_mask);
                haystack += offset;

                if (haystack + N <= haystack_end && isPageSafe(haystack))
                {
                    /// Already find the haystack position where the [pos:pos + 1] two characters exactly match the first two characters of needle.
                    /// Compare the 16 bytes from needle (cache) and the first 16 bytes from haystack at once if the haystack size >= 16 bytes.
                    const __m128i haystack_characters = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
                    const __m128i comparison_result_cache = _mm_cmpeq_epi8(haystack_characters, cache);
                    const uint16_t mask_offset = _mm_movemask_epi8(comparison_result_cache);

                    if (0xffff == cachemask)
                    {
                        if (mask_offset == cachemask)
                        {
                            const auto * haystack_pos = haystack + N;
                            const auto * needle_pos = needle + N;

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

            if (*haystack == first_needle_character)
            {
                const auto * haystack_pos = haystack + 1;
                const auto * needle_pos = needle + 1;

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

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    const CharT * search(const CharT * haystack, size_t haystack_size) const
    {
        return search(haystack, haystack + haystack_size);
    }
};


/// Case-insensitive ASCII searcher
template <>
class StringSearcher<false, true> : public StringSearcherBase
{
private:
    /// string to be searched for
    const uint8_t * const needle;
    const uint8_t * const needle_end;
    /// lower and uppercase variants of the first character in `needle`
    uint8_t l = 0;
    uint8_t u = 0;

#ifdef __SSE4_1__
    /// vectors filled with `l` and `u`, for determining leftmost position of the first symbol
    __m128i patl, patu;
    /// lower and uppercase vectors of first 16 characters of `needle`
    __m128i cachel = _mm_setzero_si128(), cacheu = _mm_setzero_si128();
    int cachemask = 0;
#endif

public:
    template <typename CharT>
    requires (sizeof(CharT) == 1)
    StringSearcher(const CharT * needle_, size_t needle_size)
        : needle(reinterpret_cast<const uint8_t *>(needle_))
        , needle_end(needle + needle_size)
    {
        if (needle_size == 0)
            return;

        l = static_cast<uint8_t>(std::tolower(*needle));
        u = static_cast<uint8_t>(std::toupper(*needle));

#ifdef __SSE4_1__
        patl = _mm_set1_epi8(l);
        patu = _mm_set1_epi8(u);

        const auto * needle_pos = needle;

        for (size_t i = 0; i < N; ++i)
        {
            cachel = _mm_srli_si128(cachel, 1);
            cacheu = _mm_srli_si128(cacheu, 1);

            if (needle_pos != needle_end)
            {
                cachel = _mm_insert_epi8(cachel, std::tolower(*needle_pos), N - 1);
                cacheu = _mm_insert_epi8(cacheu, std::toupper(*needle_pos), N - 1);
                cachemask |= 1 << i;
                ++needle_pos;
            }
        }
#endif
    }

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    ALWAYS_INLINE bool compare(const CharT * /*haystack*/, const CharT * /*haystack_end*/, const CharT * pos) const
    {
#ifdef __SSE4_1__
        if (isPageSafe(pos))
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
                    pos += N;
                    const auto * needle_pos = needle + N;

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
            const auto * needle_pos = needle + 1;

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

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    const CharT * search(const CharT * haystack, const CharT * const haystack_end) const
    {
        if (needle == needle_end)
            return haystack;

        while (haystack < haystack_end)
        {
#ifdef __SSE4_1__
            if (haystack + N <= haystack_end && isPageSafe(haystack))
            {
                const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
                const auto v_against_l = _mm_cmpeq_epi8(v_haystack, patl);
                const auto v_against_u = _mm_cmpeq_epi8(v_haystack, patu);
                const auto v_against_l_or_u = _mm_or_si128(v_against_l, v_against_u);

                const auto mask = _mm_movemask_epi8(v_against_l_or_u);

                if (mask == 0)
                {
                    haystack += N;
                    continue;
                }

                const auto offset = __builtin_ctz(mask);
                haystack += offset;

                if (haystack + N <= haystack_end && isPageSafe(haystack))
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
                            const auto * haystack_pos = haystack + N;
                            const auto * needle_pos = needle + N;

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
                const auto * haystack_pos = haystack + 1;
                const auto * needle_pos = needle + 1;

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

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    const CharT * search(const CharT * haystack, size_t haystack_size) const
    {
        return search(haystack, haystack + haystack_size);
    }
};


/// Case-insensitive UTF-8 searcher
template <>
class StringSearcher<false, false> : public StringSearcherBase
{
private:
    using UTF8SequenceBuffer = uint8_t[6];

    /// substring to be searched for
    const uint8_t * const needle;
    const size_t needle_size;
    const uint8_t * const needle_end = needle + needle_size;
    /// lower and uppercase variants of the first octet of the first character in `needle`
    bool first_needle_symbol_is_ascii = false;
    uint8_t l = 0;
    uint8_t u = 0;

#ifdef __SSE4_1__
    /// vectors filled with `l` and `u`, for determining leftmost position of the first symbol
    __m128i patl;
    __m128i patu;
    /// lower and uppercase vectors of first 16 characters of `needle`
    __m128i cachel = _mm_setzero_si128();
    __m128i cacheu = _mm_setzero_si128();
    int cachemask = 0;
    size_t cache_valid_len = 0;
    size_t cache_actual_len = 0;
#endif

public:
    template <typename CharT>
    requires (sizeof(CharT) == 1)
    StringSearcher(const CharT * needle_, size_t needle_size_)
        : needle(reinterpret_cast<const uint8_t *>(needle_))
        , needle_size(needle_size_)
    {
        if (needle_size == 0)
            return;

        UTF8SequenceBuffer l_seq;
        UTF8SequenceBuffer u_seq;

        if (*needle < 0x80u)
        {
            first_needle_symbol_is_ascii = true;
            l = std::tolower(*needle);
            u = std::toupper(*needle);
        }
        else
        {
            auto first_u32 = UTF8::convertUTF8ToCodePoint(reinterpret_cast<const char *>(needle), needle_size);

            /// Invalid UTF-8
            if (!first_u32)
            {
                /// Process it verbatim as a sequence of bytes.
                size_t src_len = UTF8::seqLength(*needle);

                memcpy(l_seq, needle, src_len);
                memcpy(u_seq, needle, src_len);
            }
            else
            {
                uint32_t first_l_u32 = Poco::Unicode::toLower(*first_u32);
                uint32_t first_u_u32 = Poco::Unicode::toUpper(*first_u32);

                /// lower and uppercase variants of the first octet of the first character in `needle`
                size_t length_l = UTF8::convertCodePointToUTF8(first_l_u32, reinterpret_cast<char *>(l_seq), sizeof(l_seq));
                size_t length_u = UTF8::convertCodePointToUTF8(first_u_u32, reinterpret_cast<char *>(u_seq), sizeof(u_seq));

                if (length_l != length_u)
                    force_fallback = true;
            }

            l = l_seq[0];
            u = u_seq[0];

            if (force_fallback)
                return;
        }

#ifdef __SSE4_1__
        /// for detecting leftmost position of the first symbol
        patl = _mm_set1_epi8(l);
        patu = _mm_set1_epi8(u);
        /// lower and uppercase vectors of first 16 octets of `needle`

        const auto * needle_pos = needle;

        for (size_t i = 0; i < N;)
        {
            if (needle_pos == needle_end)
            {
                cachel = _mm_srli_si128(cachel, 1);
                cacheu = _mm_srli_si128(cacheu, 1);
                ++i;

                continue;
            }

            size_t src_len = std::min<size_t>(needle_end - needle_pos, UTF8::seqLength(*needle_pos));
            auto c_u32 = UTF8::convertUTF8ToCodePoint(reinterpret_cast<const char *>(needle_pos), src_len);

            if (c_u32)
            {
                int c_l_u32 = Poco::Unicode::toLower(*c_u32);
                int c_u_u32 = Poco::Unicode::toUpper(*c_u32);

                size_t dst_l_len = UTF8::convertCodePointToUTF8(c_l_u32, reinterpret_cast<char *>(l_seq), sizeof(l_seq));
                size_t dst_u_len = UTF8::convertCodePointToUTF8(c_u_u32, reinterpret_cast<char *>(u_seq), sizeof(u_seq));

                /// @note Unicode standard states it is a rare but possible occasion
                if (!(dst_l_len == dst_u_len && dst_u_len == src_len))
                {
                    force_fallback = true;
                    return;
                }
            }

            cache_actual_len += src_len;
            if (cache_actual_len < N)
                cache_valid_len += src_len;

            for (size_t j = 0; j < src_len && i < N; ++j, ++i)
            {
                cachel = _mm_srli_si128(cachel, 1);
                cacheu = _mm_srli_si128(cacheu, 1);

                if (needle_pos != needle_end)
                {
                    cachel = _mm_insert_epi8(cachel, l_seq[j], N - 1);
                    cacheu = _mm_insert_epi8(cacheu, u_seq[j], N - 1);

                    cachemask |= 1 << i;
                    ++needle_pos;
                }
            }
        }
#endif
    }

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    ALWAYS_INLINE bool compareTrivial(const CharT * haystack_pos, const CharT * const haystack_end, const uint8_t * needle_pos) const
    {
        while (haystack_pos < haystack_end && needle_pos < needle_end)
        {
            auto haystack_code_point = UTF8::convertUTF8ToCodePoint(reinterpret_cast<const char *>(haystack_pos), haystack_end - haystack_pos);
            auto needle_code_point = UTF8::convertUTF8ToCodePoint(reinterpret_cast<const char *>(needle_pos), needle_end - needle_pos);

            /// Invalid UTF-8, should not compare equals
            if (!haystack_code_point || !needle_code_point)
                break;

            /// Not equals case insensitive.
            if (Poco::Unicode::toLower(*haystack_code_point) != Poco::Unicode::toLower(*needle_code_point))
                break;

            auto len = UTF8::seqLength(*haystack_pos);
            haystack_pos += len;

            len = UTF8::seqLength(*needle_pos);
            needle_pos += len;
        }

        return needle_pos == needle_end;
    }

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    ALWAYS_INLINE bool compare(const CharT * /*haystack*/, const CharT * haystack_end, const CharT * pos) const
    {

#ifdef __SSE4_1__
        if (isPageSafe(pos) && !force_fallback)
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
                    if (compareTrivial(pos, haystack_end, needle))
                        return true;
                }
            }
            else if ((mask & cachemask) == cachemask)
            {
                if (compareTrivial(pos, haystack_end, needle))
                    return true;
            }

            return false;
        }
#endif

        if (*pos == l || *pos == u)
        {
            pos += first_needle_symbol_is_ascii;
            const auto * needle_pos = needle + first_needle_symbol_is_ascii;

            if (compareTrivial(pos, haystack_end, needle_pos))
                return true;
        }

        return false;
    }

    /** Returns haystack_end if not found.
      */
    template <typename CharT>
    requires (sizeof(CharT) == 1)
    const CharT * search(const CharT * haystack, const CharT * const haystack_end) const
    {
        if (needle_size == 0)
            return haystack;

        while (haystack < haystack_end)
        {
#ifdef __SSE4_1__
            if (haystack + N <= haystack_end && isPageSafe(haystack) && !force_fallback)
            {
                const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
                const auto v_against_l = _mm_cmpeq_epi8(v_haystack, patl);
                const auto v_against_u = _mm_cmpeq_epi8(v_haystack, patu);
                const auto v_against_l_or_u = _mm_or_si128(v_against_l, v_against_u);

                const auto mask = _mm_movemask_epi8(v_against_l_or_u);

                if (mask == 0)
                {
                    haystack += N;
                    UTF8::syncForward(haystack, haystack_end);
                    continue;
                }

                const auto offset = __builtin_ctz(mask);
                haystack += offset;

                if (haystack + N <= haystack_end && isPageSafe(haystack))
                {
                    const auto v_haystack_offset = _mm_loadu_si128(reinterpret_cast<const __m128i *>(haystack));
                    const auto v_against_l_offset = _mm_cmpeq_epi8(v_haystack_offset, cachel);
                    const auto v_against_u_offset = _mm_cmpeq_epi8(v_haystack_offset, cacheu);
                    const auto v_against_l_or_u_offset = _mm_or_si128(v_against_l_offset, v_against_u_offset);
                    const auto mask_offset_both = _mm_movemask_epi8(v_against_l_or_u_offset);

                    if (0xffff == cachemask)
                    {
                        if (mask_offset_both == cachemask)
                        {
                            if (compareTrivial(haystack, haystack_end, needle))
                                return haystack;
                        }
                    }
                    else if ((mask_offset_both & cachemask) == cachemask)
                    {
                        if (compareTrivial(haystack, haystack_end, needle))
                            return haystack;
                    }

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
                const auto * needle_pos = needle + first_needle_symbol_is_ascii;

                if (compareTrivial(haystack_pos, haystack_end, needle_pos))
                    return haystack;
            }

            /// advance to the start of the next sequence
            haystack += UTF8::seqLength(*haystack);
        }

        return haystack_end;
    }

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    const CharT * search(const CharT * haystack, size_t haystack_size) const
    {
        return search(haystack, haystack + haystack_size);
    }
};

}

using ASCIICaseSensitiveStringSearcher =   impl::StringSearcher<true, true>;
using ASCIICaseInsensitiveStringSearcher = impl::StringSearcher<false, true>;
using UTF8CaseSensitiveStringSearcher =    impl::StringSearcher<true, false>;
using UTF8CaseInsensitiveStringSearcher =  impl::StringSearcher<false, false>;

/// Use only with short haystacks where cheap initialization is required.
template <bool CaseInsensitive>
struct StdLibASCIIStringSearcher
{
    const char * const needle_start;
    const char * const needle_end;

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    StdLibASCIIStringSearcher(const CharT * const needle_start_, size_t needle_size_)
        : needle_start(reinterpret_cast<const char *>(needle_start_))
        , needle_end(reinterpret_cast<const char *>(needle_start) + needle_size_)
    {}

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    const CharT * search(const CharT * haystack_start, const CharT * const haystack_end) const
    {
        if constexpr (CaseInsensitive)
            return std::search(
                haystack_start, haystack_end, needle_start, needle_end,
                [](char c1, char c2) { return std::toupper(c1) == std::toupper(c2); });
        else
            return std::search(
                haystack_start, haystack_end, needle_start, needle_end,
                [](char c1, char c2) { return c1 == c2; });
    }

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    const CharT * search(const CharT * haystack_start, size_t haystack_length) const
    {
        return search(haystack_start, haystack_start + haystack_length);
    }
};

}
