#include <Common/StringSearcher.h>

namespace DB
{

namespace TargetSpecific::Default
{

using UTF8SequenceBuffer = uint8_t[6];

UTF8CaseInsensitiveSearcherImpl::UTF8CaseInsensitiveSearcherImpl(const UInt8 * needle_, size_t needle_size_)
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
        l = static_cast<uint8_t>(std::tolower(*needle));
        u = static_cast<uint8_t>(std::toupper(*needle));
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

    constexpr size_t N = sizeof(__m128i);
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

bool UTF8CaseInsensitiveSearcherImpl::compareTrivial(const UInt8 * haystack_pos, const UInt8 * const haystack_end, const uint8_t * needle_pos) const
{
    while (haystack_pos < haystack_end && needle_pos < needle_end)
    {
        auto haystack_code_point
            = UTF8::convertUTF8ToCodePoint(reinterpret_cast<const char *>(haystack_pos), haystack_end - haystack_pos);
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

bool UTF8CaseInsensitiveSearcherImpl::compare(const UInt8 * /*haystack*/, const UInt8 * haystack_end, const UInt8 * pos) const
{
#ifdef __SSE4_1__
    constexpr size_t N = sizeof(__m128i);
    const Int64 page_size = ::getPageSize();
    auto isPageSafe = [page_size](const void * const ptr)
    {
        return ((page_size - 1) & reinterpret_cast<std::uintptr_t>(ptr)) <= page_size - N;
    };

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

const UInt8 * UTF8CaseInsensitiveSearcherImpl::search(const UInt8 * haystack, const UInt8 * const haystack_end) const
{
    if (needle_size == 0)
        return haystack;

    while (haystack < haystack_end)
    {
#ifdef __SSE4_1__
        constexpr size_t N = sizeof(__m128i);
        const Int64 page_size = ::getPageSize();
        auto isPageSafe = [page_size](const void * const ptr)
        {
            return ((page_size - 1) & reinterpret_cast<std::uintptr_t>(ptr)) <= page_size - N;
        };

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
            const auto * haystack_pos = haystack + first_needle_symbol_is_ascii;
            const auto * needle_pos = needle + first_needle_symbol_is_ascii;

            if (compareTrivial(haystack_pos, haystack_end, needle_pos))
                return haystack;
        }

        /// advance to the start of the next sequence
        haystack += UTF8::seqLength(*haystack);
    }

    return haystack_end;
}

} // namespace TargetSpecific::Default

namespace impl
{

template class StringSearcher<true, true>;
template class StringSearcher<false, true>;
template class StringSearcher<true, false>;
template class StringSearcher<false, false>;

}

}
