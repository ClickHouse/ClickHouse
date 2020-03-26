#include <Columns/ColumnString.h>
#include <Poco/UTF8Encoding.h>
#include <Common/UTF8Helpers.h>

#ifdef __SSE2__
#include <emmintrin.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    /// xor or do nothing
    template <bool>
    UInt8 xor_or_identity(const UInt8 c, const int mask)
    {
        return c ^ mask;
    }

    template <>
    inline UInt8 xor_or_identity<false>(const UInt8 c, const int)
    {
        return c;
    }

    /// It is caller's responsibility to ensure the presence of a valid cyrillic sequence in array
    template <bool to_lower>
    inline void UTF8CyrillicToCase(const UInt8 *& src, UInt8 *& dst)
    {
        if (src[0] == 0xD0u && (src[1] >= 0x80u && src[1] <= 0x8Fu))
        {
            /// ЀЁЂЃЄЅІЇЈЉЊЋЌЍЎЏ
            *dst++ = xor_or_identity<to_lower>(*src++, 0x1);
            *dst++ = xor_or_identity<to_lower>(*src++, 0x10);
        }
        else if (src[0] == 0xD1u && (src[1] >= 0x90u && src[1] <= 0x9Fu))
        {
            /// ѐёђѓєѕіїјљњћќѝўџ
            *dst++ = xor_or_identity<!to_lower>(*src++, 0x1);
            *dst++ = xor_or_identity<!to_lower>(*src++, 0x10);
        }
        else if (src[0] == 0xD0u && (src[1] >= 0x90u && src[1] <= 0x9Fu))
        {
            /// А-П
            *dst++ = *src++;
            *dst++ = xor_or_identity<to_lower>(*src++, 0x20);
        }
        else if (src[0] == 0xD0u && (src[1] >= 0xB0u && src[1] <= 0xBFu))
        {
            /// а-п
            *dst++ = *src++;
            *dst++ = xor_or_identity<!to_lower>(*src++, 0x20);
        }
        else if (src[0] == 0xD0u && (src[1] >= 0xA0u && src[1] <= 0xAFu))
        {
            /// Р-Я
            *dst++ = xor_or_identity<to_lower>(*src++, 0x1);
            *dst++ = xor_or_identity<to_lower>(*src++, 0x20);
        }
        else if (src[0] == 0xD1u && (src[1] >= 0x80u && src[1] <= 0x8Fu))
        {
            /// р-я
            *dst++ = xor_or_identity<!to_lower>(*src++, 0x1);
            *dst++ = xor_or_identity<!to_lower>(*src++, 0x20);
        }
    }
}


/** If the string contains UTF-8 encoded text, convert it to the lower (upper) case.
  * Note: It is assumed that after the character is converted to another case,
  *  the length of its multibyte sequence in UTF-8 does not change.
  * Otherwise, the behavior is undefined.
  */
template <char not_case_lower_bound,
    char not_case_upper_bound,
    int to_case(int),
    void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
struct LowerUpperUTF8Impl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        array(data.data(), data.data() + data.size(), res_data.data());
    }

    static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception("Functions lowerUTF8 and upperUTF8 cannot work with FixedString argument", ErrorCodes::BAD_ARGUMENTS);
    }

    /** Converts a single code point starting at `src` to desired case, storing result starting at `dst`.
     *    `src` and `dst` are incremented by corresponding sequence lengths. */
    static void toCase(const UInt8 *& src, const UInt8 * src_end, UInt8 *& dst)
    {
        if (src[0] <= ascii_upper_bound)
        {
            if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
                *dst++ = *src++ ^ flip_case_mask;
            else
                *dst++ = *src++;
        }
        else if (src + 1 < src_end
            && ((src[0] == 0xD0u && (src[1] >= 0x80u && src[1] <= 0xBFu)) || (src[0] == 0xD1u && (src[1] >= 0x80u && src[1] <= 0x9Fu))))
        {
            cyrillic_to_case(src, dst);
        }
        else if (src + 1 < src_end && src[0] == 0xC2u)
        {
            /// Punctuation U+0080 - U+00BF, UTF-8: C2 80 - C2 BF
            *dst++ = *src++;
            *dst++ = *src++;
        }
        else if (src + 2 < src_end && src[0] == 0xE2u)
        {
            /// Characters U+2000 - U+2FFF, UTF-8: E2 80 80 - E2 BF BF
            *dst++ = *src++;
            *dst++ = *src++;
            *dst++ = *src++;
        }
        else
        {
            static const Poco::UTF8Encoding utf8;

            int src_sequence_length = UTF8::seqLength(*src);

            int src_code_point = UTF8::queryConvert(src, src_end - src);
            if (src_code_point > 0)
            {
                int dst_code_point = to_case(src_code_point);
                if (dst_code_point > 0)
                {
                    int dst_sequence_length = UTF8::convert(dst_code_point, dst, src_end - src);

                    /// We don't support cases when lowercase and uppercase characters occupy different number of bytes in UTF-8.
                    /// As an example, this happens for ß and ẞ.
                    if (dst_sequence_length == src_sequence_length)
                    {
                        src += dst_sequence_length;
                        dst += dst_sequence_length;
                        return;
                    }
                }
            }

            *dst++ = *src++;
        }
    }

private:
    static constexpr auto ascii_upper_bound = '\x7f';
    static constexpr auto flip_case_mask = 'A' ^ 'a';

    static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
    {
#ifdef __SSE2__
        static constexpr auto bytes_sse = sizeof(__m128i);
        auto src_end_sse = src + (src_end - src) / bytes_sse * bytes_sse;

        /// SSE2 packed comparison operate on signed types, hence compare (c < 0) instead of (c > 0x7f)
        const auto v_zero = _mm_setzero_si128();
        const auto v_not_case_lower_bound = _mm_set1_epi8(not_case_lower_bound - 1);
        const auto v_not_case_upper_bound = _mm_set1_epi8(not_case_upper_bound + 1);
        const auto v_flip_case_mask = _mm_set1_epi8(flip_case_mask);

        while (src < src_end_sse)
        {
            const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

            /// check for ASCII
            const auto is_not_ascii = _mm_cmplt_epi8(chars, v_zero);
            const auto mask_is_not_ascii = _mm_movemask_epi8(is_not_ascii);

            /// ASCII
            if (mask_is_not_ascii == 0)
            {
                const auto is_not_case
                    = _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound), _mm_cmplt_epi8(chars, v_not_case_upper_bound));
                const auto mask_is_not_case = _mm_movemask_epi8(is_not_case);

                /// everything in correct case ASCII
                if (mask_is_not_case == 0)
                    _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), chars);
                else
                {
                    /// ASCII in mixed case
                    /// keep `flip_case_mask` only where necessary, zero out elsewhere
                    const auto xor_mask = _mm_and_si128(v_flip_case_mask, is_not_case);

                    /// flip case by applying calculated mask
                    const auto cased_chars = _mm_xor_si128(chars, xor_mask);

                    /// store result back to destination
                    _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), cased_chars);
                }

                src += bytes_sse;
                dst += bytes_sse;
            }
            else
            {
                /// UTF-8
                const auto expected_end = src + bytes_sse;

                while (src < expected_end)
                    toCase(src, src_end, dst);

                /// adjust src_end_sse by pushing it forward or backward
                const auto diff = src - expected_end;
                if (diff != 0)
                {
                    if (src_end_sse + diff < src_end)
                        src_end_sse += diff;
                    else
                        src_end_sse -= bytes_sse - diff;
                }
            }
        }
#endif
        /// handle remaining symbols
        while (src < src_end)
            toCase(src, src_end, dst);
    }
};

}
