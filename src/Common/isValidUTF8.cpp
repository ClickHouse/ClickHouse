#include "config.h"

#include <Common/isValidUTF8.h>

#if defined(__SSE4_1__)
#    include <smmintrin.h>
#    include <tmmintrin.h>
#elif USE_SIMDUTF && defined(__aarch64__)
#    include <simdutf.h>
#endif

/// inspired by https://github.com/cyb70289/utf8/

/*
MIT License

Copyright (c) 2019 Yibo Cai

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/*
* http://www.unicode.org/versions/Unicode6.0.0/ch03.pdf - page 94
*
* Table 3-7. Well-Formed UTF-8 Byte Sequences
*
* +--------------------+------------+-------------+------------+-------------+
* | Code Points        | First Byte | Second Byte | Third Byte | Fourth Byte |
* +--------------------+------------+-------------+------------+-------------+
* | U+0000..U+007F     | 00..7F     |             |            |             |
* +--------------------+------------+-------------+------------+-------------+
* | U+0080..U+07FF     | C2..DF     | 80..BF      |            |             |
* +--------------------+------------+-------------+------------+-------------+
* | U+0800..U+0FFF     | E0         | A0..BF      | 80..BF     |             |
* +--------------------+------------+-------------+------------+-------------+
* | U+1000..U+CFFF     | E1..EC     | 80..BF      | 80..BF     |             |
* +--------------------+------------+-------------+------------+-------------+
* | U+D000..U+D7FF     | ED         | 80..9F      | 80..BF     |             |
* +--------------------+------------+-------------+------------+-------------+
* | U+E000..U+FFFF     | EE..EF     | 80..BF      | 80..BF     |             |
* +--------------------+------------+-------------+------------+-------------+
* | U+10000..U+3FFFF   | F0         | 90..BF      | 80..BF     | 80..BF      |
* +--------------------+------------+-------------+------------+-------------+
* | U+40000..U+FFFFF   | F1..F3     | 80..BF      | 80..BF     | 80..BF      |
* +--------------------+------------+-------------+------------+-------------+
* | U+100000..U+10FFFF | F4         | 80..8F      | 80..BF     | 80..BF      |
* +--------------------+------------+-------------+------------+-------------+
*/
namespace DB
{

namespace UTF8
{

namespace
{

UInt8 isValidUTF8Scalar(const UInt8 * data, UInt64 len)
{
    while (len)
    {
        int bytes = 0;
        const UInt8 byte1 = data[0];
        /* 00..7F */
        if (byte1 <= 0x7F)
        {
            bytes = 1;
        }
        /* C2..DF, 80..BF */
        else if (len >= 2 && byte1 >= 0xC2 && byte1 <= 0xDF && static_cast<Int8>(data[1]) <= static_cast<Int8>(0xBF))
        {
            bytes = 2;
        }
        else if (len >= 3)
        {
            const UInt8 byte2 = data[1];
            bool byte2_ok = static_cast<Int8>(byte2) <= static_cast<Int8>(0xBF);
            bool byte3_ok = static_cast<Int8>(data[2]) <= static_cast<Int8>(0xBF);

            if (byte2_ok && byte3_ok &&
                /* E0, A0..BF, 80..BF */
                ((byte1 == 0xE0 && byte2 >= 0xA0) ||
                 /* E1..EC, 80..BF, 80..BF */
                 (byte1 >= 0xE1 && byte1 <= 0xEC) ||
                 /* ED, 80..9F, 80..BF */
                 (byte1 == 0xED && byte2 <= 0x9F) ||
                 /* EE..EF, 80..BF, 80..BF */
                 (byte1 >= 0xEE && byte1 <= 0xEF)))
            {
                bytes = 3;
            }
            else if (len >= 4)
            {
                bool byte4_ok = static_cast<Int8>(data[3]) <= static_cast<Int8>(0xBF);
                if (byte2_ok && byte3_ok && byte4_ok &&
                    /* F0, 90..BF, 80..BF, 80..BF */
                    ((byte1 == 0xF0 && byte2 >= 0x90) ||
                     /* F1..F3, 80..BF, 80..BF, 80..BF */
                     (byte1 >= 0xF1 && byte1 <= 0xF3) ||
                     /* F4, 80..8F, 80..BF, 80..BF */
                     (byte1 == 0xF4 && byte2 <= 0x8F)))
                {
                    bytes = 4;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }
        else
        {
            return false;
        }
        len -= bytes;
        data += bytes;
    }
    return true;
}

#if defined(__SSE4_1__)

/// How many bytes to rewind so that `data` lands on a code point boundary, given the last four bytes
/// of the block just consumed (lowest address in the least significant byte). Three trailing
/// continuation bytes give 0, not 4: a code point is at most four bytes, so that one is complete.
UInt64 codepointSkipBackwards(UInt32 last_four_bytes)
{
    for (UInt64 back = 1; back <= 3; ++back)
    {
        const auto byte = static_cast<UInt8>(last_four_bytes >> (8 * (4 - back)));
        if ((byte & 0xC0) != 0x80)
            return back;
    }
    return 0;
}

UInt8 isValidUTF8SSE(const UInt8 * data, UInt64 len)
{
    const UInt8 * const data_original = data;

    /*
    * Map high nibble of "First Byte" to legal character length minus 1
    * 0x00 ~ 0xBF --> 0
    * 0xC0 ~ 0xDF --> 1
    * 0xE0 ~ 0xEF --> 2
    * 0xF0 ~ 0xFF --> 3
    */
    const __m128i first_len_tbl = _mm_setr_epi8(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3);

    /* Map "First Byte" to 8-th item of range table (0xC2 ~ 0xF4) */
    const __m128i first_range_tbl = _mm_setr_epi8(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8);

    /*
    * Range table, map range index to min and max values
    */
    const __m128i range_min_tbl
        = _mm_setr_epi8(0x00, 0x80, 0x80, 0x80, 0xA0, 0x80, 0x90, 0x80, 0xC2, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F);

    const __m128i range_max_tbl
        = _mm_setr_epi8(0x7F, 0xBF, 0xBF, 0xBF, 0xBF, 0x9F, 0xBF, 0x8F, 0xF4, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80);

    /*
    * Tables for fast handling of four special First Bytes(E0,ED,F0,F4), after
    * which the Second Byte are not 80~BF. It contains "range index adjustment".
    * +------------+---------------+------------------+----------------+
    * | First Byte | original range| range adjustment | adjusted range |
    * +------------+---------------+------------------+----------------+
    * | E0         | 2             | 2                | 4              |
    * +------------+---------------+------------------+----------------+
    * | ED         | 2             | 3                | 5              |
    * +------------+---------------+------------------+----------------+
    * | F0         | 3             | 3                | 6              |
    * +------------+---------------+------------------+----------------+
    * | F4         | 4             | 4                | 8              |
    * +------------+---------------+------------------+----------------+
    */

    /* index1 -> E0, index14 -> ED */
    const __m128i df_ee_tbl = _mm_setr_epi8(0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0);

    /* index1 -> F0, index5 -> F4 */
    const __m128i ef_fe_tbl = _mm_setr_epi8(0, 3, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    __m128i prev_input = _mm_set1_epi8(0);
    __m128i prev_first_len = _mm_set1_epi8(0);
    __m128i error = _mm_set1_epi8(0);

    /// Returns false as soon as an error is seen, so that a malformed prefix does not cost a scan
    /// of the whole buffer.
    auto check_packed = [&](__m128i input) noexcept -> bool
    {
        /* high_nibbles = input >> 4 */
        const __m128i high_nibbles = _mm_and_si128(_mm_srli_epi16(input, 4), _mm_set1_epi8(0x0F));

        /* first_len = legal character length minus 1 */
        /* 0 for 00~7F, 1 for C0~DF, 2 for E0~EF, 3 for F0~FF */
        /* first_len = first_len_tbl[high_nibbles] */
        __m128i first_len = _mm_shuffle_epi8(first_len_tbl, high_nibbles);

        /* First Byte: set range index to 8 for bytes within 0xC0 ~ 0xFF */
        /* range = first_range_tbl[high_nibbles] */
        __m128i range = _mm_shuffle_epi8(first_range_tbl, high_nibbles);

        /* Second Byte: set range index to first_len */
        /* 0 for 00~7F, 1 for C0~DF, 2 for E0~EF, 3 for F0~FF */
        /* range |= (first_len, prev_first_len) << 1 byte */
        range = _mm_or_si128(range, _mm_alignr_epi8(first_len, prev_first_len, 15));

        /* Third Byte: set range index to saturate_sub(first_len, 1) */
        /* 0 for 00~7F, 0 for C0~DF, 1 for E0~EF, 2 for F0~FF */
        __m128i tmp1;
        __m128i tmp2;
        /* tmp1 = saturate_sub(first_len, 1) */
        tmp1 = _mm_subs_epu8(first_len, _mm_set1_epi8(1));
        /* tmp2 = saturate_sub(prev_first_len, 1) */
        tmp2 = _mm_subs_epu8(prev_first_len, _mm_set1_epi8(1));
        /* range |= (tmp1, tmp2) << 2 bytes */
        range = _mm_or_si128(range, _mm_alignr_epi8(tmp1, tmp2, 14));

        /* Fourth Byte: set range index to saturate_sub(first_len, 2) */
        /* 0 for 00~7F, 0 for C0~DF, 0 for E0~EF, 1 for F0~FF */
        /* tmp1 = saturate_sub(first_len, 2) */
        tmp1 = _mm_subs_epu8(first_len, _mm_set1_epi8(2));
        /* tmp2 = saturate_sub(prev_first_len, 2) */
        tmp2 = _mm_subs_epu8(prev_first_len, _mm_set1_epi8(2));
        /* range |= (tmp1, tmp2) << 3 bytes */
        range = _mm_or_si128(range, _mm_alignr_epi8(tmp1, tmp2, 13));

        /*
         * Now we have below range indices calculated
         * Correct cases:
         * - 8 for C0~FF
         * - 3 for 1st byte after F0~FF
         * - 2 for 1st byte after E0~EF or 2nd byte after F0~FF
         * - 1 for 1st byte after C0~DF or 2nd byte after E0~EF or
         *         3rd byte after F0~FF
         * - 0 for others
         * Error cases:
         *   9,10,11 if non ascii First Byte overlaps
         *   E.g., F1 80 C2 90 --> 8 3 10 2, where 10 indicates error
         */

        /* Adjust Second Byte range for special First Bytes(E0,ED,F0,F4) */
        /* Overlaps lead to index 9~15, which are illegal in range table */
        __m128i shift1;
        __m128i pos;
        __m128i range2;
        /* shift1 = (input, prev_input) << 1 byte */
        shift1 = _mm_alignr_epi8(input, prev_input, 15);
        pos = _mm_sub_epi8(shift1, _mm_set1_epi8(0xEF));
        /*
         * shift1:  | EF  F0 ... FE | FF  00  ... ...  DE | DF  E0 ... EE |
         * pos:     | 0   1      15 | 16  17           239| 240 241    255|
         * pos-240: | 0   0      0  | 0   0            0  | 0   1      15 |
         * pos+112: | 112 113    127|       >= 128        |     >= 128    |
         */
        tmp1 = _mm_subs_epu8(pos, _mm_set1_epi8(0xF0));
        range2 = _mm_shuffle_epi8(df_ee_tbl, tmp1);
        tmp2 = _mm_adds_epu8(pos, _mm_set1_epi8(112));
        range2 = _mm_add_epi8(range2, _mm_shuffle_epi8(ef_fe_tbl, tmp2));

        range = _mm_add_epi8(range, range2);

        /* Load min and max values per calculated range index */
        __m128i minv = _mm_shuffle_epi8(range_min_tbl, range);
        __m128i maxv = _mm_shuffle_epi8(range_max_tbl, range);

        /* Check value range */
        error = _mm_or_si128(error, _mm_cmplt_epi8(input, minv));
        error = _mm_or_si128(error, _mm_cmpgt_epi8(input, maxv));

        prev_input = input;
        prev_first_len = first_len;

        data += 16;
        len -= 16;
        return _mm_testz_si128(error, error) != 0;
    };

    while (len >= 16) // NOLINT
        if (!check_packed(_mm_loadu_si128(reinterpret_cast<const __m128i *>(data))))
            return false;

    /// The last consumed block can end in the middle of a code point whose continuation bytes are in
    /// the remainder. `prev_input` holds a consumed block only once the loop has run.
    if (data != data_original)
    {
        const UInt64 back = codepointSkipBackwards(static_cast<UInt32>(_mm_extract_epi32(prev_input, 3)));
        data -= back;
        len += back;
    }

    /// Reaching here means every consumed block was clean. The remainder is under 16 bytes plus the
    /// rewind, so the scalar validator finishes it while reading strictly within the input buffer.
    return isValidUTF8Scalar(data, len);
}

#endif

}

UInt8 isValidUTF8(const UInt8 * data, UInt64 len)
{
#if defined(__SSE4_1__)
    return isValidUTF8SSE(data, len);
#elif USE_SIMDUTF && defined(__aarch64__)
    /// simdutf reads its input in blocks of this size, so a shorter input costs a full block either way.
    static constexpr UInt64 simdutf_block_size = 64;
    if (len < simdutf_block_size)
        return isValidUTF8Scalar(data, len);
    /// The _with_errors variant tests for errors after every block, so malformed input stops the scan early.
    return simdutf::validate_utf8_with_errors(reinterpret_cast<const char *>(data), len).error == simdutf::SUCCESS;
#else
    return isValidUTF8Scalar(data, len);
#endif
}

}
}
