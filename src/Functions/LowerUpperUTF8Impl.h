#pragma once

#include "config.h"

#if USE_ICU

#    include <Columns/ColumnString.h>
#    include <Functions/LowerUpperImpl.h>
#    include <base/scope_guard.h>
#    include <unicode/uchar.h>
#    include <unicode/ucasemap.h>
#    include <unicode/unistr.h>
#    include <unicode/urename.h>
#    include <unicode/utypes.h>
#    include <Common/StringUtils.h>

#    include <algorithm>
#    include <array>
#    include <bitset>
#    include <string>
#    include <string_view>

#    if defined(__aarch64__) && defined(__ARM_NEON)
#        include <arm_neon.h>
#    elif defined(__SSE2__)
#        include <immintrin.h>
#    endif

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

template <char not_case_lower_bound, char not_case_upper_bound, bool upper>
struct LowerUpperUTF8Impl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        if (input_rows_count == 0)
            return;

        bool all_ascii = isAllASCIIWithEarlyExit(data.data(), data.size());
        if (all_ascii)
        {
            LowerUpperImpl<not_case_lower_bound, not_case_upper_bound>::vector(data, offsets, res_data, res_offsets, input_rows_count);
            return;
        }

        res_data.resize(data.size());
        res_offsets.resize_exact(input_rows_count);

        UErrorCode error_code = U_ZERO_ERROR;
        UCaseMap * case_map = ucasemap_open("", U_FOLD_CASE_DEFAULT, &error_code);
        if (U_FAILURE(error_code))
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Error calling ucasemap_open: {}", u_errorName(error_code));

        SCOPE_EXIT(
        {
            ucasemap_close(case_map);
        });

        const auto & two_byte_table = getTwoByteTable(case_map);

        size_t curr_offset = 0;

        auto ensure_capacity = [&](size_t size)
        {
            if (curr_offset > res_data.size() || size > res_data.size() - curr_offset)
                res_data.resize(curr_offset + size);
        };

        auto process_ascii_run = [&](size_t first_row, size_t last_row)
        {
            if (first_row == last_row)
                return;

            const size_t src_begin_offset = first_row == 0 ? 0 : offsets[first_row - 1];
            const size_t src_end_offset = offsets[last_row - 1];
            const size_t src_size = src_end_offset - src_begin_offset;

            ensure_capacity(src_size);

            const size_t dst_begin_offset = curr_offset;
            LowerUpperImpl<not_case_lower_bound, not_case_upper_bound>::vectorRaw(
                data.data() + src_begin_offset,
                data.data() + src_end_offset,
                res_data.data() + dst_begin_offset);
            curr_offset += src_size;

            for (size_t row_i = first_row; row_i < last_row; ++row_i)
                res_offsets[row_i] = dst_begin_offset + offsets[row_i] - src_begin_offset;
        };

        size_t ascii_run_start = 0;
        for (size_t row_i = 0; row_i < input_rows_count; ++row_i)
        {
            const size_t src_begin_offset = row_i == 0 ? 0 : offsets[row_i - 1];
            const size_t src_end_offset = offsets[row_i];
            const size_t src_size = src_end_offset - src_begin_offset;
            if (isAllASCIIWithEarlyExit(data.data() + src_begin_offset, src_size))
                continue;

            process_ascii_run(ascii_run_start, row_i);
            ascii_run_start = row_i + 1;

            ensure_capacity(src_size);
            const UInt8 * row = data.data() + src_begin_offset;
            size_t resume = tryMapTwoByteRow(row, src_size, res_data.data() + curr_offset, two_byte_table);
            if (resume == src_size)
            {
                curr_offset += src_size;
                res_offsets[row_i] = curr_offset;
                continue;
            }

            /// The table's output before `resume` is ICU's, so ICU maps only the rest of the row. In the root locale only
            /// final sigma depends on other characters, and it needs the last one its check does not skip.
            if constexpr (!upper)
                resume = findSigmaContextStart(row, resume, two_byte_table);
            curr_offset += resume;

            const auto * src = reinterpret_cast<const char *>(row + resume);
            const size_t icu_size = src_size - resume;

            /// ICU APIs accept `int32_t` for buffer sizes and return the required output
            /// length as `int32_t` on `U_BUFFER_OVERFLOW_ERROR`. Unicode full case mapping
            /// (Unicode `SpecialCasing.txt`, e.g. `U+0390` maps to 3 code points / 6 bytes
            /// from a 2-byte input) expands UTF-8 output by at most 3x. Reject inputs
            /// whose worst-case case-mapped output could exceed `INT32_MAX` — the retry
            /// path could otherwise receive an overflowed `dst_size` and corrupt `res_data`.
            if (static_cast<int64_t>(src_size) * 3 > INT32_MAX)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "String size {} exceeds the maximum supported length for {}: "
                    "case mapping could produce output larger than the 2 GiB ICU API limit",
                    src_size,
                    upper ? "upperUTF8" : "lowerUTF8");

            /// `res_data` accumulates output for all rows and may exceed `INT32_MAX`. Cap
            /// the destination capacity passed to ICU; the `U_BUFFER_OVERFLOW_ERROR` retry
            /// path enlarges `res_data` to fit and the guard above keeps the per-row
            /// requested length representable as `int32_t`.
            auto safe_dest_capacity = static_cast<int32_t>(std::min<size_t>(res_data.size() - curr_offset, INT32_MAX));
            auto safe_src_size = static_cast<int32_t>(icu_size);

            int32_t dst_size = 0;
            if constexpr (upper)
                dst_size = ucasemap_utf8ToUpper(
                    case_map,
                    reinterpret_cast<char *>(&res_data[curr_offset]),
                    safe_dest_capacity,
                    src,
                    safe_src_size,
                    &error_code);
            else
                dst_size = ucasemap_utf8ToLower(
                    case_map,
                    reinterpret_cast<char *>(&res_data[curr_offset]),
                    safe_dest_capacity,
                    src,
                    safe_src_size,
                    &error_code);

            if (error_code == U_BUFFER_OVERFLOW_ERROR)
            {
                size_t new_size = curr_offset + dst_size;
                res_data.resize(new_size);

                safe_dest_capacity = static_cast<int32_t>(std::min<size_t>(res_data.size() - curr_offset, INT32_MAX));

                error_code = U_ZERO_ERROR;
                if constexpr (upper)
                    dst_size = ucasemap_utf8ToUpper(
                        case_map,
                        reinterpret_cast<char *>(&res_data[curr_offset]),
                        safe_dest_capacity,
                        src,
                        safe_src_size,
                        &error_code);
                else
                    dst_size = ucasemap_utf8ToLower(
                        case_map,
                        reinterpret_cast<char *>(&res_data[curr_offset]),
                        safe_dest_capacity,
                        src,
                        safe_src_size,
                        &error_code);
            }

            if (error_code != U_ZERO_ERROR && error_code != U_STRING_NOT_TERMINATED_WARNING)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Error calling {}: {} input: {} input_size: {}",
                    upper ? "ucasemap_utf8ToUpper" : "ucasemap_utf8ToLower",
                    u_errorName(error_code),
                    std::string_view(src, icu_size),
                    icu_size);

            curr_offset += dst_size;
            res_offsets[row_i] = curr_offset;
        }

        process_ascii_run(ascii_run_start, input_rows_count);

        res_data.resize(curr_offset);
    }

    static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Functions lowerUTF8 and upperUTF8 cannot work with FixedString argument");
    }

private:
    struct TwoByteTable
    {
        /// (first << 8) | second output byte for each code point U+0080..U+07FF, 0 if a row containing it goes to ICU.
        std::array<UInt16, 0x800 - 0x80> mapped{};
        /// Case_Ignorable code points U+0000..U+07FF, which the final sigma check of ICU skips (lowerUTF8 only).
        std::bitset<0x800> sigma_ignorable;
    };

    static const TwoByteTable & getTwoByteTable(const UCaseMap * case_map)
    {
        static const TwoByteTable table = buildTwoByteTable(case_map);
        return table;
    }

    /// The entries are ICU's own mappings. The context probes drop mappings that depend on the neighbouring
    /// characters (Final_Sigma in the root locale).
    static TwoByteTable buildTwoByteTable(const UCaseMap * case_map)
    {
        auto map = [case_map](UInt32 code_point, std::string_view src)
        {
            constexpr int32_t capacity = 32;
            char dst[capacity];
            UErrorCode error_code = U_ZERO_ERROR;
            int32_t dst_size = 0;
            if constexpr (upper)
                dst_size = ucasemap_utf8ToUpper(case_map, dst, capacity, src.data(), static_cast<int32_t>(src.size()), &error_code);
            else
                dst_size = ucasemap_utf8ToLower(case_map, dst, capacity, src.data(), static_cast<int32_t>(src.size()), &error_code);

            if (error_code != U_ZERO_ERROR)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Error calling {} for code point U+{:04X}: {}",
                    upper ? "ucasemap_utf8ToUpper" : "ucasemap_utf8ToLower",
                    code_point,
                    u_errorName(error_code));

            return std::string(dst, static_cast<size_t>(dst_size));
        };

        const std::string letter = map('A', "A");

        TwoByteTable table{};
        for (UInt32 code_point = 0x80; code_point < 0x800; ++code_point)
        {
            const std::string c{static_cast<char>(0xC0 | (code_point >> 6)), static_cast<char>(0x80 | (code_point & 0x3F))};
            const std::string mapped = map(code_point, c);
            if (mapped.size() == 2
                && map(code_point, "A" + c) == letter + mapped
                && map(code_point, c + "A") == mapped + letter
                && map(code_point, "A" + c + "A") == letter + mapped + letter)
                table.mapped[code_point - 0x80] = static_cast<UInt16>(static_cast<UInt8>(mapped[0]) << 8 | static_cast<UInt8>(mapped[1]));
        }

        if constexpr (!upper)
            for (UInt32 code_point = 0; code_point < 0x800; ++code_point)
                table.sigma_ignorable[code_point] = u_hasBinaryProperty(static_cast<UChar32>(code_point), UCHAR_CASE_IGNORABLE);

        return table;
    }

    /// Maps the row up to the first non-ASCII character without a table entry. Returns the number of bytes mapped.
    static size_t tryMapTwoByteRow(const UInt8 * src, size_t size, UInt8 * dst, const TwoByteTable & table)
    {
        constexpr UInt8 flip_case_mask = 'A' ^ 'a';
        for (size_t i = 0; i < size;)
        {
            const UInt8 c = src[i];
            if (c < 0x80)
            {
                dst[i] = c ^ ((c >= not_case_lower_bound && c <= not_case_upper_bound) ? flip_case_mask : UInt8(0));
                ++i;
                continue;
            }

            if (c < 0xC2 || c > 0xDF || i + 1 >= size || (src[i + 1] & 0xC0) != 0x80)
                return i;

            const size_t code_point = static_cast<size_t>(c & 0x1F) << 6 | static_cast<size_t>(src[i + 1] & 0x3F);
            const UInt16 mapped = table.mapped[code_point - 0x80];
            if (mapped == 0)
                return i;

            dst[i] = static_cast<UInt8>(mapped >> 8);
            dst[i + 1] = static_cast<UInt8>(mapped);
            i += 2;
        }
        return size;
    }

    /// Start of the last character before `pos` that the final sigma check of ICU does not skip, or 0.
    /// The bytes before `pos` are ASCII or two-byte sequences with a table entry.
    static size_t findSigmaContextStart(const UInt8 * src, size_t pos, const TwoByteTable & table)
    {
        while (pos > 0)
        {
            pos -= src[pos - 1] < 0x80 ? 1 : 2;
            const size_t code_point = src[pos] < 0x80 ? src[pos] : (static_cast<size_t>(src[pos] & 0x1F) << 6 | (src[pos + 1] & 0x3F));
            if (!table.sigma_ignorable[code_point])
                return pos;
        }
        return 0;
    }

    static bool isAllASCIIWithEarlyExit(const UInt8 * data, size_t size)
    {
        size_t i = 0;
#    if defined(__AVX2__)
        for (; i + 128 <= size; i += 128)
        {
            auto any = _mm256_setzero_si256();
            for (size_t j = 0; j < 128; j += 32)
                any = _mm256_or_si256(any, _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + i + j)));

            if (_mm256_movemask_epi8(any))
                return false;
        }
#    elif defined(__aarch64__) && defined(__ARM_NEON)
        for (; i + 64 <= size; i += 64)
        {
            const auto bytes0 = vld1q_u8(reinterpret_cast<const uint8_t *>(data + i));
            const auto bytes1 = vld1q_u8(reinterpret_cast<const uint8_t *>(data + i + 16));
            const auto bytes2 = vld1q_u8(reinterpret_cast<const uint8_t *>(data + i + 32));
            const auto bytes3 = vld1q_u8(reinterpret_cast<const uint8_t *>(data + i + 48));
            const auto any = vorrq_u8(vorrq_u8(bytes0, bytes1), vorrq_u8(bytes2, bytes3));
            if (vmaxvq_u8(any) & 0x80)
                return false;
        }
#    elif defined(__SSE2__)
        for (; i + 64 <= size; i += 64)
        {
            auto any = _mm_setzero_si128();
            for (size_t j = 0; j < 64; j += 16)
                any = _mm_or_si128(any, _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + i + j)));

            if (_mm_movemask_epi8(any))
                return false;
        }
#    endif

        /// Keep the existing vectorized scan for larger tails. For short rows,
        /// stop at the first non-ASCII byte because this check is on the hot path.
        if (size - i >= 32)
            return isAllASCII(data + i, size - i);

        for (; i < size; ++i)
            if (data[i] & 0x80)
                return false;

        return true;
    }
};

}

#pragma clang diagnostic pop

#endif
