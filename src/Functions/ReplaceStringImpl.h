#pragma once

#include <base/types.h>
#include <Common/Volnitsky.h>
#include <Columns/ColumnString.h>


namespace DB
{

struct ReplaceStringTraits
{
    enum class Replace : uint8_t
    {
        First,
        All
    };
};

/** Replace one or all occurencies of substring 'needle' to 'replacement'.
  */
template <typename Name, ReplaceStringTraits::Replace replace>
struct ReplaceStringImpl
{
    static constexpr auto name = Name::name;

    static void vectorConstantConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const String & needle,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        if (needle.empty())
        {
            res_data.assign(haystack_data.begin(), haystack_data.end());
            res_offsets.assign(haystack_offsets.begin(), haystack_offsets.end());
            return;
        }

        const UInt8 * const begin = haystack_data.data();
        const UInt8 * const end = haystack_data.data() + haystack_data.size();
        const UInt8 * pos = begin;

        ColumnString::Offset res_offset = 0;
        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        /// The current index in the array of strings.
        size_t i = 0;

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all rows at once.
        while (pos < end)
        {
            const UInt8 * match = searcher.search(pos, end - pos);

            /// Copy the data without changing
            res_data.resize(res_data.size() + (match - pos));
            memcpy(&res_data[res_offset], pos, match - pos);

            /// Determine which index it belongs to.
            while (i < haystack_offsets.size() && begin + haystack_offsets[i] <= match)
            {
                res_offsets[i] = res_offset + ((begin + haystack_offsets[i]) - pos);
                ++i;
            }
            res_offset += (match - pos);

            /// If you have reached the end, it's time to stop
            if (i == haystack_offsets.size())
                break;

            /// Is it true that this string no longer needs to perform transformations.
            bool can_finish_current_string = false;

            /// We check that the entry does not go through the boundaries of strings.
            if (match + needle.size() < begin + haystack_offsets[i])
            {
                res_data.resize(res_data.size() + replacement.size());
                memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                res_offset += replacement.size();
                pos = match + needle.size();
                if constexpr (replace == ReplaceStringTraits::Replace::First)
                    can_finish_current_string = true;
            }
            else
            {
                pos = match;
                can_finish_current_string = true;
            }

            if (can_finish_current_string)
            {
                res_data.resize(res_data.size() + (begin + haystack_offsets[i] - pos));
                memcpy(&res_data[res_offset], pos, (begin + haystack_offsets[i] - pos));
                res_offset += (begin + haystack_offsets[i] - pos);
                res_offsets[i] = res_offset;
                pos = begin + haystack_offsets[i];
                ++i;
            }
        }
    }

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    static void copyToOutput(
        const CharT * what_start, size_t what_size,
        ColumnString::Chars & output, ColumnString::Offset & output_offset)
    {
        output.resize(output.size() + what_size);
        memcpy(&output[output_offset], what_start, what_size);
        output_offset += what_size;
    }

    static void vectorVectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        chassert(haystack_offsets.size() == needle_offsets.size());

        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        ColumnString::Offset res_offset = 0;

        size_t prev_haystack_offset = 0;
        size_t prev_needle_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto * const cur_haystack_data = &haystack_data[prev_haystack_offset];
            const size_t cur_haystack_length = haystack_offsets[i] - prev_haystack_offset - 1;

            const auto * const cur_needle_data = &needle_data[prev_needle_offset];
            const size_t cur_needle_length = needle_offsets[i] - prev_needle_offset - 1;

            const auto * last_match = static_cast<UInt8 *>(nullptr);
            const auto * start_pos = cur_haystack_data;
            const auto * const cur_haystack_end = cur_haystack_data + cur_haystack_length;

            if (cur_needle_length)
            {
                /// Using "slow" "stdlib searcher instead of Volnitsky because there is a different pattern in each row
                StdLibASCIIStringSearcher</*CaseInsensitive*/ false> searcher(cur_needle_data, cur_needle_length);

                while (start_pos < cur_haystack_end)
                {
                    if (const auto * const match = searcher.search(start_pos, cur_haystack_end); match != cur_haystack_end)
                    {
                        /// Copy prefix before match
                        copyToOutput(start_pos, match - start_pos, res_data, res_offset);

                        /// Insert replacement for match
                        copyToOutput(replacement.data(), replacement.size(), res_data, res_offset);

                        last_match = match;
                        start_pos = match + cur_needle_length;

                        if constexpr (replace == ReplaceStringTraits::Replace::First)
                            break;
                    }
                    else
                        break;
                }
            }

            /// Copy suffix after last match
            size_t bytes = (last_match == nullptr) ? (cur_haystack_end - cur_haystack_data + 1)
                                                   : (cur_haystack_end - last_match - cur_needle_length + 1);
            copyToOutput(start_pos, bytes, res_data, res_offset);

            res_offsets[i] = res_offset;

            prev_haystack_offset = haystack_offsets[i];
            prev_needle_offset = needle_offsets[i];
        }
    }

    static void vectorConstantVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const String & needle,
        const ColumnString::Chars & replacement_data,
        const ColumnString::Offsets & replacement_offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        chassert(haystack_offsets.size() == replacement_offsets.size());

        if (needle.empty())
        {
            res_data.assign(haystack_data.begin(), haystack_data.end());
            res_offsets.assign(haystack_offsets.begin(), haystack_offsets.end());
            return;
        }

        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        ColumnString::Offset res_offset = 0;

        size_t prev_haystack_offset = 0;
        size_t prev_replacement_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto * const cur_haystack_data = &haystack_data[prev_haystack_offset];
            const size_t cur_haystack_length = haystack_offsets[i] - prev_haystack_offset - 1;

            const auto * const cur_replacement_data = &replacement_data[prev_replacement_offset];
            const size_t cur_replacement_length = replacement_offsets[i] - prev_replacement_offset - 1;

            /// Using "slow" "stdlib searcher instead of Volnitsky just to keep things simple
            StdLibASCIIStringSearcher</*CaseInsensitive*/ false> searcher(needle.data(), needle.size());

            const auto * last_match = static_cast<UInt8 *>(nullptr);
            const auto * start_pos = cur_haystack_data;
            const auto * const cur_haystack_end = cur_haystack_data + cur_haystack_length;

            while (start_pos < cur_haystack_end)
            {
                if (const auto * const match = searcher.search(start_pos, cur_haystack_end); match != cur_haystack_end)
                {
                    /// Copy prefix before match
                    copyToOutput(start_pos, match - start_pos, res_data, res_offset);

                    /// Insert replacement for match
                    copyToOutput(cur_replacement_data, cur_replacement_length, res_data, res_offset);

                    last_match = match;
                    start_pos = match + needle.size();

                    if constexpr (replace == ReplaceStringTraits::Replace::First)
                        break;
                }
                else
                    break;
            }

            /// Copy suffix after last match
            size_t bytes = (last_match == nullptr) ? (cur_haystack_end - cur_haystack_data + 1)
                                                   : (cur_haystack_end - last_match - needle.size() + 1);
            copyToOutput(start_pos, bytes, res_data, res_offset);

            res_offsets[i] = res_offset;

            prev_haystack_offset = haystack_offsets[i];
            prev_replacement_offset = replacement_offsets[i];
        }
    }

    static void vectorVectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const ColumnString::Chars & replacement_data,
        const ColumnString::Offsets & replacement_offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        chassert(haystack_offsets.size() == needle_offsets.size());
        chassert(needle_offsets.size() == replacement_offsets.size());

        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        ColumnString::Offset res_offset = 0;

        size_t prev_haystack_offset = 0;
        size_t prev_needle_offset = 0;
        size_t prev_replacement_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto * const cur_haystack_data = &haystack_data[prev_haystack_offset];
            const size_t cur_haystack_length = haystack_offsets[i] - prev_haystack_offset - 1;

            const auto * const cur_needle_data = &needle_data[prev_needle_offset];
            const size_t cur_needle_length = needle_offsets[i] - prev_needle_offset - 1;

            const auto * const cur_replacement_data = &replacement_data[prev_replacement_offset];
            const size_t cur_replacement_length = replacement_offsets[i] - prev_replacement_offset - 1;

            const auto * last_match = static_cast<UInt8 *>(nullptr);
            const auto * start_pos = cur_haystack_data;
            const auto * const cur_haystack_end = cur_haystack_data + cur_haystack_length;

            if (cur_needle_length)
            {
                /// Using "slow" "stdlib searcher instead of Volnitsky because there is a different pattern in each row
                StdLibASCIIStringSearcher</*CaseInsensitive*/ false> searcher(cur_needle_data, cur_needle_length);

                while (start_pos < cur_haystack_end)
                {
                    if (const auto * const match = searcher.search(start_pos, cur_haystack_end); match != cur_haystack_end)
                    {
                        /// Copy prefix before match
                        copyToOutput(start_pos, match - start_pos, res_data, res_offset);

                        /// Insert replacement for match
                        copyToOutput(cur_replacement_data, cur_replacement_length, res_data, res_offset);

                        last_match = match;
                        start_pos = match + cur_needle_length;

                        if constexpr (replace == ReplaceStringTraits::Replace::First)
                            break;
                    }
                    else
                        break;
                }
            }
            /// Copy suffix after last match
            size_t bytes = (last_match == nullptr) ? (cur_haystack_end - cur_haystack_data + 1)
                                                   : (cur_haystack_end - last_match - cur_needle_length + 1);
            copyToOutput(start_pos, bytes, res_data, res_offset);

            res_offsets[i] = res_offset;

            prev_haystack_offset = haystack_offsets[i];
            prev_needle_offset = needle_offsets[i];
            prev_replacement_offset = replacement_offsets[i];
        }
    }

    /// Note: this function converts fixed-length strings to variable-length strings
    ///       and each variable-length string should ends with zero byte.
    static void vectorFixedConstantConstant(
        const ColumnString::Chars & haystack_data,
        size_t n,
        const String & needle,
        const String & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        if (needle.empty())
        {
            chassert(input_rows_count == haystack_data.size() / n);
            /// Since ColumnFixedString does not have a zero byte at the end, while ColumnString does,
            /// we need to split haystack_data into strings of length n, add 1 zero byte to the end of each string
            /// and then copy to res_data, ref: ColumnString.h and ColumnFixedString.h
            res_data.reserve(haystack_data.size() + input_rows_count);
            res_offsets.resize(input_rows_count);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                res_data.insert(res_data.end(), haystack_data.begin() + i * n, haystack_data.begin() + (i + 1) * n);
                res_data.push_back(0);
                res_offsets[i] = (i + 1) * n + 1;
            }
            return;
        }

        const UInt8 * const begin = haystack_data.data();
        const UInt8 * const end = haystack_data.data() + haystack_data.size();
        const UInt8 * pos = begin;

        ColumnString::Offset res_offset = 0;
        res_data.reserve(haystack_data.size());
        res_offsets.resize(input_rows_count);

        /// The current index in the string array.
        size_t i = 0;

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all rows at once.
        while (pos < end)
        {
            const UInt8 * match = searcher.search(pos, end - pos);

#define COPY_REST_OF_CURRENT_STRING() \
    do \
    { \
        const size_t len = begin + n * (i + 1) - pos; \
        res_data.resize(res_data.size() + len + 1); \
        memcpy(&res_data[res_offset], pos, len); \
        res_offset += len; \
        res_data[res_offset++] = 0; \
        res_offsets[i] = res_offset; \
        pos = begin + n * (i + 1); \
        ++i; \
    } while (false)

            /// Copy skipped strings without any changes but
            /// add zero byte to the end of each string.
            while (i < input_rows_count && begin + n * (i + 1) <= match)
            {
                COPY_REST_OF_CURRENT_STRING();
            }

            /// If you have reached the end, it's time to stop
            if (i == input_rows_count)
                break;

            /// Copy unchanged part of current string.
            res_data.resize(res_data.size() + (match - pos));
            memcpy(&res_data[res_offset], pos, match - pos);
            res_offset += (match - pos);

            /// Is it true that this string no longer needs to perform conversions.
            bool can_finish_current_string = false;

            /// We check that the entry does not pass through the boundaries of strings.
            if (match + needle.size() <= begin + n * (i + 1))
            {
                res_data.resize(res_data.size() + replacement.size());
                memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                res_offset += replacement.size();
                pos = match + needle.size();
                if (replace == ReplaceStringTraits::Replace::First || pos == begin + n * (i + 1))
                    can_finish_current_string = true;
            }
            else
            {
                pos = match;
                can_finish_current_string = true;
            }

            if (can_finish_current_string)
            {
                COPY_REST_OF_CURRENT_STRING();
            }
#undef COPY_REST_OF_CURRENT_STRING
        }
    }
};

}
