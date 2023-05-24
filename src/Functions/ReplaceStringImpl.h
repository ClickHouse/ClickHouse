#pragma once

#include <base/types.h>
#include <Common/Volnitsky.h>
#include <Columns/ColumnString.h>


namespace DB
{

/** Replace one or all occurencies of substring 'needle' to 'replacement'. 'needle' and 'replacement' are constants.
  */
template <bool replace_one = false>
struct ReplaceStringImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & needle,
        const std::string & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        const UInt8 * begin = data.data();
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        ColumnString::Offset res_offset = 0;
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

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
            while (i < offsets.size() && begin + offsets[i] <= match)
            {
                res_offsets[i] = res_offset + ((begin + offsets[i]) - pos);
                ++i;
            }
            res_offset += (match - pos);

            /// If you have reached the end, it's time to stop
            if (i == offsets.size())
                break;

            /// Is it true that this string no longer needs to perform transformations.
            bool can_finish_current_string = false;

            /// We check that the entry does not go through the boundaries of strings.
            if (match + needle.size() < begin + offsets[i])
            {
                res_data.resize(res_data.size() + replacement.size());
                memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                res_offset += replacement.size();
                pos = match + needle.size();
                if (replace_one)
                    can_finish_current_string = true;
            }
            else
            {
                pos = match;
                can_finish_current_string = true;
            }

            if (can_finish_current_string)
            {
                res_data.resize(res_data.size() + (begin + offsets[i] - pos));
                memcpy(&res_data[res_offset], pos, (begin + offsets[i] - pos));
                res_offset += (begin + offsets[i] - pos);
                res_offsets[i] = res_offset;
                pos = begin + offsets[i];
                ++i;
            }
        }
    }

    /// Note: this function converts fixed-length strings to variable-length strings
    ///       and each variable-length string should ends with zero byte.
    static void vectorFixed(
        const ColumnString::Chars & data,
        size_t n,
        const std::string & needle,
        const std::string & replacement,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        const UInt8 * begin = data.data();
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        ColumnString::Offset res_offset = 0;
        size_t count = data.size() / n;
        res_data.reserve(data.size());
        res_offsets.resize(count);

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
            while (i < count && begin + n * (i + 1) <= match)
            {
                COPY_REST_OF_CURRENT_STRING();
            }

            /// If you have reached the end, it's time to stop
            if (i == count)
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
                if (replace_one || pos == begin + n * (i + 1))
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
