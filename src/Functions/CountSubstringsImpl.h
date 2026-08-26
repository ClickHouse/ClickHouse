#pragma once

#include <Functions/PositionImpl.h>

#include <string>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


/// Implementation of the countSubstrings() using helpers for position()
///
/// NOTE: Intersecting substrings in haystack accounted only once, i.e.:
///
///     countSubstrings('aaaa', 'aa') == 2
template <typename Name, typename Impl>
struct CountSubstringsImpl
{
    static constexpr bool use_default_implementation_for_constants = true;
    static constexpr bool supports_start_pos = true;
    static constexpr auto name = Name::name;

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {};}

    using ResultType = UInt64;

    /// Count occurrences of one substring in many strings.
    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const std::string & needle,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt64> & res,
        [[maybe_unused]] ColumnUInt8 * res_null,
        size_t input_rows_count)
    {
        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant, and it's irrelevant for this function.
        chassert(!res_null);

        const UInt8 * const begin = haystack_data.data();
        const UInt8 * const end = haystack_data.data() + haystack_data.size();
        const UInt8 * pos = begin;

        memset(res.data(), 0, res.size() * sizeof(res[0]));

        if (needle.empty())
            return; // Return all zeros

        /// Current index in the column of strings.
        size_t i = 0;

        typename Impl::SearcherInBigHaystack searcher = Impl::createSearcherInBigHaystack(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all strings at once.
        while (pos < end && end != (pos = searcher.search(pos, end - pos)))
        {
            /// Determine which index it refers to.
            while (i < input_rows_count && begin + haystack_offsets[i] <= pos)
                ++i;

            if (i == input_rows_count)
                break;

            size_t start = 0;
            if (start_pos)
            {
                start = start_pos->getUInt(i);
                if (start)  /// Positions are 1-based.
                    --start;
            }

            /// We check that the entry does not pass through the boundaries of strings.
            if (pos + needle.size() <= begin + haystack_offsets[i])
            {
                auto found_offset = Impl::countChars(reinterpret_cast<const char *>(begin + haystack_offsets[i - 1]), reinterpret_cast<const char *>(pos));
                if (found_offset >= start)
                    ++res[i];

                /// Intersecting substrings in haystack accounted only once
                pos += needle.size();
            }
            else
            {
                pos = begin + haystack_offsets[i];
                ++i;
            }
        }
    }

    /// Count number of occurrences of substring each time for a different inside each time different string.
    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt64> & res,
        [[maybe_unused]] ColumnUInt8 * res_null,
        size_t input_rows_count)
    {
        chassert(input_rows_count == haystack_offsets.size());

        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant.
        chassert(!res_null);

        ColumnString::Offset prev_haystack_offset = 0;
        ColumnString::Offset prev_needle_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t needle_size = needle_offsets[i] - prev_needle_offset;
            size_t haystack_size = haystack_offsets[i] - prev_haystack_offset;

            size_t start = 0;
            if (start_pos)
            {
                start = start_pos->getUInt(i);
                if (start)  /// Positions are 1-based.
                    --start;
            }

            res[i] = 0;
            if (start > haystack_size)
            {
                /// 0 already
            }
            else if (0 == needle_size)
            {
                /// 0 already
            }
            else
            {
                /// It is assumed that the StringSearcher is not very difficult to initialize.
                typename Impl::SearcherInSmallHaystack searcher = Impl::createSearcherInSmallHaystack(
                    reinterpret_cast<const char *>(&needle_data[prev_needle_offset]), needle_size);

                const UInt8 * end = reinterpret_cast<const UInt8 *>(&haystack_data[haystack_offsets[i]]);
                const UInt8 * beg = reinterpret_cast<const UInt8 *>(Impl::advancePos(reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset]), reinterpret_cast<const char *>(end), start));

                const UInt8 * pos;
                /// searcher returns a pointer to the found substring or to the end of `haystack`.
                while ((pos = searcher.search(beg, end)) < end)
                {
                    ++res[i];
                    beg = pos + needle_size;
                }
            }

            prev_haystack_offset = haystack_offsets[i];
            prev_needle_offset = needle_offsets[i];
        }
    }

    /// Count number of substrings occurrences in the single string.
    static void constantVector(
        const String & haystack,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt64> & res,
        [[maybe_unused]] ColumnUInt8 * res_null,
        size_t input_rows_count)
    {
        chassert(input_rows_count == needle_offsets.size());

        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant.
        chassert(!res_null);

        /// NOTE You could use haystack indexing. But this is a rare case.
        ColumnString::Offset prev_needle_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            res[i] = 0;
            size_t start = 0;
            if (start_pos)
            {
                start = start_pos->getUInt(i);
                if (start)  /// Positions are 1-based.
                    --start;
            }
            if (start <= haystack.size())
            {
                const char * needle_beg = reinterpret_cast<const char *>(&needle_data[prev_needle_offset]);
                size_t needle_size = needle_offsets[i] - prev_needle_offset;

                if (needle_size > 0)
                {
                    typename Impl::SearcherInSmallHaystack searcher = Impl::createSearcherInSmallHaystack(needle_beg, needle_size);

                    const UInt8 * end = reinterpret_cast<const UInt8 *>(haystack.data() + haystack.size());
                    const UInt8 * beg = reinterpret_cast<const UInt8 *>(Impl::advancePos(haystack.data(), reinterpret_cast<const char *>(end), start));

                    const UInt8 * pos;
                    while ((pos = searcher.search(beg, end)) < end)
                    {
                        ++res[i];
                        beg = pos + needle_size;
                    }
                }
            }

            prev_needle_offset = needle_offsets[i];
        }
    }

    template <typename... Args>
    static void vectorFixedConstant(Args &&...)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support FixedString haystack argument", name);
    }

    template <typename... Args>
    static void vectorFixedVector(Args &&...)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support FixedString haystack argument", name);
    }
};

}
