#pragma once

#include "PositionImpl.h"

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
    static constexpr bool use_default_implementation_for_constants = false;
    static constexpr bool supports_start_pos = true;
    static constexpr auto name = Name::name;

    using ResultType = UInt64;

    /// Count occurrences of one substring in many strings.
    static void vectorConstant(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & needle,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt64> & res)
    {
        const UInt8 * begin = data.data();
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        /// FIXME: suboptimal
        memset(&res[0], 0, res.size() * sizeof(res[0]));

        /// Current index in the array of strings.
        size_t i = 0;

        typename Impl::SearcherInBigHaystack searcher = Impl::createSearcherInBigHaystack(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all strings at once.
        while (pos < end && end != (pos = searcher.search(pos, end - pos)))
        {
            /// Determine which index it refers to.
            while (begin + offsets[i] <= pos)
                ++i;

            auto start = start_pos != nullptr ? start_pos->getUInt(i) : 0;

            /// We check that the entry does not pass through the boundaries of strings.
            if (pos + needle.size() < begin + offsets[i])
            {
                auto res_pos = needle.size() + Impl::countChars(reinterpret_cast<const char *>(begin + offsets[i - 1]), reinterpret_cast<const char *>(pos));
                if (res_pos >= start)
                {
                    ++res[i];
                }
                /// Intersecting substrings in haystack accounted only once
                pos += needle.size();
                continue;
            }
            pos = begin + offsets[i];
            ++i;
        }
    }

    /// Count number of occurrences of substring in string.
    static void constantConstantScalar(
        std::string data,
        std::string needle,
        UInt64 start_pos,
        UInt64 & res)
    {
        res = 0;

        if (needle.empty())
            return;

        auto start = std::max(start_pos, UInt64(1));
        size_t start_byte = Impl::advancePos(data.data(), data.data() + data.size(), start - 1) - data.data();
        size_t new_start_byte;
        while ((new_start_byte = data.find(needle, start_byte)) != std::string::npos)
        {
            ++res;
            /// Intersecting substrings in haystack accounted only once
            start_byte = new_start_byte + needle.size();
        }
    }

    /// Count number of occurrences of substring in string starting from different positions.
    static void constantConstant(
        std::string data,
        std::string needle,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt64> & res)
    {
        Impl::toLowerIfNeed(data);
        Impl::toLowerIfNeed(needle);

        if (start_pos == nullptr)
        {
            constantConstantScalar(data, needle, 0, res[0]);
            return;
        }

        size_t haystack_size = Impl::countChars(data.data(), data.data() + data.size());

        size_t size = start_pos != nullptr ? start_pos->size() : 0;
        for (size_t i = 0; i < size; ++i)
        {
            auto start = start_pos->getUInt(i);

            if (start > haystack_size + 1)
            {
                res[i] = 0;
                continue;
            }
            constantConstantScalar(data, needle, start, res[i]);
        }
    }

    /// Count number of occurrences of substring each time for a different inside each time different string.
    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt64> & res)
    {
        ColumnString::Offset prev_haystack_offset = 0;
        ColumnString::Offset prev_needle_offset = 0;

        size_t size = haystack_offsets.size();

        for (size_t i = 0; i < size; ++i)
        {
            size_t needle_size = needle_offsets[i] - prev_needle_offset - 1;
            size_t haystack_size = haystack_offsets[i] - prev_haystack_offset - 1;

            auto start = start_pos != nullptr ? std::max(start_pos->getUInt(i), UInt64(1)) : UInt64(1);

            res[i] = 0;
            if (start > haystack_size + 1)
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
                    reinterpret_cast<const char *>(&needle_data[prev_needle_offset]),
                    needle_offsets[i] - prev_needle_offset - 1); /// zero byte at the end

                const UInt8 * end = reinterpret_cast<const UInt8 *>(&haystack_data[haystack_offsets[i] - 1]);
                const UInt8 * beg = reinterpret_cast<const UInt8 *>(Impl::advancePos(reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset]), reinterpret_cast<const char *>(end), start - 1));

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
        PaddedPODArray<UInt64> & res)
    {
        /// NOTE You could use haystack indexing. But this is a rare case.

        ColumnString::Offset prev_needle_offset = 0;

        size_t size = needle_offsets.size();

        for (size_t i = 0; i < size; ++i)
        {
            res[i] = 0;
            auto start = start_pos != nullptr ? std::max(start_pos->getUInt(i), UInt64(1)) : UInt64(1);
            if (start <= haystack.size() + 1)
            {
                const char * needle_beg = reinterpret_cast<const char *>(&needle_data[prev_needle_offset]);
                size_t needle_size = needle_offsets[i] - prev_needle_offset - 1;

                typename Impl::SearcherInSmallHaystack searcher = Impl::createSearcherInSmallHaystack(needle_beg, needle_size);

                const UInt8 * end = reinterpret_cast<const UInt8 *>(haystack.data() + haystack.size());
                const UInt8 * beg = reinterpret_cast<const UInt8 *>(Impl::advancePos(haystack.data(), reinterpret_cast<const char *>(end), start - 1));

                const UInt8 * pos;
                while ((pos = searcher.search(beg, end)) < end)
                {
                    ++res[i];
                    beg = pos + needle_size;
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
};

}
