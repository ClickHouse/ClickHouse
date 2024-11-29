#pragma once
#include "FunctionsStringSearch.h"

#include <algorithm>
#include <string>
#include <vector>
#include <Poco/UTF8String.h>
#include <Common/Volnitsky.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

/** Implementation details for functions of 'position' family depending on ASCII/UTF8 and case sensitiveness.
  */
struct PositionCaseSensitiveASCII
{
    /// For searching single substring inside big-enough contiguous chunk of data. Coluld have slightly expensive initialization.
    using SearcherInBigHaystack = Volnitsky;

    /// For search many substrings in one string
    using MultiSearcherInBigHaystack = MultiVolnitsky;

    /// For searching single substring, that is different each time. This object is created for each row of data. It must have cheap initialization.
    using SearcherInSmallHaystack = StdLibASCIIStringSearcher</*CaseInsensitive*/ false>;

    static SearcherInBigHaystack createSearcherInBigHaystack(const char * needle_data, size_t needle_size, size_t haystack_size_hint)
    {
        return SearcherInBigHaystack(needle_data, needle_size, haystack_size_hint);
    }

    static SearcherInSmallHaystack createSearcherInSmallHaystack(const char * needle_data, size_t needle_size)
    {
        return SearcherInSmallHaystack(needle_data, needle_size);
    }

    static MultiSearcherInBigHaystack createMultiSearcherInBigHaystack(const std::vector<std::string_view> & needles)
    {
        return MultiSearcherInBigHaystack(needles);
    }

    static const char * advancePos(const char * pos, const char * end, size_t n)
    {
        return std::min(pos + n, end);
    }

    /// Number of code points between 'begin' and 'end' (this has different behaviour for ASCII and UTF-8).
    static size_t countChars(const char * begin, const char * end) { return end - begin; }

    /// Convert string to lowercase. Only for case-insensitive search.
    /// Implementation is permitted to be inefficient because it is called for single string.
    static void toLowerIfNeed(std::string &) { }
};


struct PositionCaseInsensitiveASCII
{
    /// `Volnitsky` is not used here, because one person has measured that this is better. It will be good if you question it.
    using SearcherInBigHaystack = ASCIICaseInsensitiveStringSearcher;
    using MultiSearcherInBigHaystack = MultiVolnitskyCaseInsensitive;
    using SearcherInSmallHaystack = StdLibASCIIStringSearcher</*CaseInsensitive*/ true>;

    static SearcherInBigHaystack createSearcherInBigHaystack(const char * needle_data, size_t needle_size, size_t /*haystack_size_hint*/)
    {
        return SearcherInBigHaystack(needle_data, needle_size);
    }

    static SearcherInSmallHaystack createSearcherInSmallHaystack(const char * needle_data, size_t needle_size)
    {
        return SearcherInSmallHaystack(needle_data, needle_size);
    }

    static MultiSearcherInBigHaystack createMultiSearcherInBigHaystack(const std::vector<std::string_view> & needles)
    {
        return MultiSearcherInBigHaystack(needles);
    }

    static const char * advancePos(const char * pos, const char * end, size_t n)
    {
        return std::min(pos + n, end);
    }

    static size_t countChars(const char * begin, const char * end) { return end - begin; }

    static void toLowerIfNeed(std::string & s) { std::transform(std::begin(s), std::end(s), std::begin(s), tolower); }
};


struct PositionCaseSensitiveUTF8
{
    using SearcherInBigHaystack = VolnitskyUTF8;
    using MultiSearcherInBigHaystack = MultiVolnitskyUTF8;
    using SearcherInSmallHaystack = StdLibASCIIStringSearcher</*CaseInsensitive*/ false>;

    static SearcherInBigHaystack createSearcherInBigHaystack(const char * needle_data, size_t needle_size, size_t haystack_size_hint)
    {
        return SearcherInBigHaystack(needle_data, needle_size, haystack_size_hint);
    }

    static SearcherInSmallHaystack createSearcherInSmallHaystack(const char * needle_data, size_t needle_size)
    {
        return SearcherInSmallHaystack(needle_data, needle_size);
    }

    static MultiSearcherInBigHaystack createMultiSearcherInBigHaystack(const std::vector<std::string_view> & needles)
    {
        return MultiSearcherInBigHaystack(needles);
    }

    static const char * advancePos(const char * pos, const char * end, size_t n)
    {
        for (const auto *it = pos; it != end; ++it)
        {
            if (!UTF8::isContinuationOctet(static_cast<UInt8>(*it)))
            {
                if (n == 0)
                    return it;
                n--;
            }
        }
        return end;
    }

    static size_t countChars(const char * begin, const char * end)
    {
        size_t res = 0;
        for (const auto *it = begin; it != end; ++it)
            if (!UTF8::isContinuationOctet(static_cast<UInt8>(*it)))
                ++res;
        return res;
    }

    static void toLowerIfNeed(std::string &) { }
};


struct PositionCaseInsensitiveUTF8
{
    using SearcherInBigHaystack = VolnitskyCaseInsensitiveUTF8;
    using MultiSearcherInBigHaystack = MultiVolnitskyCaseInsensitiveUTF8;
    using SearcherInSmallHaystack = UTF8CaseInsensitiveStringSearcher; /// TODO Very suboptimal.

    static SearcherInBigHaystack createSearcherInBigHaystack(const char * needle_data, size_t needle_size, size_t haystack_size_hint)
    {
        return SearcherInBigHaystack(needle_data, needle_size, haystack_size_hint);
    }

    static SearcherInSmallHaystack createSearcherInSmallHaystack(const char * needle_data, size_t needle_size)
    {
        return SearcherInSmallHaystack(needle_data, needle_size);
    }

    static MultiSearcherInBigHaystack createMultiSearcherInBigHaystack(const std::vector<std::string_view> & needles)
    {
        return MultiSearcherInBigHaystack(needles);
    }

    static const char * advancePos(const char * pos, const char * end, size_t n)
    {
        // reuse implementation that doesn't depend on case
        return PositionCaseSensitiveUTF8::advancePos(pos, end, n);
    }

    static size_t countChars(const char * begin, const char * end)
    {
        // reuse implementation that doesn't depend on case
        return PositionCaseSensitiveUTF8::countChars(begin, end);
    }

    static void toLowerIfNeed(std::string & s) { Poco::UTF8::toLowerInPlace(s); }
};


template <typename Name, typename Impl>
struct PositionImpl
{
    static constexpr bool use_default_implementation_for_constants = false;
    static constexpr bool supports_start_pos = true;
    static constexpr auto name = Name::name;

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {};}

    using ResultType = UInt64;

    /// Find one substring in many strings.
    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const std::string & needle,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt64> & res,
        [[maybe_unused]] ColumnUInt8 * res_null,
        size_t input_rows_count)
    {
        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant.
        assert(!res_null);

        const UInt8 * const begin = haystack_data.data();
        const UInt8 * const end = haystack_data.data() + haystack_data.size();
        const UInt8 * pos = begin;

        /// Fast path when needle is empty
        if (needle.empty())
        {
            /// Needle is empty and start_pos doesn't exist --> always return 1
            if (start_pos == nullptr)
            {
                for (auto & r : res)
                    r = 1;
                return;
            }

            ColumnString::Offset prev_offset = 0;

            if (const ColumnConst * start_pos_const = typeid_cast<const ColumnConst *>(&*start_pos))
            {
                /// Needle is empty and start_pos is constant
                UInt64 start = std::max(start_pos_const->getUInt(0), static_cast<UInt64>(1));
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    size_t haystack_size = Impl::countChars(
                        reinterpret_cast<const char *>(pos), reinterpret_cast<const char *>(pos + haystack_offsets[i] - prev_offset - 1));
                    res[i] = (start <= haystack_size + 1) ? start : 0;

                    pos = begin + haystack_offsets[i];
                    prev_offset = haystack_offsets[i];
                }
                return;
            }
            else
            {
                /// Needle is empty and start_pos is not constant
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    size_t haystack_size = Impl::countChars(
                        reinterpret_cast<const char *>(pos), reinterpret_cast<const char *>(pos + haystack_offsets[i] - prev_offset - 1));
                    UInt64 start = start_pos->getUInt(i);
                    start = std::max(static_cast<UInt64>(1), start);
                    res[i] = (start <= haystack_size + 1) ? start : 0;

                    pos = begin + haystack_offsets[i];
                    prev_offset = haystack_offsets[i];
                }
                return;
            }
        }

        /// Current index in the array of strings.
        size_t i = 0;

        typename Impl::SearcherInBigHaystack searcher = Impl::createSearcherInBigHaystack(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all strings at once.
        while (pos < end && end != (pos = searcher.search(pos, end - pos)))
        {
            /// Determine which index it refers to.
            while (begin + haystack_offsets[i] <= pos)
            {
                res[i] = 0;
                ++i;
            }
            auto start = start_pos != nullptr ? start_pos->getUInt(i) : 0;

            /// We check that the entry does not pass through the boundaries of strings.
            if (pos + needle.size() < begin + haystack_offsets[i])
            {
                auto res_pos = 1 + Impl::countChars(reinterpret_cast<const char *>(begin + haystack_offsets[i - 1]), reinterpret_cast<const char *>(pos));
                if (res_pos < start)
                {
                    pos = reinterpret_cast<const UInt8 *>(Impl::advancePos(
                        reinterpret_cast<const char *>(pos),
                        reinterpret_cast<const char *>(begin + haystack_offsets[i]),
                        start - res_pos));
                    continue;
                }
                res[i] = res_pos;
            }
            else
            {
                res[i] = 0;
            }
            pos = begin + haystack_offsets[i];
            ++i;
        }

        if (i < res.size())
            memset(&res[i], 0, (res.size() - i) * sizeof(res[0]));
    }

    /// Search for substring in string.
    static void constantConstantScalar(
        std::string data,
        std::string needle,
        UInt64 start_pos,
        UInt64 & res)
    {
        auto start = std::max(start_pos, UInt64(1));

        if (needle.empty())
        {
            size_t haystack_size = Impl::countChars(data.data(), data.data() + data.size());
            res = start <= haystack_size + 1 ? start : 0;
            return;
        }

        size_t start_byte = Impl::advancePos(data.data(), data.data() + data.size(), start - 1) - data.data();
        res = data.find(needle, start_byte);
        if (res == std::string::npos)
            res = 0;
        else
            res = 1 + Impl::countChars(data.data(), data.data() + res);
    }

    /// Search for substring in string starting from different positions.
    static void constantConstant(
        std::string data,
        std::string needle,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt64> & res,
        [[maybe_unused]] ColumnUInt8 * res_null)
    {
        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant.
        assert(!res_null);

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

    /// Search each time for a different single substring inside each time different string.
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
        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant.
        assert(!res_null);

        ColumnString::Offset prev_haystack_offset = 0;
        ColumnString::Offset prev_needle_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t needle_size = needle_offsets[i] - prev_needle_offset - 1;
            size_t haystack_size = haystack_offsets[i] - prev_haystack_offset - 1;

            auto start = start_pos != nullptr ? std::max(start_pos->getUInt(i), UInt64(1)) : UInt64(1);

            if (start > haystack_size + 1)
            {
                res[i] = 0;
            }
            else if (0 == needle_size)
            {
                /// An empty string is always at any position in `haystack`.
                res[i] = start;
            }
            else
            {
                /// It is assumed that the StringSearcher is not very difficult to initialize.
                typename Impl::SearcherInSmallHaystack searcher = Impl::createSearcherInSmallHaystack(
                    reinterpret_cast<const char *>(&needle_data[prev_needle_offset]),
                    needle_offsets[i] - prev_needle_offset - 1); /// zero byte at the end

                const char * beg = Impl::advancePos(
                    reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset]),
                    reinterpret_cast<const char *>(&haystack_data[haystack_offsets[i] - 1]),
                    start - 1);
                /// searcher returns a pointer to the found substring or to the end of `haystack`.
                size_t pos = searcher.search(reinterpret_cast<const UInt8 *>(beg), &haystack_data[haystack_offsets[i] - 1])
                    - &haystack_data[prev_haystack_offset];

                if (pos != haystack_size)
                {
                    res[i] = 1
                        + Impl::countChars(
                                 reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset]),
                                 reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset + pos]));
                }
                else
                    res[i] = 0;
            }

            prev_haystack_offset = haystack_offsets[i];
            prev_needle_offset = needle_offsets[i];
        }
    }

    /// Find many substrings in single string.
    static void constantVector(
        const String & haystack,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt64> & res,
        [[maybe_unused]] ColumnUInt8 * res_null,
        size_t input_rows_count)
    {
        /// `res_null` serves as an output parameter for implementing an XYZOrNull variant.
        assert(!res_null);

        /// NOTE You could use haystack indexing. But this is a rare case.
        ColumnString::Offset prev_needle_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t needle_size = needle_offsets[i] - prev_needle_offset - 1;

            auto start = start_pos != nullptr ? std::max(start_pos->getUInt(i), UInt64(1)) : UInt64(1);

            if (start > haystack.size() + 1)
            {
                res[i] = 0;
            }
            else if (0 == needle_size)
            {
                res[i] = start;
            }
            else
            {
                typename Impl::SearcherInSmallHaystack searcher = Impl::createSearcherInSmallHaystack(
                    reinterpret_cast<const char *>(&needle_data[prev_needle_offset]), needle_offsets[i] - prev_needle_offset - 1);

                const char * beg = Impl::advancePos(haystack.data(), haystack.data() + haystack.size(), start - 1);
                size_t pos = searcher.search(
                                reinterpret_cast<const UInt8 *>(beg),
                                 reinterpret_cast<const UInt8 *>(haystack.data()) + haystack.size())
                    - reinterpret_cast<const UInt8 *>(haystack.data());

                if (pos != haystack.size())
                {
                    res[i] = 1 + Impl::countChars(haystack.data(), haystack.data() + pos);
                }
                else
                    res[i] = 0;
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
