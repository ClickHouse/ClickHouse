#include "FunctionsStringSearch.h"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/Regexps.h>
#include <IO/WriteHelpers.h>
#include <Poco/UTF8String.h>
#include <Common/Volnitsky.h>

namespace DB
{
/** Implementation details for functions of 'position' family depending on ASCII/UTF8 and case sensitiveness.
  */
struct PositionCaseSensitiveASCII
{
    /// For searching single substring inside big-enough contiguous chunk of data. Coluld have slightly expensive initialization.
    using SearcherInBigHaystack = Volnitsky;

    /// For search many substrings in one string
    using MultiSearcherInBigHaystack = MultiVolnitsky;

    /// For searching single substring, that is different each time. This object is created for each row of data. It must have cheap initialization.
    using SearcherInSmallHaystack = LibCASCIICaseSensitiveStringSearcher;

    static SearcherInBigHaystack createSearcherInBigHaystack(const char * needle_data, size_t needle_size, size_t haystack_size_hint)
    {
        return SearcherInBigHaystack(needle_data, needle_size, haystack_size_hint);
    }

    static SearcherInSmallHaystack createSearcherInSmallHaystack(const char * needle_data, size_t needle_size)
    {
        return SearcherInSmallHaystack(needle_data, needle_size);
    }

    static MultiSearcherInBigHaystack createMultiSearcherInBigHaystack(const std::vector<StringRef> & needles)
    {
        return MultiSearcherInBigHaystack(needles);
    }

    /// Number of code points between 'begin' and 'end' (this has different behaviour for ASCII and UTF-8).
    static size_t countChars(const char * begin, const char * end) { return end - begin; }

    /// Convert string to lowercase. Only for case-insensitive search.
    /// Implementation is permitted to be inefficient because it is called for single string.
    static void toLowerIfNeed(std::string &) {}
};

struct PositionCaseInsensitiveASCII
{
    /// `Volnitsky` is not used here, because one person has measured that this is better. It will be good if you question it.
    using SearcherInBigHaystack = ASCIICaseInsensitiveStringSearcher;
    using MultiSearcherInBigHaystack = MultiVolnitskyCaseInsensitive;
    using SearcherInSmallHaystack = LibCASCIICaseInsensitiveStringSearcher;

    static SearcherInBigHaystack createSearcherInBigHaystack(const char * needle_data, size_t needle_size, size_t /*haystack_size_hint*/)
    {
        return SearcherInBigHaystack(needle_data, needle_size);
    }

    static SearcherInSmallHaystack createSearcherInSmallHaystack(const char * needle_data, size_t needle_size)
    {
        return SearcherInSmallHaystack(needle_data, needle_size);
    }

    static MultiSearcherInBigHaystack createMultiSearcherInBigHaystack(const std::vector<StringRef> & needles)
    {
        return MultiSearcherInBigHaystack(needles);
    }

    static size_t countChars(const char * begin, const char * end) { return end - begin; }

    static void toLowerIfNeed(std::string & s) { std::transform(std::begin(s), std::end(s), std::begin(s), tolower); }
};

struct PositionCaseSensitiveUTF8
{
    using SearcherInBigHaystack = VolnitskyUTF8;
    using MultiSearcherInBigHaystack = MultiVolnitskyUTF8;
    using SearcherInSmallHaystack = LibCASCIICaseSensitiveStringSearcher;

    static SearcherInBigHaystack createSearcherInBigHaystack(const char * needle_data, size_t needle_size, size_t haystack_size_hint)
    {
        return SearcherInBigHaystack(needle_data, needle_size, haystack_size_hint);
    }

    static SearcherInSmallHaystack createSearcherInSmallHaystack(const char * needle_data, size_t needle_size)
    {
        return SearcherInSmallHaystack(needle_data, needle_size);
    }

    static MultiSearcherInBigHaystack createMultiSearcherInBigHaystack(const std::vector<StringRef> & needles)
    {
        return MultiSearcherInBigHaystack(needles);
    }

    static size_t countChars(const char * begin, const char * end)
    {
        size_t res = 0;
        for (auto it = begin; it != end; ++it)
            if (!UTF8::isContinuationOctet(static_cast<UInt8>(*it)))
                ++res;
        return res;
    }

    static void toLowerIfNeed(std::string &) {}
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

    static MultiSearcherInBigHaystack createMultiSearcherInBigHaystack(const std::vector<StringRef> & needles)
    {
        return MultiSearcherInBigHaystack(needles);
    }

    static size_t countChars(const char * begin, const char * end)
    {
        size_t res = 0;
        for (auto it = begin; it != end; ++it)
            if (!UTF8::isContinuationOctet(static_cast<UInt8>(*it)))
                ++res;
        return res;
    }

    static void toLowerIfNeed(std::string & s) { Poco::UTF8::toLowerInPlace(s); }
};

template <typename Impl>
struct PositionImpl
{
    using ResultType = UInt64;

    /// Find one substring in many strings.
    static void vector_constant(
        const ColumnString::Chars & data, const ColumnString::Offsets & offsets, const std::string & needle, PaddedPODArray<UInt64> & res)
    {
        const UInt8 * begin = data.data();
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        /// Current index in the array of strings.
        size_t i = 0;

        typename Impl::SearcherInBigHaystack searcher = Impl::createSearcherInBigHaystack(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all strings at once.
        while (pos < end && end != (pos = searcher.search(pos, end - pos)))
        {
            /// Determine which index it refers to.
            while (begin + offsets[i] <= pos)
            {
                res[i] = 0;
                ++i;
            }

            /// We check that the entry does not pass through the boundaries of strings.
            if (pos + needle.size() < begin + offsets[i])
                res[i] = 1 + Impl::countChars(reinterpret_cast<const char *>(begin + offsets[i - 1]), reinterpret_cast<const char *>(pos));
            else
                res[i] = 0;

            pos = begin + offsets[i];
            ++i;
        }

        if (i < res.size())
            memset(&res[i], 0, (res.size() - i) * sizeof(res[0]));
    }

    /// Search for substring in string.
    static void constant_constant(std::string data, std::string needle, UInt64 & res)
    {
        Impl::toLowerIfNeed(data);
        Impl::toLowerIfNeed(needle);

        res = data.find(needle);
        if (res == std::string::npos)
            res = 0;
        else
            res = 1 + Impl::countChars(data.data(), data.data() + res);
    }

    /// Search each time for a different single substring inside each time different string.
    static void vector_vector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        PaddedPODArray<UInt64> & res)
    {
        ColumnString::Offset prev_haystack_offset = 0;
        ColumnString::Offset prev_needle_offset = 0;

        size_t size = haystack_offsets.size();

        for (size_t i = 0; i < size; ++i)
        {
            size_t needle_size = needle_offsets[i] - prev_needle_offset - 1;
            size_t haystack_size = haystack_offsets[i] - prev_haystack_offset - 1;

            if (0 == needle_size)
            {
                /// An empty string is always at the very beginning of `haystack`.
                res[i] = 1;
            }
            else
            {
                /// It is assumed that the StringSearcher is not very difficult to initialize.
                typename Impl::SearcherInSmallHaystack searcher = Impl::createSearcherInSmallHaystack(
                    reinterpret_cast<const char *>(&needle_data[prev_needle_offset]),
                    needle_offsets[i] - prev_needle_offset - 1); /// zero byte at the end

                /// searcher returns a pointer to the found substring or to the end of `haystack`.
                size_t pos = searcher.search(&haystack_data[prev_haystack_offset], &haystack_data[haystack_offsets[i] - 1])
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
    static void constant_vector(
        const String & haystack,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        PaddedPODArray<UInt64> & res)
    {
        // NOTE You could use haystack indexing. But this is a rare case.

        ColumnString::Offset prev_needle_offset = 0;

        size_t size = needle_offsets.size();

        for (size_t i = 0; i < size; ++i)
        {
            size_t needle_size = needle_offsets[i] - prev_needle_offset - 1;

            if (0 == needle_size)
            {
                res[i] = 1;
            }
            else
            {
                typename Impl::SearcherInSmallHaystack searcher = Impl::createSearcherInSmallHaystack(
                    reinterpret_cast<const char *>(&needle_data[prev_needle_offset]), needle_offsets[i] - prev_needle_offset - 1);

                size_t pos = searcher.search(
                                 reinterpret_cast<const UInt8 *>(haystack.data()),
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
};

template <typename Impl>
struct MultiSearchAllPositionsImpl
{
    using ResultType = UInt64;

    static void vector_constant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const std::vector<StringRef> & needles,
        PaddedPODArray<UInt64> & res)
    {
        auto res_callback = [](const UInt8 * start, const UInt8 * end) -> UInt64
        {
            return 1 + Impl::countChars(reinterpret_cast<const char *>(start), reinterpret_cast<const char *>(end));
        };
        Impl::createMultiSearcherInBigHaystack(needles).searchAllPositions(haystack_data, haystack_offsets, res_callback, res);
    }
};

template <typename Impl>
struct MultiSearchImpl
{
    using ResultType = UInt8;
    static constexpr bool is_using_hyperscan = false;

    static void vector_constant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const std::vector<StringRef> & needles,
        PaddedPODArray<UInt8> & res)
    {
        Impl::createMultiSearcherInBigHaystack(needles).search(haystack_data, haystack_offsets, res);
    }
};

template <typename Impl>
struct MultiSearchFirstPositionImpl
{
    using ResultType = UInt64;
    static constexpr bool is_using_hyperscan = false;

    static void vector_constant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const std::vector<StringRef> & needles,
        PaddedPODArray<UInt64> & res)
    {
        auto res_callback = [](const UInt8 * start, const UInt8 * end) -> UInt64
        {
            return 1 + Impl::countChars(reinterpret_cast<const char *>(start), reinterpret_cast<const char *>(end));
        };
        Impl::createMultiSearcherInBigHaystack(needles).searchFirstPosition(haystack_data, haystack_offsets, res_callback, res);
    }
};

template <typename Impl>
struct MultiSearchFirstIndexImpl
{
    using ResultType = UInt64;
    static constexpr bool is_using_hyperscan = false;

    static void vector_constant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const std::vector<StringRef> & needles,
        PaddedPODArray<UInt64> & res)
    {
        Impl::createMultiSearcherInBigHaystack(needles).searchIndex(haystack_data, haystack_offsets, res);
    }
};


struct NamePosition
{
    static constexpr auto name = "position";
};
struct NamePositionUTF8
{
    static constexpr auto name = "positionUTF8";
};
struct NamePositionCaseInsensitive
{
    static constexpr auto name = "positionCaseInsensitive";
};
struct NamePositionCaseInsensitiveUTF8
{
    static constexpr auto name = "positionCaseInsensitiveUTF8";
};
struct NameMultiSearchAllPositions
{
    static constexpr auto name = "multiSearchAllPositions";
};
struct NameMultiSearchAllPositionsUTF8
{
    static constexpr auto name = "multiSearchAllPositionsUTF8";
};
struct NameMultiSearchAllPositionsCaseInsensitive
{
    static constexpr auto name = "multiSearchAllPositionsCaseInsensitive";
};
struct NameMultiSearchAllPositionsCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchAllPositionsCaseInsensitiveUTF8";
};
struct NameMultiSearchAny
{
    static constexpr auto name = "multiSearchAny";
};
struct NameMultiSearchAnyUTF8
{
    static constexpr auto name = "multiSearchAnyUTF8";
};
struct NameMultiSearchAnyCaseInsensitive
{
    static constexpr auto name = "multiSearchAnyCaseInsensitive";
};
struct NameMultiSearchAnyCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchAnyCaseInsensitiveUTF8";
};
struct NameMultiSearchFirstIndex
{
    static constexpr auto name = "multiSearchFirstIndex";
};
struct NameMultiSearchFirstIndexUTF8
{
    static constexpr auto name = "multiSearchFirstIndexUTF8";
};
struct NameMultiSearchFirstIndexCaseInsensitive
{
    static constexpr auto name = "multiSearchFirstIndexCaseInsensitive";
};
struct NameMultiSearchFirstIndexCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchFirstIndexCaseInsensitiveUTF8";
};
struct NameMultiSearchFirstPosition
{
    static constexpr auto name = "multiSearchFirstPosition";
};
struct NameMultiSearchFirstPositionUTF8
{
    static constexpr auto name = "multiSearchFirstPositionUTF8";
};
struct NameMultiSearchFirstPositionCaseInsensitive
{
    static constexpr auto name = "multiSearchFirstPositionCaseInsensitive";
};
struct NameMultiSearchFirstPositionCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchFirstPositionCaseInsensitiveUTF8";
};


using FunctionPosition = FunctionsStringSearch<PositionImpl<PositionCaseSensitiveASCII>, NamePosition>;
using FunctionPositionUTF8 = FunctionsStringSearch<PositionImpl<PositionCaseSensitiveUTF8>, NamePositionUTF8>;
using FunctionPositionCaseInsensitive = FunctionsStringSearch<PositionImpl<PositionCaseInsensitiveASCII>, NamePositionCaseInsensitive>;
using FunctionPositionCaseInsensitiveUTF8 = FunctionsStringSearch<PositionImpl<PositionCaseInsensitiveUTF8>, NamePositionCaseInsensitiveUTF8>;

using FunctionMultiSearchAllPositions = FunctionsMultiStringPosition<MultiSearchAllPositionsImpl<PositionCaseSensitiveASCII>, NameMultiSearchAllPositions>;
using FunctionMultiSearchAllPositionsUTF8 = FunctionsMultiStringPosition<MultiSearchAllPositionsImpl<PositionCaseSensitiveUTF8>, NameMultiSearchAllPositionsUTF8>;
using FunctionMultiSearchAllPositionsCaseInsensitive = FunctionsMultiStringPosition<MultiSearchAllPositionsImpl<PositionCaseInsensitiveASCII>, NameMultiSearchAllPositionsCaseInsensitive>;
using FunctionMultiSearchAllPositionsCaseInsensitiveUTF8 = FunctionsMultiStringPosition<MultiSearchAllPositionsImpl<PositionCaseInsensitiveUTF8>, NameMultiSearchAllPositionsCaseInsensitiveUTF8>;

using FunctionMultiSearch = FunctionsMultiStringSearch<MultiSearchImpl<PositionCaseSensitiveASCII>, NameMultiSearchAny>;
using FunctionMultiSearchUTF8 = FunctionsMultiStringSearch<MultiSearchImpl<PositionCaseSensitiveUTF8>, NameMultiSearchAnyUTF8>;
using FunctionMultiSearchCaseInsensitive = FunctionsMultiStringSearch<MultiSearchImpl<PositionCaseInsensitiveASCII>, NameMultiSearchAnyCaseInsensitive>;
using FunctionMultiSearchCaseInsensitiveUTF8 = FunctionsMultiStringSearch<MultiSearchImpl<PositionCaseInsensitiveUTF8>, NameMultiSearchAnyCaseInsensitiveUTF8>;

using FunctionMultiSearchFirstIndex = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<PositionCaseSensitiveASCII>, NameMultiSearchFirstIndex>;
using FunctionMultiSearchFirstIndexUTF8 = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<PositionCaseSensitiveUTF8>, NameMultiSearchFirstIndexUTF8>;
using FunctionMultiSearchFirstIndexCaseInsensitive = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<PositionCaseInsensitiveASCII>, NameMultiSearchFirstIndexCaseInsensitive>;
using FunctionMultiSearchFirstIndexCaseInsensitiveUTF8 = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<PositionCaseInsensitiveUTF8>, NameMultiSearchFirstIndexCaseInsensitiveUTF8>;

using FunctionMultiSearchFirstPosition = FunctionsMultiStringSearch<MultiSearchFirstPositionImpl<PositionCaseSensitiveASCII>, NameMultiSearchFirstPosition>;
using FunctionMultiSearchFirstPositionUTF8 = FunctionsMultiStringSearch<MultiSearchFirstPositionImpl<PositionCaseSensitiveUTF8>, NameMultiSearchFirstPositionUTF8>;
using FunctionMultiSearchFirstPositionCaseInsensitive = FunctionsMultiStringSearch<MultiSearchFirstPositionImpl<PositionCaseInsensitiveASCII>, NameMultiSearchFirstPositionCaseInsensitive>;
using FunctionMultiSearchFirstPositionCaseInsensitiveUTF8 = FunctionsMultiStringSearch<MultiSearchFirstPositionImpl<PositionCaseInsensitiveUTF8>, NameMultiSearchFirstPositionCaseInsensitiveUTF8>;


void registerFunctionsStringSearch(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPosition>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionPositionUTF8>();
    factory.registerFunction<FunctionPositionCaseInsensitive>();
    factory.registerFunction<FunctionPositionCaseInsensitiveUTF8>();

    factory.registerFunction<FunctionMultiSearchAllPositions>();
    factory.registerFunction<FunctionMultiSearchAllPositionsUTF8>();
    factory.registerFunction<FunctionMultiSearchAllPositionsCaseInsensitive>();
    factory.registerFunction<FunctionMultiSearchAllPositionsCaseInsensitiveUTF8>();

    factory.registerFunction<FunctionMultiSearch>();
    factory.registerFunction<FunctionMultiSearchUTF8>();
    factory.registerFunction<FunctionMultiSearchCaseInsensitive>();
    factory.registerFunction<FunctionMultiSearchCaseInsensitiveUTF8>();

    factory.registerFunction<FunctionMultiSearchFirstIndex>();
    factory.registerFunction<FunctionMultiSearchFirstIndexUTF8>();
    factory.registerFunction<FunctionMultiSearchFirstIndexCaseInsensitive>();
    factory.registerFunction<FunctionMultiSearchFirstIndexCaseInsensitiveUTF8>();

    factory.registerFunction<FunctionMultiSearchFirstPosition>();
    factory.registerFunction<FunctionMultiSearchFirstPositionUTF8>();
    factory.registerFunction<FunctionMultiSearchFirstPositionCaseInsensitive>();
    factory.registerFunction<FunctionMultiSearchFirstPositionCaseInsensitiveUTF8>();

    factory.registerAlias("locate", NamePosition::name, FunctionFactory::CaseInsensitive);
}
}
