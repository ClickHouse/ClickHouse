#pragma once

#include <vector>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/scope_guard.h>
#include "config.h"

#if USE_AHO_CORASICK
#    include <Functions/MultiSearchAhoCorasickCache.h>
#    include <aho_corasick.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// The Volnitsky multi-searcher used for a constant array of needles stores
/// the matched needle index in a single byte, so it cannot handle more than 255 needles.
inline void checkMultiSearchNeedlesLimit(std::string_view function_name, size_t needles_count)
{
    if (needles_count > std::numeric_limits<UInt8>::max())
        throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
            "Number of arguments for function {} doesn't match: passed {}, should be at most {}",
            function_name, needles_count, std::to_string(std::numeric_limits<UInt8>::max()));
}

template <typename Name, typename Impl>
struct MultiSearchImpl
{
    using ResultType = UInt8;
    /// Variable for understanding, if we used offsets for the output, most
    /// likely to determine whether the function returns ColumnVector of ColumnArray.
    static constexpr bool is_column_array = false;
    static constexpr auto name = Name::name;

    static auto getReturnType() { return std::make_shared<DataTypeNumber<ResultType>>(); }

    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const Array & needles_arr,
        PaddedPODArray<UInt8> & res,
        PaddedPODArray<UInt64> & /*offsets*/,
        bool /*allow_hyperscan*/,
        size_t /*max_hyperscan_regexp_length*/,
        size_t /*max_hyperscan_regexp_total_length*/,
        bool /*reject_expensive_hyperscan_regexps*/,
        bool force_daachorse,
        size_t input_rows_count)
    {
        // For performance of Volnitsky search, it is crucial to save only one byte for pattern number.
        // When more than 255 patterns are provided, or when force_daachorse is set,
        // use Aho-Corasick (daachorse) which handles thousands of patterns efficiently.
        if (force_daachorse || needles_arr.size() > std::numeric_limits<UInt8>::max())
        {
#if USE_AHO_CORASICK
            vectorConstantAhoCorasick(haystack_data, haystack_offsets, needles_arr, res, input_rows_count);
            return;
#else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Function {} with more than 255 patterns requires Aho-Corasick support which is not available in this build. "
                "Either recompile with Aho-Corasick support enabled or reduce patterns to 255 or fewer.",
                name);
#endif
        }

        VectorWithMemoryTracking<std::string_view> needles;
        needles.reserve(needles_arr.size());
        for (const auto & needle : needles_arr)
            needles.emplace_back(needle.safeGet<String>());

        auto searcher = Impl::createMultiSearcherInBigHaystack(needles);

        res.resize(input_rows_count);

        size_t iteration = 0;
        while (searcher.hasMoreToSearch())
        {
            size_t prev_haystack_offset = 0;
            for (size_t j = 0; j < input_rows_count; ++j)
            {
                const auto * haystack = &haystack_data[prev_haystack_offset];
                const auto * haystack_end = haystack + haystack_offsets[j] - prev_haystack_offset;
                if (iteration == 0 || !res[j])
                    res[j] = searcher.searchOne(haystack, haystack_end);
                prev_haystack_offset = haystack_offsets[j];
            }
            ++iteration;
        }
        if (iteration == 0)
            std::fill(res.begin(), res.end(), 0);
    }

#if USE_AHO_CORASICK
    /// Aho-Corasick based search for large pattern sets (>255 patterns) or when forced.
    /// The compiled automaton is reused across blocks via a server-global, memory-bounded cache.
    static void vectorConstantAhoCorasick(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const Array & needles_arr,
        PaddedPODArray<UInt8> & res,
        size_t input_rows_count)
    {
        res.resize(input_rows_count);

        if (needles_arr.empty())
        {
            std::fill(res.begin(), res.end(), 0);
            return;
        }

        /// An empty needle is a substring of every haystack, so any empty needle means every row
        /// matches — short-circuit without building an automaton.
        for (const auto & needle : needles_arr)
        {
            if (needle.safeGet<String>().empty())
            {
                std::fill(res.begin(), res.end(), 1);
                return;
            }
        }

        constexpr uint8_t case_mode = Impl::case_sensitive
            ? AHO_CORASICK_CASE_SENSITIVE
            : (Impl::is_utf8 ? AHO_CORASICK_CASE_INSENSITIVE_UTF8 : AHO_CORASICK_CASE_INSENSITIVE_ASCII);

        /// Keep the automaton alive for the whole search even if it is evicted concurrently.
        auto automaton = getOrBuildAhoCorasickAutomaton(case_mode, needles_arr);

        aho_corasick_search_batch(
            automaton->handle,
            reinterpret_cast<const uint8_t *>(haystack_data.data()),
            reinterpret_cast<const uint64_t *>(haystack_offsets.data()),
            static_cast<uint64_t>(input_rows_count),
            reinterpret_cast<uint8_t *>(res.data()));
    }
#endif

    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const IColumn & needles_data,
        const ColumnArray::Offsets & needles_offsets,
        PaddedPODArray<ResultType> & res,
        PaddedPODArray<UInt64> & /*offsets*/,
        bool /*allow_hyperscan*/,
        size_t /*max_hyperscan_regexp_length*/,
        size_t /*max_hyperscan_regexp_total_length*/,
        bool /*reject_expensive_hyperscan_regexps*/,
        bool /*force_daachorse*/,
        size_t input_rows_count)
    {
        res.resize(input_rows_count);

        size_t prev_haystack_offset = 0;
        size_t prev_needles_offset = 0;

        const ColumnString & needles_data_string = checkAndGetColumn<ColumnString>(needles_data);

        VectorWithMemoryTracking<std::string_view> needles;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            needles.reserve(needles_offsets[i] - prev_needles_offset);

            for (size_t j = prev_needles_offset; j < needles_offsets[i]; ++j)
                needles.emplace_back(needles_data_string.getDataAt(j));

            const auto * const haystack = &haystack_data[prev_haystack_offset];
            const size_t haystack_length = haystack_offsets[i] - prev_haystack_offset;

            size_t iteration = 0;
            for (const auto & needle : needles)
            {
                auto searcher = Impl::createSearcherInSmallHaystack(needle.data(), needle.size());
                if (iteration == 0 || !res[i])
                {
                    const auto * match = searcher.search(haystack, haystack_length);
                    res[i] = (match != haystack + haystack_length);
                }
                ++iteration;
            }
            if (iteration == 0)
                res[i] = 0;

            prev_haystack_offset = haystack_offsets[i];
            prev_needles_offset = needles_offsets[i];
            needles.clear();
        }
    }
};

}
