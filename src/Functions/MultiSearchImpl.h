#pragma once

#include <vector>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Functions/checkMultiSearchAnyAvailability.h>
#include "config.h"

#if USE_AHO_CORASICK
#    include <Functions/MultiSearchAhoCorasickCache.h>
#    include <aho_corasick.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename Name, typename Impl>
struct MultiSearchImpl
{
    using ResultType = UInt8;
    /// Variable for understanding, if we used offsets for the output, most
    /// likely to determine whether the function returns ColumnVector of ColumnArray.
    static constexpr bool is_column_array = false;
    static constexpr bool accepts_force_daachorse = true;
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
        checkMultiSearchAnyAvailability(name, needles_arr.size(), force_daachorse);
#if USE_AHO_CORASICK
        if (multiSearchAnyNeedsAhoCorasick(needles_arr.size(), force_daachorse))
        {
            vectorConstantAhoCorasick(haystack_data, haystack_offsets, needles_arr, res, input_rows_count);
            return;
        }
#endif

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
    /// The compiled automaton is reused across blocks via a process-wide direct-mapped cache.
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

        constexpr MultiSearchCaseMode case_mode = Impl::case_sensitive
            ? MultiSearchCaseMode::Sensitive
            : (Impl::is_utf8 ? MultiSearchCaseMode::InsensitiveUtf8 : MultiSearchCaseMode::InsensitiveAscii);

        /// Keep the automaton alive for the whole search even if it is evicted concurrently.
        auto automaton = getOrBuildAhoCorasickAutomaton(case_mode, needles_arr);

        auto search_batch = [&](const uint8_t * data, const uint64_t * offsets)
        {
            if (aho_corasick_search_batch(
                    automaton->handle,
                    data,
                    offsets,
                    static_cast<uint64_t>(input_rows_count),
                    reinterpret_cast<uint8_t *>(res.data())) != 0)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Aho-Corasick search panicked while evaluating function {}", name);
            }
        };

        if constexpr (case_mode == MultiSearchCaseMode::Sensitive)
        {
            search_batch(
                reinterpret_cast<const uint8_t *>(haystack_data.data()),
                reinterpret_cast<const uint64_t *>(haystack_offsets.data()));
            return;
        }

        /// Fold haystacks the same way the needles were folded, then search the folded copy. Folding
        /// here (not in the Rust crate) keeps case semantics identical to the legacy searcher, whose
        /// exact one-code-point mapping the automaton must reproduce.
        PaddedPODArray<UInt8> folded_data;
        PaddedPODArray<UInt64> folded_offsets(input_rows_count);
        folded_data.reserve(haystack_data.size());

        size_t prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const size_t end_offset = haystack_offsets[i];
            appendFoldedForMultiSearch(
                case_mode,
                reinterpret_cast<const char *>(&haystack_data[prev_offset]),
                end_offset - prev_offset,
                folded_data);
            folded_offsets[i] = folded_data.size();
            prev_offset = end_offset;
        }

        search_batch(
            reinterpret_cast<const uint8_t *>(folded_data.data()),
            reinterpret_cast<const uint64_t *>(folded_offsets.data()));
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
