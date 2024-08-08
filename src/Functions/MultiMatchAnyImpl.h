#pragma once

#include <base/types.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/checkHyperscanRegexp.h>
#include <Functions/Regexps.h>

#include "config.h"

#if USE_VECTORSCAN
#    include <hs.h>
#else
#    include "MatchImpl.h"
    #include <Common/Volnitsky.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int FUNCTION_NOT_ALLOWED;
    extern const int HYPERSCAN_CANNOT_SCAN_TEXT;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_MANY_BYTES;
}

/// For more readable instantiations of MultiMatchAnyImpl<>
struct MultiMatchTraits
{
enum class Find : uint8_t
{
    Any,
    AnyIndex
};
};

template <typename Name, typename ResultType_, MultiMatchTraits::Find Find, bool WithEditDistance>
struct MultiMatchAnyImpl
{
    using ResultType = ResultType_;

    static constexpr bool FindAny = (Find == MultiMatchTraits::Find::Any);
    static constexpr bool FindAnyIndex = (Find == MultiMatchTraits::Find::AnyIndex);

    /// Variable for understanding, if we used offsets for the output, most
    /// likely to determine whether the function returns ColumnVector of ColumnArray.
    static constexpr bool is_column_array = false;
    static constexpr auto name = Name::name;

    static auto getReturnType()
    {
        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const Array & needles_arr,
        PaddedPODArray<ResultType> & res,
        PaddedPODArray<UInt64> & offsets,
        bool allow_hyperscan,
        size_t max_hyperscan_regexp_length,
        size_t max_hyperscan_regexp_total_length,
        bool reject_expensive_hyperscan_regexps,
        size_t input_rows_count)
    {
        vectorConstant(haystack_data, haystack_offsets, needles_arr, res, offsets, std::nullopt, allow_hyperscan, max_hyperscan_regexp_length, max_hyperscan_regexp_total_length, reject_expensive_hyperscan_regexps, input_rows_count);
    }

    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const Array & needles_arr,
        PaddedPODArray<ResultType> & res,
        PaddedPODArray<UInt64> & /*offsets*/,
        [[maybe_unused]] std::optional<UInt32> edit_distance,
        bool allow_hyperscan,
        size_t max_hyperscan_regexp_length,
        size_t max_hyperscan_regexp_total_length,
        bool reject_expensive_hyperscan_regexps,
        size_t input_rows_count)
    {
        if (!allow_hyperscan)
            throw Exception(ErrorCodes::FUNCTION_NOT_ALLOWED, "Hyperscan functions are disabled, because setting 'allow_hyperscan' is set to 0");

        std::vector<std::string_view> needles;
        needles.reserve(needles_arr.size());
        for (const auto & needle : needles_arr)
            needles.emplace_back(needle.get<String>());

        checkHyperscanRegexp(needles, max_hyperscan_regexp_length, max_hyperscan_regexp_total_length);

        if (reject_expensive_hyperscan_regexps)
        {
            SlowWithHyperscanChecker checker;
            for (auto needle : needles)
                if (checker.isSlow(needle))
                    throw Exception(ErrorCodes::HYPERSCAN_CANNOT_SCAN_TEXT, "Regular expression evaluation in vectorscan will be too slow. To ignore this error, disable setting 'reject_expensive_hyperscan_regexps'.");
        }

        res.resize(input_rows_count);

        if (needles_arr.empty())
        {
            std::fill(res.begin(), res.end(), 0);
            return;
        }
#if USE_VECTORSCAN
        MultiRegexps::DeferredConstructedRegexpsPtr deferred_constructed_regexps = MultiRegexps::getOrSet</*SaveIndices*/ FindAnyIndex, WithEditDistance>(needles, edit_distance);
        MultiRegexps::Regexps * regexps = deferred_constructed_regexps->get();

        hs_scratch_t * scratch = nullptr;
        hs_error_t err = hs_clone_scratch(regexps->getScratch(), &scratch);

        if (err != HS_SUCCESS)
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not clone scratch space for vectorscan");

        MultiRegexps::ScratchPtr smart_scratch(scratch);

        auto on_match = []([[maybe_unused]] unsigned int id,
                           unsigned long long /* from */, // NOLINT
                           unsigned long long /* to */, // NOLINT
                           unsigned int /* flags */,
                           void * context) -> int
        {
            if constexpr (FindAnyIndex)
                *reinterpret_cast<ResultType *>(context) = id;
            else if constexpr (FindAny)
                *reinterpret_cast<ResultType *>(context) = 1;
            /// Once we hit the callback, there is no need to search for others.
            return 1;
        };
        UInt64 offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            UInt64 length = haystack_offsets[i] - offset - 1;
            /// vectorscan restriction.
            if (length > std::numeric_limits<UInt32>::max())
                throw Exception(ErrorCodes::TOO_MANY_BYTES, "Too long string to search");
            /// zero the result, scan, check, update the offset.
            res[i] = 0;
            err = hs_scan(
                regexps->getDB(),
                reinterpret_cast<const char *>(haystack_data.data()) + offset,
                static_cast<unsigned>(length),
                0,
                smart_scratch.get(),
                on_match,
                &res[i]);
            if (err != HS_SUCCESS && err != HS_SCAN_TERMINATED)
                throw Exception(ErrorCodes::HYPERSCAN_CANNOT_SCAN_TEXT, "Failed to scan with vectorscan");
            offset = haystack_offsets[i];
        }
#else
        /// fallback if vectorscan is not compiled
        if constexpr (WithEditDistance)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Edit distance multi-search is not implemented when vectorscan is off");
        PaddedPODArray<UInt8> accum(res.size());
        memset(res.data(), 0, res.size() * sizeof(res.front()));
        memset(accum.data(), 0, accum.size());
        for (size_t j = 0; j < needles.size(); ++j)
        {
            MatchImpl<Name, MatchTraits::Syntax::Re2, MatchTraits::Case::Sensitive, MatchTraits::Result::DontNegate>::vectorConstant(haystack_data, haystack_offsets, String(needles[j].data(), needles[j].size()), nullptr, accum, nullptr, input_rows_count);
            for (size_t i = 0; i < res.size(); ++i)
            {
                if constexpr (FindAny)
                    res[i] |= accum[i];
                else if (FindAnyIndex && accum[i])
                    res[i] = j + 1;
            }
        }
#endif // USE_VECTORSCAN
    }

    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const IColumn & needles_data,
        const ColumnArray::Offsets & needles_offsets,
        PaddedPODArray<ResultType> & res,
        PaddedPODArray<UInt64> & offsets,
        bool allow_hyperscan,
        size_t max_hyperscan_regexp_length,
        size_t max_hyperscan_regexp_total_length,
        bool reject_expensive_hyperscan_regexps,
        size_t input_rows_count)
    {
        vectorVector(haystack_data, haystack_offsets, needles_data, needles_offsets, res, offsets, std::nullopt, allow_hyperscan, max_hyperscan_regexp_length, max_hyperscan_regexp_total_length, reject_expensive_hyperscan_regexps, input_rows_count);
    }

    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const IColumn & needles_data,
        const ColumnArray::Offsets & needles_offsets,
        PaddedPODArray<ResultType> & res,
        PaddedPODArray<UInt64> & /*offsets*/,
        std::optional<UInt32> edit_distance,
        bool allow_hyperscan,
        size_t max_hyperscan_regexp_length,
        size_t max_hyperscan_regexp_total_length,
        bool reject_expensive_hyperscan_regexps,
        size_t input_rows_count)
    {
        if (!allow_hyperscan)
            throw Exception(ErrorCodes::FUNCTION_NOT_ALLOWED, "Hyperscan functions are disabled, because setting 'allow_hyperscan' is set to 0");

        res.resize(input_rows_count);
#if USE_VECTORSCAN
        size_t prev_haystack_offset = 0;
        size_t prev_needles_offset = 0;

        const ColumnString & needles_data_string = checkAndGetColumn<ColumnString>(needles_data);

        std::vector<std::string_view> needles;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            needles.reserve(needles_offsets[i] - prev_needles_offset);

            for (size_t j = prev_needles_offset; j < needles_offsets[i]; ++j)
                needles.emplace_back(needles_data_string.getDataAt(j).toView());

            if (needles.empty())
            {
                res[i] = 0;
                prev_haystack_offset = haystack_offsets[i];
                prev_needles_offset = needles_offsets[i];
                continue;
            }

            checkHyperscanRegexp(needles, max_hyperscan_regexp_length, max_hyperscan_regexp_total_length);

            if (reject_expensive_hyperscan_regexps)
            {
                SlowWithHyperscanChecker checker;
                for (auto needle : needles)
                    if (checker.isSlow(needle))
                        throw Exception(ErrorCodes::HYPERSCAN_CANNOT_SCAN_TEXT, "Regular expression evaluation in vectorscan will be too slow. To ignore this error, disable setting 'reject_expensive_hyperscan_regexps'.");
            }

            MultiRegexps::DeferredConstructedRegexpsPtr deferred_constructed_regexps = MultiRegexps::getOrSet</*SaveIndices*/ FindAnyIndex, WithEditDistance>(needles, edit_distance);
            MultiRegexps::Regexps * regexps = deferred_constructed_regexps->get();
            hs_scratch_t * scratch = nullptr;
            hs_error_t err = hs_clone_scratch(regexps->getScratch(), &scratch);

            if (err != HS_SUCCESS)
                throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not clone scratch space for vectorscan");

            MultiRegexps::ScratchPtr smart_scratch(scratch);

            auto on_match = []([[maybe_unused]] unsigned int id,
                               unsigned long long /* from */, // NOLINT
                               unsigned long long /* to */, // NOLINT
                               unsigned int /* flags */,
                               void * context) -> int
            {
                if constexpr (FindAnyIndex)
                    *reinterpret_cast<ResultType *>(context) = id;
                else if constexpr (FindAny)
                    *reinterpret_cast<ResultType *>(context) = 1;
                /// Once we hit the callback, there is no need to search for others.
                return 1;
            };

            const size_t cur_haystack_length = haystack_offsets[i] - prev_haystack_offset - 1;

            /// vectorscan restriction.
            if (cur_haystack_length > std::numeric_limits<UInt32>::max())
                throw Exception(ErrorCodes::TOO_MANY_BYTES, "Too long string to search");

            /// zero the result, scan, check, update the offset.
            res[i] = 0;
            err = hs_scan(
                regexps->getDB(),
                reinterpret_cast<const char *>(haystack_data.data()) + prev_haystack_offset,
                static_cast<unsigned>(cur_haystack_length),
                0,
                smart_scratch.get(),
                on_match,
                &res[i]);
            if (err != HS_SUCCESS && err != HS_SCAN_TERMINATED)
                throw Exception(ErrorCodes::HYPERSCAN_CANNOT_SCAN_TEXT, "Failed to scan with vectorscan");

            prev_haystack_offset = haystack_offsets[i];
            prev_needles_offset = needles_offsets[i];
            needles.clear();
        }
#else
        /// fallback if vectorscan is not compiled
        /// -- the code is copypasted from vectorVector() in MatchImpl.h and quite complex code ... all of it can be removed once vectorscan is
        ///    enabled on all platforms (#38906)
        if constexpr (WithEditDistance)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Edit distance multi-search is not implemented when vectorscan is off");

        (void)edit_distance;

        memset(res.data(), 0, res.size() * sizeof(res.front()));

        size_t prev_haystack_offset = 0;
        size_t prev_needles_offset = 0;

        const ColumnString * needles_data_string = checkAndGetColumn<ColumnString>(&needles_data);

        std::vector<std::string_view> needles;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto * const cur_haystack_data = &haystack_data[prev_haystack_offset];
            const size_t cur_haystack_length = haystack_offsets[i] - prev_haystack_offset - 1;

            needles.reserve(needles_offsets[i] - prev_needles_offset);

            for (size_t j = prev_needles_offset; j < needles_offsets[i]; ++j)
            {
                needles.emplace_back(needles_data_string->getDataAt(j).toView());
            }

            if (needles.empty())
            {
                prev_haystack_offset = haystack_offsets[i];
                prev_needles_offset = needles_offsets[i];
                continue;
            }

            checkHyperscanRegexp(needles, max_hyperscan_regexp_length, max_hyperscan_regexp_total_length);

            if (reject_expensive_hyperscan_regexps)
            {
                for (auto needle : needles)
                {
                    SlowWithHyperscanChecker checker;
                    if (checker.isSlow(needle))
                        throw Exception(ErrorCodes::HYPERSCAN_CANNOT_SCAN_TEXT, "Regular expression evaluation in vectorscan will be too slow. To ignore this error, disable setting 'reject_expensive_hyperscan_regexps'.");
                }
            }

            for (size_t j = 0; j < needles.size(); ++j)
            {
                String needle(needles[j]);

                const auto & regexp = OptimizedRegularExpression(Regexps::createRegexp</*like*/ false, /*no_capture*/ true, /*case_insensitive*/ false>(needle));

                String required_substr;
                bool is_trivial;
                bool required_substring_is_prefix; /// for `anchored` execution of the regexp.

                regexp.getAnalyzeResult(required_substr, is_trivial, required_substring_is_prefix);

                if (required_substr.empty())
                {
                    if (!regexp.getRE2()) /// An empty regexp. Always matches.
                    {
                        if constexpr (FindAny)
                            res[i] |= 1;
                        else if (FindAnyIndex)
                            res[i] = j + 1;
                    }
                    else
                    {
                        const bool match = regexp.getRE2()->Match(
                                {reinterpret_cast<const char *>(cur_haystack_data), cur_haystack_length},
                                0,
                                cur_haystack_length,
                                re2::RE2::UNANCHORED,
                                nullptr,
                                0);
                        if constexpr (FindAny)
                            res[i] |= match;
                        else if (FindAnyIndex && match)
                            res[i] = j + 1;
                    }
                }
                else
                {
                    Volnitsky searcher(required_substr.data(), required_substr.size(), cur_haystack_length);
                    const auto * match = searcher.search(cur_haystack_data, cur_haystack_length);

                    if (match == cur_haystack_data + cur_haystack_length)
                    {
                        /// no match
                    }
                    else
                    {
                        if (is_trivial)
                        {
                            /// no wildcards in pattern
                            if constexpr (FindAny)
                                res[i] |= 1;
                            else if (FindAnyIndex)
                                res[i] = j + 1;
                        }
                        else
                        {
                            const size_t start_pos = (required_substring_is_prefix) ? (match - cur_haystack_data) : 0;
                            const size_t end_pos = cur_haystack_length;

                            const bool match2 = regexp.getRE2()->Match(
                                    {reinterpret_cast<const char *>(cur_haystack_data), cur_haystack_length},
                                    start_pos,
                                    end_pos,
                                    re2::RE2::UNANCHORED,
                                    nullptr,
                                    0);
                            if constexpr (FindAny)
                                res[i] |= match2;
                            else if (FindAnyIndex && match2)
                                res[i] = j + 1;
                            }
                    }
                }
            }

            prev_haystack_offset = haystack_offsets[i];
            prev_needles_offset = needles_offsets[i];
            needles.clear();
        }
#endif // USE_VECTORSCAN
    }
};

}
