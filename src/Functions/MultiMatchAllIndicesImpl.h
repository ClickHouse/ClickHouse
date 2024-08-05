#pragma once

#include <base/types.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/checkHyperscanRegexp.h>
#include <Functions/Regexps.h>

#include "config.h"

#if USE_VECTORSCAN
#    include <hs.h>
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


template <typename Name, typename ResultType_, bool WithEditDistance>
struct MultiMatchAllIndicesImpl
{
    using ResultType = ResultType_;

    /// Variable for understanding, if we used offsets for the output, most
    /// likely to determine whether the function returns ColumnVector of ColumnArray.
    static constexpr bool is_column_array = true;
    static constexpr auto name = Name::name;

    static auto getReturnType()
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
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
        PaddedPODArray<UInt64> & offsets,
        std::optional<UInt32> edit_distance,
        bool allow_hyperscan,
        size_t max_hyperscan_regexp_length,
        size_t max_hyperscan_regexp_total_length,
        bool reject_expensive_hyperscan_regexps,
        size_t input_rows_count)
    {
        if (!allow_hyperscan)
            throw Exception(ErrorCodes::FUNCTION_NOT_ALLOWED, "Hyperscan functions are disabled, because setting 'allow_hyperscan' is set to 0");
#if USE_VECTORSCAN
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

        offsets.resize(input_rows_count);

        if (needles_arr.empty())
        {
            std::fill(offsets.begin(), offsets.end(), 0);
            return;
        }

        MultiRegexps::DeferredConstructedRegexpsPtr deferred_constructed_regexps = MultiRegexps::getOrSet</*SaveIndices*/ true, WithEditDistance>(needles, edit_distance);
        MultiRegexps::Regexps * regexps = deferred_constructed_regexps->get();
        hs_scratch_t * scratch = nullptr;
        hs_error_t err = hs_clone_scratch(regexps->getScratch(), &scratch);

        if (err != HS_SUCCESS)
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not clone scratch space for vectorscan");

        MultiRegexps::ScratchPtr smart_scratch(scratch);

        auto on_match = [](unsigned int id,
                           unsigned long long /* from */, // NOLINT
                           unsigned long long /* to */, // NOLINT
                           unsigned int /* flags */,
                           void * context) -> int
        {
            static_cast<PaddedPODArray<ResultType>*>(context)->push_back(id);
            return 0;
        };
        UInt64 offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            UInt64 length = haystack_offsets[i] - offset - 1;
            /// vectorscan restriction.
            if (length > std::numeric_limits<UInt32>::max())
                throw Exception(ErrorCodes::TOO_MANY_BYTES, "Too long string to search");
            /// scan, check, update the offsets array and the offset of haystack.
            err = hs_scan(
                regexps->getDB(),
                reinterpret_cast<const char *>(haystack_data.data()) + offset,
                static_cast<unsigned>(length),
                0,
                smart_scratch.get(),
                on_match,
                &res);
            if (err != HS_SUCCESS)
                throw Exception(ErrorCodes::HYPERSCAN_CANNOT_SCAN_TEXT, "Failed to scan with vectorscan");
            offsets[i] = res.size();
            offset = haystack_offsets[i];
        }
#else
        (void)haystack_data;
        (void)haystack_offsets;
        (void)needles_arr;
        (void)res;
        (void)offsets;
        (void)edit_distance;
        (void)max_hyperscan_regexp_length;
        (void)max_hyperscan_regexp_total_length;
        (void)reject_expensive_hyperscan_regexps;
        (void)input_rows_count;
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "multi-search all indices is not implemented when vectorscan is off");
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
        PaddedPODArray<UInt64> & offsets,
        std::optional<UInt32> edit_distance,
        bool allow_hyperscan,
        size_t max_hyperscan_regexp_length,
        size_t max_hyperscan_regexp_total_length,
        bool reject_expensive_hyperscan_regexps,
        size_t input_rows_count)
    {
        if (!allow_hyperscan)
            throw Exception(ErrorCodes::FUNCTION_NOT_ALLOWED, "Hyperscan functions are disabled, because setting 'allow_hyperscan' is set to 0");
#if USE_VECTORSCAN
        offsets.resize(input_rows_count);
        size_t prev_haystack_offset = 0;
        size_t prev_needles_offset = 0;

        const ColumnString & needles_data_string = checkAndGetColumn<ColumnString>(needles_data);

        std::vector<std::string_view> needles;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            needles.reserve(needles_offsets[i] - prev_needles_offset);

            for (size_t j = prev_needles_offset; j < needles_offsets[i]; ++j)
            {
                needles.emplace_back(needles_data_string.getDataAt(j).toView());
            }

            if (needles.empty())
            {
                offsets[i] = (i == 0) ? 0 : offsets[i-1];
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

            MultiRegexps::DeferredConstructedRegexpsPtr deferred_constructed_regexps = MultiRegexps::getOrSet</*SaveIndices*/ true, WithEditDistance>(needles, edit_distance);
            MultiRegexps::Regexps * regexps = deferred_constructed_regexps->get();
            hs_scratch_t * scratch = nullptr;
            hs_error_t err = hs_clone_scratch(regexps->getScratch(), &scratch);

            if (err != HS_SUCCESS)
                throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not clone scratch space for vectorscan");

            MultiRegexps::ScratchPtr smart_scratch(scratch);

            auto on_match = [](unsigned int id,
                               unsigned long long /* from */, // NOLINT
                               unsigned long long /* to */, // NOLINT
                               unsigned int /* flags */,
                               void * context) -> int
            {
                static_cast<PaddedPODArray<ResultType>*>(context)->push_back(id);
                return 0;
            };

            const size_t cur_haystack_length = haystack_offsets[i] - prev_haystack_offset - 1;

            /// vectorscan restriction.
            if (cur_haystack_length > std::numeric_limits<UInt32>::max())
                throw Exception(ErrorCodes::TOO_MANY_BYTES, "Too long string to search");

            /// scan, check, update the offsets array and the offset of haystack.
            err = hs_scan(
                regexps->getDB(),
                reinterpret_cast<const char *>(haystack_data.data()) + prev_haystack_offset,
                static_cast<unsigned>(cur_haystack_length),
                0,
                smart_scratch.get(),
                on_match,
                &res);
            if (err != HS_SUCCESS)
                throw Exception(ErrorCodes::HYPERSCAN_CANNOT_SCAN_TEXT, "Failed to scan with vectorscan");

            offsets[i] = res.size();

            prev_haystack_offset = haystack_offsets[i];
            prev_needles_offset = needles_offsets[i];
            needles.clear();
        }
#else
        (void)haystack_data;
        (void)haystack_offsets;
        (void)needles_data;
        (void)needles_offsets;
        (void)res;
        (void)offsets;
        (void)edit_distance;
        (void)max_hyperscan_regexp_length;
        (void)max_hyperscan_regexp_total_length;
        (void)reject_expensive_hyperscan_regexps;
        (void)input_rows_count;
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "multi-search all indices is not implemented when vectorscan is off");
#endif // USE_VECTORSCAN
    }
};

}
