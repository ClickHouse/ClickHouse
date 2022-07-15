#pragma once

#include <base/types.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/checkHyperscanRegexp.h>
#include "Regexps.h"

#include "config_functions.h"
#include <Common/config.h>

#if USE_VECTORSCAN
#    include <hs.h>
#else
#    include "MatchImpl.h"
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

// For more readable instantiations of MultiMatchAnyImpl<>
struct MultiMatchTraits
{
enum class Find
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
        size_t max_hyperscan_regexp_total_length)
    {
        vectorConstant(haystack_data, haystack_offsets, needles_arr, res, offsets, std::nullopt, allow_hyperscan, max_hyperscan_regexp_length, max_hyperscan_regexp_total_length);
    }

    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const Array & needles_arr,
        PaddedPODArray<ResultType> & res,
        [[maybe_unused]] PaddedPODArray<UInt64> & offsets,
        [[maybe_unused]] std::optional<UInt32> edit_distance,
        bool allow_hyperscan,
        size_t max_hyperscan_regexp_length,
        size_t max_hyperscan_regexp_total_length)
    {
        if (!allow_hyperscan)
            throw Exception(ErrorCodes::FUNCTION_NOT_ALLOWED, "Hyperscan functions are disabled, because setting 'allow_hyperscan' is set to 0");

        std::vector<std::string_view> needles;
        needles.reserve(needles_arr.size());
        for (const auto & needle : needles_arr)
            needles.emplace_back(needle.get<String>());

        checkHyperscanRegexp(needles, max_hyperscan_regexp_length, max_hyperscan_regexp_total_length);

        res.resize(haystack_offsets.size());
#if USE_VECTORSCAN
        const auto & hyperscan_regex = MultiRegexps::get<FindAnyIndex, WithEditDistance>(needles, edit_distance);
        hs_scratch_t * scratch = nullptr;
        hs_error_t err = hs_clone_scratch(hyperscan_regex->getScratch(), &scratch);

        if (err != HS_SUCCESS)
            throw Exception("Could not clone scratch space for vectorscan", ErrorCodes::CANNOT_ALLOCATE_MEMORY);

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
        const size_t haystack_offsets_size = haystack_offsets.size();
        UInt64 offset = 0;
        for (size_t i = 0; i < haystack_offsets_size; ++i)
        {
            UInt64 length = haystack_offsets[i] - offset - 1;
            /// Vectorscan restriction.
            if (length > std::numeric_limits<UInt32>::max())
                throw Exception("Too long string to search", ErrorCodes::TOO_MANY_BYTES);
            /// Zero the result, scan, check, update the offset.
            res[i] = 0;
            err = hs_scan(
                hyperscan_regex->getDB(),
                reinterpret_cast<const char *>(haystack_data.data()) + offset,
                length,
                0,
                smart_scratch.get(),
                on_match,
                &res[i]);
            if (err != HS_SUCCESS && err != HS_SCAN_TERMINATED)
                throw Exception("Failed to scan with vectorscan", ErrorCodes::HYPERSCAN_CANNOT_SCAN_TEXT);
            offset = haystack_offsets[i];
        }
#else
        // fallback if vectorscan is not compiled
        if constexpr (WithEditDistance)
            throw Exception(
                "Edit distance multi-search is not implemented when vectorscan is off",
                ErrorCodes::NOT_IMPLEMENTED);
        PaddedPODArray<UInt8> accum(res.size());
        memset(res.data(), 0, res.size() * sizeof(res.front()));
        memset(accum.data(), 0, accum.size());
        for (size_t j = 0; j < needles.size(); ++j)
        {
            MatchImpl<Name, MatchTraits::Syntax::Re2, MatchTraits::Case::Sensitive, MatchTraits::Result::DontNegate>::vectorConstant(haystack_data, haystack_offsets, std::string(needles[j].data(), needles[j].size()), nullptr, accum);
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
};

}
