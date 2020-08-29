#pragma once

#include <common/types.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesNumber.h>
#include "Regexps.h"

#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#    include <Common/config.h>
#endif

#if USE_HYPERSCAN
#    include <hs.h>
#else
#    include "MatchImpl.h"
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int HYPERSCAN_CANNOT_SCAN_TEXT;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_MANY_BYTES;
}


template <typename Type, bool FindAny, bool FindAnyIndex, bool MultiSearchDistance>
struct MultiMatchAnyImpl
{
    static_assert(static_cast<int>(FindAny) + static_cast<int>(FindAnyIndex) == 1);
    using ResultType = Type;
    static constexpr bool is_using_hyperscan = true;
    /// Variable for understanding, if we used offsets for the output, most
    /// likely to determine whether the function returns ColumnVector of ColumnArray.
    static constexpr bool is_column_array = false;
    static auto getReturnType()
    {
        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const std::vector<StringRef> & needles,
        PaddedPODArray<Type> & res,
        PaddedPODArray<UInt64> & offsets)
    {
        vectorConstant(haystack_data, haystack_offsets, needles, res, offsets, std::nullopt);
    }

    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const std::vector<StringRef> & needles,
        PaddedPODArray<Type> & res,
        [[maybe_unused]] PaddedPODArray<UInt64> & offsets,
        [[maybe_unused]] std::optional<UInt32> edit_distance)
    {
        (void)FindAny;
        (void)FindAnyIndex;
        res.resize(haystack_offsets.size());
#if USE_HYPERSCAN
        const auto & hyperscan_regex = MultiRegexps::get<FindAnyIndex, MultiSearchDistance>(needles, edit_distance);
        hs_scratch_t * scratch = nullptr;
        hs_error_t err = hs_clone_scratch(hyperscan_regex->getScratch(), &scratch);

        if (err != HS_SUCCESS)
            throw Exception("Could not clone scratch space for hyperscan", ErrorCodes::CANNOT_ALLOCATE_MEMORY);

        MultiRegexps::ScratchPtr smart_scratch(scratch);

        auto on_match = []([[maybe_unused]] unsigned int id,
                           unsigned long long /* from */, // NOLINT
                           unsigned long long /* to */, // NOLINT
                           unsigned int /* flags */,
                           void * context) -> int
        {
            if constexpr (FindAnyIndex)
                *reinterpret_cast<Type *>(context) = id;
            else if constexpr (FindAny)
                *reinterpret_cast<Type *>(context) = 1;
            /// Once we hit the callback, there is no need to search for others.
            return 1;
        };
        const size_t haystack_offsets_size = haystack_offsets.size();
        UInt64 offset = 0;
        for (size_t i = 0; i < haystack_offsets_size; ++i)
        {
            UInt64 length = haystack_offsets[i] - offset - 1;
            /// Hyperscan restriction.
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
                throw Exception("Failed to scan with hyperscan", ErrorCodes::HYPERSCAN_CANNOT_SCAN_TEXT);
            offset = haystack_offsets[i];
        }
#else
        /// Fallback if do not use hyperscan
        if constexpr (MultiSearchDistance)
            throw Exception(
                "Edit distance multi-search is not implemented when hyperscan is off (is it x86 processor?)",
                ErrorCodes::NOT_IMPLEMENTED);
        PaddedPODArray<UInt8> accum(res.size());
        memset(res.data(), 0, res.size() * sizeof(res.front()));
        memset(accum.data(), 0, accum.size());
        for (size_t j = 0; j < needles.size(); ++j)
        {
            MatchImpl<false, false>::vectorConstant(haystack_data, haystack_offsets, needles[j].toString(), nullptr, accum);
            for (size_t i = 0; i < res.size(); ++i)
            {
                if constexpr (FindAny)
                    res[i] |= accum[i];
                else if (FindAnyIndex && accum[i])
                    res[i] = j + 1;
            }
        }
#endif // USE_HYPERSCAN
    }
};

}
