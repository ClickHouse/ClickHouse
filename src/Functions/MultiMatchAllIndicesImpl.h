#pragma once

#include <base/types.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include "Regexps.h"

#include "config_functions.h"
#include <Common/config.h>

#if USE_HYPERSCAN
#    include <hs.h>
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


template <typename Name, typename Type, bool MultiSearchDistance>
struct MultiMatchAllIndicesImpl
{
    using ResultType = Type;
    static constexpr bool is_using_hyperscan = true;
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
        PaddedPODArray<UInt64> & offsets,
        [[maybe_unused]] std::optional<UInt32> edit_distance)
    {
        offsets.resize(haystack_offsets.size());
#if USE_HYPERSCAN
        const auto & hyperscan_regex = MultiRegexps::get</*SaveIndices=*/true, MultiSearchDistance>(needles, edit_distance);
        hs_scratch_t * scratch = nullptr;
        hs_error_t err = hs_clone_scratch(hyperscan_regex->getScratch(), &scratch);

        if (err != HS_SUCCESS)
            throw Exception("Could not clone scratch space for hyperscan", ErrorCodes::CANNOT_ALLOCATE_MEMORY);

        MultiRegexps::ScratchPtr smart_scratch(scratch);

        auto on_match = [](unsigned int id,
                           unsigned long long /* from */, // NOLINT
                           unsigned long long /* to */, // NOLINT
                           unsigned int /* flags */,
                           void * context) -> int
        {
            static_cast<PaddedPODArray<Type>*>(context)->push_back(id);
            return 0;
        };
        const size_t haystack_offsets_size = haystack_offsets.size();
        UInt64 offset = 0;
        for (size_t i = 0; i < haystack_offsets_size; ++i)
        {
            UInt64 length = haystack_offsets[i] - offset - 1;
            /// Hyperscan restriction.
            if (length > std::numeric_limits<UInt32>::max())
                throw Exception("Too long string to search", ErrorCodes::TOO_MANY_BYTES);
            /// Scan, check, update the offsets array and the offset of haystack.
            err = hs_scan(
                hyperscan_regex->getDB(),
                reinterpret_cast<const char *>(haystack_data.data()) + offset,
                length,
                0,
                smart_scratch.get(),
                on_match,
                &res);
            if (err != HS_SUCCESS)
                throw Exception("Failed to scan with hyperscan", ErrorCodes::HYPERSCAN_CANNOT_SCAN_TEXT);
            offsets[i] = res.size();
            offset = haystack_offsets[i];
        }
#else
        (void)haystack_data;
        (void)haystack_offsets;
        (void)needles;
        (void)res;
        (void)offsets;
        throw Exception(
            "multi-search all indices is not implemented when hyperscan is off (is it x86 processor?)",
            ErrorCodes::NOT_IMPLEMENTED);
#endif // USE_HYPERSCAN
    }
};

}
