#pragma once

#include <vector>
#include <Columns/ColumnString.h>


namespace DB
{

template <typename Impl>
struct MultiSearchFirstIndexImpl
{
    using ResultType = UInt64;
    static constexpr bool is_using_hyperscan = false;
    /// Variable for understanding, if we used offsets for the output, most
    /// likely to determine whether the function returns ColumnVector of ColumnArray.
    static constexpr bool is_column_array = false;
    static auto getReturnType() { return std::make_shared<DataTypeNumber<ResultType>>(); }

    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const std::vector<StringRef> & needles,
        PaddedPODArray<UInt64> & res,
        [[maybe_unused]] PaddedPODArray<UInt64> & offsets)
    {
        auto searcher = Impl::createMultiSearcherInBigHaystack(needles);
        const size_t haystack_string_size = haystack_offsets.size();
        res.resize(haystack_string_size);
        size_t iteration = 0;
        while (searcher.hasMoreToSearch())
        {
            size_t prev_offset = 0;
            for (size_t j = 0; j < haystack_string_size; ++j)
            {
                const auto * haystack = &haystack_data[prev_offset];
                const auto * haystack_end = haystack + haystack_offsets[j] - prev_offset - 1;
                /// hasMoreToSearch traverse needles in increasing order
                if (iteration == 0 || res[j] == 0)
                    res[j] = searcher.searchOneFirstIndex(haystack, haystack_end);
                prev_offset = haystack_offsets[j];
            }
            ++iteration;
        }
        if (iteration == 0)
            std::fill(res.begin(), res.end(), 0);
    }
};

}
