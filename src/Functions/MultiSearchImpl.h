#pragma once

#include <vector>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
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
        size_t input_rows_count)
    {
        // For performance of Volnitsky search, it is crucial to save only one byte for pattern number.
        if (needles_arr.size() > std::numeric_limits<UInt8>::max())
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                "Number of arguments for function {} doesn't match: passed {}, should be at most {}",
                name, needles_arr.size(), std::to_string(std::numeric_limits<UInt8>::max()));

        std::vector<std::string_view> needles;
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
                const auto * haystack_end = haystack + haystack_offsets[j] - prev_haystack_offset - 1;
                if (iteration == 0 || !res[j])
                    res[j] = searcher.searchOne(haystack, haystack_end);
                prev_haystack_offset = haystack_offsets[j];
            }
            ++iteration;
        }
        if (iteration == 0)
            std::fill(res.begin(), res.end(), 0);
    }

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

        std::vector<std::string_view> needles;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            needles.reserve(needles_offsets[i] - prev_needles_offset);

            for (size_t j = prev_needles_offset; j < needles_offsets[i]; ++j)
                needles.emplace_back(needles_data_string.getDataAt(j).toView());

            const auto * const haystack = &haystack_data[prev_haystack_offset];
            const size_t haystack_length = haystack_offsets[i] - prev_haystack_offset - 1;

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
