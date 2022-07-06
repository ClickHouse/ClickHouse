#pragma once

#include <vector>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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
        size_t /*max_hyperscan_regexp_total_length*/)
    {
        // For performance of Volnitsky search, it is crucial to save only one byte for pattern number.
        if (needles_arr.size() > std::numeric_limits<UInt8>::max())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be at most {}",
                name, std::to_string(needles_arr.size()), std::to_string(std::numeric_limits<UInt8>::max()));

        std::vector<std::string_view> needles;
        needles.reserve(needles_arr.size());
        for (const auto & needle : needles_arr)
            needles.emplace_back(needle.get<String>());

        auto searcher = Impl::createMultiSearcherInBigHaystack(needles);

        const size_t haystack_size = haystack_offsets.size();
        res.resize(haystack_size);

        size_t iteration = 0;
        while (searcher.hasMoreToSearch())
        {
            size_t prev_offset = 0;
            for (size_t j = 0; j < haystack_size; ++j)
            {
                const auto * haystack = &haystack_data[prev_offset];
                const auto * haystack_end = haystack + haystack_offsets[j] - prev_offset - 1;
                if (iteration == 0 || !res[j])
                    res[j] = searcher.searchOne(haystack, haystack_end);
                prev_offset = haystack_offsets[j];
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
        size_t /*max_hyperscan_regexp_total_length*/)
    {
        const size_t haystack_size = haystack_offsets.size();
        res.resize(haystack_size);

        size_t prev_offset = 0;

        const ColumnString * needles_data_string = checkAndGetColumn<ColumnString>(&needles_data);
        const ColumnString::Offsets & needles_data_string_offsets = needles_data_string->getOffsets();
        const ColumnString::Chars & needles_data_string_chars = needles_data_string->getChars();

        std::vector<std::string_view> needles;

        size_t prev_needles_offsets_offset = 0;
        size_t prev_needles_data_offset = 0;

        for (size_t i = 0; i < haystack_size; ++i)
        {
            needles.reserve(needles_offsets[i] - prev_needles_offsets_offset);

            for (size_t j = prev_needles_offsets_offset; j < needles_offsets[i]; ++j)
            {
                const auto * p = reinterpret_cast<const char *>(needles_data_string_chars.data()) + prev_needles_data_offset;
                auto sz = needles_data_string_offsets[j] - prev_needles_data_offset - 1;
                needles.emplace_back(std::string_view(p, sz));
                prev_needles_data_offset = needles_data_string_offsets[j];
            }

            const auto * const haystack = &haystack_data[prev_offset];
            const size_t haystack_length = haystack_offsets[i] - prev_offset - 1;

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

            prev_offset = haystack_offsets[i];
            prev_needles_offsets_offset = needles_offsets[i];
            needles.clear();
        }
    }
};

}
