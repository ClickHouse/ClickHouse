#pragma once

#include <vector>
#include <Columns/ColumnString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename Name, typename Impl>
struct MultiSearchAllPositionsImpl
{
    using ResultType = UInt64;
    static constexpr auto name = Name::name;

    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const Array & needles_arr,
        PaddedPODArray<UInt64> & vec_res,
        PaddedPODArray<UInt64> & offsets_res)
    {
        if (needles_arr.size() > std::numeric_limits<UInt8>::max())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be at most 255", name, needles_arr.size());

        std::vector<std::string_view> needles;
        needles.reserve(needles_arr.size());
        for (const auto & needle : needles_arr)
            needles.emplace_back(needle.get<String>());

        auto res_callback = [](const UInt8 * start, const UInt8 * end) -> UInt64
        {
            return 1 + Impl::countChars(reinterpret_cast<const char *>(start), reinterpret_cast<const char *>(end));
        };

        auto searcher = Impl::createMultiSearcherInBigHaystack(needles);

        const size_t haystack_size = haystack_offsets.size();
        const size_t needles_size = needles.size();

        vec_res.resize(haystack_size * needles.size());
        offsets_res.resize(haystack_size);

        /// Something can be uninitialized after the search itself
        std::fill(vec_res.begin(), vec_res.end(), 0);

        while (searcher.hasMoreToSearch())
        {
            size_t prev_haystack_offset = 0;
            for (size_t j = 0, from = 0; j < haystack_size; ++j, from += needles_size)
            {
                const auto * haystack = &haystack_data[prev_haystack_offset];
                const auto * haystack_end = haystack + haystack_offsets[j] - prev_haystack_offset - 1;
                searcher.searchOneAll(haystack, haystack_end, vec_res.begin() + from, res_callback);
                prev_haystack_offset = haystack_offsets[j];
            }
        }

        size_t accum = needles_size;
        for (size_t i = 0; i < haystack_size; ++i)
        {
            offsets_res[i] = accum;
            accum += needles_size;
        }
    }

    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const IColumn & needles_data,
        const ColumnArray::Offsets & needles_offsets,
        PaddedPODArray<UInt64> & vec_res,
        PaddedPODArray<UInt64> & offsets_res)
    {
        size_t prev_haystack_offset = 0;
        size_t prev_needles_offset = 0;

        auto res_callback = [](const UInt8 * start, const UInt8 * end) -> UInt64
        {
            return 1 + Impl::countChars(reinterpret_cast<const char *>(start), reinterpret_cast<const char *>(end));
        };

        offsets_res.reserve(haystack_offsets.size());

        const ColumnString * needles_data_string = checkAndGetColumn<ColumnString>(&needles_data);

        std::vector<std::string_view> needles;

        for (size_t i = 0; i < haystack_offsets.size(); ++i)
        {
            needles.reserve(needles_offsets[i] - prev_needles_offset);

            for (size_t j = prev_needles_offset; j < needles_offsets[i]; ++j)
            {
                needles.emplace_back(needles_data_string->getDataAt(j).toView());
            }

            const size_t needles_size = needles.size();

            if (needles_size > std::numeric_limits<UInt8>::max())
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {}, should be at most 255", name, needles_size);

            vec_res.resize(vec_res.size() + needles_size);

            auto searcher = Impl::createMultiSearcherInBigHaystack(needles); /// sub-optimal

            /// Something can be uninitialized after the search itself
            std::fill(vec_res.begin() + vec_res.size() - needles_size, vec_res.end(), 0);

            while (searcher.hasMoreToSearch())
            {
                const auto * haystack = &haystack_data[prev_haystack_offset];
                const auto * haystack_end = haystack + haystack_offsets[i] - prev_haystack_offset - 1;
                searcher.searchOneAll(haystack, haystack_end, vec_res.begin() + vec_res.size() - needles_size, res_callback);
            }

            if (offsets_res.empty())
                offsets_res.push_back(needles_size);
            else
                offsets_res.push_back(offsets_res.back() + needles_size);

            prev_haystack_offset = haystack_offsets[i];
            prev_needles_offset = needles_offsets[i];
            needles.clear();
        }
    }
};

}
