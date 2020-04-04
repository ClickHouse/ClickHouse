#pragma once

#include <vector>
#include <Columns/ColumnString.h>


namespace DB
{

template <typename Impl>
struct MultiSearchAllPositionsImpl
{
    using ResultType = UInt64;

    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const std::vector<StringRef> & needles,
        PaddedPODArray<UInt64> & res)
    {
        auto res_callback = [](const UInt8 * start, const UInt8 * end) -> UInt64
        {
            return 1 + Impl::countChars(reinterpret_cast<const char *>(start), reinterpret_cast<const char *>(end));
        };

        auto searcher = Impl::createMultiSearcherInBigHaystack(needles);

        const size_t haystack_string_size = haystack_offsets.size();
        const size_t needles_size = needles.size();

        /// Something can be uninitialized after the search itself
        std::fill(res.begin(), res.end(), 0);

        while (searcher.hasMoreToSearch())
        {
            size_t prev_offset = 0;
            for (size_t j = 0, from = 0; j < haystack_string_size; ++j, from += needles_size)
            {
                const auto * haystack = &haystack_data[prev_offset];
                const auto * haystack_end = haystack + haystack_offsets[j] - prev_offset - 1;
                searcher.searchOneAll(haystack, haystack_end, res.data() + from, res_callback);
                prev_offset = haystack_offsets[j];
            }
        }
    }
};

}
