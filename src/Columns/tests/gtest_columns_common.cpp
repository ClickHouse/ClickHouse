#include <gtest/gtest.h>

#include <Columns/ColumnsCommon.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace DB
{
namespace
{

UInt8 valueForPattern(size_t pattern, size_t index)
{
    switch (pattern)
    {
        case 0: return 0;
        case 1: return 1;
        case 2: return static_cast<UInt8>(index % 2);
        case 3: return index % 7 == 0 ? 1 : 0;
        case 4: return index % 11 == 0 ? 0x80 : 0;
        case 5: return static_cast<UInt8>((index * 37 + 1) | 0x80);
        default: UNREACHABLE();
    }
}

size_t countNonZero(const UInt8 * data, size_t start, size_t end)
{
    size_t result = 0;
    for (size_t i = start; i < end; ++i)
        result += data[i] != 0;
    return result;
}

size_t countNonNull(const IColumn::Filter & filter, const std::vector<UInt8> & null_map, size_t start, size_t end)
{
    size_t result = 0;
    for (size_t i = start; i < end; ++i)
        result += filter[i] != 0 && null_map[i] == 0;
    return result;
}

}

TEST(ColumnsCommon, CountBytesInFilter)
{
    constexpr std::array sizes{0uz, 1uz, 15uz, 16uz, 17uz, 31uz, 32uz, 33uz, 63uz, 64uz, 65uz, 127uz, 128uz, 129uz, 193uz};
    constexpr std::array lengths{0uz, 1uz, 15uz, 16uz, 31uz, 32uz, 63uz, 64uz, 65uz, 127uz, 128uz, 129uz};

    for (size_t size : sizes)
    {
        IColumn::Filter filter(size);
        std::vector<UInt8> null_map(size);

        for (size_t filter_pattern = 0; filter_pattern < 6; ++filter_pattern)
        {
            for (size_t i = 0; i < size; ++i)
            {
                filter[i] = valueForPattern(filter_pattern, i);
                null_map[i] = valueForPattern((filter_pattern + 2) % 4, i);
            }

            EXPECT_EQ(countNonZero(filter.data(), 0, size), countBytesInFilter(filter));

            for (size_t start = 0; start <= size; ++start)
            {
                for (size_t length : lengths)
                {
                    if (start + length > size)
                        continue;

                    const size_t end = start + length;
                    EXPECT_EQ(countNonZero(filter.data(), start, end), countBytesInFilter(filter.data(), start, end))
                        << "size=" << size << ", pattern=" << filter_pattern << ", start=" << start << ", end=" << end;
                    EXPECT_EQ(countNonNull(filter, null_map, start, end), countBytesInFilterWithNull(filter, null_map.data(), start, end))
                        << "size=" << size << ", pattern=" << filter_pattern << ", start=" << start << ", end=" << end;
                }
            }
        }
    }
}

}
