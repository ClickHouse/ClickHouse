#include <gtest/gtest.h>

#include <Columns/ColumnFixedString.h>

#include <cstddef>
#include <string>

using namespace DB;

namespace
{

static void setRange(IColumn::Filter & filter, size_t begin, size_t end)
{
    for (size_t i = begin; i < end; ++i)
        filter[i] = 1;
}

static IColumn::Filter createClusteredFilter()
{
    IColumn::Filter filter;
    filter.resize_fill(4 * 64 + 7, 0);

    setRange(filter, 4, 12);
    setRange(filter, 20, 23);
    setRange(filter, 32, 33);
    setRange(filter, 40, 56);
    setRange(filter, 63, 64);

    setRange(filter, 64, 65);
    setRange(filter, 68, 72);
    setRange(filter, 80, 82);
    setRange(filter, 96, 97);
    setRange(filter, 108, 124);
    setRange(filter, 127, 128);

    setRange(filter, 128, 192);
    filter[160] = 0;

    setRange(filter, 192, 256);

    filter[256] = 1;
    filter[260] = 1;
    filter[262] = 1;

    return filter;
}

static ColumnFixedString::MutablePtr createColumn(size_t width, size_t rows)
{
    auto column = ColumnFixedString::create(width);
    std::string value(width, '\0');

    for (size_t row = 0; row < rows; ++row)
    {
        for (size_t byte = 0; byte < width; ++byte)
            value[byte] = static_cast<char>((row * 13 + byte) % 251 + 1);
        column->insertData(value.data(), value.size());
    }

    return column;
}

static void expectFiltered(const IColumn & source, const IColumn::Filter & filter, const IColumn & filtered)
{
    size_t expected_size = 0;
    for (UInt8 value : filter)
        expected_size += value != 0;

    ASSERT_EQ(filtered.size(), expected_size);

    size_t filtered_index = 0;
    for (size_t source_index = 0; source_index < filter.size(); ++source_index)
    {
        if (filter[source_index] == 0)
            continue;

        EXPECT_EQ(source.compareAt(source_index, filtered_index, filtered, 0), 0);
        ++filtered_index;
    }
}

static void expectFilterResult(const IColumn::Filter & filter, size_t width)
{
    auto source = createColumn(width, filter.size());

    auto filtered = source->filter(filter, -1);
    expectFiltered(*source, filter, *filtered);

    auto filtered_in_place = source->cloneResized(source->size());
    filtered_in_place->filter(filter);
    expectFiltered(*source, filter, *filtered_in_place);
}

}

TEST(ColumnFixedString, FilterContiguousRuns)
{
    const auto filter = createClusteredFilter();

    for (const size_t width : {1ULL, 8ULL, 17ULL})
        expectFilterResult(filter, width);
}

TEST(ColumnFixedString, FilterEdgeMasks)
{
    for (const size_t width : {1ULL, 16ULL})
    {
        IColumn::Filter empty_filter;
        empty_filter.resize_fill(129, 0);
        expectFilterResult(empty_filter, width);

        IColumn::Filter full_filter;
        full_filter.resize_fill(129, 1);
        expectFilterResult(full_filter, width);

        IColumn::Filter single_filter;
        single_filter.resize_fill(129, 0);
        single_filter[0] = 1;
        single_filter[2] = 1;
        single_filter[63] = 1;
        single_filter[64] = 1;
        single_filter[128] = 1;
        expectFilterResult(single_filter, width);
    }
}
