#include <Columns/ColumnFixedString.h>
#include <Columns/IColumn.h>

#include <benchmark/benchmark.h>

#include <array>
#include <cstddef>
#include <cstdint>

using namespace DB;

namespace
{

enum class FilterPattern
{
    Clustered,
    Random,
    Alternating,
    DenseWithHole,
};

static IColumn::Filter createFilter(size_t rows, FilterPattern pattern)
{
    IColumn::Filter filter;
    filter.resize_fill(rows, 0);

    switch (pattern)
    {
        case FilterPattern::Clustered:
            for (size_t block = 0; block < rows; block += 64)
            {
                for (size_t i = block + 4; i < block + 12 && i < rows; ++i)
                    filter[i] = 1;
                for (size_t i = block + 20; i < block + 36 && i < rows; ++i)
                    filter[i] = 1;
                for (size_t i = block + 48; i < block + 56 && i < rows; ++i)
                    filter[i] = 1;
            }
            break;
        case FilterPattern::Random:
        {
            UInt64 state = 0x9e3779b97f4a7c15ULL;
            for (size_t i = 0; i < rows; ++i)
            {
                state ^= state << 7;
                state ^= state >> 9;
                filter[i] = static_cast<UInt8>(state >> 63);
            }
            break;
        }
        case FilterPattern::Alternating:
            for (size_t i = 0; i < rows; i += 2)
                filter[i] = 1;
            break;
        case FilterPattern::DenseWithHole:
            filter.resize_fill(rows, 1);
            for (size_t block = 0; block < rows; block += 64)
                if (block + 32 < rows)
                    filter[block + 32] = 0;
            break;
    }

    return filter;
}

static ColumnFixedString::MutablePtr createColumn(size_t rows)
{
    constexpr size_t width = 16;
    auto column = ColumnFixedString::create(width);
    std::array<char, width> value{};

    for (size_t row = 0; row < rows; ++row)
    {
        for (size_t byte = 0; byte < width; ++byte)
            value[byte] = static_cast<char>((row + byte) & 0x7f);
        column->insertData(value.data(), value.size());
    }

    return column;
}

template <FilterPattern pattern>
static void BM_filter(benchmark::State & state)
{
    const size_t rows = state.range(0);
    auto column = createColumn(rows);
    auto filter = createFilter(rows, pattern);

    for (auto _ : state)
    {
        auto result = column->filter(filter, -1);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * rows);
}

template <FilterPattern pattern>
static void BM_filterInPlace(benchmark::State & state)
{
    const size_t rows = state.range(0);
    auto source = createColumn(rows);
    auto filter = createFilter(rows, pattern);

    for (auto _ : state)
    {
        state.PauseTiming();
        auto column = source->cloneResized(rows);
        state.ResumeTiming();

        column->filter(filter);
        benchmark::DoNotOptimize(column);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * rows);
}

}

BENCHMARK_TEMPLATE(BM_filter, FilterPattern::Clustered)->Arg(1 << 20);
BENCHMARK_TEMPLATE(BM_filter, FilterPattern::Random)->Arg(1 << 20);
BENCHMARK_TEMPLATE(BM_filter, FilterPattern::Alternating)->Arg(1 << 20);
BENCHMARK_TEMPLATE(BM_filter, FilterPattern::DenseWithHole)->Arg(1 << 20);

BENCHMARK_TEMPLATE(BM_filterInPlace, FilterPattern::Clustered)->Arg(1 << 20);
BENCHMARK_TEMPLATE(BM_filterInPlace, FilterPattern::Random)->Arg(1 << 20);
BENCHMARK_TEMPLATE(BM_filterInPlace, FilterPattern::Alternating)->Arg(1 << 20);
BENCHMARK_TEMPLATE(BM_filterInPlace, FilterPattern::DenseWithHole)->Arg(1 << 20);
