#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <benchmark/benchmark.h>

#include <cstddef>

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

static MutableColumnPtr createColumn(size_t rows)
{
    auto column = ColumnUInt128::create();
    auto & data = column->getData();
    data.resize(rows);
    for (size_t i = 0; i < rows; ++i)
        data[i] = i;
    return column;
}

template <FilterPattern pattern>
static void BM_filter(benchmark::State & state)
{
    const size_t rows = state.range(0);
    auto column = createColumn(rows);
    auto filter = createFilter(rows, pattern);

    for ([[maybe_unused]] auto _ : state)
    {
        auto result = column->filter(filter, -1);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * rows);
}

}

BENCHMARK_TEMPLATE(BM_filter, FilterPattern::Clustered)->Arg(1 << 20);
BENCHMARK_TEMPLATE(BM_filter, FilterPattern::Random)->Arg(1 << 20);
BENCHMARK_TEMPLATE(BM_filter, FilterPattern::Alternating)->Arg(1 << 20);
BENCHMARK_TEMPLATE(BM_filter, FilterPattern::DenseWithHole)->Arg(1 << 20);
