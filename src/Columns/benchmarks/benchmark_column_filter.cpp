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

IColumn::Filter createFilter(size_t rows, FilterPattern pattern)
{
    IColumn::Filter filter;
    const UInt8 initial_value = pattern == FilterPattern::DenseWithHole ? 1 : 0;
    filter.resize_fill(rows, initial_value);

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
        case FilterPattern::Random: {
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
            for (size_t block = 0; block < rows; block += 64)
                if (block + 32 < rows)
                    filter[block + 32] = 0;
            break;
    }

    return filter;
}

template <typename T>
MutableColumnPtr createColumn(size_t rows)
{
    auto column = ColumnVector<T>::create();
    auto & data = column->getData();
    data.resize(rows);
    for (size_t i = 0; i < rows; ++i)
        data[i] = static_cast<T>(i);
    return column;
}

template <typename T, FilterPattern pattern>
void BM_filter(benchmark::State & state)
{
    const size_t rows = state.range(0);
    auto column = createColumn<T>(rows);
    auto filter = createFilter(rows, pattern);

    for ([[maybe_unused]] auto _ : state)
    {
        auto result = column->filter(filter, -1);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * rows);
}

template <typename T, FilterPattern pattern>
void BM_filter_in_place(benchmark::State & state)
{
    const size_t rows = state.range(0);
    auto filter = createFilter(rows, pattern);

    for ([[maybe_unused]] auto _ : state)
    {
        state.PauseTiming();
        auto column = createColumn<T>(rows);
        state.ResumeTiming();

        column->filter(filter);
        benchmark::DoNotOptimize(column->size());

        state.PauseTiming();
        column.reset();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * rows);
}

}

BENCHMARK_TEMPLATE(BM_filter, UInt128, FilterPattern::Clustered)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter, UInt128, FilterPattern::Random)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter, UInt128, FilterPattern::Alternating)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter, UInt128, FilterPattern::DenseWithHole)->Arg(1 << 20)->MinTime(1.0);

BENCHMARK_TEMPLATE(BM_filter_in_place, UInt8, FilterPattern::Clustered)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, UInt8, FilterPattern::Random)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, UInt8, FilterPattern::Alternating)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, UInt8, FilterPattern::DenseWithHole)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, UInt64, FilterPattern::Clustered)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, UInt64, FilterPattern::Random)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, UInt64, FilterPattern::Alternating)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, UInt64, FilterPattern::DenseWithHole)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, UInt128, FilterPattern::Clustered)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, UInt128, FilterPattern::Random)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, UInt128, FilterPattern::Alternating)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, UInt128, FilterPattern::DenseWithHole)->Arg(1 << 20)->MinTime(1.0);
