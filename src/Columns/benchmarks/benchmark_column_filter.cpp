#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <benchmark/benchmark.h>

using namespace DB;

namespace
{

enum class FilterPattern
{
    Clustered,
    Random,
    SparseRandom,
    DenseRandom,
    FourRuns,
    FiveRuns,
    Alternating,
    DenseWithHole,
    ShortRuns,
};

IColumn::Filter createFilter(size_t rows, FilterPattern pattern)
{
    IColumn::Filter filter;
    filter.resize_fill(rows, pattern == FilterPattern::DenseWithHole);

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
        case FilterPattern::SparseRandom:
        case FilterPattern::DenseRandom:
        {
            UInt64 state = 0x9e3779b97f4a7c15ULL;
            for (size_t i = 0; i < rows; ++i)
            {
                state ^= state << 7;
                state ^= state >> 9;

                if (pattern == FilterPattern::Random)
                    filter[i] = static_cast<UInt8>(state >> 63);
                else if (pattern == FilterPattern::SparseRandom)
                    filter[i] = static_cast<UInt8>(state % 10 == 0);
                else
                    filter[i] = static_cast<UInt8>(state % 10 != 0);
            }
            break;
        }

        case FilterPattern::FourRuns:
        case FilterPattern::FiveRuns:
        {
            const size_t runs = pattern == FilterPattern::FourRuns ? 4 : 5;
            for (size_t i = 0; i < rows; ++i)
            {
                const size_t offset = i % 64;
                filter[i] = static_cast<UInt8>(offset / 12 < runs && offset % 12 < 8);
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

        case FilterPattern::ShortRuns:
            for (size_t i = 0; i < rows; ++i)
                filter[i] = (i % 14 < 3) || (i % 14 >= 7 && i % 14 < 11);
            break;
    }

    return filter;
}

MutableColumnPtr createColumn(size_t rows)
{
    auto column = ColumnVector<UInt128>::create();
    auto & data = column->getData();
    data.resize(rows);
    for (size_t i = 0; i < rows; ++i)
        data[i] = static_cast<UInt128>(i);
    return column;
}

template <FilterPattern pattern>
void BM_filter(benchmark::State & state)
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

template <FilterPattern pattern>
void BM_filter_in_place(benchmark::State & state)
{
    const size_t rows = state.range(0);
    auto filter = createFilter(rows, pattern);

    for ([[maybe_unused]] auto _ : state)
    {
        state.PauseTiming();
        auto column = createColumn(rows);
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

BENCHMARK_TEMPLATE(BM_filter, FilterPattern::Clustered)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter, FilterPattern::Random)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter, FilterPattern::SparseRandom)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter, FilterPattern::DenseRandom)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter, FilterPattern::FourRuns)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter, FilterPattern::FiveRuns)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter, FilterPattern::Alternating)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter, FilterPattern::DenseWithHole)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter, FilterPattern::ShortRuns)->Arg(1 << 20)->MinTime(1.0);

BENCHMARK_TEMPLATE(BM_filter_in_place, FilterPattern::Clustered)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, FilterPattern::Random)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, FilterPattern::SparseRandom)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, FilterPattern::DenseRandom)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, FilterPattern::FourRuns)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, FilterPattern::FiveRuns)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, FilterPattern::Alternating)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, FilterPattern::DenseWithHole)->Arg(1 << 20)->MinTime(1.0);
BENCHMARK_TEMPLATE(BM_filter_in_place, FilterPattern::ShortRuns)->Arg(1 << 20)->MinTime(1.0);
