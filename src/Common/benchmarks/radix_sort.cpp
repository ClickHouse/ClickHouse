#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Storages/StorageGenerateRandom.h>
#include <benchmark/benchmark.h>

using namespace DB;

static void BM_RadixSort_UInt8(benchmark::State & state)
{
    pcg64 rng;
    UInt64 limit = DEFAULT_BLOCK_SIZE;
    auto type = std::make_shared<DataTypeUInt8>();
    auto column = fillColumnWithRandomData(type, limit, 0, 0, rng, nullptr);

    for (auto _ : state)
    {
        IColumn::Permutation res;
        column->getPermutation(IColumn::PermutationSortDirection::Ascending, IColumn::PermutationSortStability::Unstable, 0, 0, res);
        benchmark::DoNotOptimize(res);
    }
}

static void BM_RadixSort_Int16(benchmark::State & state)
{
    pcg64 rng;
    UInt64 limit = DEFAULT_BLOCK_SIZE;
    auto type = std::make_shared<DataTypeInt16>();
    auto column = fillColumnWithRandomData(type, limit, 0, 0, rng, nullptr);

    for (auto _ : state)
    {
        IColumn::Permutation res;
        column->getPermutation(IColumn::PermutationSortDirection::Ascending, IColumn::PermutationSortStability::Unstable, 0, 0, res);
        benchmark::DoNotOptimize(res);
    }
}

static void BM_RadixSort_Int32(benchmark::State & state)
{
    pcg64 rng;
    UInt64 limit = DEFAULT_BLOCK_SIZE;
    auto type = std::make_shared<DataTypeInt32>();
    auto column = fillColumnWithRandomData(type, limit, 0, 0, rng, nullptr);

    for (auto _ : state)
    {
        IColumn::Permutation res;
        column->getPermutation(IColumn::PermutationSortDirection::Ascending, IColumn::PermutationSortStability::Unstable, 0, 0, res);
        benchmark::DoNotOptimize(res);
    }
}

static void BM_RadixSort_UInt64(benchmark::State & state)
{
    pcg64 rng;
    UInt64 limit = DEFAULT_BLOCK_SIZE;
    auto type = std::make_shared<DataTypeUInt64>();
    auto column = fillColumnWithRandomData(type, limit, 0, 0, rng, nullptr);

    for (auto _ : state)
    {
        IColumn::Permutation res;
        column->getPermutation(IColumn::PermutationSortDirection::Ascending, IColumn::PermutationSortStability::Unstable, 0, 0, res);
        benchmark::DoNotOptimize(res);
    }
}

BENCHMARK(BM_RadixSort_UInt8);
BENCHMARK(BM_RadixSort_Int16);
BENCHMARK(BM_RadixSort_Int32);
BENCHMARK(BM_RadixSort_UInt64);
