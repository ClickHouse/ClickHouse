#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Storages/StorageGenerateRandom.h>
#include <benchmark/benchmark.h>
#include "pcg_random.hpp"

using namespace DB;

static void BM_RadixSort1(benchmark::State & state)
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

BENCHMARK(BM_RadixSort1);
