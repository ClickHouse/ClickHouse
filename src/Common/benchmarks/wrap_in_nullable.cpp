#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>
#include <Storages/StorageGenerateRandom.h>
#include <benchmark/benchmark.h>

using namespace DB;

static void BM_WrapInNullable1(benchmark::State & state)
{
    pcg64 rng;
    UInt64 limit = DEFAULT_BLOCK_SIZE;
    UInt64 max_array_length = 10;
    UInt64 max_string_length = 10;
    DataTypePtr nullable_float64 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>());

    ColumnPtr arg_column = fillColumnWithRandomData(nullable_float64, limit, max_array_length, max_string_length, rng, nullptr);
    ColumnsWithTypeAndName args{{arg_column, nullable_float64, "arg"}};

    ColumnPtr denull_src_column
        = fillColumnWithRandomData(removeNullable(nullable_float64), limit, max_array_length, max_string_length, rng, nullptr);

    DataTypePtr uint8_type = std::make_shared<DataTypeUInt8>();
    for (auto _ : state)
    {
        state.PauseTiming();
        ColumnPtr null_map_column = fillColumnWithRandomData(uint8_type, limit, max_array_length, max_string_length, rng, nullptr);
        ColumnPtr src_column = ColumnNullable::create(denull_src_column, std::move(null_map_column));
        state.ResumeTiming();

        auto result = wrapInNullable(std::move(src_column), args, nullable_float64, limit);
        benchmark::DoNotOptimize(result);
    }
}

static void BM_WrapInNullable2(benchmark::State & state)
{
    pcg64 rng;
    UInt64 limit = DEFAULT_BLOCK_SIZE;
    UInt64 max_array_length = 10;
    UInt64 max_string_length = 10;

    DataTypePtr nullable_float64 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>());
    ColumnPtr src_column = fillColumnWithRandomData(nullable_float64, limit, max_array_length, max_string_length, rng, nullptr);

    DataTypePtr uint8_type = std::make_shared<DataTypeUInt8>();
    for (auto _ : state)
    {
        state.PauseTiming();
        ColumnPtr null_map_column = fillColumnWithRandomData(uint8_type, limit, max_array_length, max_string_length, rng, nullptr);
        state.ResumeTiming();

        auto result = wrapInNullable(src_column, std::move(null_map_column));
        benchmark::DoNotOptimize(result);
    }
}

BENCHMARK(BM_WrapInNullable1);
BENCHMARK(BM_WrapInNullable2);
