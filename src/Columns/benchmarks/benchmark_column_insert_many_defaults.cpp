#include <cstddef>

#include <Columns/IColumn.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <benchmark/benchmark.h>

using namespace DB;

static constexpr size_t ROWS = 65536;

template <const std::string & str_type>
static void BM_insertManyDefaults(benchmark::State & state)
{
    const auto type = DataTypeFactory::instance().get(str_type);
    const size_t length = state.range(0);
    size_t allocated_bytes = 0;

    for ([[maybe_unused]] auto _ : state)
    {
        state.PauseTiming();
        auto column = type->createColumn();
        state.ResumeTiming();

        column->insertManyDefaults(length);
        allocated_bytes = column->allocatedBytes();
        benchmark::DoNotOptimize(column);
    }

    state.SetItemsProcessed(state.iterations() * length);
    state.counters["allocated_bytes"] = static_cast<double>(allocated_bytes);
}

template <const std::string & str_type>
static void BM_insertManyDefaultsOneByOne(benchmark::State & state)
{
    const auto type = DataTypeFactory::instance().get(str_type);
    const size_t length = state.range(0);
    size_t allocated_bytes = 0;

    for ([[maybe_unused]] auto _ : state)
    {
        state.PauseTiming();
        auto column = type->createColumn();
        state.ResumeTiming();

        for (size_t i = 0; i < length; ++i)
            column->insertManyDefaults(1);
        allocated_bytes = column->allocatedBytes();
        benchmark::DoNotOptimize(column);
    }

    state.SetItemsProcessed(state.iterations() * length);
    state.counters["allocated_bytes"] = static_cast<double>(allocated_bytes);
}

static const String type_map_uint64 = "Map(UInt64, UInt64)";
static const String type_map_uint8 = "Map(UInt8, UInt8)";
static const String type_map_string = "Map(String, String)";
static const String type_map_wide = "Map(UInt64, FixedString(256))";

#define REGISTER_MAP_DEFAULT_BENCHMARKS(type) \
    BENCHMARK_TEMPLATE(BM_insertManyDefaults, type) \
        ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(4096) \
        ->Arg(DEFAULT_BLOCK_SIZE)->Arg(65520)->Arg(65521)->Arg(ROWS); \
    BENCHMARK_TEMPLATE(BM_insertManyDefaultsOneByOne, type) \
        ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(4096)

REGISTER_MAP_DEFAULT_BENCHMARKS(type_map_uint64);
REGISTER_MAP_DEFAULT_BENCHMARKS(type_map_uint8);
REGISTER_MAP_DEFAULT_BENCHMARKS(type_map_string);
REGISTER_MAP_DEFAULT_BENCHMARKS(type_map_wide);

#undef REGISTER_MAP_DEFAULT_BENCHMARKS
