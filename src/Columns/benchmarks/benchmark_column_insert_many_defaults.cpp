#include <cstddef>

#include <Columns/IColumn.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <benchmark/benchmark.h>

using namespace DB;

static constexpr size_t ROWS = 65536;

/// Keep the inherited implementation available as a same-binary baseline.
/// Qualifying the call deliberately bypasses ColumnMap's override.
template <bool generic_defaults>
static void insertDefaults(IColumn & column, size_t length)
{
    if constexpr (generic_defaults)
        column.IColumn::insertManyDefaults(length);
    else
        column.insertManyDefaults(length);
}

template <const std::string & str_type, bool generic_defaults>
static void BM_insertManyDefaults(benchmark::State & state)
{
    const auto type = DataTypeFactory::instance().get(str_type);
    const size_t length = state.range(0);
    size_t retained_allocated_bytes = 0;

    for ([[maybe_unused]] auto _ : state)
    {
        state.PauseTiming();
        auto column = type->createColumn();
        state.ResumeTiming();

        insertDefaults<generic_defaults>(*column, length);
        benchmark::DoNotOptimize(column->size());
        benchmark::ClobberMemory();

        state.PauseTiming();
        retained_allocated_bytes = column->allocatedBytes();
        column.reset();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * length);
    // allocatedBytes() reports retained column capacity, not peak process RSS.
    state.counters["retained_allocated_bytes"] = static_cast<double>(retained_allocated_bytes);
}

template <const std::string & str_type, bool generic_defaults>
static void BM_insertManyDefaultsOneByOne(benchmark::State & state)
{
    const auto type = DataTypeFactory::instance().get(str_type);
    const size_t length = state.range(0);
    size_t retained_allocated_bytes = 0;

    for ([[maybe_unused]] auto _ : state)
    {
        state.PauseTiming();
        auto column = type->createColumn();
        state.ResumeTiming();

        for (size_t i = 0; i < length; ++i)
            insertDefaults<generic_defaults>(*column, 1);
        benchmark::DoNotOptimize(column->size());
        benchmark::ClobberMemory();

        state.PauseTiming();
        retained_allocated_bytes = column->allocatedBytes();
        column.reset();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * length);
    state.counters["retained_allocated_bytes"] = static_cast<double>(retained_allocated_bytes);
}

template <const std::string & str_type, bool generic_defaults>
static void BM_insertManyDefaultsBatches(benchmark::State & state)
{
    const auto type = DataTypeFactory::instance().get(str_type);
    const size_t batch_size = state.range(0);
    size_t retained_allocated_bytes = 0;

    for ([[maybe_unused]] auto _ : state)
    {
        state.PauseTiming();
        auto column = type->createColumn();
        state.ResumeTiming();

        /// Hold the total row count fixed while crossing allocation boundaries
        /// on an existing column. Do not reserve nested Map storage in setup.
        for (size_t inserted = 0; inserted < ROWS; inserted += batch_size)
            insertDefaults<generic_defaults>(*column, batch_size);
        benchmark::DoNotOptimize(column->size());
        benchmark::ClobberMemory();

        state.PauseTiming();
        retained_allocated_bytes = column->allocatedBytes();
        column.reset();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * ROWS);
    state.counters["retained_allocated_bytes"] = static_cast<double>(retained_allocated_bytes);
}

static const String type_map_uint64 = "Map(UInt64, UInt64)";
static const String type_map_uint8 = "Map(UInt8, UInt8)";
static const String type_map_string = "Map(String, String)";
static const String type_map_wide = "Map(UInt64, FixedString(256))";

#define REGISTER_MAP_DEFAULT_BENCHMARKS(type, generic_defaults) \
    BENCHMARK_TEMPLATE(BM_insertManyDefaults, type, generic_defaults) \
        ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(4096) \
        ->Arg(DEFAULT_BLOCK_SIZE)->Arg(65520)->Arg(65521)->Arg(ROWS); \
    BENCHMARK_TEMPLATE(BM_insertManyDefaultsOneByOne, type, generic_defaults) \
        ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(64)->Arg(256)->Arg(4096); \
    BENCHMARK_TEMPLATE(BM_insertManyDefaultsBatches, type, generic_defaults) \
        ->Arg(16)->Arg(256)->Arg(4096)

REGISTER_MAP_DEFAULT_BENCHMARKS(type_map_uint64, false);
REGISTER_MAP_DEFAULT_BENCHMARKS(type_map_uint64, true);
REGISTER_MAP_DEFAULT_BENCHMARKS(type_map_uint8, false);
REGISTER_MAP_DEFAULT_BENCHMARKS(type_map_uint8, true);
REGISTER_MAP_DEFAULT_BENCHMARKS(type_map_string, false);
REGISTER_MAP_DEFAULT_BENCHMARKS(type_map_string, true);
REGISTER_MAP_DEFAULT_BENCHMARKS(type_map_wide, false);
REGISTER_MAP_DEFAULT_BENCHMARKS(type_map_wide, true);

#undef REGISTER_MAP_DEFAULT_BENCHMARKS
