#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionUniqCombined.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/StatisticsDescription.h>
#include <base/defines.h>

#include <benchmark/benchmark.h>

#include "config.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

constexpr size_t benchmark_rows = 65536;

AggregateFunctionPtr
createBenchmarkUniqCombined64(const String &, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Benchmark uniqCombined64 requires exactly one argument, got {}", argument_types.size());

    if (parameters.size() != 1 || parameters[0].getType() != Field::Types::UInt64 || parameters[0].safeGet<UInt64>() != 12)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Benchmark uniqCombined64 requires precision 12");

    const WhichDataType which(argument_types[0]);
    /// Keep precision 12 and the UInt64 hash type in sync with StatisticsUniqV2.
    if (which.isUInt64())
        return std::make_shared<AggregateFunctionUniqCombined<UInt64, ColumnVector<UInt64>, 12, UInt64>>(argument_types, parameters);
    if (which.isFloat64())
        return std::make_shared<AggregateFunctionUniqCombined<Float64, ColumnVector<Float64>, 12, UInt64>>(argument_types, parameters);

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Benchmark uniqCombined64 supports only UInt64 and Float64, got {}",
        argument_types[0]->getName());
}

enum class ColumnKind
{
    UInt64 = 0,
    Float64 = 1,
    String = 2,
    NullableUInt64 = 3,
};

DataTypePtr makeDataType(ColumnKind kind)
{
    switch (kind)
    {
        case ColumnKind::UInt64: return std::make_shared<DataTypeUInt64>();
        case ColumnKind::Float64: return std::make_shared<DataTypeFloat64>();
        case ColumnKind::String: return std::make_shared<DataTypeString>();
        case ColumnKind::NullableUInt64: return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    }

    UNREACHABLE();
}

ColumnPtr makeColumn(ColumnKind kind, size_t rows)
{
    switch (kind)
    {
        case ColumnKind::UInt64: {
            auto column = ColumnUInt64::create(rows);
            auto & data = column->getData();
            for (size_t row = 0; row < rows; ++row)
                data[row] = static_cast<UInt64>(row * 13 + row % 17);
            return column;
        }
        case ColumnKind::Float64: {
            auto column = ColumnFloat64::create(rows);
            auto & data = column->getData();
            for (size_t row = 0; row < rows; ++row)
                data[row] = static_cast<Float64>(row * 13 + row % 17) / 10.0;
            return column;
        }
        case ColumnKind::String: {
            auto column = ColumnString::create();
            column->reserve(rows);
            for (size_t row = 0; row < rows; ++row)
            {
                const std::string value = "statistics_serialization_" + std::to_string(row % 1000);
                column->insertData(value.data(), value.size());
            }
            return column;
        }
        case ColumnKind::NullableUInt64: {
            auto nested = ColumnUInt64::create(rows);
            auto & nested_data = nested->getData();
            auto null_map = ColumnUInt8::create(rows);
            auto & null_map_data = null_map->getData();

            for (size_t row = 0; row < rows; ++row)
            {
                nested_data[row] = static_cast<UInt64>(row * 13 + row % 17);
                null_map_data[row] = row % 10 == 0;
            }

            return ColumnNullable::create(std::move(nested), std::move(null_map));
        }
    }

    UNREACHABLE();
}

void ensureAggregateFunctionsRegistered()
{
    static const bool registered = []
    {
        AggregateFunctionFactory::instance().registerFunction(
            "uniqCombined64",
            {createBenchmarkUniqCombined64,
             {.description = "Benchmark-local uniqCombined64 registration.",
              .category = FunctionDocumentation::Category::AggregateFunction}});
        return true;
    }();
    static_cast<void>(registered);
}

ColumnStatisticsPtr makeStatistics(const std::vector<StatisticsType> & types, const DataTypePtr & data_type)
{
    ColumnStatisticsDescription description;
    description.data_type = data_type;
    for (auto type : types)
        description.types_to_desc.emplace(type, SingleStatisticsDescription(type, nullptr, false));
    return MergeTreeStatisticsFactory::instance().get(description);
}

ColumnStatisticsPtr buildStatistics(const std::vector<StatisticsType> & types, ColumnKind kind, size_t rows)
{
    ensureAggregateFunctionsRegistered();

    auto data_type = makeDataType(kind);
    auto column = makeColumn(kind, rows);
    auto statistics = makeStatistics(types, data_type);
    statistics->build(column);
    return statistics;
}

size_t serializedSize(const ColumnStatisticsPtr & statistics)
{
    String data;
    WriteBufferFromString buffer(data);
    statistics->serialize(buffer);
    buffer.finalize();
    return data.size();
}

void benchmarkSerialize(benchmark::State & state, const std::vector<StatisticsType> & types, ColumnKind kind)
{
    auto statistics = buildStatistics(types, kind, benchmark_rows);
    const size_t bytes = serializedSize(statistics);

    String data;
    data.reserve(bytes);

    for (auto _ [[maybe_unused]] : state)
    {
        data.clear();
        WriteBufferFromString buffer(data);
        statistics->serialize(buffer);
        buffer.finalize();
        benchmark::DoNotOptimize(data.data());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(bytes));
}

void benchmarkBuild(benchmark::State & state, const std::vector<StatisticsType> & types, ColumnKind kind)
{
    ensureAggregateFunctionsRegistered();

    auto data_type = makeDataType(kind);
    auto column = makeColumn(kind, benchmark_rows);
    const auto column_bytes = column->byteSize();

    for (auto _ [[maybe_unused]] : state)
    {
        auto statistics = makeStatistics(types, data_type);
        statistics->build(column);
        benchmark::DoNotOptimize(statistics->getNumRows());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(benchmark_rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(column_bytes));
}

}

static void BM_StatisticsSerializeBasicUInt64(benchmark::State & state)
{
    benchmarkSerialize(state, {StatisticsType::Basic}, ColumnKind::UInt64);
}

static void BM_StatisticsBuildBasicUInt64(benchmark::State & state)
{
    benchmarkBuild(state, {StatisticsType::Basic}, ColumnKind::UInt64);
}

static void BM_StatisticsSerializeMinMaxUInt64(benchmark::State & state)
{
    benchmarkSerialize(state, {StatisticsType::MinMax}, ColumnKind::UInt64);
}

static void BM_StatisticsBuildMinMaxUInt64(benchmark::State & state)
{
    benchmarkBuild(state, {StatisticsType::MinMax}, ColumnKind::UInt64);
}

static void BM_StatisticsSerializeBasicFloat64(benchmark::State & state)
{
    benchmarkSerialize(state, {StatisticsType::Basic}, ColumnKind::Float64);
}

static void BM_StatisticsBuildBasicFloat64(benchmark::State & state)
{
    benchmarkBuild(state, {StatisticsType::Basic}, ColumnKind::Float64);
}

static void BM_StatisticsSerializeMinMaxFloat64(benchmark::State & state)
{
    benchmarkSerialize(state, {StatisticsType::MinMax}, ColumnKind::Float64);
}

static void BM_StatisticsBuildMinMaxFloat64(benchmark::State & state)
{
    benchmarkBuild(state, {StatisticsType::MinMax}, ColumnKind::Float64);
}

static void BM_StatisticsSerializeUniqV2UInt64(benchmark::State & state)
{
    benchmarkSerialize(state, {StatisticsType::UniqV2}, ColumnKind::UInt64);
}

static void BM_StatisticsBuildUniqV2UInt64(benchmark::State & state)
{
    benchmarkBuild(state, {StatisticsType::UniqV2}, ColumnKind::UInt64);
}

static void BM_StatisticsSerializeUniqV2Float64(benchmark::State & state)
{
    benchmarkSerialize(state, {StatisticsType::UniqV2}, ColumnKind::Float64);
}

static void BM_StatisticsBuildUniqV2Float64(benchmark::State & state)
{
    benchmarkBuild(state, {StatisticsType::UniqV2}, ColumnKind::Float64);
}

static void BM_StatisticsSerializeBasicString(benchmark::State & state)
{
    benchmarkSerialize(state, {StatisticsType::Basic}, ColumnKind::String);
}

static void BM_StatisticsBuildBasicString(benchmark::State & state)
{
    benchmarkBuild(state, {StatisticsType::Basic}, ColumnKind::String);
}

static void BM_StatisticsSerializeBasicNullableUInt64(benchmark::State & state)
{
    benchmarkSerialize(state, {StatisticsType::Basic}, ColumnKind::NullableUInt64);
}

static void BM_StatisticsBuildBasicNullableUInt64(benchmark::State & state)
{
    benchmarkBuild(state, {StatisticsType::Basic}, ColumnKind::NullableUInt64);
}

#if USE_DATASKETCHES
static void BM_StatisticsSerializeCountMinSketchUInt64(benchmark::State & state)
{
    benchmarkSerialize(state, {StatisticsType::CountMinSketch}, ColumnKind::UInt64);
}

static void BM_StatisticsBuildCountMinSketchUInt64(benchmark::State & state)
{
    benchmarkBuild(state, {StatisticsType::CountMinSketch}, ColumnKind::UInt64);
}

static void BM_StatisticsSerializeCountMinSketchFloat64(benchmark::State & state)
{
    benchmarkSerialize(state, {StatisticsType::CountMinSketch}, ColumnKind::Float64);
}

static void BM_StatisticsBuildCountMinSketchFloat64(benchmark::State & state)
{
    benchmarkBuild(state, {StatisticsType::CountMinSketch}, ColumnKind::Float64);
}

#endif

BENCHMARK(BM_StatisticsSerializeBasicUInt64);
BENCHMARK(BM_StatisticsBuildBasicUInt64);

BENCHMARK(BM_StatisticsSerializeMinMaxUInt64);
BENCHMARK(BM_StatisticsBuildMinMaxUInt64);

BENCHMARK(BM_StatisticsSerializeBasicFloat64);
BENCHMARK(BM_StatisticsBuildBasicFloat64);

BENCHMARK(BM_StatisticsSerializeMinMaxFloat64);
BENCHMARK(BM_StatisticsBuildMinMaxFloat64);

BENCHMARK(BM_StatisticsSerializeUniqV2UInt64);
BENCHMARK(BM_StatisticsBuildUniqV2UInt64);

BENCHMARK(BM_StatisticsSerializeUniqV2Float64);
BENCHMARK(BM_StatisticsBuildUniqV2Float64);

BENCHMARK(BM_StatisticsSerializeBasicString);
BENCHMARK(BM_StatisticsBuildBasicString);

BENCHMARK(BM_StatisticsSerializeBasicNullableUInt64);
BENCHMARK(BM_StatisticsBuildBasicNullableUInt64);

#if USE_DATASKETCHES
BENCHMARK(BM_StatisticsSerializeCountMinSketchUInt64);
BENCHMARK(BM_StatisticsBuildCountMinSketchUInt64);

BENCHMARK(BM_StatisticsSerializeCountMinSketchFloat64);
BENCHMARK(BM_StatisticsBuildCountMinSketchFloat64);
#endif
