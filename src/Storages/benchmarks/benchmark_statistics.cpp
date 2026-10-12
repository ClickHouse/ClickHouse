#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionUniqCombined.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/StatisticsDescription.h>
#include <base/defines.h>

#include <benchmark/benchmark.h>

#include "config.h"

#include <algorithm>
#include <cmath>
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
namespace
{

enum class HistogramData
{
    /// All values distinct, uniformly spread.
    Uniform = 0,
    /// Zipf with exponent 1.1 over 100000 values.
    Zipf = 1,
    /// 100 distinct values, fewer than the most common values that are tracked by default.
    LowCardinality = 2,
};

ColumnPtr makeHistogramColumn(HistogramData data, size_t rows, UInt64 seed)
{
    /// splitmix64, so the data does not depend on the standard library.
    UInt64 random_state = seed;
    auto next_random = [&]
    {
        random_state += 0x9e3779b97f4a7c15ULL;
        UInt64 value = random_state;
        value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
        value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
        return value ^ (value >> 31);
    };

    auto column = ColumnFloat64::create(rows);
    auto & values = column->getData();
    switch (data)
    {
        case HistogramData::Uniform:
            for (size_t row = 0; row < rows; ++row)
                values[row] = static_cast<Float64>(next_random() >> 11) * 0x1.0p-53;
            break;
        case HistogramData::Zipf: {
            constexpr size_t ranks = 100000;
            static const std::vector<Float64> cumulative = []
            {
                std::vector<Float64> result(ranks);
                Float64 total = 0;
                for (size_t rank = 0; rank < ranks; ++rank)
                {
                    total += 1.0 / std::pow(static_cast<Float64>(rank + 1), 1.1);
                    result[rank] = total;
                }
                return result;
            }();
            for (size_t row = 0; row < rows; ++row)
            {
                const Float64 target = static_cast<Float64>(next_random() >> 11) * 0x1.0p-53 * cumulative.back();
                const auto rank = std::upper_bound(cumulative.begin(), cumulative.end(), target) - cumulative.begin();
                values[row] = static_cast<Float64>((static_cast<size_t>(rank) * 7919) % 1000003);
            }
            break;
        }
        case HistogramData::LowCardinality:
            for (size_t row = 0; row < rows; ++row)
                values[row] = static_cast<Float64>(next_random() % 100);
            break;
    }
    return column;
}

ColumnStatisticsPtr makeHistogramStatistics(UInt64 buckets, UInt64 mcv_values, UInt64 seed)
{
    auto ast = makeASTFunction("histogram", make_intrusive<ASTLiteral>(buckets), make_intrusive<ASTLiteral>(mcv_values));
    ColumnStatisticsDescription description;
    description.data_type = std::make_shared<DataTypeFloat64>();
    description.types_to_desc.emplace(StatisticsType::Histogram, SingleStatisticsDescription(StatisticsType::Histogram, ast, false));
    return MergeTreeStatisticsFactory::instance().get(description, seed);
}

/// Arguments: data kind and M. M = 1 tracks only 6 values, so (almost) all rows are inserted into the
/// KLL sketch, which is the closest to a histogram without most common values.
void benchmarkHistogramBuild(benchmark::State & state)
{
    const auto data = static_cast<HistogramData>(state.range(0));
    const auto mcv_values = static_cast<UInt64>(state.range(1));
    auto column = makeHistogramColumn(data, benchmark_rows, 1);

    for (auto _ [[maybe_unused]] : state)
    {
        auto statistics = makeHistogramStatistics(128, mcv_values, 1);
        statistics->build(column);
        benchmark::DoNotOptimize(statistics->getNumRows());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(benchmark_rows));
}

void benchmarkHistogramSerialize(benchmark::State & state)
{
    const auto data = static_cast<HistogramData>(state.range(0));
    const auto mcv_values = static_cast<UInt64>(state.range(1));
    auto statistics = makeHistogramStatistics(128, mcv_values, 1);
    statistics->build(makeHistogramColumn(data, benchmark_rows, 1));
    const size_t bytes = serializedSize(statistics);

    String serialized;
    serialized.reserve(bytes);
    for (auto _ [[maybe_unused]] : state)
    {
        serialized.clear();
        WriteBufferFromString buffer(serialized);
        statistics->serialize(buffer);
        buffer.finalize();
        benchmark::DoNotOptimize(serialized.data());
        benchmark::ClobberMemory();
    }

    state.counters["payload_bytes"] = static_cast<double>(bytes);
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(bytes));
}

/// Deserializes and merges 8 parts into an accumulator, like the query-time estimator and `MergeTask` do.
void benchmarkHistogramMerge(benchmark::State & state)
{
    const auto data = static_cast<HistogramData>(state.range(0));
    const auto mcv_values = static_cast<UInt64>(state.range(1));
    constexpr size_t parts = 8;
    std::vector<String> serialized_parts;
    for (size_t part = 0; part < parts; ++part)
    {
        auto statistics = makeHistogramStatistics(128, mcv_values, part);
        statistics->build(makeHistogramColumn(data, benchmark_rows, part + 1));
        String serialized;
        WriteBufferFromString buffer(serialized);
        statistics->serialize(buffer);
        buffer.finalize();
        serialized_parts.push_back(std::move(serialized));
    }
    const auto data_type = std::make_shared<DataTypeFloat64>();

    for (auto _ [[maybe_unused]] : state)
    {
        auto accumulator = makeHistogramStatistics(128, mcv_values, 100);
        for (const auto & serialized : serialized_parts)
        {
            ReadBufferFromString buffer(serialized);
            accumulator->merge(ColumnStatistics::deserialize(buffer, data_type));
        }
        benchmark::DoNotOptimize(accumulator->estimateLess(Field(Float64(0.5))));
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(parts));
}

void histogramArguments(benchmark::internal::Benchmark * benchmark)
{
    benchmark->ArgNames({"data", "mcv_values"});
    for (auto data : {HistogramData::Uniform, HistogramData::Zipf, HistogramData::LowCardinality})
        for (int64_t mcv_values : {1, 64, 768})
            benchmark->Args({static_cast<int64_t>(data), mcv_values});
}

}

static void BM_StatisticsBuildHistogram(benchmark::State & state)
{
    benchmarkHistogramBuild(state);
}

static void BM_StatisticsSerializeHistogram(benchmark::State & state)
{
    benchmarkHistogramSerialize(state);
}

static void BM_StatisticsMergeHistogram(benchmark::State & state)
{
    benchmarkHistogramMerge(state);
}

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

BENCHMARK(BM_StatisticsBuildHistogram)->Apply(histogramArguments);
BENCHMARK(BM_StatisticsSerializeHistogram)->Apply(histogramArguments);
BENCHMARK(BM_StatisticsMergeHistogram)->Apply(histogramArguments);
#endif
