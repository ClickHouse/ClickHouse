#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/registerFunctions.h>
#include <Processors/Chunk.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <base/defines.h>
#include <base/types.h>

#include <benchmark/benchmark.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

using namespace DB;

namespace
{

/// This executable is optional low-level diagnostics tooling for isolated runtime-filter paths such as `insert`, `find`,
/// `merge`, casts, and null-map handling. It intentionally forces some modes below, for example exact-set vs bloom-filter
/// behavior and adaptive skipping, so it should not be used as evidence for production-default performance. End-to-end
/// production scenarios, including planner settings, `BuildRuntimeFilterTransform`, `__applyFilter`, query context lookup,
/// pipeline scheduling, and CI comparison against `master`, belong in XML performance tests.
constexpr Float64 DISABLE_ADAPTIVE_SKIP_THRESHOLD = 2.0;
constexpr Float64 DEFAULT_ADAPTIVE_SKIP_THRESHOLD = 0.7;
constexpr UInt64 BLOCKS_TO_SKIP_BEFORE_REENABLING = 30;
constexpr UInt64 EXACT_VALUES_BYTES_LIMIT = 64 * 1024 * 1024;
constexpr UInt64 EXACT_VALUES_LIMIT_FOR_EXACT_FILTER = 1'000'000;
constexpr UInt64 EXACT_VALUES_LIMIT_FOR_BLOOM_FILTER = 1;
constexpr UInt64 BLOOM_FILTER_BYTES = 512 * 1024;
constexpr UInt64 BLOOM_FILTER_HASH_FUNCTIONS = 3;
constexpr Float64 DISABLE_BLOOM_FULLNESS_CHECK = 1.0;

void ensureFunctionsRegistered()
{
    static const bool registered = []
    {
        registerFunctions();
        return true;
    }();
    (void)registered;
}

enum class HitRatio
{
    Zero = 0,
    Half = 50,
    All = 100,
};

enum class ValuePattern
{
    Sequential = 0,
    Mixed = 1,
};

enum class RuntimeFilterKind
{
    ExactContains,
    ExactNotContains,
    Approximate,
};

/// `mix` spreads sequential row numbers across key buckets for non-contiguous benchmark access patterns.
/// It is a SplitMix64-style permutation of the full `UInt64` domain. The odd 64-bit golden-ratio
/// increment has full period modulo `2^64`, so repeated addition would visit every `UInt64` value once.
/// The following xor-shifts and odd multiplications are also bijective on `UInt64`, which makes
/// `mix(row)` a valid source of unique high-entropy keys for a non-dense build-side distribution.
/// Do not add `% key_count` when cardinality matters: that maps many unique `UInt64` values into the
/// same bucket. For dense-key benchmarks, keep the key set `0..rows - 1` and randomize only its order.
UInt64 mix(UInt64 value)
{
    /// Odd 64-bit golden-ratio increment; repeated addition visits every `UInt64` value before repeating.
    value += 0x9e3779b97f4a7c15ULL;
    /// SplitMix64 avalanche multiplier; the preceding xor-shift folds high bits into lower positions before spreading them.
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    /// Second SplitMix64 avalanche multiplier; it further breaks correlations between nearby row numbers.
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

UInt64 presentKey(size_t row, size_t key_count, ValuePattern pattern)
{
    if (key_count == 0)
        return 0;

    if (pattern == ValuePattern::Mixed)
        return mix(row) % key_count;

    return row % key_count;
}

UInt64 absentKey(size_t row, size_t key_count, ValuePattern pattern)
{
    UInt64 value = key_count + 1 + row;
    if (pattern == ValuePattern::Mixed)
        value = key_count + 1 + mix(row);
    return value;
}

UInt64 probeKey(size_t row, size_t key_count, HitRatio hit_ratio, ValuePattern pattern)
{
    switch (hit_ratio)
    {
        case HitRatio::Zero: return absentKey(row, key_count, pattern);
        case HitRatio::Half: return row % 2 == 0 ? presentKey(row, key_count, pattern) : absentKey(row, key_count, pattern);
        case HitRatio::All: return presentKey(row, key_count, pattern);
    }
    return absentKey(row, key_count, pattern);
}

String stringKey(UInt64 value)
{
    return "runtime_filter_key_" + std::to_string(value);
}

std::vector<UInt64> makeShuffledKeyPermutation(size_t rows, UInt64 offset = 0)
{
    std::vector<UInt64> keys(rows);
    for (size_t row = 0; row < rows; ++row)
        keys[row] = row;

    /// Sort by `mix` to get randomized order without reducing cardinality with `mix(row) % rows`.
    std::sort(keys.begin(), keys.end(), [](UInt64 lhs, UInt64 rhs) { return mix(lhs) < mix(rhs); });

    for (auto & key : keys)
        key += offset;

    return keys;
}

ColumnPtr makeShuffledUInt64Column(size_t rows, UInt64 offset = 0)
{
    auto keys = makeShuffledKeyPermutation(rows, offset);
    auto column = ColumnUInt64::create(rows);
    auto & data = column->getData();
    for (size_t row = 0; row < rows; ++row)
        data[row] = keys[row];
    return column;
}

ColumnPtr makeShuffledUInt32Column(size_t rows)
{
    auto keys = makeShuffledKeyPermutation(rows);
    auto column = ColumnUInt32::create(rows);
    auto & data = column->getData();
    for (size_t row = 0; row < rows; ++row)
        data[row] = static_cast<UInt32>(keys[row]);
    return column;
}

ColumnPtr makeShuffledStringColumn(size_t rows)
{
    auto keys = makeShuffledKeyPermutation(rows);
    auto column = ColumnString::create();
    column->reserve(rows);
    for (auto key : keys)
    {
        const auto value = stringKey(key);
        column->insertData(value.data(), value.size());
    }
    return column;
}

ColumnPtr makeUInt64Column(size_t rows, size_t key_count, HitRatio hit_ratio, ValuePattern pattern)
{
    auto column = ColumnUInt64::create(rows);
    auto & data = column->getData();
    for (size_t row = 0; row < rows; ++row)
        data[row] = probeKey(row, key_count, hit_ratio, pattern);
    return std::move(column);
}

ColumnPtr makeStringColumn(size_t rows, size_t key_count, HitRatio hit_ratio, ValuePattern pattern)
{
    auto column = ColumnString::create();
    column->reserve(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        const auto value = stringKey(probeKey(row, key_count, hit_ratio, pattern));
        column->insertData(value.data(), value.size());
    }
    return std::move(column);
}

ColumnPtr makeLowCardinalityStringColumn(size_t rows, size_t key_count, HitRatio hit_ratio, ValuePattern pattern)
{
    auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto column = type->createColumn();
    column->reserve(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        const auto value = stringKey(probeKey(row, key_count, hit_ratio, pattern));
        column->insertData(value.data(), value.size());
    }
    return std::move(column);
}

ColumnPtr makeNullableUInt64Column(size_t rows, size_t key_count, HitRatio hit_ratio, ValuePattern pattern, size_t null_percent)
{
    auto nested = ColumnUInt64::create(rows);
    auto & nested_data = nested->getData();
    auto null_map = ColumnUInt8::create(rows);
    auto & null_map_data = null_map->getData();

    for (size_t row = 0; row < rows; ++row)
    {
        nested_data[row] = probeKey(row, key_count, hit_ratio, pattern);
        null_map_data[row] = null_percent != 0 && rows != 0 && (row * 100 / rows) < null_percent;
    }

    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

DataTypePtr uint64Type()
{
    return std::make_shared<DataTypeUInt64>();
}

DataTypePtr uint32Type()
{
    return std::make_shared<DataTypeUInt32>();
}

DataTypePtr nullableUInt64Type()
{
    return std::make_shared<DataTypeNullable>(uint64Type());
}

DataTypePtr stringType()
{
    return std::make_shared<DataTypeString>();
}

DataTypePtr lowCardinalityStringType()
{
    return std::make_shared<DataTypeLowCardinality>(stringType());
}

UniqueRuntimeFilterPtr makeRuntimeFilter(RuntimeFilterKind kind, const DataTypePtr & type, Float64 adaptive_skip_threshold)
{
    switch (kind)
    {
        case RuntimeFilterKind::ExactContains:
            return std::make_unique<ExactContainsRuntimeFilter>(
                /*filters_to_merge_=*/0,
                type,
                adaptive_skip_threshold,
                BLOCKS_TO_SKIP_BEFORE_REENABLING,
                EXACT_VALUES_BYTES_LIMIT,
                EXACT_VALUES_LIMIT_FOR_EXACT_FILTER);
        case RuntimeFilterKind::ExactNotContains:
            return std::make_unique<ExactNotContainsRuntimeFilter>(
                /*filters_to_merge_=*/0,
                type,
                adaptive_skip_threshold,
                BLOCKS_TO_SKIP_BEFORE_REENABLING,
                EXACT_VALUES_BYTES_LIMIT,
                EXACT_VALUES_LIMIT_FOR_EXACT_FILTER);
        case RuntimeFilterKind::Approximate:
            return std::make_unique<ApproximateRuntimeFilter>(
                /*filters_to_merge_=*/0,
                type,
                adaptive_skip_threshold,
                BLOCKS_TO_SKIP_BEFORE_REENABLING,
                BLOOM_FILTER_BYTES,
                EXACT_VALUES_LIMIT_FOR_BLOOM_FILTER,
                BLOOM_FILTER_HASH_FUNCTIONS,
                DISABLE_BLOOM_FULLNESS_CHECK,
                /*distinct_keys_hint_=*/std::nullopt);
    }
    UNREACHABLE();
}

UniqueRuntimeFilterPtr makeMergeDestination(RuntimeFilterKind kind, const DataTypePtr & type, size_t filters_to_merge)
{
    switch (kind)
    {
        case RuntimeFilterKind::ExactContains:
            return std::make_unique<ExactContainsRuntimeFilter>(
                filters_to_merge,
                type,
                DISABLE_ADAPTIVE_SKIP_THRESHOLD,
                BLOCKS_TO_SKIP_BEFORE_REENABLING,
                EXACT_VALUES_BYTES_LIMIT,
                EXACT_VALUES_LIMIT_FOR_EXACT_FILTER);
        case RuntimeFilterKind::ExactNotContains:
            return std::make_unique<ExactNotContainsRuntimeFilter>(
                filters_to_merge,
                type,
                DISABLE_ADAPTIVE_SKIP_THRESHOLD,
                BLOCKS_TO_SKIP_BEFORE_REENABLING,
                EXACT_VALUES_BYTES_LIMIT,
                EXACT_VALUES_LIMIT_FOR_EXACT_FILTER);
        case RuntimeFilterKind::Approximate:
            return std::make_unique<ApproximateRuntimeFilter>(
                filters_to_merge,
                type,
                DISABLE_ADAPTIVE_SKIP_THRESHOLD,
                BLOCKS_TO_SKIP_BEFORE_REENABLING,
                BLOOM_FILTER_BYTES,
                EXACT_VALUES_LIMIT_FOR_BLOOM_FILTER,
                BLOOM_FILTER_HASH_FUNCTIONS,
                DISABLE_BLOOM_FULLNESS_CHECK,
                /*distinct_keys_hint_=*/std::nullopt);
    }
    UNREACHABLE();
}

UniqueRuntimeFilterPtr buildRuntimeFilter(
    RuntimeFilterKind kind,
    const DataTypePtr & type,
    const ColumnPtr & build_column,
    Float64 adaptive_skip_threshold = DISABLE_ADAPTIVE_SKIP_THRESHOLD)
{
    auto filter = makeRuntimeFilter(kind, type, adaptive_skip_threshold);
    if (build_column)
        filter->insert(build_column);
    filter->finishInsert();
    return filter;
}

ColumnWithTypeAndName makeArgument(const ColumnPtr & column, const DataTypePtr & type)
{
    return ColumnWithTypeAndName(column, type, "key");
}

void recordRows(benchmark::State & state, size_t rows)
{
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(rows));
}

void benchmarkFind(
    benchmark::State & state, RuntimeFilterKind kind, const DataTypePtr & type, ColumnPtr build_column, ColumnPtr probe_column)
{
    ensureFunctionsRegistered();

    auto filter = buildRuntimeFilter(kind, type, build_column);
    auto argument = makeArgument(probe_column, type);

    for (auto _ : state)
    {
        auto result = filter->find(argument);
        benchmark::DoNotOptimize(result);
    }

    recordRows(state, probe_column->size());
}

Block makeHeader(const DataTypePtr & type)
{
    return Block{ColumnWithTypeAndName(type->createColumn(), type, "key")};
}

std::vector<ColumnPtr> splitColumn(const ColumnPtr & column, size_t chunk_rows)
{
    std::vector<ColumnPtr> chunks;
    for (size_t offset = 0; offset < column->size(); offset += chunk_rows)
        chunks.push_back(column->cut(offset, std::min(chunk_rows, column->size() - offset)));
    return chunks;
}

}

static void BM_RuntimeFilterExactContainsFindUInt64(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto hit_ratio = static_cast<HitRatio>(state.range(2));
    const auto type = uint64Type();

    auto build_column = makeUInt64Column(key_count, key_count, HitRatio::All, ValuePattern::Sequential);
    auto probe_column = makeUInt64Column(rows, key_count, hit_ratio, ValuePattern::Mixed);
    benchmarkFind(state, RuntimeFilterKind::ExactContains, type, build_column, probe_column);
}

static void BM_RuntimeFilterExactNotContainsFindUInt64(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto hit_ratio = static_cast<HitRatio>(state.range(2));
    const auto type = uint64Type();

    auto build_column = makeUInt64Column(key_count, key_count, HitRatio::All, ValuePattern::Sequential);
    auto probe_column = makeUInt64Column(rows, key_count, hit_ratio, ValuePattern::Mixed);
    benchmarkFind(state, RuntimeFilterKind::ExactNotContains, type, build_column, probe_column);
}

static void BM_RuntimeFilterExactContainsFindNullableUInt64(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto null_percent = static_cast<size_t>(state.range(2));
    const auto type = nullableUInt64Type();

    auto build_column = makeNullableUInt64Column(key_count, key_count, HitRatio::All, ValuePattern::Sequential, 0);
    auto probe_column = makeNullableUInt64Column(rows, key_count, HitRatio::Half, ValuePattern::Mixed, null_percent);
    benchmarkFind(state, RuntimeFilterKind::ExactContains, type, build_column, probe_column);
}

static void BM_RuntimeFilterApproximateFindUInt64(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto hit_ratio = static_cast<HitRatio>(state.range(2));
    const auto type = uint64Type();

    auto build_column = makeUInt64Column(key_count, key_count, HitRatio::All, ValuePattern::Sequential);
    auto probe_column = makeUInt64Column(rows, key_count, hit_ratio, ValuePattern::Mixed);
    benchmarkFind(state, RuntimeFilterKind::Approximate, type, build_column, probe_column);
}

static void BM_RuntimeFilterApproximateFindNullableUInt64(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto null_percent = static_cast<size_t>(state.range(2));
    const auto type = nullableUInt64Type();

    auto build_column = makeNullableUInt64Column(key_count, key_count, HitRatio::All, ValuePattern::Sequential, 0);
    auto probe_column = makeNullableUInt64Column(rows, key_count, HitRatio::Half, ValuePattern::Mixed, null_percent);
    benchmarkFind(state, RuntimeFilterKind::Approximate, type, build_column, probe_column);
}

static void BM_RuntimeFilterApproximateFindString(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto hit_ratio = static_cast<HitRatio>(state.range(2));
    const auto type = stringType();

    auto build_column = makeStringColumn(key_count, key_count, HitRatio::All, ValuePattern::Sequential);
    auto probe_column = makeStringColumn(rows, key_count, hit_ratio, ValuePattern::Mixed);
    benchmarkFind(state, RuntimeFilterKind::Approximate, type, build_column, probe_column);
}

static void BM_RuntimeFilterApproximateFindLowCardinalityString(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto hit_ratio = static_cast<HitRatio>(state.range(2));
    const auto type = lowCardinalityStringType();

    auto build_column = makeLowCardinalityStringColumn(key_count, key_count, HitRatio::All, ValuePattern::Sequential);
    auto probe_column = makeLowCardinalityStringColumn(rows, key_count, hit_ratio, ValuePattern::Mixed);
    benchmarkFind(state, RuntimeFilterKind::Approximate, type, build_column, probe_column);
}

static void BM_RuntimeFilterApproximateBuildUInt64(benchmark::State & state)
{
    const auto rows = static_cast<size_t>(state.range(0));
    const auto type = uint64Type();
    auto build_column = makeShuffledUInt64Column(rows);

    for (auto _ : state)
    {
        auto filter = buildRuntimeFilter(RuntimeFilterKind::Approximate, type, build_column);
        benchmark::DoNotOptimize(filter);
    }

    recordRows(state, rows);
}

static void BM_RuntimeFilterApproximateBuildString(benchmark::State & state)
{
    const auto rows = static_cast<size_t>(state.range(0));
    const auto type = stringType();
    auto build_column = makeShuffledStringColumn(rows);

    for (auto _ : state)
    {
        auto filter = buildRuntimeFilter(RuntimeFilterKind::Approximate, type, build_column);
        benchmark::DoNotOptimize(filter);
    }

    recordRows(state, rows);
}

static void BM_RuntimeFilterApproximateMergeUInt64(benchmark::State & state)
{
    const auto filters_to_merge = static_cast<size_t>(state.range(0));
    const auto keys_per_filter = static_cast<size_t>(state.range(1));
    const auto type = uint64Type();

    std::vector<UniqueRuntimeFilterPtr> sources;
    sources.reserve(filters_to_merge);
    for (size_t filter_index = 0; filter_index < filters_to_merge; ++filter_index)
    {
        auto column = makeShuffledUInt64Column(keys_per_filter, static_cast<UInt64>(filter_index) * static_cast<UInt64>(keys_per_filter));
        sources.push_back(buildRuntimeFilter(RuntimeFilterKind::Approximate, type, column));
    }

    for (auto _ : state)
    {
        auto destination = makeMergeDestination(RuntimeFilterKind::Approximate, type, filters_to_merge);
        for (const auto & source : sources)
            destination->merge(source.get());
        destination->finishInsert();
        benchmark::DoNotOptimize(destination);
    }

    recordRows(state, filters_to_merge * keys_per_filter);
}

static void BM_RuntimeFilterExactMergeUInt64(benchmark::State & state)
{
    const auto filters_to_merge = static_cast<size_t>(state.range(0));
    const auto keys_per_filter = static_cast<size_t>(state.range(1));
    const auto type = uint64Type();

    std::vector<UniqueRuntimeFilterPtr> sources;
    sources.reserve(filters_to_merge);
    for (size_t filter_index = 0; filter_index < filters_to_merge; ++filter_index)
    {
        auto column = makeShuffledUInt64Column(keys_per_filter, static_cast<UInt64>(filter_index) * static_cast<UInt64>(keys_per_filter));
        sources.push_back(buildRuntimeFilter(RuntimeFilterKind::ExactContains, type, column));
    }

    for (auto _ : state)
    {
        auto destination = makeMergeDestination(RuntimeFilterKind::ExactContains, type, filters_to_merge);
        for (const auto & source : sources)
            destination->merge(source.get());
        destination->finishInsert();
        benchmark::DoNotOptimize(destination);
    }

    recordRows(state, filters_to_merge * keys_per_filter);
}

static void BM_RuntimeFilterAdaptiveSkipApproximateUInt64(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto type = uint64Type();

    auto build_column = makeUInt64Column(key_count, key_count, HitRatio::All, ValuePattern::Sequential);
    auto probe_column = makeUInt64Column(rows, key_count, HitRatio::All, ValuePattern::Mixed);
    auto filter = buildRuntimeFilter(RuntimeFilterKind::Approximate, type, build_column, DEFAULT_ADAPTIVE_SKIP_THRESHOLD);
    auto argument = makeArgument(probe_column, type);

    for (auto _ : state)
    {
        auto result = filter->find(argument);
        benchmark::DoNotOptimize(result);
    }

    recordRows(state, rows);
}

static void BM_RuntimeFilterBuildTransformUInt64(benchmark::State & state)
{
    const auto rows = static_cast<size_t>(state.range(0));
    const auto chunk_rows = static_cast<size_t>(state.range(1));
    const auto type = uint64Type();
    auto build_column = makeShuffledUInt64Column(rows);
    auto column_chunks = splitColumn(build_column, chunk_rows);
    auto header = std::make_shared<const Block>(makeHeader(type));

    for (auto _ : state)
    {
        BuildRuntimeFilterTransform transform(
            header,
            /*filter_column_name_=*/"key",
            /*filter_column_type_=*/type,
            /*filter_name_=*/"_runtime_filter_benchmark",
            /*filter_key_=*/String{},
            /*filters_to_merge_=*/0,
            EXACT_VALUES_LIMIT_FOR_BLOOM_FILTER,
            BLOOM_FILTER_BYTES,
            BLOOM_FILTER_HASH_FUNCTIONS,
            DISABLE_ADAPTIVE_SKIP_THRESHOLD,
            BLOCKS_TO_SKIP_BEFORE_REENABLING,
            DISABLE_BLOOM_FULLNESS_CHECK,
            /*allow_to_use_not_exact_filter_=*/true,
            /*track_key_range_=*/false,
            /*distinct_keys_hint_=*/std::nullopt,
            /*query_context_=*/nullptr);

        for (const auto & column_chunk : column_chunks)
        {
            Chunk chunk({column_chunk}, column_chunk->size());
            transform.transform(chunk);
        }
        benchmark::DoNotOptimize(&transform);
    }

    recordRows(state, rows);
}

static void BM_RuntimeFilterBuildTransformCastUInt32ToUInt64(benchmark::State & state)
{
    ensureFunctionsRegistered();

    const auto rows = static_cast<size_t>(state.range(0));
    const auto chunk_rows = static_cast<size_t>(state.range(1));
    const auto source_type = uint32Type();
    const auto target_type = uint64Type();
    auto build_column = makeShuffledUInt32Column(rows);
    auto column_chunks = splitColumn(build_column, chunk_rows);
    auto header = std::make_shared<const Block>(makeHeader(source_type));

    for (auto _ : state)
    {
        BuildRuntimeFilterTransform transform(
            header,
            /*filter_column_name_=*/"key",
            /*filter_column_type_=*/target_type,
            /*filter_name_=*/"_runtime_filter_benchmark",
            /*filter_key_=*/String{},
            /*filters_to_merge_=*/0,
            EXACT_VALUES_LIMIT_FOR_BLOOM_FILTER,
            BLOOM_FILTER_BYTES,
            BLOOM_FILTER_HASH_FUNCTIONS,
            DISABLE_ADAPTIVE_SKIP_THRESHOLD,
            BLOCKS_TO_SKIP_BEFORE_REENABLING,
            DISABLE_BLOOM_FULLNESS_CHECK,
            /*allow_to_use_not_exact_filter_=*/true,
            /*track_key_range_=*/false,
            /*distinct_keys_hint_=*/std::nullopt,
            /*query_context_=*/nullptr);

        for (const auto & column_chunk : column_chunks)
        {
            Chunk chunk({column_chunk}, column_chunk->size());
            transform.transform(chunk);
        }
        benchmark::DoNotOptimize(&transform);
    }

    recordRows(state, rows);
}

BENCHMARK(BM_RuntimeFilterExactContainsFindUInt64)
    ->Args({/*key_count=*/0, /*rows=*/65536, /*hit_ratio=*/0})
    ->Args({/*key_count=*/1, /*rows=*/65536, /*hit_ratio=*/100})
    ->Args({/*key_count=*/100, /*rows=*/65536, /*hit_ratio=*/50})
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/50});

BENCHMARK(BM_RuntimeFilterExactNotContainsFindUInt64)
    ->Args({/*key_count=*/100, /*rows=*/65536, /*hit_ratio=*/50})
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/50});

BENCHMARK(BM_RuntimeFilterExactContainsFindNullableUInt64)
    ->Args({/*key_count=*/1, /*rows=*/65536, /*null_percent=*/0})
    ->Args({/*key_count=*/1, /*rows=*/65536, /*null_percent=*/1})
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*null_percent=*/50});

BENCHMARK(BM_RuntimeFilterApproximateFindUInt64)
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/0})
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/50})
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/100})
    ->Args({/*key_count=*/100000, /*rows=*/65536, /*hit_ratio=*/50});

/// ApproximateRuntimeFilter does not support hashing actual NULL values in ColumnNullable.
/// Benchmark only the non-null ColumnNullable overhead here; NULL-heavy cases are covered by the exact filter benchmark above.
BENCHMARK(BM_RuntimeFilterApproximateFindNullableUInt64)->Args({/*key_count=*/10000, /*rows=*/65536, /*null_percent=*/0});

BENCHMARK(BM_RuntimeFilterApproximateFindString)
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/0})
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/50});

BENCHMARK(BM_RuntimeFilterApproximateFindLowCardinalityString)
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/0})
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/50});

BENCHMARK(BM_RuntimeFilterApproximateBuildUInt64)->Arg(/*rows=*/10000)->Arg(/*rows=*/100000);

BENCHMARK(BM_RuntimeFilterApproximateBuildString)->Arg(/*rows=*/10000)->Arg(/*rows=*/100000);

BENCHMARK(BM_RuntimeFilterApproximateMergeUInt64)
    ->Args({/*filters_to_merge=*/2, /*keys_per_filter=*/10000})
    ->Args({/*filters_to_merge=*/8, /*keys_per_filter=*/10000})
    ->Args({/*filters_to_merge=*/32, /*keys_per_filter=*/10000});

BENCHMARK(BM_RuntimeFilterExactMergeUInt64)
    ->Args({/*filters_to_merge=*/2, /*keys_per_filter=*/1000})
    ->Args({/*filters_to_merge=*/8, /*keys_per_filter=*/1000})
    ->Args({/*filters_to_merge=*/32, /*keys_per_filter=*/1000});

BENCHMARK(BM_RuntimeFilterAdaptiveSkipApproximateUInt64)->Args({/*key_count=*/10000, /*rows=*/65536});

BENCHMARK(BM_RuntimeFilterBuildTransformUInt64)->Args({/*rows=*/10000, /*chunk_rows=*/8192})->Args({/*rows=*/100000, /*chunk_rows=*/8192});

BENCHMARK(BM_RuntimeFilterBuildTransformCastUInt32ToUInt64)
    ->Args({/*rows=*/10000, /*chunk_rows=*/8192})
    ->Args({/*rows=*/100000, /*chunk_rows=*/8192});
