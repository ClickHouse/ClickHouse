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
#include <fmt/format.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

using namespace DB;

namespace
{

/// Low-level runtime-filter microbenchmarks. Use XML performance tests for end-to-end production comparisons.
constexpr Float64 DISABLE_ADAPTIVE_SKIP_THRESHOLD = 2.0;
constexpr Float64 DEFAULT_ADAPTIVE_SKIP_THRESHOLD = 0.7;
constexpr UInt64 BLOCKS_TO_SKIP_BEFORE_REENABLING = 30;
constexpr UInt64 EXACT_VALUES_BYTES_LIMIT = 64 * 1024 * 1024;
constexpr UInt64 EXACT_VALUES_LIMIT_FOR_EXACT_FILTER = 1'000'000;
/// The limit is checked after each column insertion, not after each row.
constexpr UInt64 ADAPTIVE_EXACT_VALUES_LIMIT = 1;
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

/// SplitMix64 permutation used to spread sequential row numbers in benchmark data.
UInt64 mix(UInt64 value)
{
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
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
    if (key_count == 0)
        return row + 1;

    return key_count + presentKey(row, key_count, pattern);
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
    return fmt::format("runtime_filter_key_{:016x}", value);
}

std::vector<UInt64> makeShuffledKeyPermutation(size_t rows, UInt64 offset = 0)
{
    std::vector<UInt64> keys(rows);
    for (size_t row = 0; row < rows; ++row)
        keys[row] = row;

    /// Randomize dense-key order without changing cardinality.
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

    const auto bounded_null_percent = std::min<size_t>(null_percent, 100);
    const auto null_count = (rows / 100) * bounded_null_percent + ((rows % 100) * bounded_null_percent + 99) / 100;
    /// Avoid clustering nulls in mixed probe data while preserving their exact count.
    const auto null_order = pattern == ValuePattern::Mixed && null_count != 0 ? makeShuffledKeyPermutation(rows) : std::vector<UInt64>{};

    for (size_t row = 0; row < rows; ++row)
    {
        nested_data[row] = probeKey(row, key_count, hit_ratio, pattern);
        const auto null_rank = null_order.empty() ? row : null_order[row];
        null_map_data[row] = null_rank < null_count;
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

UniqueRuntimeFilterPtr buildAdaptiveRuntimeFilter(
    const DataTypePtr & type, const ColumnPtr & build_column, Float64 adaptive_skip_threshold = DISABLE_ADAPTIVE_SKIP_THRESHOLD)
{
    const RuntimeFilterConfig config{adaptive_skip_threshold, BLOCKS_TO_SKIP_BEFORE_REENABLING};
    auto filter = std::make_unique<RuntimeFilter>(
        /*filters_to_merge_=*/0,
        config,
        AdaptiveSetRuntimeFilter(
            type,
            BLOOM_FILTER_BYTES,
            ADAPTIVE_EXACT_VALUES_LIMIT,
            BLOOM_FILTER_HASH_FUNCTIONS,
            DISABLE_BLOOM_FULLNESS_CHECK,
            /*distinct_keys_hint_=*/std::nullopt,
            /*distinct_keys_hint_matches_filter_key_=*/false));
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

template <typename Filter>
void benchmarkFind(benchmark::State & state, const Filter & filter, const DataTypePtr & type, const ColumnPtr & probe_column)
{
    ensureFunctionsRegistered();
    auto argument = makeArgument(probe_column, type);

    for (auto _ [[maybe_unused]] : state)
    {
        std::optional<size_t> rows_passed;
        auto result = filter.find(argument, rows_passed);
        benchmark::DoNotOptimize(result);
        benchmark::DoNotOptimize(rows_passed);
    }

    recordRows(state, probe_column->size());
}

Block makeHeader(const DataTypePtr & type)
{
    return Block{ColumnWithTypeAndName(type->createColumn(), type, "key")};
}

std::vector<Chunk> splitColumnIntoChunks(const ColumnPtr & column, size_t chunk_rows)
{
    std::vector<Chunk> chunks;
    for (size_t offset = 0; offset < column->size(); offset += chunk_rows)
    {
        const auto rows = std::min(chunk_rows, column->size() - offset);
        chunks.emplace_back(Columns{column->cut(offset, rows)}, rows);
    }
    return chunks;
}

}

static void BM_ExactSetRuntimeFilterContainsFindUInt64(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto hit_ratio = static_cast<HitRatio>(state.range(2));
    const auto type = uint64Type();

    auto build_column = makeUInt64Column(key_count, key_count, HitRatio::All, ValuePattern::Sequential);
    auto probe_column = makeUInt64Column(rows, key_count, hit_ratio, ValuePattern::Mixed);
    ExactSetRuntimeFilter<false> filter(type, EXACT_VALUES_BYTES_LIMIT, EXACT_VALUES_LIMIT_FOR_EXACT_FILTER);
    filter.insert(build_column);
    filter.finishInsert();
    benchmarkFind(state, filter, type, probe_column);
}

static void BM_ExactSetRuntimeFilterNotContainsFindUInt64(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto hit_ratio = static_cast<HitRatio>(state.range(2));
    const auto type = uint64Type();

    auto build_column = makeUInt64Column(key_count, key_count, HitRatio::All, ValuePattern::Sequential);
    auto probe_column = makeUInt64Column(rows, key_count, hit_ratio, ValuePattern::Mixed);
    ExactSetRuntimeFilter<true> filter(type, EXACT_VALUES_BYTES_LIMIT, EXACT_VALUES_LIMIT_FOR_EXACT_FILTER);
    filter.insert(build_column);
    filter.finishInsert();
    benchmarkFind(state, filter, type, probe_column);
}

static void BM_ExactSetRuntimeFilterContainsFindNullableUInt64(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto null_percent = static_cast<size_t>(state.range(2));
    const auto type = nullableUInt64Type();

    auto build_column = makeNullableUInt64Column(key_count, key_count, HitRatio::All, ValuePattern::Sequential, 0);
    auto probe_column = makeNullableUInt64Column(rows, key_count, HitRatio::Half, ValuePattern::Mixed, null_percent);
    ExactSetRuntimeFilter<false> filter(type, EXACT_VALUES_BYTES_LIMIT, EXACT_VALUES_LIMIT_FOR_EXACT_FILTER);
    filter.insert(build_column);
    filter.finishInsert();
    benchmarkFind(state, filter, type, probe_column);
}

static void BM_ApproximateSetRuntimeFilterFindUInt64(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto hit_ratio = static_cast<HitRatio>(state.range(2));
    const auto type = uint64Type();

    auto build_column = makeUInt64Column(key_count, key_count, HitRatio::All, ValuePattern::Sequential);
    auto probe_column = makeUInt64Column(rows, key_count, hit_ratio, ValuePattern::Mixed);
    ApproximateSetRuntimeFilter filter(BLOOM_FILTER_BYTES, BLOOM_FILTER_HASH_FUNCTIONS);
    filter.insert(build_column);
    benchmarkFind(state, filter, type, probe_column);
}

/// Reference only: production uses `ExactSetRuntimeFilter` for nullable join keys.
static void BM_ApproximateSetRuntimeFilterFindNullableUInt64(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto null_percent = static_cast<size_t>(state.range(2));
    const auto type = nullableUInt64Type();

    auto build_column = makeNullableUInt64Column(key_count, key_count, HitRatio::All, ValuePattern::Sequential, 0);
    auto probe_column = makeNullableUInt64Column(rows, key_count, HitRatio::Half, ValuePattern::Mixed, null_percent);
    ApproximateSetRuntimeFilter filter(BLOOM_FILTER_BYTES, BLOOM_FILTER_HASH_FUNCTIONS);
    filter.insert(build_column);
    benchmarkFind(state, filter, type, probe_column);
}

static void BM_ApproximateSetRuntimeFilterFindString(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto hit_ratio = static_cast<HitRatio>(state.range(2));
    const auto type = stringType();

    auto build_column = makeStringColumn(key_count, key_count, HitRatio::All, ValuePattern::Sequential);
    auto probe_column = makeStringColumn(rows, key_count, hit_ratio, ValuePattern::Mixed);
    ApproximateSetRuntimeFilter filter(BLOOM_FILTER_BYTES, BLOOM_FILTER_HASH_FUNCTIONS);
    filter.insert(build_column);
    benchmarkFind(state, filter, type, probe_column);
}

static void BM_ApproximateSetRuntimeFilterFindLowCardinalityString(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto hit_ratio = static_cast<HitRatio>(state.range(2));
    const auto type = lowCardinalityStringType();

    auto build_column = makeLowCardinalityStringColumn(key_count, key_count, HitRatio::All, ValuePattern::Sequential);
    auto probe_column = makeLowCardinalityStringColumn(rows, key_count, hit_ratio, ValuePattern::Mixed);
    ApproximateSetRuntimeFilter filter(BLOOM_FILTER_BYTES, BLOOM_FILTER_HASH_FUNCTIONS);
    filter.insert(build_column);
    benchmarkFind(state, filter, type, probe_column);
}

static void BM_ApproximateSetRuntimeFilterBuildUInt64(benchmark::State & state)
{
    const auto rows = static_cast<size_t>(state.range(0));
    auto build_column = makeShuffledUInt64Column(rows);

    for (auto _ [[maybe_unused]] : state)
    {
        std::optional<ApproximateSetRuntimeFilter> filter;
        filter.emplace(BLOOM_FILTER_BYTES, BLOOM_FILTER_HASH_FUNCTIONS);
        filter->insert(build_column);
        benchmark::DoNotOptimize(&*filter);

        state.PauseTiming();
        filter.reset();
        state.ResumeTiming();
    }

    recordRows(state, rows);
}

static void BM_ApproximateSetRuntimeFilterBuildString(benchmark::State & state)
{
    const auto rows = static_cast<size_t>(state.range(0));
    auto build_column = makeShuffledStringColumn(rows);

    for (auto _ [[maybe_unused]] : state)
    {
        std::optional<ApproximateSetRuntimeFilter> filter;
        filter.emplace(BLOOM_FILTER_BYTES, BLOOM_FILTER_HASH_FUNCTIONS);
        filter->insert(build_column);
        benchmark::DoNotOptimize(&*filter);

        state.PauseTiming();
        filter.reset();
        state.ResumeTiming();
    }

    recordRows(state, rows);
}

static void BM_RuntimeFilterAdaptiveBuildUInt64(benchmark::State & state)
{
    const auto rows = static_cast<size_t>(state.range(0));
    const auto type = uint64Type();
    auto build_column = makeShuffledUInt64Column(rows);

    for (auto _ [[maybe_unused]] : state)
    {
        auto filter = buildAdaptiveRuntimeFilter(type, build_column);
        benchmark::DoNotOptimize(filter.get());

        state.PauseTiming();
        filter.reset();
        state.ResumeTiming();
    }

    recordRows(state, rows);
}

static void BM_RuntimeFilterAdaptiveBuildString(benchmark::State & state)
{
    const auto rows = static_cast<size_t>(state.range(0));
    const auto type = stringType();
    auto build_column = makeShuffledStringColumn(rows);

    for (auto _ [[maybe_unused]] : state)
    {
        auto filter = buildAdaptiveRuntimeFilter(type, build_column);
        benchmark::DoNotOptimize(filter.get());

        state.PauseTiming();
        filter.reset();
        state.ResumeTiming();
    }

    recordRows(state, rows);
}

static void BM_ApproximateSetRuntimeFilterMergeUInt64(benchmark::State & state)
{
    const auto filters_to_merge = static_cast<size_t>(state.range(0));
    const auto keys_per_filter = static_cast<size_t>(state.range(1));

    std::vector<std::unique_ptr<ApproximateSetRuntimeFilter>> sources;
    sources.reserve(filters_to_merge);
    for (size_t filter_index = 0; filter_index < filters_to_merge; ++filter_index)
    {
        auto column = makeShuffledUInt64Column(keys_per_filter, static_cast<UInt64>(filter_index) * static_cast<UInt64>(keys_per_filter));
        auto source = std::make_unique<ApproximateSetRuntimeFilter>(BLOOM_FILTER_BYTES, BLOOM_FILTER_HASH_FUNCTIONS);
        source->insert(column);
        sources.push_back(std::move(source));
    }

    for (auto _ [[maybe_unused]] : state)
    {
        std::optional<ApproximateSetRuntimeFilter> destination;
        destination.emplace(BLOOM_FILTER_BYTES, BLOOM_FILTER_HASH_FUNCTIONS);
        for (const auto & source : sources)
            destination->mergeFrom(*source);
        benchmark::DoNotOptimize(&*destination);

        state.PauseTiming();
        destination.reset();
        state.ResumeTiming();
    }

    recordRows(state, filters_to_merge * keys_per_filter);
}

static void BM_ExactSetRuntimeFilterMergeUInt64(benchmark::State & state)
{
    using Filter = ExactSetRuntimeFilter<false>;

    const auto filters_to_merge = static_cast<size_t>(state.range(0));
    const auto keys_per_filter = static_cast<size_t>(state.range(1));
    const auto type = uint64Type();

    std::vector<std::unique_ptr<Filter>> sources;
    sources.reserve(filters_to_merge);
    for (size_t filter_index = 0; filter_index < filters_to_merge; ++filter_index)
    {
        auto column = makeShuffledUInt64Column(keys_per_filter, static_cast<UInt64>(filter_index) * static_cast<UInt64>(keys_per_filter));
        auto source = std::make_unique<Filter>(type, EXACT_VALUES_BYTES_LIMIT, EXACT_VALUES_LIMIT_FOR_EXACT_FILTER);
        source->insert(column);
        source->finishInsert();
        sources.push_back(std::move(source));
    }

    for (auto _ [[maybe_unused]] : state)
    {
        std::optional<Filter> destination;
        destination.emplace(type, EXACT_VALUES_BYTES_LIMIT, EXACT_VALUES_LIMIT_FOR_EXACT_FILTER);
        for (const auto & source : sources)
            destination->mergeFrom(*source);
        benchmark::DoNotOptimize(&*destination);

        state.PauseTiming();
        destination.reset();
        state.ResumeTiming();
    }

    recordRows(state, filters_to_merge * keys_per_filter);
}

/// Measures the Bloom-filter popcount scan in `isWorthUsing`.
static void BM_ApproximateSetRuntimeFilterWorthinessUInt64(benchmark::State & state)
{
    const auto rows = static_cast<size_t>(state.range(0));
    auto build_column = makeShuffledUInt64Column(rows);
    ApproximateSetRuntimeFilter filter(BLOOM_FILTER_BYTES, BLOOM_FILTER_HASH_FUNCTIONS);
    filter.insert(build_column);

    for (auto _ [[maybe_unused]] : state)
    {
        const bool worth_using = filter.isWorthUsing(DISABLE_BLOOM_FULLNESS_CHECK);
        benchmark::DoNotOptimize(worth_using);
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(BLOOM_FILTER_BYTES));
}

/// Includes `finishInsert`, which selects the empty, single-value, or set-backed lookup state.
static void BM_ExactSetRuntimeFilterContainsBuildUInt64(benchmark::State & state)
{
    const auto rows = static_cast<size_t>(state.range(0));
    const auto type = uint64Type();
    auto build_column = makeShuffledUInt64Column(rows);

    for (auto _ [[maybe_unused]] : state)
    {
        std::optional<ExactSetRuntimeFilter<false>> filter;
        filter.emplace(type, EXACT_VALUES_BYTES_LIMIT, EXACT_VALUES_LIMIT_FOR_EXACT_FILTER);
        filter->insert(build_column);
        filter->finishInsert();
        benchmark::DoNotOptimize(&*filter);

        state.PauseTiming();
        filter.reset();
        state.ResumeTiming();
    }

    recordRows(state, rows);
}

static void BM_AdaptiveSetRuntimeFilterExactToApproximateTransitionUInt64(benchmark::State & state)
{
    const auto exact_rows = static_cast<size_t>(state.range(0));
    const auto type = uint64Type();
    auto exact_column = makeShuffledUInt64Column(exact_rows);
    auto trigger_column = makeShuffledUInt64Column(/*rows=*/1, static_cast<UInt64>(exact_rows));

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        {
            AdaptiveSetRuntimeFilter filter(
                type,
                BLOOM_FILTER_BYTES,
                exact_rows,
                BLOOM_FILTER_HASH_FUNCTIONS,
                DISABLE_BLOOM_FULLNESS_CHECK,
                /*distinct_keys_hint_=*/std::nullopt,
                /*distinct_keys_hint_matches_filter_key_=*/false);
            filter.insert(exact_column);
            state.ResumeTiming();

            filter.insert(trigger_column);
            benchmark::DoNotOptimize(&filter);

            state.PauseTiming();
        }
        state.ResumeTiming();
    }

    recordRows(state, exact_rows + 1);
}

static void BM_RuntimeFilterAdaptiveSkipApproximateUInt64(benchmark::State & state)
{
    const auto key_count = static_cast<size_t>(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    const auto type = uint64Type();

    auto build_column = makeUInt64Column(key_count, key_count, HitRatio::All, ValuePattern::Sequential);
    auto probe_column = makeUInt64Column(rows, key_count, HitRatio::All, ValuePattern::Mixed);
    auto filter = buildAdaptiveRuntimeFilter(type, build_column, DEFAULT_ADAPTIVE_SKIP_THRESHOLD);
    auto argument = makeArgument(probe_column, type);

    for (auto _ [[maybe_unused]] : state)
    {
        auto result = filter->find(argument);
        benchmark::DoNotOptimize(result);
    }

    recordRows(state, rows);
}

static void BM_RuntimeFilterAdaptiveBuildTransformInsertOnlyUInt64(benchmark::State & state)
{
    const auto rows = static_cast<size_t>(state.range(0));
    const auto chunk_rows = static_cast<size_t>(state.range(1));
    const auto type = uint64Type();
    auto build_column = makeShuffledUInt64Column(rows);
    auto chunks = splitColumnIntoChunks(build_column, chunk_rows);
    auto header = std::make_shared<const Block>(makeHeader(type));

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        {
            BuildRuntimeFilterTransform transform(
                header,
                /*filter_column_name_=*/"key",
                /*filter_column_type_=*/type,
                /*filter_name_=*/"_runtime_filter_benchmark",
                /*filter_key_=*/String{},
                /*filters_to_merge_=*/0,
                ADAPTIVE_EXACT_VALUES_LIMIT,
                BLOOM_FILTER_BYTES,
                BLOOM_FILTER_HASH_FUNCTIONS,
                DISABLE_ADAPTIVE_SKIP_THRESHOLD,
                BLOCKS_TO_SKIP_BEFORE_REENABLING,
                DISABLE_BLOOM_FULLNESS_CHECK,
                /*allow_to_use_not_exact_filter_=*/true,
                /*track_key_range_=*/false,
                /*distinct_keys_hint_=*/std::nullopt,
                /*distinct_keys_hint_matches_filter_key_=*/false,
                /*query_context_=*/nullptr);
            state.ResumeTiming();

            for (auto & chunk : chunks)
                transform.transform(chunk);
            benchmark::DoNotOptimize(&transform);

            state.PauseTiming();
        }
        state.ResumeTiming();
    }

    recordRows(state, rows);
}

static void BM_RuntimeFilterAdaptiveBuildTransformInsertOnlyCastUInt32ToUInt64(benchmark::State & state)
{
    ensureFunctionsRegistered();

    const auto rows = static_cast<size_t>(state.range(0));
    const auto chunk_rows = static_cast<size_t>(state.range(1));
    const auto source_type = uint32Type();
    const auto target_type = uint64Type();
    auto build_column = makeShuffledUInt32Column(rows);
    auto chunks = splitColumnIntoChunks(build_column, chunk_rows);
    auto header = std::make_shared<const Block>(makeHeader(source_type));

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        {
            BuildRuntimeFilterTransform transform(
                header,
                /*filter_column_name_=*/"key",
                /*filter_column_type_=*/target_type,
                /*filter_name_=*/"_runtime_filter_benchmark",
                /*filter_key_=*/String{},
                /*filters_to_merge_=*/0,
                ADAPTIVE_EXACT_VALUES_LIMIT,
                BLOOM_FILTER_BYTES,
                BLOOM_FILTER_HASH_FUNCTIONS,
                DISABLE_ADAPTIVE_SKIP_THRESHOLD,
                BLOCKS_TO_SKIP_BEFORE_REENABLING,
                DISABLE_BLOOM_FULLNESS_CHECK,
                /*allow_to_use_not_exact_filter_=*/true,
                /*track_key_range_=*/false,
                /*distinct_keys_hint_=*/std::nullopt,
                /*distinct_keys_hint_matches_filter_key_=*/false,
                /*query_context_=*/nullptr);
            state.ResumeTiming();

            for (auto & chunk : chunks)
                transform.transform(chunk);
            benchmark::DoNotOptimize(&transform);

            state.PauseTiming();
        }
        state.ResumeTiming();
    }

    recordRows(state, rows);
}

BENCHMARK(BM_ExactSetRuntimeFilterContainsFindUInt64)
    ->Args({/*key_count=*/0, /*rows=*/65536, /*hit_ratio=*/0})
    ->Args({/*key_count=*/1, /*rows=*/65536, /*hit_ratio=*/100})
    ->Args({/*key_count=*/100, /*rows=*/65536, /*hit_ratio=*/50})
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/50});

BENCHMARK(BM_ExactSetRuntimeFilterNotContainsFindUInt64)
    ->Args({/*key_count=*/100, /*rows=*/65536, /*hit_ratio=*/50})
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/50});

BENCHMARK(BM_ExactSetRuntimeFilterContainsFindNullableUInt64)
    ->Args({/*key_count=*/1, /*rows=*/65536, /*null_percent=*/0})
    ->Args({/*key_count=*/1, /*rows=*/65536, /*null_percent=*/1})
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*null_percent=*/50});

BENCHMARK(BM_ApproximateSetRuntimeFilterFindUInt64)
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/0})
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/50})
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/100})
    ->Args({/*key_count=*/100000, /*rows=*/65536, /*hit_ratio=*/50});

BENCHMARK(BM_ApproximateSetRuntimeFilterFindNullableUInt64)->Args({/*key_count=*/10000, /*rows=*/65536, /*null_percent=*/0});

BENCHMARK(BM_ApproximateSetRuntimeFilterFindString)
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/0})
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/50});

BENCHMARK(BM_ApproximateSetRuntimeFilterFindLowCardinalityString)
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/0})
    ->Args({/*key_count=*/10000, /*rows=*/65536, /*hit_ratio=*/50});

BENCHMARK(BM_ApproximateSetRuntimeFilterBuildUInt64)->Arg(/*rows=*/10000)->Arg(/*rows=*/100000);

BENCHMARK(BM_ApproximateSetRuntimeFilterBuildString)->Arg(/*rows=*/10000)->Arg(/*rows=*/100000);

BENCHMARK(BM_ApproximateSetRuntimeFilterMergeUInt64)
    ->Args({/*filters_to_merge=*/2, /*keys_per_filter=*/10000})
    ->Args({/*filters_to_merge=*/8, /*keys_per_filter=*/10000})
    ->Args({/*filters_to_merge=*/32, /*keys_per_filter=*/10000});

BENCHMARK(BM_ApproximateSetRuntimeFilterWorthinessUInt64)->Arg(/*rows=*/10000)->Arg(/*rows=*/100000);

BENCHMARK(BM_ExactSetRuntimeFilterMergeUInt64)
    ->Args({/*filters_to_merge=*/2, /*keys_per_filter=*/1000})
    ->Args({/*filters_to_merge=*/8, /*keys_per_filter=*/1000})
    ->Args({/*filters_to_merge=*/32, /*keys_per_filter=*/1000});

BENCHMARK(BM_ExactSetRuntimeFilterContainsBuildUInt64)->Arg(/*rows=*/0)->Arg(/*rows=*/1)->Arg(/*rows=*/10000)->Arg(/*rows=*/100000);

BENCHMARK(BM_AdaptiveSetRuntimeFilterExactToApproximateTransitionUInt64)
    ->Arg(/*exact_rows=*/1)
    ->Arg(/*exact_rows=*/1024)
    ->Arg(/*exact_rows=*/8192);

BENCHMARK(BM_RuntimeFilterAdaptiveBuildUInt64)->Arg(/*rows=*/10000)->Arg(/*rows=*/100000);

BENCHMARK(BM_RuntimeFilterAdaptiveBuildString)->Arg(/*rows=*/10000)->Arg(/*rows=*/100000);

BENCHMARK(BM_RuntimeFilterAdaptiveSkipApproximateUInt64)->Args({/*key_count=*/10000, /*rows=*/65536});

BENCHMARK(BM_RuntimeFilterAdaptiveBuildTransformInsertOnlyUInt64)
    ->Args({/*rows=*/10000, /*chunk_rows=*/8192})
    ->Args({/*rows=*/100000, /*chunk_rows=*/8192});

BENCHMARK(BM_RuntimeFilterAdaptiveBuildTransformInsertOnlyCastUInt32ToUInt64)
    ->Args({/*rows=*/10000, /*chunk_rows=*/8192})
    ->Args({/*rows=*/100000, /*chunk_rows=*/8192});
