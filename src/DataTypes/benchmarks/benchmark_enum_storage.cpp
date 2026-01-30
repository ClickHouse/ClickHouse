#include <chrono>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#include "EnumStorageImplementations.h"


using namespace DB::Benchmark;

// ============================================================================
// Global test data - pre-generated to avoid measuring generation time
// ============================================================================

namespace
{

// Tiny enum - 3 entries (like boolean-ish enums)
const auto g_tiny_enum_int8 = generateRandomEnum<int8_t>(3, 4, 8, 11111);

// Small enum (like status codes) - 10 entries, short names
const auto g_small_enum_int8 = generateStatusEnum<int8_t>();

// Medium enum (like country codes) - 50 entries, 2-char names
const auto g_medium_enum_int8 = generateCountryEnum<int8_t>();

// Large enum - 100 entries, medium names (10-20 chars)
const auto g_large_enum_int8 = generateRandomEnum<int8_t>(100, 10, 20, 12345);

// Very large enum - 200 entries, longer names (15-30 chars)
const auto g_xlarge_enum_int8 = generateRandomEnum<int8_t>(200, 15, 30, 54321);

// Max enum8 - 255 entries (maximum for Enum8)
const auto g_max_enum_int8 = generateRandomEnum<int8_t>(255, 8, 15, 99999);

// Category enum - longer descriptive names
const auto g_category_enum_int8 = generateCategoryEnum<int8_t>();

// Short names enum - testing with very short names (1-3 chars)
const auto g_short_names_enum_int8 = generateRandomEnum<int8_t>(50, 1, 3, 77777);

// Long names enum - testing with very long names (40-60 chars)
const auto g_long_names_enum_int8 = generateRandomEnum<int8_t>(30, 40, 60, 88888);

// Int16 variants for larger enums
const auto g_small_enum_int16 = generateStatusEnum<int16_t>();
const auto g_medium_enum_int16 = generateCountryEnum<int16_t>();
const auto g_large_enum_int16 = generateRandomEnum<int16_t>(100, 10, 20, 12345);
const auto g_huge_enum_int16 = generateRandomEnum<int16_t>(500, 10, 20, 66666);

// Pre-computed lookup keys for consistent benchmarking
std::vector<std::string> g_lookup_names_tiny;
std::vector<std::string> g_lookup_names_small;
std::vector<std::string> g_lookup_names_medium;
std::vector<std::string> g_lookup_names_large;
std::vector<std::string> g_lookup_names_xlarge;
std::vector<std::string> g_lookup_names_max;
std::vector<int8_t> g_lookup_values_tiny;
std::vector<int8_t> g_lookup_values_small;
std::vector<int8_t> g_lookup_values_medium;
std::vector<int8_t> g_lookup_values_large;
std::vector<int8_t> g_lookup_values_xlarge;
std::vector<int8_t> g_lookup_values_max;

void initializeLookupData()
{
    std::mt19937 gen(42);

    // Generate lookup names (existing names from each enum)
    auto fillNames = [&](const auto & enum_values, std::vector<std::string> & names, size_t count) {
        names.clear();
        names.reserve(count);
        std::uniform_int_distribution<size_t> dist(0, enum_values.size() - 1);
        for (size_t i = 0; i < count; ++i)
            names.push_back(enum_values[dist(gen)].first);
    };

    auto fillValues = [&](const auto & enum_values, std::vector<int8_t> & values, size_t count) {
        values.clear();
        values.reserve(count);
        std::uniform_int_distribution<size_t> dist(0, enum_values.size() - 1);
        for (size_t i = 0; i < count; ++i)
            values.push_back(enum_values[dist(gen)].second);
    };

    const size_t lookup_count = 10000;  // More lookups for better accuracy

    fillNames(g_tiny_enum_int8, g_lookup_names_tiny, lookup_count);
    fillNames(g_small_enum_int8, g_lookup_names_small, lookup_count);
    fillNames(g_medium_enum_int8, g_lookup_names_medium, lookup_count);
    fillNames(g_large_enum_int8, g_lookup_names_large, lookup_count);
    fillNames(g_xlarge_enum_int8, g_lookup_names_xlarge, lookup_count);
    fillNames(g_max_enum_int8, g_lookup_names_max, lookup_count);

    fillValues(g_tiny_enum_int8, g_lookup_values_tiny, lookup_count);
    fillValues(g_small_enum_int8, g_lookup_values_small, lookup_count);
    fillValues(g_medium_enum_int8, g_lookup_values_medium, lookup_count);
    fillValues(g_large_enum_int8, g_lookup_values_large, lookup_count);
    fillValues(g_xlarge_enum_int8, g_lookup_values_xlarge, lookup_count);
    fillValues(g_max_enum_int8, g_lookup_values_max, lookup_count);
}

// Initialize lookup data at startup
struct LookupDataInitializer {
    LookupDataInitializer() { initializeLookupData(); }
} g_lookup_data_initializer;

}  // anonymous namespace


// ============================================================================
// CONSTRUCTION BENCHMARKS
// ============================================================================

// Note: We use explicit int8_t functions instead of template instantiation
// because BENCHMARK_CAPTURE macro doesn't work with explicit template arguments
// like BM_Function<int8_t> - the angle brackets break macro expansion.

// --- Current Implementation ---
static void BM_Construction_Current_Int8(benchmark::State & state, const EnumValues<int8_t> & values)
{
    for (auto _ : state)
    {
        EnumStorageCurrent<int8_t> storage(values);
        benchmark::DoNotOptimize(storage);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(values.size()));
    state.counters["entries"] = static_cast<double>(values.size());
}

// --- HashMap Both ---
static void BM_Construction_HashMapBoth_Int8(benchmark::State & state, const EnumValues<int8_t> & values)
{
    for (auto _ : state)
    {
        EnumStorageHashMapBoth<int8_t> storage(values);
        benchmark::DoNotOptimize(storage);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(values.size()));
    state.counters["entries"] = static_cast<double>(values.size());
}

// --- Compact Sorted ---
static void BM_Construction_CompactSorted_Int8(benchmark::State & state, const EnumValues<int8_t> & values)
{
    for (auto _ : state)
    {
        EnumStorageCompactSorted<int8_t> storage(values);
        benchmark::DoNotOptimize(storage);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(values.size()));
    state.counters["entries"] = static_cast<double>(values.size());
}

// --- Compact Direct Array (Enum8 only) ---
static void BM_Construction_CompactDirectArray(benchmark::State & state, const EnumValues<int8_t> & values)
{
    for (auto _ : state)
    {
        EnumStorageCompactDirectArray storage(values);
        benchmark::DoNotOptimize(storage);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(values.size()));
    state.counters["entries"] = static_cast<double>(values.size());
}

// --- Minimal Compact ---
static void BM_Construction_MinimalCompact_Int8(benchmark::State & state, const EnumValues<int8_t> & values)
{
    for (auto _ : state)
    {
        EnumStorageMinimalCompact<int8_t> storage(values);
        benchmark::DoNotOptimize(storage);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(values.size()));
    state.counters["entries"] = static_cast<double>(values.size());
}

// Register construction benchmarks for different enum sizes
#define REGISTER_CONSTRUCTION_BENCHMARKS(suffix, values) \
    BENCHMARK_CAPTURE(BM_Construction_Current_Int8, Current_##suffix, values); \
    BENCHMARK_CAPTURE(BM_Construction_HashMapBoth_Int8, HashMapBoth_##suffix, values); \
    BENCHMARK_CAPTURE(BM_Construction_CompactSorted_Int8, CompactSorted_##suffix, values); \
    BENCHMARK_CAPTURE(BM_Construction_CompactDirectArray, CompactDirect_##suffix, values); \
    BENCHMARK_CAPTURE(BM_Construction_MinimalCompact_Int8, MinimalCompact_##suffix, values);

REGISTER_CONSTRUCTION_BENCHMARKS(Small_10, g_small_enum_int8)
REGISTER_CONSTRUCTION_BENCHMARKS(Medium_50, g_medium_enum_int8)
REGISTER_CONSTRUCTION_BENCHMARKS(Large_100, g_large_enum_int8)
REGISTER_CONSTRUCTION_BENCHMARKS(XLarge_200, g_xlarge_enum_int8)
REGISTER_CONSTRUCTION_BENCHMARKS(Category_15, g_category_enum_int8)


// ============================================================================
// NAME → VALUE LOOKUP BENCHMARKS
// ============================================================================

// --- Current Implementation ---
static void BM_LookupByName_Current_Int8(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<std::string> & names)
{
    EnumStorageCurrent<int8_t> storage(values);
    size_t idx = 0;

    for (auto _ : state)
    {
        auto result = storage.tryGetValue(names[idx]);
        benchmark::DoNotOptimize(result);
        idx = (idx + 1) % names.size();
    }
    state.SetItemsProcessed(state.iterations());
}

// --- HashMap Both ---
static void BM_LookupByName_HashMapBoth_Int8(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<std::string> & names)
{
    EnumStorageHashMapBoth<int8_t> storage(values);
    size_t idx = 0;

    for (auto _ : state)
    {
        auto result = storage.tryGetValue(names[idx]);
        benchmark::DoNotOptimize(result);
        idx = (idx + 1) % names.size();
    }
    state.SetItemsProcessed(state.iterations());
}

// --- Compact Sorted ---
static void BM_LookupByName_CompactSorted_Int8(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<std::string> & names)
{
    EnumStorageCompactSorted<int8_t> storage(values);
    size_t idx = 0;

    for (auto _ : state)
    {
        auto result = storage.tryGetValue(names[idx]);
        benchmark::DoNotOptimize(result);
        idx = (idx + 1) % names.size();
    }
    state.SetItemsProcessed(state.iterations());
}

// --- Compact Direct Array ---
static void BM_LookupByName_CompactDirectArray(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<std::string> & names)
{
    EnumStorageCompactDirectArray storage(values);
    size_t idx = 0;

    for (auto _ : state)
    {
        auto result = storage.tryGetValue(names[idx]);
        benchmark::DoNotOptimize(result);
        idx = (idx + 1) % names.size();
    }
    state.SetItemsProcessed(state.iterations());
}

// --- Minimal Compact ---
static void BM_LookupByName_MinimalCompact_Int8(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<std::string> & names)
{
    EnumStorageMinimalCompact<int8_t> storage(values);
    size_t idx = 0;

    for (auto _ : state)
    {
        auto result = storage.tryGetValue(names[idx]);
        benchmark::DoNotOptimize(result);
        idx = (idx + 1) % names.size();
    }
    state.SetItemsProcessed(state.iterations());
}

// Register name lookup benchmarks
#define REGISTER_NAME_LOOKUP_BENCHMARKS(suffix, values, names) \
    BENCHMARK_CAPTURE(BM_LookupByName_Current_Int8, Current_##suffix, values, names); \
    BENCHMARK_CAPTURE(BM_LookupByName_HashMapBoth_Int8, HashMapBoth_##suffix, values, names); \
    BENCHMARK_CAPTURE(BM_LookupByName_CompactSorted_Int8, CompactSorted_##suffix, values, names); \
    BENCHMARK_CAPTURE(BM_LookupByName_CompactDirectArray, CompactDirect_##suffix, values, names); \
    BENCHMARK_CAPTURE(BM_LookupByName_MinimalCompact_Int8, MinimalCompact_##suffix, values, names);

REGISTER_NAME_LOOKUP_BENCHMARKS(Small_10, g_small_enum_int8, g_lookup_names_small)
REGISTER_NAME_LOOKUP_BENCHMARKS(Medium_50, g_medium_enum_int8, g_lookup_names_medium)
REGISTER_NAME_LOOKUP_BENCHMARKS(Large_100, g_large_enum_int8, g_lookup_names_large)


// ============================================================================
// VALUE → NAME LOOKUP BENCHMARKS
// ============================================================================

// --- Current Implementation ---
static void BM_LookupByValue_Current_Int8(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<int8_t> & lookup_values)
{
    EnumStorageCurrent<int8_t> storage(values);
    size_t idx = 0;

    for (auto _ : state)
    {
        auto result = storage.tryGetName(lookup_values[idx]);
        benchmark::DoNotOptimize(result);
        idx = (idx + 1) % lookup_values.size();
    }
    state.SetItemsProcessed(state.iterations());
}

// --- HashMap Both ---
static void BM_LookupByValue_HashMapBoth_Int8(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<int8_t> & lookup_values)
{
    EnumStorageHashMapBoth<int8_t> storage(values);
    size_t idx = 0;

    for (auto _ : state)
    {
        auto result = storage.tryGetName(lookup_values[idx]);
        benchmark::DoNotOptimize(result);
        idx = (idx + 1) % lookup_values.size();
    }
    state.SetItemsProcessed(state.iterations());
}

// --- Compact Sorted ---
static void BM_LookupByValue_CompactSorted_Int8(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<int8_t> & lookup_values)
{
    EnumStorageCompactSorted<int8_t> storage(values);
    size_t idx = 0;

    for (auto _ : state)
    {
        auto result = storage.tryGetName(lookup_values[idx]);
        benchmark::DoNotOptimize(result);
        idx = (idx + 1) % lookup_values.size();
    }
    state.SetItemsProcessed(state.iterations());
}

// --- Compact Direct Array ---
static void BM_LookupByValue_CompactDirectArray(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<int8_t> & lookup_values)
{
    EnumStorageCompactDirectArray storage(values);
    size_t idx = 0;

    for (auto _ : state)
    {
        auto result = storage.tryGetName(lookup_values[idx]);
        benchmark::DoNotOptimize(result);
        idx = (idx + 1) % lookup_values.size();
    }
    state.SetItemsProcessed(state.iterations());
}

// --- Minimal Compact (linear scan) ---
static void BM_LookupByValue_MinimalCompact_Int8(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<int8_t> & lookup_values)
{
    EnumStorageMinimalCompact<int8_t> storage(values);
    size_t idx = 0;

    for (auto _ : state)
    {
        auto result = storage.tryGetName(lookup_values[idx]);
        benchmark::DoNotOptimize(result);
        idx = (idx + 1) % lookup_values.size();
    }
    state.SetItemsProcessed(state.iterations());
}

// Register value lookup benchmarks
#define REGISTER_VALUE_LOOKUP_BENCHMARKS(suffix, values, lookup_vals) \
    BENCHMARK_CAPTURE(BM_LookupByValue_Current_Int8, Current_##suffix, values, lookup_vals); \
    BENCHMARK_CAPTURE(BM_LookupByValue_HashMapBoth_Int8, HashMapBoth_##suffix, values, lookup_vals); \
    BENCHMARK_CAPTURE(BM_LookupByValue_CompactSorted_Int8, CompactSorted_##suffix, values, lookup_vals); \
    BENCHMARK_CAPTURE(BM_LookupByValue_CompactDirectArray, CompactDirect_##suffix, values, lookup_vals); \
    BENCHMARK_CAPTURE(BM_LookupByValue_MinimalCompact_Int8, MinimalCompact_##suffix, values, lookup_vals);

REGISTER_VALUE_LOOKUP_BENCHMARKS(Small_10, g_small_enum_int8, g_lookup_values_small)
REGISTER_VALUE_LOOKUP_BENCHMARKS(Medium_50, g_medium_enum_int8, g_lookup_values_medium)
REGISTER_VALUE_LOOKUP_BENCHMARKS(Large_100, g_large_enum_int8, g_lookup_values_large)


// ============================================================================
// MEMORY USAGE REPORTING
// ============================================================================

static void BM_MemoryUsage_Report(benchmark::State & state)
{
    // This benchmark just reports memory usage, doesn't measure performance
    for (auto _ : state)
    {
        // Small enum
        {
            EnumStorageCurrent<int8_t> current(g_small_enum_int8);
            EnumStorageHashMapBoth<int8_t> hashmap_both(g_small_enum_int8);
            EnumStorageCompactSorted<int8_t> compact_sorted(g_small_enum_int8);
            EnumStorageCompactDirectArray compact_direct(g_small_enum_int8);
            EnumStorageMinimalCompact<int8_t> minimal_compact(g_small_enum_int8);

            state.counters["Small_Current"] = static_cast<double>(current.memoryUsage());
            state.counters["Small_HashMapBoth"] = static_cast<double>(hashmap_both.memoryUsage());
            state.counters["Small_CompactSorted"] = static_cast<double>(compact_sorted.memoryUsage());
            state.counters["Small_CompactDirect"] = static_cast<double>(compact_direct.memoryUsage());
            state.counters["Small_MinimalCompact"] = static_cast<double>(minimal_compact.memoryUsage());
        }

        // Medium enum
        {
            EnumStorageCurrent<int8_t> current(g_medium_enum_int8);
            EnumStorageHashMapBoth<int8_t> hashmap_both(g_medium_enum_int8);
            EnumStorageCompactSorted<int8_t> compact_sorted(g_medium_enum_int8);
            EnumStorageCompactDirectArray compact_direct(g_medium_enum_int8);
            EnumStorageMinimalCompact<int8_t> minimal_compact(g_medium_enum_int8);

            state.counters["Medium_Current"] = static_cast<double>(current.memoryUsage());
            state.counters["Medium_HashMapBoth"] = static_cast<double>(hashmap_both.memoryUsage());
            state.counters["Medium_CompactSorted"] = static_cast<double>(compact_sorted.memoryUsage());
            state.counters["Medium_CompactDirect"] = static_cast<double>(compact_direct.memoryUsage());
            state.counters["Medium_MinimalCompact"] = static_cast<double>(minimal_compact.memoryUsage());
        }

        // Large enum
        {
            EnumStorageCurrent<int8_t> current(g_large_enum_int8);
            EnumStorageHashMapBoth<int8_t> hashmap_both(g_large_enum_int8);
            EnumStorageCompactSorted<int8_t> compact_sorted(g_large_enum_int8);
            EnumStorageCompactDirectArray compact_direct(g_large_enum_int8);
            EnumStorageMinimalCompact<int8_t> minimal_compact(g_large_enum_int8);

            state.counters["Large_Current"] = static_cast<double>(current.memoryUsage());
            state.counters["Large_HashMapBoth"] = static_cast<double>(hashmap_both.memoryUsage());
            state.counters["Large_CompactSorted"] = static_cast<double>(compact_sorted.memoryUsage());
            state.counters["Large_CompactDirect"] = static_cast<double>(compact_direct.memoryUsage());
            state.counters["Large_MinimalCompact"] = static_cast<double>(minimal_compact.memoryUsage());
        }

    }
}
BENCHMARK(BM_MemoryUsage_Report)->Iterations(1);


// ============================================================================
// MIXED WORKLOAD BENCHMARKS (simulating parsing)
// ============================================================================

// 80% name→value lookups, 20% constructions
static void BM_MixedWorkload_Current_Int8(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<std::string> & names)
{
    size_t idx = 0;
    size_t construct_count = 0;

    for (auto _ : state)
    {
        if ((idx % 5) == 0)  // 20% constructions
        {
            EnumStorageCurrent<int8_t> storage(values);
            benchmark::DoNotOptimize(storage);
            ++construct_count;
        }
        else  // 80% lookups
        {
            static EnumStorageCurrent<int8_t> storage(values);
            auto result = storage.tryGetValue(names[idx % names.size()]);
            benchmark::DoNotOptimize(result);
        }
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
    state.counters["constructions"] = static_cast<double>(construct_count);
}

static void BM_MixedWorkload_CompactSorted_Int8(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<std::string> & names)
{
    size_t idx = 0;
    size_t construct_count = 0;

    for (auto _ : state)
    {
        if ((idx % 5) == 0)
        {
            EnumStorageCompactSorted<int8_t> storage(values);
            benchmark::DoNotOptimize(storage);
            ++construct_count;
        }
        else
        {
            static EnumStorageCompactSorted<int8_t> storage(values);
            auto result = storage.tryGetValue(names[idx % names.size()]);
            benchmark::DoNotOptimize(result);
        }
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
    state.counters["constructions"] = static_cast<double>(construct_count);
}

static void BM_MixedWorkload_CompactDirectArray(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<std::string> & names)
{
    size_t idx = 0;
    size_t construct_count = 0;

    for (auto _ : state)
    {
        if ((idx % 5) == 0)
        {
            EnumStorageCompactDirectArray storage(values);
            benchmark::DoNotOptimize(storage);
            ++construct_count;
        }
        else
        {
            static EnumStorageCompactDirectArray storage(values);
            auto result = storage.tryGetValue(names[idx % names.size()]);
            benchmark::DoNotOptimize(result);
        }
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
    state.counters["constructions"] = static_cast<double>(construct_count);
}

BENCHMARK_CAPTURE(BM_MixedWorkload_Current_Int8, Current_Medium, g_medium_enum_int8, g_lookup_names_medium);
BENCHMARK_CAPTURE(BM_MixedWorkload_CompactSorted_Int8, CompactSorted_Medium, g_medium_enum_int8, g_lookup_names_medium);
BENCHMARK_CAPTURE(BM_MixedWorkload_CompactDirectArray, CompactDirect_Medium, g_medium_enum_int8, g_lookup_names_medium);


// ============================================================================
// SEQUENTIAL ACCESS BENCHMARKS (cache-friendly patterns)
// ============================================================================

static void BM_SequentialNameLookup_Current_Int8(benchmark::State & state, const EnumValues<int8_t> & values)
{
    EnumStorageCurrent<int8_t> storage(values);
    const auto & stored_values = storage.getValues();

    for (auto _ : state)
    {
        for (const auto & v : stored_values)
        {
            auto result = storage.tryGetValue(v.first);
            benchmark::DoNotOptimize(result);
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(stored_values.size()));
}

static void BM_SequentialNameLookup_CompactSorted_Int8(benchmark::State & state, const EnumValues<int8_t> & values)
{
    EnumStorageCompactSorted<int8_t> storage(values);
    const auto stored_values = storage.getValues();

    for (auto _ : state)
    {
        for (const auto & v : stored_values)
        {
            auto result = storage.tryGetValue(v.first);
            benchmark::DoNotOptimize(result);
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(stored_values.size()));
}

BENCHMARK_CAPTURE(BM_SequentialNameLookup_Current_Int8, Current_Large, g_large_enum_int8);
BENCHMARK_CAPTURE(BM_SequentialNameLookup_CompactSorted_Int8, CompactSorted_Large, g_large_enum_int8);


// ============================================================================
// BATCH LOOKUP BENCHMARKS
// ============================================================================

static void BM_BatchNameLookup_Current_Int8(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<std::string> & names)
{
    EnumStorageCurrent<int8_t> storage(values);

    for (auto _ : state)
    {
        int8_t sum = 0;
        for (const auto & name : names)
        {
            if (auto result = storage.tryGetValue(name))
                sum += *result;
        }
        benchmark::DoNotOptimize(sum);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(names.size()));
}

static void BM_BatchNameLookup_CompactSorted_Int8(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<std::string> & names)
{
    EnumStorageCompactSorted<int8_t> storage(values);

    for (auto _ : state)
    {
        int8_t sum = 0;
        for (const auto & name : names)
        {
            if (auto result = storage.tryGetValue(name))
                sum += *result;
        }
        benchmark::DoNotOptimize(sum);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(names.size()));
}

static void BM_BatchNameLookup_CompactDirectArray(benchmark::State & state, const EnumValues<int8_t> & values, const std::vector<std::string> & names)
{
    EnumStorageCompactDirectArray storage(values);

    for (auto _ : state)
    {
        int8_t sum = 0;
        for (const auto & name : names)
        {
            if (auto result = storage.tryGetValue(name))
                sum += *result;
        }
        benchmark::DoNotOptimize(sum);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(names.size()));
}

BENCHMARK_CAPTURE(BM_BatchNameLookup_Current_Int8, Current_Large_1000, g_large_enum_int8, g_lookup_names_large);
BENCHMARK_CAPTURE(BM_BatchNameLookup_CompactSorted_Int8, CompactSorted_Large_1000, g_large_enum_int8, g_lookup_names_large);
BENCHMARK_CAPTURE(BM_BatchNameLookup_CompactDirectArray, CompactDirect_Large_1000, g_large_enum_int8, g_lookup_names_large);


// ============================================================================
// NON-EXISTING KEY LOOKUP BENCHMARKS
// ============================================================================

static void BM_LookupNonExisting_Current_Int8(benchmark::State & state, const EnumValues<int8_t> & values)
{
    EnumStorageCurrent<int8_t> storage(values);
    std::vector<std::string> non_existing = {"__nonexistent__", "zzz_not_here", "INVALID_KEY"};
    size_t idx = 0;

    for (auto _ : state)
    {
        auto result = storage.tryGetValue(non_existing[idx % non_existing.size()]);
        benchmark::DoNotOptimize(result);
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}

static void BM_LookupNonExisting_CompactSorted_Int8(benchmark::State & state, const EnumValues<int8_t> & values)
{
    EnumStorageCompactSorted<int8_t> storage(values);
    std::vector<std::string> non_existing = {"__nonexistent__", "zzz_not_here", "INVALID_KEY"};
    size_t idx = 0;

    for (auto _ : state)
    {
        auto result = storage.tryGetValue(non_existing[idx % non_existing.size()]);
        benchmark::DoNotOptimize(result);
        ++idx;
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_CAPTURE(BM_LookupNonExisting_Current_Int8, Current_Large, g_large_enum_int8);
BENCHMARK_CAPTURE(BM_LookupNonExisting_CompactSorted_Int8, CompactSorted_Large, g_large_enum_int8);


// ============================================================================
// COMPARISON TABLE HELPERS
// ============================================================================

namespace
{

// Color codes for terminal output
const char * const GREEN = "\033[32m";
const char * const RED = "\033[31m";
const char * const YELLOW = "\033[33m";
const char * const RESET = "\033[0m";
const char * const BOLD = "\033[1m";

// Format ratio with fixed visual width (pads to account for ANSI codes)
std::string formatRatioFixed(double ratio, bool lower_is_better, int width)
{
    std::ostringstream num_ss;
    num_ss << std::fixed << std::setprecision(2) << ratio << "x";
    std::string num_str = num_ss.str();

    // Pad to desired width
    int padding = width - static_cast<int>(num_str.length());
    std::string padded = std::string(padding > 0 ? padding : 0, ' ') + num_str;

    // Add color
    const char * color;
    if (ratio < 0.95)
        color = (lower_is_better ? GREEN : RED);
    else if (ratio > 1.05)
        color = (lower_is_better ? RED : GREEN);
    else
        color = YELLOW;

    return std::string(color) + padded + RESET;
}

std::string formatBytes(size_t bytes)
{
    std::ostringstream ss;
    if (bytes < 1024)
        ss << bytes << " B";
    else if (bytes < 1024 * 1024)
        ss << std::fixed << std::setprecision(1) << (static_cast<double>(bytes) / 1024.0) << " KB";
    else
        ss << std::fixed << std::setprecision(1) << (static_cast<double>(bytes) / (1024.0 * 1024.0)) << " MB";
    return ss.str();
}

// More iterations for better accuracy
constexpr size_t CONSTRUCTION_ITERATIONS = 50000;
constexpr size_t LOOKUP_ITERATIONS = 500000;

template <typename Storage>
double measureConstruction(const EnumValues<int8_t> & values, size_t iterations = CONSTRUCTION_ITERATIONS)
{
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < iterations; ++i)
    {
        Storage storage(values);
        benchmark::DoNotOptimize(storage);
    }
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::nano>(end - start).count() / static_cast<double>(iterations);
}

template <typename Storage>
double measureNameLookup(const Storage & storage, const std::vector<std::string> & names, size_t iterations = LOOKUP_ITERATIONS)
{
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < iterations; ++i)
    {
        auto result = storage.tryGetValue(names[i % names.size()]);
        benchmark::DoNotOptimize(result);
    }
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::nano>(end - start).count() / static_cast<double>(iterations);
}

template <typename Storage>
double measureValueLookup(const Storage & storage, const std::vector<int8_t> & values, size_t iterations = LOOKUP_ITERATIONS)
{
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < iterations; ++i)
    {
        auto result = storage.tryGetName(values[i % values.size()]);
        benchmark::DoNotOptimize(result);
    }
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::nano>(end - start).count() / static_cast<double>(iterations);
}

void printComparisonTable()
{
    const char * implementations[] = {"Current", "HashMapBoth", "CompactSorted", "CompactDirect", "MinimalCompact"};
    const int num_impl = 5;
    const int num_sizes = 6;

    // Size variants: tiny(3), small(10), medium(50), large(100), xlarge(200), max(255)
    // Column width is 9 chars - keep names at 9 or less
    const char * size_names[] = {"  Tiny:3", " Small:10", "  Med:50", "Large:100", "  XL:200", " Max:255"};
    const EnumValues<int8_t> * enum_data[] = {
        &g_tiny_enum_int8, &g_small_enum_int8, &g_medium_enum_int8,
        &g_large_enum_int8, &g_xlarge_enum_int8, &g_max_enum_int8
    };
    const std::vector<std::string> * lookup_names[] = {
        &g_lookup_names_tiny, &g_lookup_names_small, &g_lookup_names_medium,
        &g_lookup_names_large, &g_lookup_names_xlarge, &g_lookup_names_max
    };
    const std::vector<int8_t> * lookup_values[] = {
        &g_lookup_values_tiny, &g_lookup_values_small, &g_lookup_values_medium,
        &g_lookup_values_large, &g_lookup_values_xlarge, &g_lookup_values_max
    };

    // Measure all metrics
    struct Metrics
    {
        size_t memory[num_sizes];
        double construct[num_sizes];
        double name_lookup[num_sizes];
        double value_lookup[num_sizes];
    };

    Metrics metrics[num_impl];

    std::cout << "\n" << BOLD << "Running benchmarks with " << CONSTRUCTION_ITERATIONS << " construction iterations and "
              << LOOKUP_ITERATIONS << " lookup iterations..." << RESET << "\n";
    std::cout << "This may take a minute...\n\n";

    // Current
    std::cout << "  Measuring Current...        \r" << std::flush;
    for (int s = 0; s < num_sizes; ++s)
    {
        EnumStorageCurrent<int8_t> storage(*enum_data[s]);
        metrics[0].memory[s] = storage.memoryUsage();
        metrics[0].construct[s] = measureConstruction<EnumStorageCurrent<int8_t>>(*enum_data[s]);
        metrics[0].name_lookup[s] = measureNameLookup(storage, *lookup_names[s]);
        metrics[0].value_lookup[s] = measureValueLookup(storage, *lookup_values[s]);
    }

    // HashMapBoth
    std::cout << "  Measuring HashMapBoth...    \r" << std::flush;
    for (int s = 0; s < num_sizes; ++s)
    {
        EnumStorageHashMapBoth<int8_t> storage(*enum_data[s]);
        metrics[1].memory[s] = storage.memoryUsage();
        metrics[1].construct[s] = measureConstruction<EnumStorageHashMapBoth<int8_t>>(*enum_data[s]);
        metrics[1].name_lookup[s] = measureNameLookup(storage, *lookup_names[s]);
        metrics[1].value_lookup[s] = measureValueLookup(storage, *lookup_values[s]);
    }

    // CompactSorted
    std::cout << "  Measuring CompactSorted...  \r" << std::flush;
    for (int s = 0; s < num_sizes; ++s)
    {
        EnumStorageCompactSorted<int8_t> storage(*enum_data[s]);
        metrics[2].memory[s] = storage.memoryUsage();
        metrics[2].construct[s] = measureConstruction<EnumStorageCompactSorted<int8_t>>(*enum_data[s]);
        metrics[2].name_lookup[s] = measureNameLookup(storage, *lookup_names[s]);
        metrics[2].value_lookup[s] = measureValueLookup(storage, *lookup_values[s]);
    }

    // CompactDirect
    std::cout << "  Measuring CompactDirect...  \r" << std::flush;
    for (int s = 0; s < num_sizes; ++s)
    {
        EnumStorageCompactDirectArray storage(*enum_data[s]);
        metrics[3].memory[s] = storage.memoryUsage();
        metrics[3].construct[s] = measureConstruction<EnumStorageCompactDirectArray>(*enum_data[s]);
        metrics[3].name_lookup[s] = measureNameLookup(storage, *lookup_names[s]);
        metrics[3].value_lookup[s] = measureValueLookup(storage, *lookup_values[s]);
    }

    // MinimalCompact
    std::cout << "  Measuring MinimalCompact... \r" << std::flush;
    for (int s = 0; s < num_sizes; ++s)
    {
        EnumStorageMinimalCompact<int8_t> storage(*enum_data[s]);
        metrics[4].memory[s] = storage.memoryUsage();
        metrics[4].construct[s] = measureConstruction<EnumStorageMinimalCompact<int8_t>>(*enum_data[s]);
        metrics[4].name_lookup[s] = measureNameLookup(storage, *lookup_names[s]);
        metrics[4].value_lookup[s] = measureValueLookup(storage, *lookup_values[s]);
    }

    std::cout << "                              \r" << std::flush;

    // Print comparison table header
    std::cout << "\n" << BOLD << "╔════════════════════════════════════════════════════════════════════════════════════════════════════╗" << RESET << "\n";
    std::cout << BOLD << "║                            ENUM STORAGE IMPLEMENTATION COMPARISON                                  ║" << RESET << "\n";
    std::cout << BOLD << "║                       (" << CONSTRUCTION_ITERATIONS << " constructions, " << LOOKUP_ITERATIONS << " lookups per test)                                   ║" << RESET << "\n";
    std::cout << BOLD << "╚════════════════════════════════════════════════════════════════════════════════════════════════════╝" << RESET << "\n";

    // Helper lambda for printing a metric table
    // Column widths: Implementation=14, Size cols=9 each, vs Current=12
    auto printTable = [&](const char * title, const char * description, auto getMetric, bool lower_is_better, bool is_time) {
        std::cout << "\n" << BOLD << "┌────────────────────────────────────────────────────────────────────────────────────────────────────┐" << RESET << "\n";
        std::cout << BOLD << "│ " << std::setw(98) << std::left << (std::string(title) + " (" + description + ")") << "│" << RESET << "\n";
        std::cout << BOLD << "├────────────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────────┤" << RESET << "\n";
        std::cout << BOLD << "│ Implementation │" << std::setw(9) << std::right << size_names[0] << " │" << std::setw(9) << size_names[1]
                  << " │" << std::setw(9) << size_names[2] << " │" << std::setw(9) << size_names[3]
                  << " │" << std::setw(9) << size_names[4] << " │" << std::setw(9) << size_names[5] << " │  vs Current  │" << RESET << "\n";
        std::cout << BOLD << "├────────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤" << RESET << "\n";

        for (int i = 0; i < num_impl; ++i)
        {
            double sum_ratio = 0;
            std::cout << "│ " << std::setw(14) << std::left << implementations[i];

            for (int s = 0; s < num_sizes; ++s)
            {
                double val = getMetric(metrics[i], s);
                double baseline = getMetric(metrics[0], s);
                sum_ratio += val / baseline;

                std::ostringstream ss;
                if (is_time)
                    ss << std::fixed << std::setprecision(val < 10 ? 1 : 0) << val << " ns";
                else
                    ss << formatBytes(static_cast<size_t>(val));

                std::cout << " │" << std::setw(9) << std::right << ss.str();
            }

            double avg_ratio = sum_ratio / num_sizes;
            std::cout << " │ " << formatRatioFixed(avg_ratio, lower_is_better, 12) << " │\n";
        }

        std::cout << BOLD << "└────────────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────────┘" << RESET << "\n";
    };

    // Print all tables
    printTable("MEMORY USAGE", "lower is better, green = less memory", [](const Metrics & m, int s) { return static_cast<double>(m.memory[s]); }, true, false);
    printTable("CONSTRUCTION TIME", "lower is better, green = faster", [](const Metrics & m, int s) { return m.construct[s]; }, true, true);
    printTable("NAME → VALUE LOOKUP", "lower is better, green = faster", [](const Metrics & m, int s) { return m.name_lookup[s]; }, true, true);
    printTable("VALUE → NAME LOOKUP", "lower is better, green = faster", [](const Metrics & m, int s) { return m.value_lookup[s]; }, true, true);

    // Summary table
    std::cout << "\n" << BOLD << "┌────────────────────────────────────────────────────────────────────────────────────────────────────┐" << RESET << "\n";
    std::cout << BOLD << "│ OVERALL SUMMARY (average across all sizes)                                                         │" << RESET << "\n";
    std::cout << BOLD << "├────────────────┬──────────┬──────────┬──────────┬──────────┬────────────────────────────────────────┤" << RESET << "\n";
    std::cout << BOLD << "│ Implementation │  Memory  │ Construct│ Name Lkp │ Value Lkp│               Best For                 │" << RESET << "\n";
    std::cout << BOLD << "├────────────────┼──────────┼──────────┼──────────┼──────────┼────────────────────────────────────────┤" << RESET << "\n";

    for (int i = 0; i < num_impl; ++i)
    {
        double mem_ratio = 0, const_ratio = 0, name_ratio = 0, val_ratio = 0;
        for (int s = 0; s < num_sizes; ++s)
        {
            mem_ratio += static_cast<double>(metrics[i].memory[s]) / static_cast<double>(metrics[0].memory[s]);
            const_ratio += metrics[i].construct[s] / metrics[0].construct[s];
            name_ratio += metrics[i].name_lookup[s] / metrics[0].name_lookup[s];
            val_ratio += metrics[i].value_lookup[s] / metrics[0].value_lookup[s];
        }
        mem_ratio /= num_sizes;
        const_ratio /= num_sizes;
        name_ratio /= num_sizes;
        val_ratio /= num_sizes;

        const char * best_for;
        if (i == 0) best_for = "Baseline (current implementation)";
        else if (i == 1) best_for = "Fast construction + value lookup";
        else if (i == 2) best_for = "Memory-efficient, moderate lookups";
        else if (i == 3) best_for = "Memory-efficient + fast val lookup";
        else best_for = "Minimum memory (slow val lookup)";

        std::cout << "│ " << std::setw(14) << std::left << implementations[i]
                  << " │ " << formatRatioFixed(mem_ratio, true, 8)
                  << " │ " << formatRatioFixed(const_ratio, true, 8)
                  << " │ " << formatRatioFixed(name_ratio, true, 8)
                  << " │ " << formatRatioFixed(val_ratio, true, 8)
                  << " │ " << std::setw(38) << std::left << best_for << " │\n";
    }
    std::cout << BOLD << "└────────────────┴──────────┴──────────┴──────────┴──────────┴────────────────────────────────────────┘" << RESET << "\n";

    // Legend
    std::cout << "\n" << BOLD << "LEGEND:" << RESET << "\n";
    std::cout << "  • " << GREEN << "Green (< 0.95x)" << RESET << " = better than Current baseline\n";
    std::cout << "  • " << RED << "Red (> 1.05x)" << RESET << " = worse than Current baseline\n";
    std::cout << "  • " << YELLOW << "Yellow (0.95x - 1.05x)" << RESET << " = similar to Current\n";
    std::cout << "\n";
}

}  // namespace

// ============================================================================
// MAIN - with custom comparison table
// ============================================================================

int main(int argc, char ** argv)
{
    // Check if user wants just the comparison table
    bool comparison_only = false;
    for (int i = 1; i < argc; ++i)
    {
        if (std::string(argv[i]) == "--comparison" || std::string(argv[i]) == "-c")
        {
            comparison_only = true;
            break;
        }
    }

    // Print comparison table
    printComparisonTable();

    if (comparison_only)
    {
        std::cout << "Skipping detailed Google Benchmark output (use without --comparison for full output)\n";
        return 0;
    }

    std::cout << "\n" << BOLD << "=== Detailed Google Benchmark Results ===" << RESET << "\n\n";

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();

    return 0;
}
