/// Microbenchmark for IColumn::computeHashInto.
///
/// Measures per-row throughput for the hash-production step used by
/// hash-partitioning operators (e.g. BufferedShardByHashTransform).
///
/// Each benchmark name encodes: <ColumnType>_K<K>_B<batch>
///   K = number of key columns chained
///   B = batch size (rows)
///
/// Run with:
///   ./benchmark_compute_hash_into --benchmark_filter=.
///   ./benchmark_compute_hash_into --benchmark_filter=UInt32 --benchmark_format=json

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/JoinUtils.h>
#include <Common/MapToRange.h>
#include <Common/PODArray.h>
#include <Common/randomSeed.h>

#include <benchmark/benchmark.h>

#include <random>
#include <string>
#include <vector>

using namespace DB;

namespace
{

/// ─── Data generators ──────────────────────────────────────────────────────

std::vector<ColumnPtr> makeUInt32Columns(size_t K, size_t n)
{
    std::mt19937 rng(randomSeed());
    std::vector<ColumnPtr> cols;
    for (size_t k = 0; k < K; ++k)
    {
        auto col = ColumnUInt32::create();
        for (size_t i = 0; i < n; ++i)
            col->insert(static_cast<UInt64>(rng()));
        cols.push_back(std::move(col));
    }
    return cols;
}

std::vector<ColumnPtr> makeUInt64Columns(size_t K, size_t n)
{
    std::mt19937_64 rng(randomSeed());
    std::vector<ColumnPtr> cols;
    for (size_t k = 0; k < K; ++k)
    {
        auto col = ColumnUInt64::create();
        for (size_t i = 0; i < n; ++i)
            col->insert(rng());
        cols.push_back(std::move(col));
    }
    return cols;
}

std::vector<ColumnPtr> makeStringColumns(size_t K, size_t n, size_t avg_len = 16)
{
    std::mt19937 rng(randomSeed());
    std::uniform_int_distribution<size_t> len_dist(4, 2 * avg_len);
    std::vector<ColumnPtr> cols;
    for (size_t k = 0; k < K; ++k)
    {
        auto col = ColumnString::create();
        for (size_t i = 0; i < n; ++i)
        {
            const size_t len = len_dist(rng);
            std::string s(len, static_cast<char>('a' + (rng() % 26)));
            col->insertData(s.data(), s.size());
        }
        cols.push_back(std::move(col));
    }
    return cols;
}

std::vector<ColumnPtr> makeNullableUInt32Columns(size_t K, size_t n)
{
    std::mt19937 rng(randomSeed());
    std::vector<ColumnPtr> cols;
    for (size_t k = 0; k < K; ++k)
    {
        auto nested = ColumnUInt32::create();
        auto null_map = ColumnUInt8::create();
        for (size_t i = 0; i < n; ++i)
        {
            nested->insert(static_cast<UInt64>(rng()));
            null_map->insert(static_cast<UInt64>(rng() % 10 == 0 ? 1 : 0)); // ~10% nulls
        }
        cols.push_back(ColumnNullable::create(std::move(nested), std::move(null_map)));
    }
    return cols;
}

/// ─── computeHashInto benchmark ────────────────────────────────────────────

void BM_ComputeHashInto(benchmark::State & state, const std::vector<ColumnPtr> & cols)
{
    const size_t n = cols[0]->size();
    const size_t ncols = cols.size();
    PaddedPODArray<UInt32> hash_buf(n);

    for (auto _ : state)
    {
        bool initial = true;
        for (size_t k = 0; k < ncols; ++k)
        {
            cols[k]->computeHashInto(0, n, hash_buf.data(), initial);
            initial = false;
        }
        benchmark::DoNotOptimize(hash_buf.data());
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(n));
    state.counters["rows/iter"] = static_cast<double>(n);
    state.counters["K"] = static_cast<double>(ncols);
}

/// ─── End-to-end hash → selector benchmark ────────────────────────────────
///
/// Captures the full pipeline as executed by
/// BufferedShardByHashTransform::generateOutputChunks:
/// computeHashInto chain (0 allocations) + mapToRange SIMD.
///
/// Both buffers are constructed outside the timing loop so the per-iteration
/// cost reflects steady-state behaviour.

void BM_HashAndFastrange_New(benchmark::State & state, const std::vector<ColumnPtr> & cols, size_t num_shards)
{
    const size_t n = cols[0]->size();
    const size_t ncols = cols.size();
    PaddedPODArray<UInt32> hash_buf(n);
    PaddedPODArray<UInt32> pids(n);  // mapToRange now writes UInt32
    const UInt32 p32 = static_cast<UInt32>(num_shards);
    for (auto _ : state)
    {
        bool initial = true;
        for (size_t k = 0; k < ncols; ++k)
        {
            cols[k]->computeHashInto(0, n, hash_buf.data(), initial);
            initial = false;
        }
        mapToRange(hash_buf.data(), n, p32, pids.data());
        benchmark::DoNotOptimize(pids.data());
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(n));
    state.counters["rows/iter"] = static_cast<double>(n);
    state.counters["K"] = static_cast<double>(ncols);
    state.counters["P"] = static_cast<double>(num_shards);
}

/// ─── Registration macro ───────────────────────────────────────────────────

#define REGISTER_E2E(tag, make_fn, K, B, P) \
    do \
    { \
        static const auto cols_e2e_##tag##_K##K##_B##B##_P##P = make_fn(K, B); \
        benchmark::RegisterBenchmark( \
            "BM_hashAndFastrange_new/" #tag "_K" #K "_B" #B "_P" #P, \
            [](benchmark::State & st) { BM_HashAndFastrange_New(st, cols_e2e_##tag##_K##K##_B##B##_P##P, P); }); \
    } while (false)

#define REGISTER_BOTH(tag, make_fn, K, B) \
    do \
    { \
        static const auto cols_##tag##_K##K##_B##B = make_fn(K, B); \
        benchmark::RegisterBenchmark( \
            "BM_computeHashInto/" #tag "_K" #K "_B" #B, [](benchmark::State & st) { BM_ComputeHashInto(st, cols_##tag##_K##K##_B##B); }); \
    } while (false)

}


int main(int argc, char ** argv)
{
    // Register benchmarks for the sweep: type × K × batch.

    // UInt32 (the most common partitioning key type)
    REGISTER_BOTH(UInt32, makeUInt32Columns, 1, 1024);
    REGISTER_BOTH(UInt32, makeUInt32Columns, 1, 4096);
    REGISTER_BOTH(UInt32, makeUInt32Columns, 1, 16384);
    REGISTER_BOTH(UInt32, makeUInt32Columns, 2, 1024);
    REGISTER_BOTH(UInt32, makeUInt32Columns, 2, 4096);
    REGISTER_BOTH(UInt32, makeUInt32Columns, 2, 16384);
    REGISTER_BOTH(UInt32, makeUInt32Columns, 4, 1024);
    REGISTER_BOTH(UInt32, makeUInt32Columns, 4, 4096);
    REGISTER_BOTH(UInt32, makeUInt32Columns, 4, 16384);
    REGISTER_BOTH(UInt32, makeUInt32Columns, 8, 1024);
    REGISTER_BOTH(UInt32, makeUInt32Columns, 8, 4096);
    REGISTER_BOTH(UInt32, makeUInt32Columns, 8, 16384);

    // UInt64
    REGISTER_BOTH(UInt64, makeUInt64Columns, 1, 16384);
    REGISTER_BOTH(UInt64, makeUInt64Columns, 4, 16384);
    REGISTER_BOTH(UInt64, makeUInt64Columns, 8, 16384);

    // String (avg ~16 B)
    REGISTER_BOTH(String, makeStringColumns, 1, 1024);
    REGISTER_BOTH(String, makeStringColumns, 1, 4096);
    REGISTER_BOTH(String, makeStringColumns, 1, 16384);
    REGISTER_BOTH(String, makeStringColumns, 4, 4096);
    REGISTER_BOTH(String, makeStringColumns, 4, 16384);

    // Nullable(UInt32)
    REGISTER_BOTH(NullableUInt32, makeNullableUInt32Columns, 1, 16384);
    REGISTER_BOTH(NullableUInt32, makeNullableUInt32Columns, 4, 16384);

    // End-to-end hash → selector (full pipeline including allocation deltas).
    // Same type × K × batch cells as above, fixed P=64 for comparison with
    // the hash-only benchmarks.
    REGISTER_E2E(UInt32, makeUInt32Columns, 1, 1024, 64);
    REGISTER_E2E(UInt32, makeUInt32Columns, 1, 4096, 64);
    REGISTER_E2E(UInt32, makeUInt32Columns, 1, 16384, 64);
    REGISTER_E2E(UInt32, makeUInt32Columns, 2, 16384, 64);
    REGISTER_E2E(UInt32, makeUInt32Columns, 4, 1024, 64);
    REGISTER_E2E(UInt32, makeUInt32Columns, 4, 4096, 64);
    REGISTER_E2E(UInt32, makeUInt32Columns, 4, 16384, 64);
    REGISTER_E2E(UInt32, makeUInt32Columns, 8, 16384, 64);
    REGISTER_E2E(UInt64, makeUInt64Columns, 4, 16384, 64);
    REGISTER_E2E(String, makeStringColumns, 4, 16384, 64);
    REGISTER_E2E(NullableUInt32, makeNullableUInt32Columns, 4, 16384, 64);

    // P sweep on the busiest UInt32 cell to show fastrange cost vs partition count.
    REGISTER_E2E(UInt32, makeUInt32Columns, 4, 16384, 16);
    REGISTER_E2E(UInt32, makeUInt32Columns, 4, 16384, 256);

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}
