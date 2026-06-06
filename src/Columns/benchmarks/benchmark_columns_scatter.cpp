/// Microbenchmark for DB::ColumnsScatter::scatter vs the legacy per-chunk
/// IColumn::scatter() path.
///
/// Sweep: K columns × P shards × num_sources source columns (≈ block size rows total).
/// Each benchmark cell measures the amortised cost per source row of the
/// physical-split step. The ratio (legacy_ns / batched_ns) reproduces the
/// RFC's 1.43× geomean target for K∈{4,8} × P∈{64,128,256}.
///
/// Run:
///   ./benchmark_columns_scatter --benchmark_filter=.
///   ./benchmark_columns_scatter --benchmark_filter=UInt64 --benchmark_format=json

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsScatter.h>
#include <Common/PODArray.h>
#include <Common/randomSeed.h>
#include <Common/thread_local_rng.h>

#include <benchmark/benchmark.h>

#include <vector>

using namespace DB;

namespace
{

pcg64 rng_bm(randomSeed()); // NOLINT(cert-err58-cpp,bugprone-throwing-static-initialization)

std::vector<UInt32> randomPids(size_t n, size_t num_shards)
{
    std::vector<UInt32> pids(n);
    for (auto & p : pids)
        p = static_cast<UInt32>(static_cast<size_t>(rng_bm()) % num_shards);
    return pids;
}

/// Build K * num_sources ColumnUInt64 columns of n rows each.
std::vector<ColumnPtr> makeUInt64Sources(size_t k, size_t n, size_t num_sources)
{
    std::vector<ColumnPtr> cols;
    cols.reserve(k * num_sources);
    for (size_t i = 0; i < k * num_sources; ++i)
    {
        auto col = ColumnUInt64::create();
        col->reserve(n);
        for (size_t j = 0; j < n; ++j)
            col->insertValue(j + i * n);
        cols.push_back(std::move(col));
    }
    return cols;
}

// ── Legacy reference ──────────────────────────────────────────────────────────

void legacyScatter(
    const std::vector<ColumnPtr> & key_cols, // K * num_sources columns
    const std::vector<std::vector<UInt32>> & pids_per_b,
    size_t k,
    size_t num_shards)
{
    const size_t num_sources = pids_per_b.size();
    const size_t n = pids_per_b[0].size();

    for (size_t b = 0; b < num_sources; ++b)
    {
        IColumn::Selector sel(n);
        for (size_t j = 0; j < n; ++j)
            sel[j] = pids_per_b[b][j];

        for (size_t ki = 0; ki < k; ++ki)
        {
            auto parts = key_cols[b * k + ki]->scatter(num_shards, sel);
            benchmark::DoNotOptimize(parts.data());
        }
    }
}

// ── ColumnsScatter batched path ───────────────────────────────────────────────

void batchedScatter(
    const std::vector<ColumnPtr> & key_cols,
    const std::vector<std::vector<UInt32>> & pids_per_b,
    size_t k,
    size_t num_shards)
{
    const size_t num_sources = pids_per_b.size();

    for (size_t ki = 0; ki < k; ++ki)
    {
        std::vector<const IColumn *> src_cols(num_sources);
        std::vector<std::span<const UInt32>> pid_spans(num_sources);
        for (size_t b = 0; b < num_sources; ++b)
        {
            src_cols[b] = key_cols[b * k + ki].get();
            pid_spans[b] = pids_per_b[b];
        }
        auto dst = ColumnsScatter::scatter(src_cols, pid_spans, num_shards);
        benchmark::DoNotOptimize(dst.data());
    }
}

// ── Benchmark wrappers ────────────────────────────────────────────────────────

void BM_Legacy_UInt64(benchmark::State & state, size_t k, size_t num_shards, size_t num_sources, size_t n)
{
    auto cols = makeUInt64Sources(k, n, num_sources);
    std::vector<std::vector<UInt32>> pids(num_sources);
    for (auto & p : pids)
        p = randomPids(n, num_shards);

    for (auto _ : state)
        legacyScatter(cols, pids, k, num_shards);

    const auto items = static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(k * num_sources * n);
    state.SetItemsProcessed(items);
    state.counters["K"] = static_cast<double>(k);
    state.counters["P"] = static_cast<double>(num_shards);
    state.counters["B"] = static_cast<double>(num_sources);
    state.counters["n"] = static_cast<double>(n);
}

void BM_Batched_UInt64(benchmark::State & state, size_t k, size_t num_shards, size_t num_sources, size_t n)
{
    auto cols = makeUInt64Sources(k, n, num_sources);
    std::vector<std::vector<UInt32>> pids(num_sources);
    for (auto & p : pids)
        p = randomPids(n, num_shards);

    for (auto _ : state)
        batchedScatter(cols, pids, k, num_shards);

    const auto items = static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(k * num_sources * n);
    state.SetItemsProcessed(items);
    state.counters["K"] = static_cast<double>(k);
    state.counters["P"] = static_cast<double>(num_shards);
    state.counters["B"] = static_cast<double>(num_sources);
    state.counters["n"] = static_cast<double>(n);
}

} // anonymous namespace

#define REGISTER_SWEEP(K, P, SRC, N) \
    do \
    { \
        static const auto cols_K##K##_P##P##_B##SRC = makeUInt64Sources(K, N, SRC); \
        benchmark::RegisterBenchmark( \
            "BM_legacy_UInt64/K" #K "_P" #P "_B" #SRC "_N" #N, \
            [](benchmark::State & st) { BM_Legacy_UInt64(st, K, P, SRC, N); }); \
        benchmark::RegisterBenchmark( \
            "BM_batched_UInt64/K" #K "_P" #P "_B" #SRC "_N" #N, \
            [](benchmark::State & st) { BM_Batched_UInt64(st, K, P, SRC, N); }); \
    } while (false)

int main(int argc, char ** argv)
{
    // RFC sweep: K ∈ {1,2,4,8}, P ∈ {32,64,128,256}, 4 source columns,
    // 65536 rows per source column (≈ 4 × DEFAULT_BLOCK_SIZE total).
    constexpr size_t num_sources = 4;
    constexpr size_t n = 65536;

    REGISTER_SWEEP(1, 32,  num_sources, n);
    REGISTER_SWEEP(1, 64,  num_sources, n);
    REGISTER_SWEEP(1, 128, num_sources, n);
    REGISTER_SWEEP(1, 256, num_sources, n);
    REGISTER_SWEEP(2, 64,  num_sources, n);
    REGISTER_SWEEP(2, 128, num_sources, n);
    REGISTER_SWEEP(2, 256, num_sources, n);
    REGISTER_SWEEP(4, 64,  num_sources, n);
    REGISTER_SWEEP(4, 128, num_sources, n);
    REGISTER_SWEEP(4, 256, num_sources, n);
    REGISTER_SWEEP(8, 64,  num_sources, n);
    REGISTER_SWEEP(8, 128, num_sources, n);
    REGISTER_SWEEP(8, 256, num_sources, n);

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}
