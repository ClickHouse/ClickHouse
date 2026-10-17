#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>
#include <benchmark/benchmark.h>
#include <random>
#include <memory>

using namespace DB;

constexpr size_t BENCHMARK_NUM_ROWS = 8192;
constexpr size_t BENCHMARK_NUM_ITERATIONS = 100;

/// Helper function to create a bitmap with specific filter rate
/// @param filter_rate percentage of rows that should pass the filter (0.0 to 1.0)
/// @param starting_row the starting row number for the filter range
/// @param num_rows number of rows in the filter range (should be BENCHMARK_NUM_ROWS)
/// @param num_iterations number of times to repeat the pattern (default 100)
/// @param seed random seed for reproducibility
template <typename Offset>
static ProjectionIndexBitmapPtr createBitmapWithFilterRate(
    double filter_rate,
    Offset starting_row,
    size_t num_rows,
    size_t num_iterations,
    unsigned seed)
{
    std::mt19937_64 rng(seed);
    std::uniform_real_distribution<double> dist(0.0, 1.0);

    ProjectionIndexBitmapPtr bitmap;
    if constexpr (std::is_same_v<Offset, UInt32>)
        bitmap = ProjectionIndexBitmap::create32();
    else
        bitmap = ProjectionIndexBitmap::create64();

    std::vector<Offset> values;
    values.reserve(static_cast<size_t>(num_rows * num_iterations * filter_rate * 1.2)); // Reserve slightly more

    /// Generate values based on filter rate for multiple iterations
    for (size_t iter = 0; iter < num_iterations; ++iter)
    {
        Offset iter_offset = static_cast<Offset>(iter * num_rows);
        for (size_t i = 0; i < num_rows; ++i)
        {
            if (dist(rng) < filter_rate)
            {
                values.push_back(static_cast<Offset>(starting_row + iter_offset + i));
            }
        }
    }

    if (!values.empty())
        bitmap->addBulk<Offset>(values.data(), values.size());

    return bitmap;
}

/// Benchmark appendToFilter with 32-bit bitmap, filter rate 0.1% (very sparse)
static void BM_AppendToFilter_32bit_FilterRate_0_1(benchmark::State & state)
{
    const double filter_rate = 0.001;
    const size_t total_rows = BENCHMARK_NUM_ITERATIONS * BENCHMARK_NUM_ROWS;

    auto bitmap = createBitmapWithFilterRate<UInt32>(filter_rate, 0, BENCHMARK_NUM_ROWS, BENCHMARK_NUM_ITERATIONS, 42);

    for (auto _ : state)
    {
        PaddedPODArray<UInt8> filter;
        for (size_t i = 0; i < BENCHMARK_NUM_ITERATIONS; ++i)
        {
            UInt32 iter_start = static_cast<UInt32>(i * BENCHMARK_NUM_ROWS);
            bool result = bitmap->appendToFilter(filter, iter_start, BENCHMARK_NUM_ROWS);
            benchmark::DoNotOptimize(result);
        }
        benchmark::DoNotOptimize(filter);
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
    state.SetLabel("filter_rate=0.1%,iterations=100");
}

/// Benchmark appendToFilter with 32-bit bitmap, filter rate 1% (sparse)
static void BM_AppendToFilter_32bit_FilterRate_1(benchmark::State & state)
{
    const double filter_rate = 0.01;
    const size_t total_rows = BENCHMARK_NUM_ITERATIONS * BENCHMARK_NUM_ROWS;

    auto bitmap = createBitmapWithFilterRate<UInt32>(filter_rate, 0, BENCHMARK_NUM_ROWS, BENCHMARK_NUM_ITERATIONS, 42);

    for (auto _ : state)
    {
        PaddedPODArray<UInt8> filter;
        for (size_t i = 0; i < BENCHMARK_NUM_ITERATIONS; ++i)
        {
            UInt32 iter_start = static_cast<UInt32>(i * BENCHMARK_NUM_ROWS);
            bool result = bitmap->appendToFilter(filter, iter_start, BENCHMARK_NUM_ROWS);
            benchmark::DoNotOptimize(result);
        }
        benchmark::DoNotOptimize(filter);
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
    state.SetLabel("filter_rate=1%,iterations=100");
}

/// Benchmark appendToFilter with 32-bit bitmap, filter rate 10% (medium sparse)
static void BM_AppendToFilter_32bit_FilterRate_10(benchmark::State & state)
{
    const double filter_rate = 0.10;
    const size_t total_rows = BENCHMARK_NUM_ITERATIONS * BENCHMARK_NUM_ROWS;

    auto bitmap = createBitmapWithFilterRate<UInt32>(filter_rate, 0, BENCHMARK_NUM_ROWS, BENCHMARK_NUM_ITERATIONS, 42);

    for (auto _ : state)
    {
        PaddedPODArray<UInt8> filter;
        for (size_t i = 0; i < BENCHMARK_NUM_ITERATIONS; ++i)
        {
            UInt32 iter_start = static_cast<UInt32>(i * BENCHMARK_NUM_ROWS);
            bool result = bitmap->appendToFilter(filter, iter_start, BENCHMARK_NUM_ROWS);
            benchmark::DoNotOptimize(result);
        }
        benchmark::DoNotOptimize(filter);
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
    state.SetLabel("filter_rate=10%,iterations=100");
}

/// Benchmark appendToFilter with 32-bit bitmap, filter rate 50% (medium dense)
static void BM_AppendToFilter_32bit_FilterRate_50(benchmark::State & state)
{
    const double filter_rate = 0.50;
    const size_t total_rows = BENCHMARK_NUM_ITERATIONS * BENCHMARK_NUM_ROWS;

    auto bitmap = createBitmapWithFilterRate<UInt32>(filter_rate, 0, BENCHMARK_NUM_ROWS, BENCHMARK_NUM_ITERATIONS, 42);

    for (auto _ : state)
    {
        PaddedPODArray<UInt8> filter;
        for (size_t i = 0; i < BENCHMARK_NUM_ITERATIONS; ++i)
        {
            UInt32 iter_start = static_cast<UInt32>(i * BENCHMARK_NUM_ROWS);
            bool result = bitmap->appendToFilter(filter, iter_start, BENCHMARK_NUM_ROWS);
            benchmark::DoNotOptimize(result);
        }
        benchmark::DoNotOptimize(filter);
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
    state.SetLabel("filter_rate=50%,iterations=100");
}

/// Benchmark appendToFilter with 32-bit bitmap, filter rate 90% (dense)
static void BM_AppendToFilter_32bit_FilterRate_90(benchmark::State & state)
{
    const double filter_rate = 0.90;
    const size_t total_rows = BENCHMARK_NUM_ITERATIONS * BENCHMARK_NUM_ROWS;

    auto bitmap = createBitmapWithFilterRate<UInt32>(filter_rate, 0, BENCHMARK_NUM_ROWS, BENCHMARK_NUM_ITERATIONS, 42);

    for (auto _ : state)
    {
        PaddedPODArray<UInt8> filter;
        for (size_t i = 0; i < BENCHMARK_NUM_ITERATIONS; ++i)
        {
            UInt32 iter_start = static_cast<UInt32>(i * BENCHMARK_NUM_ROWS);
            bool result = bitmap->appendToFilter(filter, iter_start, BENCHMARK_NUM_ROWS);
            benchmark::DoNotOptimize(result);
        }
        benchmark::DoNotOptimize(filter);
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
    state.SetLabel("filter_rate=90%,iterations=100");
}

/// Benchmark appendToFilter with 32-bit bitmap, filter rate 99% (dense)
static void BM_AppendToFilter_32bit_FilterRate_99(benchmark::State & state)
{
    const double filter_rate = 0.99;
    const size_t total_rows = BENCHMARK_NUM_ITERATIONS * BENCHMARK_NUM_ROWS;

    auto bitmap = createBitmapWithFilterRate<UInt32>(filter_rate, 0, BENCHMARK_NUM_ROWS, BENCHMARK_NUM_ITERATIONS, 42);

    for (auto _ : state)
    {
        PaddedPODArray<UInt8> filter;
        for (size_t i = 0; i < BENCHMARK_NUM_ITERATIONS; ++i)
        {
            UInt32 iter_start = static_cast<UInt32>(i * BENCHMARK_NUM_ROWS);
            bool result = bitmap->appendToFilter(filter, iter_start, BENCHMARK_NUM_ROWS);
            benchmark::DoNotOptimize(result);
        }
        benchmark::DoNotOptimize(filter);
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
    state.SetLabel("filter_rate=99%,iterations=100");
}


/// Benchmark appendToFilter with 64-bit bitmap, filter rate 1% (sparse)
static void BM_AppendToFilter_64bit_FilterRate_1(benchmark::State & state)
{
    const double filter_rate = 0.01;
    const size_t total_rows = BENCHMARK_NUM_ITERATIONS * BENCHMARK_NUM_ROWS;

    auto bitmap = createBitmapWithFilterRate<UInt64>(filter_rate, 0, BENCHMARK_NUM_ROWS, BENCHMARK_NUM_ITERATIONS, 42);

    for (auto _ : state)
    {
        PaddedPODArray<UInt8> filter;
        for (size_t i = 0; i < BENCHMARK_NUM_ITERATIONS; ++i)
        {
            UInt64 iter_start = static_cast<UInt64>(i * BENCHMARK_NUM_ROWS);
            bool result = bitmap->appendToFilter(filter, iter_start, BENCHMARK_NUM_ROWS);
            benchmark::DoNotOptimize(result);
        }
        benchmark::DoNotOptimize(filter);
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
    state.SetLabel("filter_rate=1%,iterations=100");
}

/// Benchmark appendToFilter with 64-bit bitmap, filter rate 50% (medium dense)
static void BM_AppendToFilter_64bit_FilterRate_50(benchmark::State & state)
{
    const double filter_rate = 0.50;
    const size_t total_rows = BENCHMARK_NUM_ITERATIONS * BENCHMARK_NUM_ROWS;

    auto bitmap = createBitmapWithFilterRate<UInt64>(filter_rate, 0, BENCHMARK_NUM_ROWS, BENCHMARK_NUM_ITERATIONS, 42);

    for (auto _ : state)
    {
        PaddedPODArray<UInt8> filter;
        for (size_t i = 0; i < BENCHMARK_NUM_ITERATIONS; ++i)
        {
            UInt64 iter_start = static_cast<UInt64>(i * BENCHMARK_NUM_ROWS);
            bool result = bitmap->appendToFilter(filter, iter_start, BENCHMARK_NUM_ROWS);
            benchmark::DoNotOptimize(result);
        }
        benchmark::DoNotOptimize(filter);
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
    state.SetLabel("filter_rate=50%,iterations=100");
}

/// Benchmark appendToFilter with 32-bit bitmap, all zeros (empty bitmap)
static void BM_AppendToFilter_32bit_AllZeros(benchmark::State & state)
{
    const size_t total_rows = BENCHMARK_NUM_ITERATIONS * BENCHMARK_NUM_ROWS;

    auto bitmap = ProjectionIndexBitmap::create32();

    for (auto _ : state)
    {
        PaddedPODArray<UInt8> filter;
        for (size_t i = 0; i < BENCHMARK_NUM_ITERATIONS; ++i)
        {
            UInt32 iter_start = static_cast<UInt32>(i * BENCHMARK_NUM_ROWS);
            bool result = bitmap->appendToFilter(filter, iter_start, BENCHMARK_NUM_ROWS);
            benchmark::DoNotOptimize(result);
        }
        benchmark::DoNotOptimize(filter);
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
    state.SetLabel("all_zeros,iterations=100");
}

/// Benchmark appendToFilter with 64-bit bitmap, all zeros (empty bitmap)
static void BM_AppendToFilter_64bit_AllZeros(benchmark::State & state)
{
    const size_t total_rows = BENCHMARK_NUM_ITERATIONS * BENCHMARK_NUM_ROWS;

    auto bitmap = ProjectionIndexBitmap::create64();

    for (auto _ : state)
    {
        PaddedPODArray<UInt8> filter;
        for (size_t i = 0; i < BENCHMARK_NUM_ITERATIONS; ++i)
        {
            UInt64 iter_start = static_cast<UInt64>(i * BENCHMARK_NUM_ROWS);
            bool result = bitmap->appendToFilter(filter, iter_start, BENCHMARK_NUM_ROWS);
            benchmark::DoNotOptimize(result);
        }
        benchmark::DoNotOptimize(filter);
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
    state.SetLabel("all_zeros,iterations=100");
}

/// Benchmark appendToFilter with 32-bit bitmap, all ones (full bitmap)
static void BM_AppendToFilter_32bit_AllOnes(benchmark::State & state)
{
    const size_t total_rows = BENCHMARK_NUM_ITERATIONS * BENCHMARK_NUM_ROWS;

    auto bitmap = ProjectionIndexBitmap::create32();
    std::vector<UInt32> values;
    values.reserve(total_rows);
    for (size_t i = 0; i < total_rows; ++i)
        values.push_back(static_cast<UInt32>(i));
    bitmap->addBulk<UInt32>(values.data(), values.size());

    for (auto _ : state)
    {
        PaddedPODArray<UInt8> filter;
        for (size_t i = 0; i < BENCHMARK_NUM_ITERATIONS; ++i)
        {
            UInt32 iter_start = static_cast<UInt32>(i * BENCHMARK_NUM_ROWS);
            bool result = bitmap->appendToFilter(filter, iter_start, BENCHMARK_NUM_ROWS);
            benchmark::DoNotOptimize(result);
        }
        benchmark::DoNotOptimize(filter);
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
    state.SetLabel("all_ones,iterations=100");
}

/// Benchmark appendToFilter with 64-bit bitmap, all ones (full bitmap)
static void BM_AppendToFilter_64bit_AllOnes(benchmark::State & state)
{
    const size_t total_rows = BENCHMARK_NUM_ITERATIONS * BENCHMARK_NUM_ROWS;

    auto bitmap = ProjectionIndexBitmap::create64();
    std::vector<UInt64> values;
    values.reserve(total_rows);
    for (size_t i = 0; i < total_rows; ++i)
        values.push_back(static_cast<UInt64>(i));
    bitmap->addBulk<UInt64>(values.data(), values.size());

    for (auto _ : state)
    {
        PaddedPODArray<UInt8> filter;
        for (size_t i = 0; i < BENCHMARK_NUM_ITERATIONS; ++i)
        {
            UInt64 iter_start = static_cast<UInt64>(i * BENCHMARK_NUM_ROWS);
            bool result = bitmap->appendToFilter(filter, iter_start, BENCHMARK_NUM_ROWS);
            benchmark::DoNotOptimize(result);
        }
        benchmark::DoNotOptimize(filter);
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
    state.SetLabel("all_ones,iterations=100");
}

// Register benchmarks - 32-bit bitmap with different filter rates
BENCHMARK(BM_AppendToFilter_32bit_FilterRate_0_1);
BENCHMARK(BM_AppendToFilter_32bit_FilterRate_1);
BENCHMARK(BM_AppendToFilter_32bit_FilterRate_10);
BENCHMARK(BM_AppendToFilter_32bit_FilterRate_50);
BENCHMARK(BM_AppendToFilter_32bit_FilterRate_90);
BENCHMARK(BM_AppendToFilter_32bit_FilterRate_99);

// Register benchmarks - 64-bit bitmap with different filter rates
BENCHMARK(BM_AppendToFilter_64bit_FilterRate_1);
BENCHMARK(BM_AppendToFilter_64bit_FilterRate_50);

// Register benchmarks - appendToFilter with all zeros (empty bitmap)
BENCHMARK(BM_AppendToFilter_32bit_AllZeros);
BENCHMARK(BM_AppendToFilter_64bit_AllZeros);

// Register benchmarks - appendToFilter with all ones (full bitmap)
BENCHMARK(BM_AppendToFilter_32bit_AllOnes);
BENCHMARK(BM_AppendToFilter_64bit_AllOnes);
