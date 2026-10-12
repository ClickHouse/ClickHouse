#include <benchmark/benchmark.h>
#include <Functions/array/arrayRandomProjection.h>


template <typename T>
static void BM_GemmScalar(benchmark::State & state)
{
    const size_t input_dim = state.range(0);
    const size_t target_dim = state.range(1);
    const size_t num_rows = state.range(2);

    DB::NormalProjectionState<T> proj;
    proj.generate(input_dim, target_dim, 42);

    pcg64 rng(123);
    std::vector<T> input(num_rows * input_dim);
    for (auto & v : input)
        v = DB::boxMullerNormal<T>(rng);
    std::vector<T> output(num_rows * target_dim);

    for (auto _ : state)
    {
        DB::gemmBatchScalar<T>(input.data(), proj.matrix.data(), output.data(), num_rows, input_dim, target_dim);
        benchmark::DoNotOptimize(output.data());
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * num_rows);
    state.SetBytesProcessed(
        static_cast<int64_t>(state.iterations()) * num_rows * (input_dim + target_dim) * sizeof(T));
}

#if USE_MULTITARGET_CODE
template <typename T>
static void BM_GemmAVX2(benchmark::State & state)
{
    const size_t input_dim = state.range(0);
    const size_t target_dim = state.range(1);
    const size_t num_rows = state.range(2);

    DB::NormalProjectionState<T> proj;
    proj.generate(input_dim, target_dim, 42);

    pcg64 rng(123);
    std::vector<T> input(num_rows * input_dim);
    for (auto & v : input)
        v = DB::boxMullerNormal<T>(rng);
    std::vector<T> output(num_rows * target_dim);

    for (auto _ : state)
    {
        DB::gemmBatchAVX2<T>(input.data(), proj.matrix.data(), output.data(), num_rows, input_dim, target_dim);
        benchmark::DoNotOptimize(output.data());
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * num_rows);
    state.SetBytesProcessed(
        static_cast<int64_t>(state.iterations()) * num_rows * (input_dim + target_dim) * sizeof(T));
}

template <typename T>
static void BM_GemmAVX512(benchmark::State & state)
{
    const size_t input_dim = state.range(0);
    const size_t target_dim = state.range(1);
    const size_t num_rows = state.range(2);

    DB::NormalProjectionState<T> proj;
    proj.generate(input_dim, target_dim, 42);

    pcg64 rng(123);
    std::vector<T> input(num_rows * input_dim);
    for (auto & v : input)
        v = DB::boxMullerNormal<T>(rng);
    std::vector<T> output(num_rows * target_dim);

    for (auto _ : state)
    {
        DB::gemmBatchAVX512<T>(input.data(), proj.matrix.data(), output.data(), num_rows, input_dim, target_dim);
        benchmark::DoNotOptimize(output.data());
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * num_rows);
    state.SetBytesProcessed(
        static_cast<int64_t>(state.iterations()) * num_rows * (input_dim + target_dim) * sizeof(T));
}
#endif

/// Args: {input_dim, target_dim, num_rows}

BENCHMARK_TEMPLATE(BM_GemmScalar, Float32)
    ->Args({128, 64, 1000})
    ->Args({200, 100, 1000})
    ->Args({256, 128, 1000})
    ->Args({300, 150, 1000})
    ->Args({512, 256, 1000})
    ->Args({768, 256, 1000})
    ->Args({1024, 256, 1000})
    ->Args({1024, 512, 100})
    ->Args({1536, 384, 100})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_GemmScalar, Float64)
    ->Args({128, 64, 1000})
    ->Args({200, 100, 1000})
    ->Args({512, 256, 1000})
    ->Args({768, 256, 1000})
    ->Args({1024, 256, 1000})
    ->Args({1536, 384, 100})
    ->Unit(benchmark::kMicrosecond);

#if USE_MULTITARGET_CODE
BENCHMARK_TEMPLATE(BM_GemmAVX2, Float32)
    ->Args({128, 64, 1000})
    ->Args({200, 100, 1000})
    ->Args({256, 128, 1000})
    ->Args({300, 150, 1000})
    ->Args({512, 256, 1000})
    ->Args({768, 256, 1000})
    ->Args({1024, 256, 1000})
    ->Args({1024, 512, 100})
    ->Args({1536, 384, 100})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_GemmAVX2, Float64)
    ->Args({128, 64, 1000})
    ->Args({200, 100, 1000})
    ->Args({512, 256, 1000})
    ->Args({768, 256, 1000})
    ->Args({1024, 256, 1000})
    ->Args({1536, 384, 100})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_GemmAVX512, Float32)
    ->Args({128, 64, 1000})
    ->Args({200, 100, 1000})
    ->Args({256, 128, 1000})
    ->Args({300, 150, 1000})
    ->Args({512, 256, 1000})
    ->Args({768, 256, 1000})
    ->Args({1024, 256, 1000})
    ->Args({1024, 512, 100})
    ->Args({1536, 384, 100})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_GemmAVX512, Float64)
    ->Args({128, 64, 1000})
    ->Args({200, 100, 1000})
    ->Args({512, 256, 1000})
    ->Args({768, 256, 1000})
    ->Args({1024, 256, 1000})
    ->Args({1536, 384, 100})
    ->Unit(benchmark::kMicrosecond);
#endif

BENCHMARK_MAIN();
