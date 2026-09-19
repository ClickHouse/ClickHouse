#include <Columns/ColumnsCommon.h>

#include <benchmark/benchmark.h>

#include <cstddef>
#include <cstdint>

using namespace DB;

namespace
{

constexpr size_t benchmark_size = 1ULL << 20;

enum class MaskPattern
{
    AllZero,
    AllOne,
    Alternating,
    Sparse,
    Random,
};

IColumn::Filter makeMask(size_t size, MaskPattern pattern)
{
    IColumn::Filter mask;
    mask.resize_fill(size, pattern == MaskPattern::AllOne ? 1 : 0);

    switch (pattern)
    {
        case MaskPattern::AllZero:
        case MaskPattern::AllOne: break;
        case MaskPattern::Alternating:
            for (size_t i = 0; i < size; i += 2)
                mask[i] = 1;
            break;
        case MaskPattern::Sparse:
            for (size_t i = 0; i < size; i += 7)
                mask[i] = 1;
            break;
        case MaskPattern::Random: {
            UInt64 random_state = 0x9e3779b97f4a7c15ULL;
            for (size_t i = 0; i < size; ++i)
            {
                random_state ^= random_state << 7;
                random_state ^= random_state >> 9;
                mask[i] = static_cast<UInt8>(random_state >> 63);
            }
            break;
        }
    }

    return mask;
}

template <MaskPattern pattern>
void BM_countBytesInFilter(benchmark::State & state)
{
    const size_t start = static_cast<size_t>(state.range(0));
    const size_t length = static_cast<size_t>(state.range(1));
    const auto filter = makeMask(benchmark_size, pattern);
    size_t result = 0;

    for (auto _ [[maybe_unused]] : state)
    {
        result = countBytesInFilter(filter.data(), start, start + length);
        benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(length));
}

template <MaskPattern filter_pattern, MaskPattern null_pattern>
void BM_countBytesInFilterWithNull(benchmark::State & state)
{
    const size_t start = static_cast<size_t>(state.range(0));
    const size_t length = static_cast<size_t>(state.range(1));
    const auto filter = makeMask(benchmark_size, filter_pattern);
    const auto null_map = makeMask(benchmark_size, null_pattern);
    size_t result = 0;

    for (auto _ [[maybe_unused]] : state)
    {
        result = countBytesInFilterWithNull(filter, null_map.data(), start, start + length);
        benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(length) * 2);
}

}

#define REGISTER_COUNT_BYTES_BENCHMARK(benchmark) \
    benchmark->Args({0, static_cast<int64_t>(benchmark_size)}) \
        ->Args({1, static_cast<int64_t>(benchmark_size - 2)}) \
        ->Args({0, static_cast<int64_t>(benchmark_size - 1)})

REGISTER_COUNT_BYTES_BENCHMARK(BENCHMARK_TEMPLATE(BM_countBytesInFilter, MaskPattern::AllZero));
REGISTER_COUNT_BYTES_BENCHMARK(BENCHMARK_TEMPLATE(BM_countBytesInFilter, MaskPattern::AllOne));
REGISTER_COUNT_BYTES_BENCHMARK(BENCHMARK_TEMPLATE(BM_countBytesInFilter, MaskPattern::Alternating));
REGISTER_COUNT_BYTES_BENCHMARK(BENCHMARK_TEMPLATE(BM_countBytesInFilter, MaskPattern::Sparse));
REGISTER_COUNT_BYTES_BENCHMARK(BENCHMARK_TEMPLATE(BM_countBytesInFilter, MaskPattern::Random));

REGISTER_COUNT_BYTES_BENCHMARK(BENCHMARK_TEMPLATE(BM_countBytesInFilterWithNull, MaskPattern::AllZero, MaskPattern::AllZero));
REGISTER_COUNT_BYTES_BENCHMARK(BENCHMARK_TEMPLATE(BM_countBytesInFilterWithNull, MaskPattern::AllOne, MaskPattern::AllZero));
REGISTER_COUNT_BYTES_BENCHMARK(BENCHMARK_TEMPLATE(BM_countBytesInFilterWithNull, MaskPattern::Alternating, MaskPattern::Alternating));
REGISTER_COUNT_BYTES_BENCHMARK(BENCHMARK_TEMPLATE(BM_countBytesInFilterWithNull, MaskPattern::Random, MaskPattern::Sparse));

#undef REGISTER_COUNT_BYTES_BENCHMARK
