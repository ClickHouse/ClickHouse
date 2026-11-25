#include <Storages/MergeTree/MergeTreeReaderStream.h>

#include <benchmark/benchmark.h>

using namespace DB;

namespace
{

using ReadRanges = ReadRangesRegistry::ReadRanges;

void addOne(ReadRanges & read_ranges, CompressedBlockRanges & ranges, size_t begin, size_t end)
{
    ranges.emplace_back(begin, end);
    size_t capacity_grown = 0;
    benchmark::DoNotOptimize(read_ranges.add(ranges, capacity_grown));
}

/// `disjoint` ranges, none touching another, merged; then `duplicates` ranges each covered by a kept one. With
/// 65535 disjoint ranges the vector is left with a single free slot, so a merge before every growth that only makes
/// room for one more range would merge all of them again for each duplicate.
void BM_DuplicatesIntoNearlyFullRanges(benchmark::State & state)
{
    const size_t disjoint = static_cast<size_t>(state.range(0));
    const size_t duplicates = static_cast<size_t>(state.range(1));
    CompressedBlockRanges ranges;
    for (auto _ : state)
    {
        state.PauseTiming();
        auto read_ranges = std::make_unique<ReadRanges>();
        for (size_t i = 0; i < disjoint; ++i)
            addOne(*read_ranges, ranges, 2 * i, 2 * i + 1);
        benchmark::DoNotOptimize(read_ranges->merge());
        state.ResumeTiming();

        for (size_t i = 0; i < duplicates; ++i)
            addOne(*read_ranges, ranges, 2 * (i % disjoint), 2 * (i % disjoint) + 1);
        benchmark::DoNotOptimize(read_ranges->merge());

        state.PauseTiming();
        read_ranges.reset();
        state.ResumeTiming();
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * duplicates));
}

/// One reader going on from where its last block ended, which extends the last range in place.
void BM_SequentialRanges(benchmark::State & state)
{
    const size_t count = static_cast<size_t>(state.range(0));
    CompressedBlockRanges ranges;
    for (auto _ : state)
    {
        ReadRanges read_ranges;
        for (size_t i = 0; i < count; ++i)
            addOne(read_ranges, ranges, i, i + 1);
        benchmark::DoNotOptimize(read_ranges.merge());
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * count));
}

/// Several readers of one file, each reading its own stripe of blocks in order, their ranges interleaved: they touch
/// the ranges of the other readers and are coalesced by the merges, which is how most ranges arrive in practice.
void BM_InterleavedReadersRanges(benchmark::State & state)
{
    const size_t count = static_cast<size_t>(state.range(0));
    const size_t readers = static_cast<size_t>(state.range(1));
    const size_t stripe = count / readers;
    CompressedBlockRanges ranges;
    for (auto _ : state)
    {
        ReadRanges read_ranges;
        for (size_t i = 0; i < stripe; ++i)
            for (size_t reader = 0; reader < readers; ++reader)
                addOne(read_ranges, ranges, reader * stripe + i, reader * stripe + i + 1);
        benchmark::DoNotOptimize(read_ranges.merge());
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * stripe * readers));
}

}

BENCHMARK(BM_DuplicatesIntoNearlyFullRanges)->Args({65535, 4096})->Args({65536, 4096})->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_SequentialRanges)->Arg(1 << 20)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_InterleavedReadersRanges)->Args({1 << 20, 16})->Unit(benchmark::kMicrosecond);
