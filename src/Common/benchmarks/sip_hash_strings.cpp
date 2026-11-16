/// `sipHash128PaddedStrings` and `sipHash128PaddedFixedStrings` hash many strings at once. These benchmarks compare
/// them with `sipHash128Padded` per string:
///   strings/<input>       `sipHash128PaddedStrings` over a column of strings;
///   perRow/<input>        `sipHash128Padded` of each string of the same column;
///   fixedStrings/<n>      `sipHash128PaddedFixedStrings` over strings of `n` bytes;
///   fixedPerRow/<n>       `sipHash128Padded` of each of them.
/// `items_per_second` is the strings per second.
///
/// Run with:
///   ./benchmark_sip_hash_strings --benchmark_filter=_0_15

#include <Common/PODArray.h>
#include <Common/SipHash.h>
#include <Common/SipHashStrings.h>

#include <benchmark/benchmark.h>

#include <deque>
#include <random>
#include <string>
#include <vector>

namespace
{

using DB::PaddedPODArray;

/// `sipHash128` hashes a whole block in one call, and a block has at most `max_block_size` rows.
constexpr size_t max_rows = 65536;
/// Fewer rows for long strings, so that the input stays near the size of the L3 cache.
constexpr size_t max_bytes = 32 << 20;
constexpr size_t min_rows = 4096;

using Rng = std::mt19937_64;

size_t uniform(Rng & rng, size_t min, size_t max) { return std::uniform_int_distribution<size_t>(min, max)(rng); }

struct Input
{
    const char * name;
    size_t (*length)(Rng & rng);
};

std::vector<Input> makeInputs()
{
    return {
        {"fixed_4", [](Rng &) -> size_t { return 4; }},
        {"fixed_8", [](Rng &) -> size_t { return 8; }},
        {"fixed_16", [](Rng &) -> size_t { return 16; }},
        {"fixed_32", [](Rng &) -> size_t { return 32; }},
        {"fixed_64", [](Rng &) -> size_t { return 64; }},
        {"fixed_256", [](Rng &) -> size_t { return 256; }},
        {"fixed_4096", [](Rng &) -> size_t { return 4096; }},
        {"uniform_0_15", [](Rng & rng) { return uniform(rng, 0, 15); }},
        {"uniform_0_63", [](Rng & rng) { return uniform(rng, 0, 63); }},
        {"uniform_16_300", [](Rng & rng) { return uniform(rng, 16, 300); }},
        {"16_or_1100", [](Rng & rng) -> size_t { return uniform(rng, 0, 1) ? 16 : 1100; }},
        {"600_or_60000", [](Rng & rng) -> size_t { return uniform(rng, 0, 1) ? 600 : 60000; }},
    };
}

/// Strings as in a `ColumnString`, and their hashes by `sipHash128Padded`.
struct Strings
{
    PaddedPODArray<UInt8> chars;
    PaddedPODArray<UInt64> offsets;
    std::vector<UInt128> expected;

    [[nodiscard]] const char * data() const { return reinterpret_cast<const char *>(chars.data()); }
    [[nodiscard]] size_t size() const { return offsets.size(); }
    [[nodiscard]] size_t sizeAt(size_t row) const { return offsets[row] - offsets[static_cast<ssize_t>(row) - 1]; }
    [[nodiscard]] const char * dataAt(size_t row) const { return data() + offsets[static_cast<ssize_t>(row) - 1]; }
};

Strings makeStrings(const Input & input)
{
    Rng rng(std::hash<std::string>{}(input.name));
    Strings strings;
    for (size_t row = 0; row < max_rows && (strings.chars.size() < max_bytes || row < min_rows); ++row)
    {
        const size_t length = input.length(rng);
        for (size_t i = 0; i < length; ++i)
            strings.chars.push_back(static_cast<UInt8>(rng()));
        strings.offsets.push_back(strings.chars.size());
    }
    for (size_t row = 0; row < strings.size(); ++row)
        strings.expected.push_back(sipHash128Padded(strings.dataAt(row), strings.sizeAt(row)));
    return strings;
}

void setRows(benchmark::State & state, size_t rows)
{
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(rows));
}

void hashStrings(benchmark::State & state, const Strings & strings)
{
    std::vector<UInt128> hashes(strings.size());
    sipHash128PaddedStrings(strings.data(), strings.offsets.data(), strings.size(), hashes.data());
    if (hashes != strings.expected)
        state.SkipWithError("sipHash128PaddedStrings differs from sipHash128Padded");
    for (auto _ [[maybe_unused]] : state)
    {
        sipHash128PaddedStrings(strings.data(), strings.offsets.data(), strings.size(), hashes.data());
        benchmark::DoNotOptimize(hashes.data());
        benchmark::ClobberMemory();
    }
    setRows(state, strings.size());
}

void hashPerRow(benchmark::State & state, const Strings & strings)
{
    std::vector<UInt128> hashes(strings.size());
    for (auto _ [[maybe_unused]] : state)
    {
        for (size_t row = 0; row < strings.size(); ++row)
            hashes[row] = sipHash128Padded(strings.dataAt(row), strings.sizeAt(row));
        benchmark::DoNotOptimize(hashes.data());
        benchmark::ClobberMemory();
    }
    setRows(state, strings.size());
}

/// Strings of `n` bytes, as in a `ColumnFixedString`.
struct FixedStrings
{
    size_t n;
    PaddedPODArray<UInt8> chars;
    std::vector<UInt128> expected;

    [[nodiscard]] const char * data() const { return reinterpret_cast<const char *>(chars.data()); }
    [[nodiscard]] size_t size() const { return chars.size() / n; }
};

FixedStrings makeFixedStrings(size_t n)
{
    Rng rng(n);
    FixedStrings strings{n, {}, {}};
    strings.chars.resize(n * max_rows);
    for (auto & byte : strings.chars)
        byte = static_cast<UInt8>(rng());
    for (size_t row = 0; row < strings.size(); ++row)
        strings.expected.push_back(sipHash128Padded(strings.data() + row * n, n));
    return strings;
}

void hashFixedStrings(benchmark::State & state, const FixedStrings & strings)
{
    std::vector<UInt128> hashes(strings.size());
    sipHash128PaddedFixedStrings(strings.data(), strings.n, strings.size(), hashes.data());
    if (hashes != strings.expected)
        state.SkipWithError("sipHash128PaddedFixedStrings differs from sipHash128Padded");
    for (auto _ [[maybe_unused]] : state)
    {
        sipHash128PaddedFixedStrings(strings.data(), strings.n, strings.size(), hashes.data());
        benchmark::DoNotOptimize(hashes.data());
        benchmark::ClobberMemory();
    }
    setRows(state, strings.size());
}

void hashFixedPerRow(benchmark::State & state, const FixedStrings & strings)
{
    std::vector<UInt128> hashes(strings.size());
    for (auto _ [[maybe_unused]] : state)
    {
        for (size_t row = 0; row < strings.size(); ++row)
            hashes[row] = sipHash128Padded(strings.data() + row * strings.n, strings.n);
        benchmark::DoNotOptimize(hashes.data());
        benchmark::ClobberMemory();
    }
    setRows(state, strings.size());
}

}

int main(int argc, char ** argv)
{
    benchmark::MaybeReenterWithoutASLR(argc, argv);
    static std::deque<Strings> all_strings;
    for (const auto & input : makeInputs())
    {
        const auto & strings = all_strings.emplace_back(makeStrings(input));
        const std::string name = input.name;
        benchmark::RegisterBenchmark("strings/" + name, [&strings](benchmark::State & state) { hashStrings(state, strings); });
        benchmark::RegisterBenchmark("perRow/" + name, [&strings](benchmark::State & state) { hashPerRow(state, strings); });
    }
    static std::deque<FixedStrings> all_fixed_strings;
    for (const size_t n : {4, 8, 16, 32, 64, 256})
    {
        const auto & strings = all_fixed_strings.emplace_back(makeFixedStrings(n));
        const std::string name = std::to_string(n);
        benchmark::RegisterBenchmark("fixedStrings/" + name, [&strings](benchmark::State & state) { hashFixedStrings(state, strings); });
        benchmark::RegisterBenchmark("fixedPerRow/" + name, [&strings](benchmark::State & state) { hashFixedPerRow(state, strings); });
    }

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}
