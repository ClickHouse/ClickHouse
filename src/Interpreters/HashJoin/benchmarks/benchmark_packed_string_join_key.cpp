/// Microbenchmark of `PackedStringJoinKey::buildRange`, which builds the keys of a single `String` join key. Its inputs
/// take each path of `buildRange`, have the lengths of real columns, or have one length with rare outliers. For each
/// input:
///   buildRange/<input>       all the keys in one call, as `HashMethodPackedStringJoinKey::buildAllKeys` builds them;
///   buildRange256/<input>    the keys from row 77 on, 256 rows per call into one reused buffer, as
///                            `HashMethodPackedStringJoinKey::buildNextKeys` builds them for a join loop;
///   buildRangeSmall/<input>  the keys in back-to-back calls of 16 to 256 rows, a random number per call, into one
///                            reused buffer, as for small blocks and short ranges of rows;
///   buildPerRow/<input>      the same keys with `PackedStringJoinKey::build` per row;
///   hashRange/<input>        the hashing in `buildRange`: `SipHashStrings::hashRows` on the strings of each chunk that
///                            are longer than 15 bytes. It checks whether to sort the strings by length, sorts them if
///                            so, and hashes them a group at a time, then the rest one by one;
///   hashPerRow/<input>       the same strings with `PackedStringJoinKey::build` per string.
/// The time of `buildRange/<input>` minus that of `hashRange/<input>` is about the time of the rest of `buildRange`:
/// the short keys, the length buckets and the list of the longer strings. `items_per_second` is the rows of the input
/// per second, short rows included.
///
/// Run with:
///   ./benchmark_packed_string_join_key --benchmark_filter=/V3_
///   ./benchmark_packed_string_join_key --benchmark_filter='^(buildRange|hashRange)/'

#include <Columns/ColumnString.h>
#include <Interpreters/HashJoin/KeyGetter.h>
#include <Common/assert_cast.h>

#if (defined(__aarch64__) && defined(__ARM_NEON)) || defined(__AVX2__)
#    include <Common/SipHashStringsImpl.h>
#endif

#include <benchmark/benchmark.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <deque>
#include <functional>
#include <random>
#include <string>
#include <vector>

using namespace DB;

namespace
{

/// The keys of a block are built at once, and a block has at most `max_block_size` rows.
constexpr size_t max_rows = 65536;
/// Fewer rows for long strings, so that the input stays near the size of the L3 cache.
constexpr size_t max_bytes = 32 << 20;
constexpr size_t min_rows = 4096;
/// The rows of `buildRange` chunks, for the inputs with one long row per chunk.
constexpr size_t chunk_rows = 256;
/// For a join loop, `HashMethodPackedStringJoinKey` builds the keys of the loop's range 256 rows per call
/// of `buildRange`. The range can start at any row. Here it starts at row 77, so that no call starts at a multiple of
/// 256.
constexpr size_t join_loop_rows = 256;
constexpr size_t join_loop_first_row = 77;
/// The long row of a chunk is at or after `join_loop_first_row`, so that each call of `buildRange256` gets one too.
static_assert(join_loop_first_row < chunk_rows && join_loop_rows == chunk_rows);
/// A block, or a range of rows of a block, can have fewer rows than a chunk.
constexpr size_t min_small_rows = 16;
constexpr size_t max_small_rows = 256;

using Rng = std::mt19937_64;

size_t uniform(Rng & rng, size_t min, size_t max) { return std::uniform_int_distribution<size_t>(min, max)(rng); }

bool percent(Rng & rng, size_t p) { return uniform(rng, 0, 99) < p; }

bool chance(Rng & rng, double p) { return std::uniform_real_distribution<double>(0, 1)(rng) < p; }

struct Input
{
    const char * name;
    /// The length of row `row`; `position` is a random row of its chunk, from row `join_loop_first_row` of the chunk on.
    std::function<size_t(Rng & rng, size_t row, size_t position)> length;
};

/// The levels of the quantiles in `HitsColumn`.
// clang-format off
constexpr std::array<double, 23> levels{
    0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 0.99, 0.999, 1};
// clang-format on
using Quantiles = std::array<UInt16, levels.size()>;

/// A random value with the given quantiles, linear between them.
double randomQuantile(Rng & rng, const Quantiles & quantiles)
{
    const double level = std::uniform_real_distribution<double>(0, 1)(rng);
    const size_t i = std::upper_bound(levels.begin(), levels.end() - 1, level) - levels.begin() - 1;
    return quantiles[i] + (quantiles[i + 1] - quantiles[i]) * (level - levels[i]) / (levels[i + 1] - levels[i]);
}

/// The lengths of a `String` column of the ClickBench `hits` table, fitted on the 1M rows of `hits_0.parquet`. The rows
/// of one site are next to each other there, so the strings longer than 15 bytes come in runs. Here a run has 256 to
/// 4096 rows. Its share of long strings comes from `long_rows`, the quantiles of the number of long strings in a chunk
/// of the column. The lengths come from the quantiles of the short and of the long lengths.
struct HitsColumn
{
    Quantiles long_rows;
    Quantiles short_lengths;
    Quantiles long_lengths;
};

// clang-format off
constexpr HitsColumn hits_url{
    {227, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256},
    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 13, 15, 15, 15},
    {17, 27, 35, 42, 48, 54, 58, 63, 63, 68, 74, 80, 88, 95, 99, 105, 113, 121, 149, 195, 390, 500, 1991}};
constexpr HitsColumn hits_title{
    {2, 82, 229, 236, 240, 242, 244, 246, 248, 250, 254, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256},
    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 13, 13, 14, 15, 15},
    {16, 25, 30, 38, 38, 45, 55, 68, 87, 121, 121, 139, 153, 168, 188, 211, 234, 259, 340, 427, 493, 519, 1026}};
constexpr HitsColumn hits_referer{
    {76, 175, 211, 221, 226, 230, 233, 236, 239, 242, 244, 245, 247, 248, 249, 251, 252, 252, 253, 255, 256, 256, 256},
    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 14, 15},
    {16, 26, 33, 35, 43, 49, 55, 60, 64, 64, 68, 75, 84, 90, 95, 102, 110, 117, 128, 195, 448, 775, 2007}};
constexpr HitsColumn hits_search_phrase{
    {0, 0, 0, 1, 1, 2, 2, 3, 4, 5, 8, 16, 20, 24, 28, 31, 34, 38, 43, 49, 63, 80, 104},
    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 14, 15},
    {16, 22, 27, 29, 30, 33, 33, 36, 39, 43, 45, 49, 53, 56, 60, 64, 69, 76, 91, 110, 132, 209, 1939}};
constexpr HitsColumn hits_page_charset{
    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 251, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256},
    {0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7},
    {20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20}};
constexpr HitsColumn hits_original_url{
    {0, 0, 2, 2, 4, 6, 8, 10, 14, 18, 24, 29, 34, 39, 44, 48, 54, 61, 80, 174, 223, 242, 254},
    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 13},
    {19, 49, 56, 63, 79, 85, 104, 130, 137, 138, 143, 160, 192, 225, 231, 248, 253, 305, 393, 443, 500, 879, 3723}};
constexpr HitsColumn hits_utm_campaign{
    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 7, 26, 46},
    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 14, 15},
    {16, 19, 21, 21, 23, 28, 32, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 35, 60, 65, 65, 66}};
// clang-format on

std::function<size_t(Rng &, size_t, size_t)> hitsLengths(const HitsColumn & column)
{
    return [&column, run_rows = size_t{0}, long_share = 0.0](Rng & rng, size_t, size_t) mutable
    {
        if (run_rows == 0)
        {
            run_rows = uniform(rng, 256, 4096);
            long_share = randomQuantile(rng, column.long_rows) / chunk_rows;
        }
        --run_rows;
        const Quantiles & lengths = chance(rng, long_share) ? column.long_lengths : column.short_lengths;
        return static_cast<size_t>(std::lround(randomQuantile(rng, lengths)));
    };
}

/// The strings of an input, and their keys built by `PackedStringJoinKey::build`.
struct Data
{
    ColumnPtr column;
    std::vector<UInt128> expected;
};

/// The skip bytes of a block without skipped rows, as the join loops pass them to `buildRange`.
const std::vector<UInt8> no_skip(max_rows, 0);

/// S: strings of at most 15 bytes only; M: mixes of short and long strings; L: long strings of similar lengths;
/// U: long strings of very different lengths; V: strings of 512 bytes or more, in every row or in one row per chunk;
/// R: lengths of columns of the ClickBench `hits` table; O: strings of one length, with rare outliers at random rows.
std::vector<Input> makeInputs()
{
    return {
        {"S1_fixed_8", [](Rng &, size_t, size_t) -> size_t { return 8; }},
        {"S2_short_0_15", [](Rng & rng, size_t, size_t) { return uniform(rng, 0, 15); }},
        {"M1_short_and_1x20_per_chunk",
         [](Rng & rng, size_t row, size_t position) { return row % chunk_rows == position ? 20 : uniform(rng, 0, 15); }},
        {"M2_short_and_3%_16_300", [](Rng & rng, size_t, size_t) { return percent(rng, 3) ? uniform(rng, 16, 300) : uniform(rng, 0, 15); }},
        {"M3_short_and_10%_16_40", [](Rng & rng, size_t, size_t) { return percent(rng, 10) ? uniform(rng, 16, 40) : uniform(rng, 0, 15); }},
        {"M4_short_and_30%_16_40", [](Rng & rng, size_t, size_t) { return percent(rng, 30) ? uniform(rng, 16, 40) : uniform(rng, 0, 15); }},
        {"M5_short_and_50%_16_40", [](Rng & rng, size_t, size_t) { return percent(rng, 50) ? uniform(rng, 16, 40) : uniform(rng, 0, 15); }},
        {"M6_short_and_90%_16_64", [](Rng & rng, size_t, size_t) { return percent(rng, 90) ? uniform(rng, 16, 64) : uniform(rng, 0, 15); }},
        {"M7_uniform_0_63", [](Rng & rng, size_t, size_t) { return uniform(rng, 0, 63); }},
        {"L1_fixed_16", [](Rng &, size_t, size_t) -> size_t { return 16; }},
        {"L2_uniform_16_23", [](Rng & rng, size_t, size_t) { return uniform(rng, 16, 23); }},
        {"L3_fixed_32", [](Rng &, size_t, size_t) -> size_t { return 32; }},
        {"L4_uniform_16_40", [](Rng & rng, size_t, size_t) { return uniform(rng, 16, 40); }},
        {"L5_fixed_64", [](Rng &, size_t, size_t) -> size_t { return 64; }},
        {"L6_fixed_256", [](Rng &, size_t, size_t) -> size_t { return 256; }},
        {"U1_uniform_16_300", [](Rng & rng, size_t, size_t) { return uniform(rng, 16, 300); }},
        {"U2_16_or_1100", [](Rng & rng, size_t, size_t) -> size_t { return percent(rng, 50) ? 16 : 1100; }},
        {"U3_uniform_100_200", [](Rng & rng, size_t, size_t) { return uniform(rng, 100, 200); }},
        {"V1_fixed_600", [](Rng &, size_t, size_t) -> size_t { return 600; }},
        {"V2_uniform_512_1024", [](Rng & rng, size_t, size_t) { return uniform(rng, 512, 1024); }},
        {"V3_600_or_60000", [](Rng & rng, size_t, size_t) -> size_t { return percent(rng, 50) ? 600 : 60000; }},
        {"V4_fixed_4096", [](Rng &, size_t, size_t) -> size_t { return 4096; }},
        {"V5_16_23_and_1x600_per_chunk",
         [](Rng & rng, size_t row, size_t position) { return row % chunk_rows == position ? 600 : uniform(rng, 16, 23); }},
        {"R1_hits_URL", hitsLengths(hits_url)},
        {"R2_hits_Title", hitsLengths(hits_title)},
        {"R3_hits_Referer", hitsLengths(hits_referer)},
        {"R4_hits_SearchPhrase", hitsLengths(hits_search_phrase)},
        {"R5_hits_PageCharset", hitsLengths(hits_page_charset)},
        {"R6_hits_OriginalURL", hitsLengths(hits_original_url)},
        {"R7_hits_UTMCampaign", hitsLengths(hits_utm_campaign)},
        {"O1_fixed_36_and_0.5%_100_1000",
         [](Rng & rng, size_t, size_t) -> size_t { return chance(rng, 0.005) ? uniform(rng, 100, 1000) : 36; }},
        {"O2_fixed_10_and_0.5%_16_64", [](Rng & rng, size_t, size_t) -> size_t { return chance(rng, 0.005) ? uniform(rng, 16, 64) : 10; }},
        {"O3_fixed_24_and_0.1%_4096", [](Rng & rng, size_t, size_t) -> size_t { return chance(rng, 0.001) ? 4096 : 24; }},
    };
}

const char * charsOf(const ColumnString & column) { return reinterpret_cast<const char *>(column.getChars().data()); }

Data makeData(const Input & input)
{
    Rng rng(std::hash<std::string>{}(input.name));
    std::string random_bytes(1 << 17, '\0');
    for (auto & byte : random_bytes)
        byte = static_cast<char>(rng());

    /// A copy, because the lengths of the R inputs depend on the rows before.
    auto next_length = input.length;
    auto column = ColumnString::create();
    size_t bytes = 0;
    size_t position = 0;
    for (size_t row = 0; row < max_rows && (bytes < max_bytes || row < min_rows); ++row)
    {
        if (row % chunk_rows == 0)
            position = uniform(rng, join_loop_first_row, chunk_rows - 1);
        const size_t length = next_length(rng, row, position);
        column->insertData(random_bytes.data() + uniform(rng, 0, random_bytes.size() - length), length);
        bytes += length;
    }

    std::vector<UInt128> expected(column->size());
    for (size_t row = 0; row < column->size(); ++row)
    {
        const auto value = column->getDataAt(row);
        expected[row] = PackedStringJoinKey::build(value.data(), value.size());
    }
    return {std::move(column), std::move(expected)};
}

void setRows(benchmark::State & state, size_t rows)
{
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(rows));
}

void buildRange(benchmark::State & state, const Data & data)
{
    const auto & column = assert_cast<const ColumnString &>(*data.column);
    std::vector<UInt128> keys(column.size());
    PackedStringJoinKey::buildRange(charsOf(column), column.getOffsets().data(), 0, column.size(), keys.data(), no_skip.data());
    if (keys != data.expected)
        state.SkipWithError("buildRange differs from build");
    for (auto _ [[maybe_unused]] : state)
    {
        PackedStringJoinKey::buildRange(charsOf(column), column.getOffsets().data(), 0, column.size(), keys.data(), no_skip.data());
        benchmark::DoNotOptimize(keys.data());
        benchmark::ClobberMemory();
    }
    setRows(state, column.size());
}

void buildRange256(benchmark::State & state, const Data & data)
{
    const auto & column = assert_cast<const ColumnString &>(*data.column);
    const IColumn::Offset * offsets = column.getOffsets().data();
    std::vector<UInt128> keys(join_loop_rows);
    for (size_t begin = join_loop_first_row; begin < column.size(); begin += join_loop_rows)
    {
        const size_t end = std::min(column.size(), begin + join_loop_rows);
        PackedStringJoinKey::buildRange(charsOf(column), offsets, begin, end, keys.data(), no_skip.data());
        if (!std::equal(keys.data(), keys.data() + (end - begin), data.expected.data() + begin))
        {
            state.SkipWithError("buildRange of 256 rows differs from build");
            break;
        }
    }
    for (auto _ [[maybe_unused]] : state)
    {
        for (size_t begin = join_loop_first_row; begin < column.size(); begin += join_loop_rows)
        {
            PackedStringJoinKey::buildRange(
                charsOf(column), offsets, begin, std::min(column.size(), begin + join_loop_rows), keys.data(), no_skip.data());
            benchmark::DoNotOptimize(keys.data());
        }
        benchmark::ClobberMemory();
    }
    setRows(state, column.size() - join_loop_first_row);
}

void buildRangeSmall(benchmark::State & state, const Data & data)
{
    const auto & column = assert_cast<const ColumnString &>(*data.column);
    const IColumn::Offset * offsets = column.getOffsets().data();
    /// The ends of back-to-back ranges of random sizes.
    std::vector<size_t> ends;
    Rng rng(column.size());
    for (size_t end = 0; end < column.size();)
    {
        end = std::min(column.size(), end + uniform(rng, min_small_rows, max_small_rows));
        ends.push_back(end);
    }

    std::vector<UInt128> keys(max_small_rows);
    size_t begin = 0;
    for (const size_t end : ends)
    {
        PackedStringJoinKey::buildRange(charsOf(column), offsets, begin, end, keys.data(), no_skip.data());
        if (!std::equal(keys.data(), keys.data() + (end - begin), data.expected.data() + begin))
        {
            state.SkipWithError("buildRange of a small range differs from build");
            break;
        }
        begin = end;
    }
    for (auto _ [[maybe_unused]] : state)
    {
        begin = 0;
        for (const size_t end : ends)
        {
            PackedStringJoinKey::buildRange(charsOf(column), offsets, begin, end, keys.data(), no_skip.data());
            benchmark::DoNotOptimize(keys.data());
            begin = end;
        }
        benchmark::ClobberMemory();
    }
    setRows(state, column.size());
}

void buildPerRow(benchmark::State & state, const Data & data)
{
    const auto & column = assert_cast<const ColumnString &>(*data.column);
    std::vector<UInt128> keys(column.size());
    for (auto _ [[maybe_unused]] : state)
    {
        for (size_t row = 0; row < column.size(); ++row)
        {
            const auto value = column.getDataAt(row);
            keys[row] = PackedStringJoinKey::build(value.data(), value.size());
        }
        benchmark::DoNotOptimize(keys.data());
        benchmark::ClobberMemory();
    }
    setRows(state, column.size());
}

#if (defined(__aarch64__) && defined(__ARM_NEON)) || defined(__AVX2__)

static_assert(chunk_rows == SipHashStrings::chunk_rows);

/// For one chunk, the rows whose strings are longer than 15 bytes, and their length buckets, as `buildRange` passes
/// them to `SipHashStrings::hashRows`.
struct LongChunk
{
    UInt32 rows[chunk_rows];
    uint8_t buckets[chunk_rows];
    UInt32 num_long;
};

struct LongRows
{
    std::vector<LongChunk> chunks;
    /// The same rows, numbered in the column.
    std::vector<size_t> rows;
};

/// The hashing in `buildRange` of the rows [0, num_rows).
NO_INLINE void hashLongRows(const char * chars, const UInt64 * offsets, size_t num_rows, const LongChunk * chunks, UInt128 * keys)
{
    for (size_t chunk_begin = 0; chunk_begin < num_rows; chunk_begin += chunk_rows)
    {
        const LongChunk & chunk = chunks[chunk_begin / chunk_rows];
        /// `hashRows` changes the buckets.
        alignas(16) uint8_t buckets[chunk_rows + 32];
        std::copy(chunk.buckets, chunk.buckets + chunk.num_long, buckets);
        SipHashStrings::hashRows<PackedStringJoinKey::hashed_flag>(
            chars, offsets + chunk_begin - 1, chunk.rows, buckets, chunk.num_long, keys + chunk_begin);
    }
}

/// For each chunk, the rows whose strings are longer than 15 bytes, listed as `buildRange` lists them.
LongRows listLongRows(const ColumnString & column)
{
    const IColumn::Offset * offsets = column.getOffsets().data();
    LongRows long_rows{std::vector<LongChunk>((column.size() + chunk_rows - 1) / chunk_rows), {}};
    for (size_t chunk_begin = 0; chunk_begin < column.size(); chunk_begin += chunk_rows)
    {
        LongChunk & chunk = long_rows.chunks[chunk_begin / chunk_rows];
        alignas(16) uint8_t buckets[chunk_rows + 32];
        const size_t num_rows = std::min(chunk_rows, column.size() - chunk_begin);
        SipHashStrings::lengthBuckets(offsets + chunk_begin - 1, num_rows, buckets);
        for (size_t r = 0; r < num_rows; ++r)
        {
            /// The bucket is the number of 8-byte words, up to a limit: 2 or more for a string longer than 15 bytes.
            if (buckets[r] < 2)
                continue;
            chunk.rows[chunk.num_long] = static_cast<UInt32>(r);
            chunk.buckets[chunk.num_long] = buckets[r];
            ++chunk.num_long;
            long_rows.rows.push_back(chunk_begin + r);
        }
    }
    return long_rows;
}

void hashRange(benchmark::State & state, const Data & data)
{
    const auto & column = assert_cast<const ColumnString &>(*data.column);
    const auto long_rows = listLongRows(column);
    std::vector<UInt128> keys(column.size());
    hashLongRows(charsOf(column), column.getOffsets().data(), column.size(), long_rows.chunks.data(), keys.data());
    for (const size_t row : long_rows.rows)
        if (keys[row] != data.expected[row])
            state.SkipWithError("hashRows differs from build");
    for (auto _ [[maybe_unused]] : state)
    {
        hashLongRows(charsOf(column), column.getOffsets().data(), column.size(), long_rows.chunks.data(), keys.data());
        benchmark::DoNotOptimize(keys.data());
        benchmark::ClobberMemory();
    }
    setRows(state, column.size());
}

void hashPerRow(benchmark::State & state, const Data & data)
{
    const auto & column = assert_cast<const ColumnString &>(*data.column);
    const auto long_rows = listLongRows(column);
    std::vector<UInt128> keys(column.size());
    for (auto _ [[maybe_unused]] : state)
    {
        for (const size_t row : long_rows.rows)
        {
            const auto value = column.getDataAt(row);
            keys[row] = PackedStringJoinKey::build(value.data(), value.size());
        }
        benchmark::DoNotOptimize(keys.data());
        benchmark::ClobberMemory();
    }
    setRows(state, column.size());
}

#endif

}

int main(int argc, char ** argv)
{
    benchmark::MaybeReenterWithoutASLR(argc, argv);
    static std::deque<Data> inputs_data;
    for (const auto & input : makeInputs())
    {
        const auto & input_data = inputs_data.emplace_back(makeData(input));
        const std::string name = input.name;
        benchmark::RegisterBenchmark("buildRange/" + name, [&input_data](benchmark::State & state) { buildRange(state, input_data); });
        benchmark::RegisterBenchmark(
            "buildRange256/" + name, [&input_data](benchmark::State & state) { buildRange256(state, input_data); });
        benchmark::RegisterBenchmark(
            "buildRangeSmall/" + name, [&input_data](benchmark::State & state) { buildRangeSmall(state, input_data); });
        benchmark::RegisterBenchmark("buildPerRow/" + name, [&input_data](benchmark::State & state) { buildPerRow(state, input_data); });
#if (defined(__aarch64__) && defined(__ARM_NEON)) || defined(__AVX2__)
        benchmark::RegisterBenchmark("hashRange/" + name, [&input_data](benchmark::State & state) { hashRange(state, input_data); });
        benchmark::RegisterBenchmark("hashPerRow/" + name, [&input_data](benchmark::State & state) { hashPerRow(state, input_data); });
#endif
    }

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}
