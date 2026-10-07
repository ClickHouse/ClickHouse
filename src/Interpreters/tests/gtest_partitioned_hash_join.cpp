#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <optional>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/PartitionedHashJoin/DenseHyperLogLog.h>
#include <Interpreters/PartitionedHashJoin/HashJoinClause.h>
#include <Interpreters/PartitionedHashJoin/PartitionedHashJoin.h>
#include <Interpreters/PartitionedHashJoin/HashJoinTable.h>
#include <Interpreters/TableJoin.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>
#include <Common/scope_guard_safe.h>
#include <Common/typeid_cast.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int LIMIT_EXCEEDED;
extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace ProfileEvents
{
extern const Event QueryMemoryLimitExceeded;
extern const Event HashJoinPreallocatedElementsInHashTables;
}

namespace
{

constexpr size_t block_rows = 65536;
/// The multiplicative step of the build keys: `keyOf(i) = i * key_step + 1`.
constexpr UInt64 key_step = 2654435761ULL;

UInt64 keyOf(size_t i)
{
    return i * key_step + 1;
}

/// The table type the `UInt64` keys use, for its geometry helpers and its degree arithmetic.
using Key64Table = typename decltype(HashJoinTableMapsAll::key64)::element_type;

/// One joined output row: `(k, probe_id, rk, build_id)`. The sorted multiset of these over a whole
/// probe is an exact identity. A dropped, duplicated or cross-wired row changes it. A build row
/// inserted outside its owner's range is never found by the probe's walk from the key's home cell.
/// Mis-routing therefore shows up as missing tuples.
using JoinedRow = std::tuple<UInt64, UInt64, UInt64, UInt64>;
using JoinedRows = std::vector<JoinedRow>;

Block uint64Block(const std::vector<std::pair<String, std::vector<UInt64>>> & columns)
{
    Block block;
    for (const auto & [name, values] : columns)
    {
        auto column = ColumnUInt64::create();
        column->getData().assign(values.begin(), values.end());
        block.insert({std::move(column), std::make_shared<DataTypeUInt64>(), name});
    }
    return block;
}

Block twoColumnBlock(const String & key_name, const String & id_name, const std::vector<UInt64> & keys, const std::vector<UInt64> & ids)
{
    return uint64Block({{key_name, keys}, {id_name, ids}});
}

/// The values of a `UInt64` column, read through the `Nullable` wrapper where the build key has one.
const UInt64 * columnData(const Block & block, const String & name, ColumnPtr & holder)
{
    holder = block.getByName(name).column->convertToFullColumnIfReplicated();
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(holder.get()))
    {
        ColumnPtr nested = nullable->getNestedColumnPtr();
        holder = nested;
    }
    return assert_cast<const ColumnUInt64 &>(*holder).getData().data();
}

void accumulateRows(const Block & block, JoinedRows & rows)
{
    if (!block.rows())
        return;
    ColumnPtr k_holder;
    ColumnPtr probe_holder;
    ColumnPtr rk_holder;
    ColumnPtr build_holder;
    const UInt64 * k = columnData(block, "k", k_holder);
    const UInt64 * probe_id = columnData(block, "probe_id", probe_holder);
    const UInt64 * rk = columnData(block, "rk", rk_holder);
    const UInt64 * build_id = columnData(block, "build_id", build_holder);
    for (size_t i = 0; i < block.rows(); ++i)
        rows.emplace_back(k[i], probe_id[i], rk[i], build_id[i]);
}

void drainResult(IJoinResult & result, JoinedRows & rows)
{
    while (true)
    {
        auto r = result.next();
        accumulateRows(r.block, rows);
        if (r.is_last)
            return;
    }
}

/// The inequality key pair of an ASOF join: `(left column, right column)`.
struct AsofKey
{
    ASOFJoinInequality inequality;
    String left_name;
    String right_name;
};

/// Everything a test varies about a build. The settings go onto the `Settings` the `TableJoin` reads;
/// the `*_for_tests` fields go onto the join's test hooks.
struct BuildOptions
{
    size_t num_threads = 4;
    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
    std::optional<AsofKey> asof;
    size_t build_block_rows = block_rows;
    /// `parallel_hash_join_threshold`; unset keeps the default (100000 rows).
    std::optional<size_t> parallel_hash_join_threshold;
    /// `partitioned_hash_join_max_fanout_per_pass`, lowered to force refine passes without a 500M-key
    /// build. Only the pass split changes; the partition count must not.
    std::optional<size_t> max_fanout_per_pass;
    bool cap_partitions_by_l1_descriptors = true;
    /// The planner's row store switch; the saved block of a RIGHT/FULL join has two UInt64 columns
    /// here, enough for a row store.
    bool enable_row_store = false;
    const StatsCollectingParams * stats_collecting_params = nullptr;
    /// A factor below 1 sizes the table under the key count, so the build has to grow it.
    double reserve_safety_for_tests = 0;
    std::optional<size_t> reserve_override_for_tests;
    /// Pins the build and the probe onto the sequential loops instead of the AMAC ring, the
    /// prefetching insert and probe loop of `AmacRing.h`.
    bool disable_amac = false;
    std::optional<size_t> partition_bits_for_tests;
    size_t l1_cache_bytes_for_tests = 0;
};

std::shared_ptr<TableJoin> makeTableJoin(const Block & left_header, const Block & right_header, const BuildOptions & options = {})
{
    Settings settings;
    if (options.parallel_hash_join_threshold)
        settings.set("parallel_hash_join_threshold", *options.parallel_hash_join_threshold);
    if (options.max_fanout_per_pass)
        settings.set("partitioned_hash_join_max_fanout_per_pass", *options.max_fanout_per_pass);
    if (!options.cap_partitions_by_l1_descriptors)
        settings.set("partitioned_hash_join_cap_partitions_by_l1_descriptors", false);
    auto table_join = std::make_shared<TableJoin>(settings, JoinAnalyzeMode::None, /*tmp_volume=*/nullptr, /*tmp_data=*/nullptr);
    table_join->setKind(options.kind);
    table_join->getTableJoin().strictness = options.strictness;
    table_join->addDisjunct();
    table_join->getClauses().back().addKey(
        left_header.getByPosition(0).name, right_header.getByPosition(0).name, /*null_safe_comparison=*/false);
    if (options.asof)
    {
        table_join->setAsofInequality(options.asof->inequality);
        table_join->getClauses().back().addKey(options.asof->left_name, options.asof->right_name, /*null_safe_comparison=*/false);
    }

    NamesAndTypesList left_columns;
    NamesAndTypesList right_columns;
    Names used_columns;
    for (const auto & col : left_header)
    {
        left_columns.emplace_back(col.name, col.type);
        used_columns.push_back(col.name);
    }
    for (const auto & col : right_header)
    {
        right_columns.emplace_back(col.name, col.type);
        used_columns.push_back(col.name);
    }
    table_join->setInputColumns(std::move(left_columns), std::move(right_columns));
    table_join->setUsedColumns(used_columns);
    table_join->setRowStoreEnabled(options.enable_row_store);
    return table_join;
}

struct BuiltJoin
{
    std::shared_ptr<TableJoin> table_join;
    std::shared_ptr<PartitionedHashJoin> join;
};

/// Empty join with test hooks from `options` applied.
BuiltJoin makeJoin(
    const BuildOptions & options,
    const Block & probe_header = twoColumnBlock("k", "probe_id", {}, {}),
    const Block & build_header = twoColumnBlock("rk", "build_id", {}, {}))
{
    BuiltJoin result;
    result.table_join = makeTableJoin(probe_header, build_header, options);
    result.join = std::make_shared<PartitionedHashJoin>(
        result.table_join,
        std::make_shared<const Block>(build_header),
        options.num_threads,
        /*any_take_last_row_=*/false,
        HashJoinStatsCollectingParams{
            .build = options.stats_collecting_params ? *options.stats_collecting_params : StatsCollectingParams{}, .match = {}});
    if (options.reserve_safety_for_tests > 0)
        result.join->setReserveSafetyFactorForTests(options.reserve_safety_for_tests);
    if (options.reserve_override_for_tests)
        result.join->setReserveOverrideForTests(*options.reserve_override_for_tests);
    if (options.disable_amac)
        result.join->setAmacEnabledForTests(false);
    if (options.partition_bits_for_tests)
        result.join->setPartitionBitsForTests(*options.partition_bits_for_tests);
    if (options.l1_cache_bytes_for_tests > 0)
        result.join->setL1CacheSizeForTests(options.l1_cache_bytes_for_tests);
    return result;
}

/// The blocks `buildJoin` feeds, as key indexes and row ids, `options.build_block_rows` rows each.
/// Key-major: a key's rows are adjacent. Row `d` of key `i` has id `i * duplicates + d`.
template <typename Sink>
void forEachBuildBlock(size_t distinct_keys, size_t duplicates, const BuildOptions & options, Sink && sink)
{
    std::vector<size_t> key_indexes;
    std::vector<UInt64> ids;
    key_indexes.reserve(options.build_block_rows);
    ids.reserve(options.build_block_rows);
    const auto flush = [&]
    {
        if (key_indexes.empty())
            return;
        sink(key_indexes, ids);
        key_indexes.clear();
        ids.clear();
    };
    const auto push = [&](size_t i, size_t d)
    {
        key_indexes.push_back(i);
        ids.push_back(i * duplicates + d);
        if (key_indexes.size() == options.build_block_rows)
            flush();
    };
    for (size_t i = 0; i < distinct_keys; ++i)
        for (size_t d = 0; d < duplicates; ++d)
            push(i, d);
    flush();
}

std::vector<UInt64> keysOf(const std::vector<size_t> & key_indexes)
{
    std::vector<UInt64> keys(key_indexes.size());
    for (size_t i = 0; i < keys.size(); ++i)
        keys[i] = keyOf(key_indexes[i]);
    return keys;
}

bool addBuildBlock(IJoin & join, const Block & block, size_t worker_id = 0)
{
    return join.addBlockToJoin(block, block.rows(), worker_id, /*check_limits=*/true);
}

void addBuildBlocks(PartitionedHashJoin & join, size_t distinct_keys, size_t duplicates, const BuildOptions & options)
{
    forEachBuildBlock(
        distinct_keys,
        duplicates,
        options,
        [&](const std::vector<size_t> & key_indexes, const std::vector<UInt64> & ids)
        {
            EXPECT_TRUE(addBuildBlock(join, twoColumnBlock("rk", "build_id", keysOf(key_indexes), ids)));
        });
}

void addBlock(PartitionedHashJoin & join, const std::vector<UInt64> & keys, UInt64 & next_id)
{
    std::vector<UInt64> ids(keys.size());
    for (UInt64 & id : ids)
        id = next_id++;
    EXPECT_TRUE(addBuildBlock(join, twoColumnBlock("rk", "build_id", keys, ids)));
}

void finishBuild(BuiltJoin & built)
{
    built.join->onBuildPhaseFinish();
    built.join->runPostBuildPhase();
}

/// Feeds the build through the `IJoin` interface: `addBlockToJoin`, `onBuildPhaseFinish`,
/// `runPostBuildPhase`. A block of more than 65536 rows makes the scatter store each row's (block,
/// row) position in 8 bytes instead of packing it into 4.
BuiltJoin buildJoin(size_t distinct_keys, size_t duplicates, const BuildOptions & options)
{
    BuiltJoin result = makeJoin(options);
    addBuildBlocks(*result.join, distinct_keys, duplicates, options);
    finishBuild(result);
    return result;
}

/// Probes `keys` in blocks of `block_rows`, with `probe_id` the key's index, and returns every joined
/// tuple. With `rotate_lanes` the blocks go through `joinBlock(block, lane)`. The lane cycles over
/// `0..8`. The join has 2 x num_threads = 8 probe lanes, so lane 8 does not exist. That call takes
/// the shared pool of `acquireProbeScratch`.
JoinedRows probeKeys(PartitionedHashJoin & join, const std::vector<UInt64> & keys, bool rotate_lanes = false)
{
    JoinedRows rows;
    std::vector<UInt64> block_keys;
    std::vector<UInt64> ids;
    size_t block_index = 0;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        block_keys.push_back(keys[i]);
        ids.push_back(i);
        if (block_keys.size() == block_rows || i + 1 == keys.size())
        {
            Block block = twoColumnBlock("k", "probe_id", block_keys, ids);
            auto result = rotate_lanes ? join.joinBlock(std::move(block), block_index++ % 9) : join.joinBlock(std::move(block));
            drainResult(*result, rows);
            block_keys.clear();
            ids.clear();
        }
    }
    return rows;
}

void expectSameRows(JoinedRows actual, const JoinedRows & expected)
{
    std::sort(actual.begin(), actual.end());
    ASSERT_EQ(actual.size(), expected.size());
    ASSERT_TRUE(actual == expected);
}

/// Probes every distinct key once, plus `misses` absent ones, and checks the exact joined multiset.
void probeAndCheck(BuiltJoin & built, size_t distinct_keys, size_t duplicates, size_t misses, bool rotate_lanes = false)
{
    JoinedRows expected;
    expected.reserve(distinct_keys * duplicates);
    for (size_t i = 0; i < distinct_keys; ++i)
    {
        const UInt64 key = keyOf(i);
        for (size_t d = 0; d < duplicates; ++d)
            expected.emplace_back(key, i, key, i * duplicates + d);
    }
    std::sort(expected.begin(), expected.end());

    /// The +2 offset cannot collide with a built key. `i * key_step + 2 == j * key_step + 1` needs
    /// `i - j` to be the modular inverse of `-key_step`. No index here comes close.
    std::vector<UInt64> keys(distinct_keys + misses);
    for (size_t i = 0; i < keys.size(); ++i)
        keys[i] = i < distinct_keys ? keyOf(i) : i * key_step + 2;
    expectSameRows(probeKeys(*built.join, keys, rotate_lanes), expected);
}

/// The invariants every build must publish: one table, its rows conserved, its distinct count exact.
void expectTableInvariants(const PartitionedHashJoin::BuildStats & stats, size_t distinct_keys, size_t rows)
{
    EXPECT_EQ(stats.table_cells, 1uz << stats.table_size_degree);
    EXPECT_GE(stats.table_cells, 2 * distinct_keys) << "the table must keep at most 50% fill";
    EXPECT_EQ(stats.inserted_rows, rows);
    EXPECT_EQ(stats.distinct_keys, distinct_keys);
    EXPECT_TRUE(stats.predictions_exact || stats.table_resizes > 0)
        << "the created buffer must match the plan's prediction, or a grow must have replaced it";
    EXPECT_EQ(stats.partitions, 1uz << stats.bits);
    EXPECT_LE(stats.bits, stats.table_size_degree);
}

SpanWriter::Stats totalDuplicates(const PartitionedHashJoin::BuildStats & stats)
{
    SpanWriter::Stats total = stats.owner_duplicates;
    total += stats.drain_duplicates;
    return total;
}

/// Runs `fn`, which must throw an `Exception` with `code`; `what` names the expectation in the failure
/// report. Returns the message for further checks.
template <typename F>
String expectThrowsCode(int code, std::string_view what, F && fn)
{
    try
    {
        fn();
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), code) << e.message();
        return e.message();
    }
    ADD_FAILURE() << what;
    return {};
}

/// A build whose special keys all have their home cell in the last `window` cells of the last
/// partition range. `fillers` unique keys arrive first and fill that window; then 2, 3 and 9 rows of
/// three more such keys arrive in later blocks. Their owner inserts reach the range end and hand the
/// rows to the overflow. The drain must place them past the buffer end, wrapping into partition 0,
/// and build their exact runs there. The probe must wrap the same way to find them.
struct CrossingBuild
{
    BuiltJoin built;
    JoinedRows expected; /// every (key, probe_id, key, build_id) the probe of all keys must produce
    std::vector<UInt64> probe_keys;
    size_t special_rows = 0;
    size_t fillers = 0;
    size_t window = 0;
};

CrossingBuild buildCrossing(size_t num_threads, bool disable_amac, size_t bits)
{
    constexpr size_t random_keys = 100000;
    BuildOptions options;
    options.num_threads = num_threads;
    options.partition_bits_for_tests = bits;
    options.disable_amac = disable_amac;

    /// The geometry the real build will have. The same key count gives the same table degree, since
    /// the estimate would have to be off by more than 20% to move it. The partition bits are forced.
    const auto pilot = buildJoin(random_keys, /*duplicates=*/1, options).join->getBuildStats();
    EXPECT_EQ(pilot.bits, bits) << "the forced partition bits must be accepted";
    Key64Table geometry(pilot.table_size_degree, bits);
    const size_t last = geometry.partitions() - 1;
    const size_t range_end = geometry.rangeEnd(last);
    EXPECT_EQ(range_end, geometry.cellCount()) << "the crossing window must sit at the buffer end";

    CrossingBuild result;
    result.window = 64;
    result.fillers = result.window + 192;
    const size_t window_begin = range_end - result.window;

    /// Keys whose home cell lies inside the window. Found by scanning candidates that cannot collide
    /// with the random keys below. `n * key_step + 2` equals `i * key_step + 1` only for an `n - i`
    /// far outside these ranges.
    std::vector<UInt64> window_keys;
    for (UInt64 candidate = 2; window_keys.size() < result.fillers + 3; candidate += key_step)
    {
        const size_t home = geometry.place(geometry.hash(candidate));
        if (home >= window_begin && home < range_end)
            window_keys.push_back(candidate);
    }

    result.built = makeJoin(options);
    UInt64 next_id = 0;
    std::vector<std::pair<UInt64, UInt64>> build_rows; /// (key, build_id) in insertion order
    const auto add_block = [&](const std::vector<UInt64> & keys)
    {
        for (size_t i = 0; i < keys.size(); ++i)
            build_rows.emplace_back(keys[i], next_id + i);
        addBlock(*result.built.join, keys, next_id);
    };

    /// The fillers go first so every worker meets them before any random key.
    add_block(std::vector<UInt64>(window_keys.begin(), window_keys.begin() + result.fillers));
    std::vector<UInt64> keys;
    for (size_t i = 0; i < random_keys; ++i)
    {
        keys.push_back(keyOf(i));
        if (keys.size() == block_rows || i + 1 == random_keys)
        {
            add_block(keys);
            keys.clear();
        }
    }
    /// The three duplicate keys are interleaved so no key's rows are adjacent.
    const UInt64 dup_a = window_keys[result.fillers];
    const UInt64 dup_b = window_keys[result.fillers + 1];
    const UInt64 dup_c = window_keys[result.fillers + 2];
    std::vector<UInt64> dups;
    for (size_t i = 0; i < 9; ++i)
    {
        dups.push_back(dup_c);
        if (i < 3)
            dups.push_back(dup_b);
        if (i < 2)
            dups.push_back(dup_a);
    }
    result.special_rows = dups.size();
    add_block(dups);
    finishBuild(result.built);

    /// Every distinct key probed once; the expected multiset has one tuple per build row.
    std::vector<UInt64> distinct;
    for (const auto & [key, id] : build_rows)
        distinct.push_back(key);
    std::sort(distinct.begin(), distinct.end());
    distinct.erase(std::unique(distinct.begin(), distinct.end()), distinct.end());
    result.probe_keys = distinct;
    for (size_t probe_id = 0; probe_id < distinct.size(); ++probe_id)
        for (const auto & [key, id] : build_rows)
            if (key == distinct[probe_id])
                result.expected.emplace_back(key, probe_id, key, id);
    std::sort(result.expected.begin(), result.expected.end());
    return result;
}

void expectCrossingStats(const CrossingBuild & crossing)
{
    const auto stats = crossing.built.join->getBuildStats();
    /// The window holds `window` cells. Every later window key reached the range end and went through
    /// the drain, which wrapped past the buffer end. That is the extra fillers and all 14 duplicate rows.
    EXPECT_GE(stats.overflow_rows, crossing.fillers - crossing.window + crossing.special_rows);
    EXPECT_GE(stats.drain_claimed_keys, crossing.fillers - crossing.window + 3);
    /// Only the three duplicate keys had more than one row among the drained rows.
    EXPECT_EQ(stats.drain_appended_rows, 1u + 2u + 8u);
    EXPECT_EQ(stats.drain_duplicates.ranges, 3u);
    EXPECT_EQ(stats.drain_duplicates.headers, 0u);
    EXPECT_EQ(stats.drain_duplicates.arena_bytes, 8u * (2 + 3 + 9));
    EXPECT_EQ(stats.owner_duplicates.arena_bytes, 0u) << "the owners saw only unique keys";
    EXPECT_EQ(stats.distinct_keys, crossing.probe_keys.size());
}

}

TEST(PartitionedHashJoin, DenseSketchConcurrentRead)
{
    constexpr UInt32 words = 300000;
    constexpr size_t passes = 16;
    DenseHyperLogLog expected;
    expected.add(7);
    for (UInt32 i = 0; i < words; ++i)
        expected.add(i);

    DenseHyperLogLog live;
    std::atomic<size_t> reader_phase{0};
    std::atomic<size_t> writer_phase{0};
    std::thread writer([&]
    {
        for (size_t phase = 0; phase < passes; ++phase)
        {
            reader_phase.wait(phase, std::memory_order_acquire);
            for (size_t i = 0; i < 1024; ++i)
                live.add(7);
            writer_phase.store(phase + 1, std::memory_order_release);
            writer_phase.notify_one();
        }
        for (UInt32 i = 0; i < words; ++i)
            live.add(i);
    });

    bool invalid_estimate = false;
    for (size_t phase = 0; phase < passes; ++phase)
    {
        reader_phase.store(phase + 1, std::memory_order_release);
        reader_phase.notify_one();
        DenseHyperLogLog snapshot;
        snapshot.merge(live);
        const double merged = snapshot.estimate();
        const double direct = live.estimate();
        invalid_estimate |= !std::isfinite(merged) || !std::isfinite(direct) || merged < 0 || direct < 0;
        writer_phase.wait(phase, std::memory_order_acquire);
    }
    writer.join();

    EXPECT_FALSE(invalid_estimate);
    for (size_t i = 0; i < DenseHyperLogLog::register_count; ++i)
        EXPECT_EQ(live.registers[i].load(std::memory_order_relaxed), expected.registers[i].load(std::memory_order_relaxed));
    EXPECT_EQ(live.estimate(), expected.estimate());
}

/// Build blocks carrying a worker id, and probe blocks a lane, the join has no entry for must still
/// produce the exact multiset.
TEST(PartitionedHashJoin, OutOfRangeLaneFallsBackToPool)
{
    /// The lane table holds 2 x num_threads = 8 lanes, so `% 9` sends every ninth block to lane 8.
    /// That lane takes the thread-id map in `getFillLane` and the shared pool in `acquireProbeScratch`.
    constexpr size_t distinct_keys = 100000;
    constexpr size_t duplicates = 2;
    const BuildOptions options;
    BuiltJoin built = makeJoin(options);
    size_t build_block_index = 0;
    forEachBuildBlock(
        distinct_keys,
        duplicates,
        options,
        [&](const std::vector<size_t> & key_indexes, const std::vector<UInt64> & ids)
        {
            const Block block = twoColumnBlock("rk", "build_id", keysOf(key_indexes), ids);
            EXPECT_TRUE(addBuildBlock(*built.join, block, /*worker_id=*/build_block_index++ % 9));
        });
    finishBuild(built);

    expectTableInvariants(built.join->getBuildStats(), distinct_keys, distinct_keys * duplicates);
    probeAndCheck(built, distinct_keys, duplicates, /*misses=*/100, /*rotate_lanes=*/true);
}

/// Above `parallel_hash_join_threshold` rows every worker gets a partition, but never more partitions than distinct keys.
TEST(PartitionedHashJoin, PartitionFloorNeverExceedsDistinctKeys)
{
    /// Two keys with 300000 rows each on four workers: the table is tiny, so the cache-size rule alone
    /// picks one partition. The threshold rule wants one partition per worker and stops at two, one per key.
    constexpr size_t distinct_keys = 2;
    constexpr size_t duplicates = 300000;
    BuildOptions options;
    options.num_threads = 4;
    options.parallel_hash_join_threshold = 1000;
    auto built = buildJoin(distinct_keys, duplicates, options);
    const auto stats = built.join->getBuildStats();
    EXPECT_EQ(stats.partitions, 2u);
    EXPECT_GE(stats.table_cells, stats.partitions << 10) << "every partition range keeps at least 2^10 cells";
    expectTableInvariants(stats, distinct_keys, distinct_keys * duplicates);
    probeAndCheck(built, distinct_keys, duplicates, /*misses=*/10);
}

/// A table sized far below the key count grows until the build fits.
TEST(PartitionedHashJoin, UndersizedTableGrows)
{
    /// A safety factor of 0.25 reserves a quarter of the keys. Growth restores the fill; the build
    /// must not throw and the probe must still be an identity.
    constexpr size_t distinct_keys = 50000;
    BuildOptions options;
    options.reserve_safety_for_tests = 0.25;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, options);
    const auto stats = built.join->getBuildStats();
    EXPECT_GE(stats.table_resizes, 1u);
    expectTableInvariants(stats, distinct_keys, distinct_keys);
    probeAndCheck(built, distinct_keys, /*duplicates=*/1, /*misses=*/100);
}

/// The L1 descriptor cap bounds the partition count to what a quarter of L1 holds. It does so only
/// while it is switched on.
TEST(PartitionedHashJoin, DescriptorCapClampsPlan)
{
    /// With a 256-byte L1, a quarter of L1 holds four 16-byte per-partition descriptors. The cap
    /// then limits a 2M-key build to four partitions. With the cap off the same L1 changes nothing.
    constexpr size_t distinct_keys = 2000000;
    constexpr size_t l1_bytes = 256;

    BuildOptions uncapped_options;
    uncapped_options.cap_partitions_by_l1_descriptors = false;
    uncapped_options.l1_cache_bytes_for_tests = l1_bytes;
    auto uncapped = buildJoin(distinct_keys, /*duplicates=*/1, uncapped_options);
    ASSERT_GT(uncapped.join->getBuildStats().partitions, 4u) << "the cap must have a wider plan to clamp";

    BuildOptions capped_options;
    capped_options.l1_cache_bytes_for_tests = l1_bytes;
    auto capped = buildJoin(distinct_keys, /*duplicates=*/1, capped_options);
    const auto stats = capped.join->getBuildStats();
    EXPECT_EQ(stats.partitions, 4u);
    expectTableInvariants(stats, distinct_keys, distinct_keys);
    probeAndCheck(capped, distinct_keys, /*duplicates=*/1, /*misses=*/10000);
}

/// A forced partition count above the per-pass ceiling splits into passes whose bits sum to the plan, with every row
/// conserved and exact results.
TEST(PartitionedHashJoin, ForcedBitsSplitIntoPasses)
{
    /// 2048 partitions under a 1024-per-pass ceiling take two passes of 6 and 5 bits. 4096 partitions
    /// under a 16-way ceiling take three passes of 4 bits. Each key's two rows stay adjacent through
    /// all of them, so every key ends as one run.
    struct Case
    {
        size_t distinct_keys;
        size_t duplicates;
        size_t bits;
        size_t max_fanout_per_pass;
        size_t passes;
    };
    for (const auto & [distinct_keys, duplicates, bits, max_fanout_per_pass, passes] :
         {Case{1000000, 1, 11, 1024, 2}, Case{500000, 2, 12, 16, 3}})
    {
        BuildOptions options;
        options.partition_bits_for_tests = bits;
        options.max_fanout_per_pass = max_fanout_per_pass;
        auto built = buildJoin(distinct_keys, duplicates, options);

        const auto stats = built.join->getBuildStats();
        EXPECT_EQ(stats.bits, bits);
        EXPECT_EQ(stats.partitions, 1uz << bits);
        ASSERT_EQ(stats.pass_bits.size(), passes) << "bits " << bits << " under a ceiling of " << max_fanout_per_pass;
        size_t total_bits = 0;
        for (const size_t pass : stats.pass_bits)
        {
            EXPECT_LE(1uz << pass, max_fanout_per_pass) << "no pass may exceed the ceiling";
            total_bits += pass;
        }
        EXPECT_EQ(total_bits, bits);
        ASSERT_EQ(stats.partition_row_counts.size(), 1uz << bits);
        UInt64 routed = 0;
        for (const UInt64 partition_rows : stats.partition_row_counts)
            routed += partition_rows;
        EXPECT_EQ(routed, distinct_keys * duplicates);
        expectTableInvariants(stats, distinct_keys, distinct_keys * duplicates);
        EXPECT_GE(stats.table_size_degree, bits);

        const auto dup = totalDuplicates(stats);
        EXPECT_EQ(dup.headers, 0u);
        if (duplicates > 1)
        {
            EXPECT_EQ(dup.ranges, distinct_keys) << "every key is one run";
            EXPECT_EQ(dup.arena_bytes, 8u * distinct_keys * duplicates);
        }
        else
            EXPECT_EQ(dup.arena_bytes, 0u) << "unique keys never touch the arena";

        probeAndCheck(built, distinct_keys, duplicates, /*misses=*/10000);
    }
}

/// Rows whose owner insert reaches the end of the last partition range overflow. The drain wraps
/// them into partition 0 as exact runs.
TEST(PartitionedHashJoin, RangeCrossingWraparound)
{
    /// One worker over four partitions and eight workers over sixteen, AMAC on and off. The ring's
    /// combined read-and-insert step and the sequential walk must both hand the boundary rows to the
    /// overflow. The drain of that one range must wrap them. The other owners must never touch it.
    struct Case
    {
        size_t num_threads;
        size_t bits;
    };
    for (const auto & [num_threads, bits] : {Case{1, 2}, Case{8, 4}})
        for (const bool disable_amac : {false, true})
        {
            auto crossing = buildCrossing(num_threads, disable_amac, bits);
            expectCrossingStats(crossing);
            expectSameRows(probeKeys(*crossing.built.join, crossing.probe_keys), crossing.expected);
        }
}

TEST(PartitionedHashJoin, ReusesCachedDistinctCountWithoutSketching)
{
    static std::atomic<UInt64> key_counter{0};
    const UInt64 key = 0xC1D15117C4C4E000ULL + key_counter.fetch_add(1);
    const StatsCollectingParams params(
        key, /*enable_=*/true, /*max_entries_for_hash_table_stats_=*/1024, /*max_size_to_preallocate_=*/1ULL << 40);
    const StatsCollectingParams capped_params(
        key, /*enable_=*/true, /*max_entries_for_hash_table_stats_=*/1024, /*max_size_to_preallocate_=*/100000);

    constexpr size_t small_keys = 50000;
    constexpr size_t large_keys = 250000;
    constexpr size_t later_keys = 100000;
    auto & events = CurrentThread::getProfileEvents();
    const auto preallocated_before = events[ProfileEvents::HashJoinPreallocatedElementsInHashTables];

    BuildOptions options;
    options.stats_collecting_params = &params;
    options.partition_bits_for_tests = 4;
    auto small = makeJoin(options);
    addBuildBlocks(*small.join, small_keys, /*duplicates=*/1, options);
    EXPECT_GT(small.join->getFillSketchEstimateForTests(), 0.0);
    finishBuild(small);
    expectTableInvariants(small.join->getBuildStats(), small_keys, small_keys);
    EXPECT_EQ(events[ProfileEvents::HashJoinPreallocatedElementsInHashTables], preallocated_before);
    const auto published = getHashTablesStatistics<HashJoinEntry>().getSizeHint(params);
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ(published->ht_size, small_keys);

    /// A warm build still hashes every row into the same routes, including when it scatters across
    /// partitions. Its barrier uses the previous exact count instead of the lane sketches.
    auto warm = makeJoin(options);
    addBuildBlocks(*warm.join, small_keys, /*duplicates=*/2, options);
    EXPECT_EQ(warm.join->getFillSketchEstimateForTests(), 0.0);
    warm.join->onBuildPhaseFinish();
    EXPECT_EQ(events[ProfileEvents::HashJoinPreallocatedElementsInHashTables], preallocated_before + small_keys);
    warm.join->runPostBuildPhase();
    expectTableInvariants(warm.join->getBuildStats(), small_keys, 2 * small_keys);
    EXPECT_EQ(warm.join->getBuildStats().hll_estimate, static_cast<double>(small_keys));
    EXPECT_EQ(warm.join->getBuildStats().bits, 4u);
    probeAndCheck(warm, small_keys, /*duplicates=*/2, /*misses=*/1000);

    /// The cache can be stale when data grows. The table must grow and still insert every key.
    auto large = buildJoin(large_keys, /*duplicates=*/1, options);
    const auto large_stats = large.join->getBuildStats();
    EXPECT_EQ(large_stats.hll_estimate, static_cast<double>(small_keys));
    EXPECT_GT(large_stats.table_resizes, 0u);
    EXPECT_EQ(events[ProfileEvents::HashJoinPreallocatedElementsInHashTables], preallocated_before + 2 * small_keys);
    expectTableInvariants(large_stats, large_keys, large_keys);
    probeAndCheck(large, large_keys, /*duplicates=*/1, /*misses=*/1000);

    const auto republished = getHashTablesStatistics<HashJoinEntry>().getSizeHint(params);
    ASSERT_TRUE(republished.has_value());
    EXPECT_EQ(republished->ht_size, large_keys);

    /// A count above the configured cap is not consumed, so this build takes the cold sketch path.
    options.stats_collecting_params = &capped_params;
    auto capped = buildJoin(later_keys, /*duplicates=*/1, options);
    const auto capped_stats = capped.join->getBuildStats();
    EXPECT_NEAR(capped_stats.hll_estimate, static_cast<double>(later_keys), 0.05 * static_cast<double>(later_keys));
    EXPECT_EQ(events[ProfileEvents::HashJoinPreallocatedElementsInHashTables], preallocated_before + 2 * small_keys);
    expectTableInvariants(capped_stats, later_keys, later_keys);
    probeAndCheck(capped, later_keys, /*duplicates=*/1, /*misses=*/1000);
}

/// A single-partition insert grows its table mid-way. Growth happens at the last free cell and at
/// the load factor. Each grow doubles the table once.
TEST(PartitionedHashJoin, SinglePartitionGrowsMidPass)
{
    /// One partition, so the walk wraps instead of overflowing. At safety 0.4 the keys would fill
    /// every cell, so the capacity guard fires. The degree ends at the initial one plus the resizes.
    /// At safety 0.6 the cells outnumber the keys but the fill would pass 50%. The load-factor grow
    /// then fires exactly once.
    constexpr size_t more_than_2x_keys = 131072;
    BuildOptions last_cell;
    last_cell.reserve_safety_for_tests = 0.4;
    last_cell.partition_bits_for_tests = 0;
    auto last_cell_built = buildJoin(more_than_2x_keys, /*duplicates=*/1, last_cell);
    const auto last_cell_stats = last_cell_built.join->getBuildStats();
    EXPECT_EQ(last_cell_stats.partitions, 1u);
    EXPECT_FALSE(last_cell_stats.amac_build_engaged);
    EXPECT_GE(last_cell_stats.table_resizes, 1u);
    const size_t initial_degree = Key64Table::degreeFor(static_cast<size_t>(std::ceil(more_than_2x_keys * 0.4)));
    EXPECT_EQ(last_cell_stats.table_size_degree, initial_degree + last_cell_stats.table_resizes);
    expectTableInvariants(last_cell_stats, more_than_2x_keys, more_than_2x_keys);
    probeAndCheck(last_cell_built, more_than_2x_keys, /*duplicates=*/1, /*misses=*/100);

    constexpr size_t less_than_2x_keys = 200000;
    BuildOptions load_factor;
    load_factor.reserve_safety_for_tests = 0.6;
    load_factor.partition_bits_for_tests = 0;
    auto load_factor_built = buildJoin(less_than_2x_keys, /*duplicates=*/1, load_factor);
    const auto load_factor_stats = load_factor_built.join->getBuildStats();
    EXPECT_EQ(load_factor_stats.partitions, 1u);
    EXPECT_FALSE(load_factor_stats.amac_build_engaged);
    EXPECT_EQ(load_factor_stats.table_resizes, 1u);
    expectTableInvariants(load_factor_stats, less_than_2x_keys, less_than_2x_keys);
    probeAndCheck(load_factor_built, less_than_2x_keys, /*duplicates=*/1, /*misses=*/100);
}

/// Rows of a key already in the table are not projected as new keys. 100000 more rows of one key
/// therefore force no grow.
TEST(PartitionedHashJoin, DuplicateRowsDoNotForceGrowth)
{
    /// 1000 unique keys, then 100000 rows of the first key, over sixteen partitions. A wanted grow
    /// would double the table. The build must want none and still hold every row.
    constexpr size_t distinct_keys = 1000;
    constexpr size_t duplicate_rows = 100000;
    BuildOptions options;
    options.partition_bits_for_tests = 4;
    BuiltJoin built = makeJoin(options);

    UInt64 next_id = 0;
    std::vector<UInt64> keys;
    for (size_t i = 0; i < distinct_keys; ++i)
        keys.push_back(keyOf(i));
    addBlock(*built.join, keys, next_id);
    for (size_t done = 0; done < duplicate_rows; done += block_rows)
    {
        keys.assign(std::min(block_rows, duplicate_rows - done), keyOf(0));
        addBlock(*built.join, keys, next_id);
    }
    finishBuild(built);

    const auto stats = built.join->getBuildStats();
    EXPECT_EQ(stats.table_resizes, 0u) << "no grow may even be wanted";
    expectTableInvariants(stats, distinct_keys, distinct_keys + duplicate_rows);

    keys.clear();
    for (size_t i = 0; i < distinct_keys; ++i)
        keys.push_back(keyOf(i));
    const JoinedRows rows = probeKeys(*built.join, keys);
    EXPECT_EQ(rows.size(), distinct_keys + duplicate_rows);
    const auto first_key_rows = std::count_if(rows.begin(), rows.end(), [](const JoinedRow & row) { return std::get<0>(row) == keyOf(0); });
    EXPECT_EQ(static_cast<size_t>(first_key_rows), duplicate_rows + 1);
}

/// A table past 2^32 cells is refused at the plan. The degree constructor throws. A reserve that
/// maps to degree 33 fails the barrier with `LIMIT_EXCEEDED`.
TEST(PartitionedHashJoin, DegreeCapAtPlan)
{
    /// `HashJoinTable.DegreeCap` checks that this reserve maps to degree 33 and that the table refuses it.
    const size_t reserve_for_33 = (1uz << 31) + 1;

    BuildOptions options;
    options.num_threads = 2;
    options.reserve_override_for_tests = reserve_for_33;
    expectThrowsCode(
        ErrorCodes::LIMIT_EXCEEDED,
        "a reserve that maps to degree 33 must fail the barrier",
        [&] { buildJoin(/*distinct_keys=*/1, /*duplicates=*/1, options); });
}

/// An ASOF build, always one partition, grows its undersized table and answers every probe like an ungrown control.
TEST(PartitionedHashJoin, SinglePartitionAsofGrows)
{
    /// ASOF tables have no ranges to split, so the build is one partition whatever its size. At safety
    /// 0.4 the table must grow; the control at the default safety never grows. Both must answer a probe
    /// of the same key with the same one row.
    constexpr size_t distinct_keys = 131072;
    const Block probe_header = uint64Block({{"k", {}}, {"probe_id", {}}, {"probe_ts", {}}});
    const Block build_header = uint64Block({{"rk", {}}, {"ts", {}}, {"build_id", {}}});

    const auto build = [&](double safety)
    {
        BuildOptions options;
        options.num_threads = 2;
        options.strictness = JoinStrictness::Asof;
        options.asof = AsofKey{ASOFJoinInequality::GreaterOrEquals, "probe_ts", "ts"};
        options.reserve_safety_for_tests = safety;
        BuiltJoin built = makeJoin(options, probe_header, build_header);
        std::vector<UInt64> keys;
        std::vector<UInt64> ids;
        for (size_t i = 0; i < distinct_keys; ++i)
        {
            keys.push_back(keyOf(i));
            ids.push_back(i);
            if (keys.size() == block_rows || i + 1 == distinct_keys)
            {
                /// `ts` is the row id, so a probe at `probe_ts = i` matches exactly its own row.
                EXPECT_TRUE(addBuildBlock(*built.join, uint64Block({{"rk", keys}, {"ts", ids}, {"build_id", ids}})));
                keys.clear();
                ids.clear();
            }
        }
        finishBuild(built);
        return built;
    };

    auto grown = build(0.4);
    const auto stats = grown.join->getBuildStats();
    EXPECT_EQ(stats.partitions, 1u);
    EXPECT_GE(stats.table_resizes, 1u);
    EXPECT_EQ(stats.distinct_keys, distinct_keys);

    auto control = build(1.2);
    EXPECT_EQ(control.join->getBuildStats().table_resizes, 0u);

    /// The ASOF output names its columns differently, so only the row counts are compared.
    const auto probe_rows = [&](BuiltJoin & built, size_t i)
    {
        auto result = built.join->joinBlock(uint64Block({{"k", {keyOf(i)}}, {"probe_id", {i}}, {"probe_ts", {i}}}));
        size_t rows = 0;
        while (true)
        {
            const auto r = result->next();
            rows += r.block.rows();
            if (r.is_last)
                return rows;
        }
    };
    for (const size_t i : {0uz, distinct_keys / 2})
    {
        EXPECT_EQ(probe_rows(control, i), 1u) << "one build row at `ts = i`";
        EXPECT_EQ(probe_rows(grown, i), probe_rows(control, i));
    }
}

namespace
{

class CountedAsofLookup : public SortedLookupVectorBase
{
public:
    CountedAsofLookup(AsofRowRefs lookup_, size_t & destructions_)
        : lookup(std::move(lookup_))
        , destructions(destructions_)
    {
    }

    ~CountedAsofLookup() override
    {
        ++destructions;
    }

    void insert(const IColumn & column, UInt32 block_no, size_t row) override
    {
        lookup->insert(column, block_no, row);
    }

    const RowRef * findAsof(const IColumn & column, size_t row) override
    {
        return lookup->findAsof(column, row);
    }

private:
    AsofRowRefs lookup;
    size_t & destructions;
};

void checkAsofGrowthCleanup(bool fail_overflow_allocation)
{
    ThreadStatus thread_status;
    BuildOptions options;
    options.strictness = JoinStrictness::Asof;
    options.asof = AsofKey{ASOFJoinInequality::GreaterOrEquals, "probe_ts", "ts"};
    const Block probe_header = uint64Block({{"k", {}}, {"probe_ts", {}}});
    const Block build_header = uint64Block({{"rk", {}}, {"ts", {}}});
    auto table_join = makeTableJoin(probe_header, build_header, options);
    HashJoin schema(
        table_join,
        std::make_shared<const Block>(build_header),
        /*any_take_last_row_=*/false,
        /*reserve_num_=*/0,
        /*instance_id_=*/"",
        /*stats_collecting_params_=*/{},
        /*max_threads_=*/1,
        /*use_parallel_layout_=*/false,
        /*allow_set_maps_=*/false);
    std::vector<HashJoinClause::FillBlock> build_blocks;
    std::atomic<size_t> accumulated_bytes{0};
    size_t a_destructions = 0;
    size_t b_destructions = 0;
    HashJoinClause clause(schema, *table_join, false, 1, build_blocks, accumulated_bytes, getLogger("AsofGrowthCleanup"));
    clause.beginSinglePartitionInsert(1, 129, false);
    auto & table = *std::get<HashJoinTableMapsAsof>(clause.tableMaps().maps).key64;
    using Table = std::remove_reference_t<decltype(table)>;
    using Cell = Table::cell_type;
    ASSERT_EQ(clause.partitionCount(), 1u);
    ASSERT_EQ(table.sizeDegree(), 8u);
    ASSERT_EQ(table.cellCount(), 256u);

    /// Both keys wrap at the old and new table ends. Padding fills distinct old homes without displacing them.
    std::vector<UInt64> keys;
    std::array<UInt64, 128> padding{};
    size_t padding_count = 0;
    for (UInt64 key = 1; key <= 1000000 && (keys.size() < 2 || padding_count < 127); ++key)
    {
        const size_t hash = table.hash(key);
        if ((hashJoinTablePlacement(hash) >> (64 - 9)) == 511 && keys.size() < 2)
            keys.push_back(key);
        const size_t home = table.place(hash);
        if (home > 0 && home < padding.size() && !padding[home])
        {
            padding[home] = key;
            ++padding_count;
        }
    }
    ASSERT_EQ(keys.size(), 2u);
    ASSERT_EQ(padding_count, 127u);
    const UInt64 a = keys[0];
    const UInt64 b = keys[1];
    keys.insert(keys.end(), padding.begin() + 1, padding.end());
    const Block block = uint64Block({{"rk", keys}, {"ts", std::vector<UInt64>(keys.size(), 1)}});
    HashJoinClause::FillBlock fill;
    fill.key_columns = {block.getByName("rk").column.get(), block.getByName("ts").column.get()};
    fill.rows = keys.size();
    clause.insertSingleLaneBlock(fill);

    ASSERT_EQ(table.sizeDegree(), 8u);
    ASSERT_EQ(table.cellCount(), 256u);
    ASSERT_EQ(Cell::getKey(table.cellAt(255)->getValue()), a);
    ASSERT_EQ(Cell::getKey(table.cellAt(0)->getValue()), b);
    ASSERT_TRUE(table.cellAt(255)->getMapped());
    ASSERT_TRUE(table.cellAt(0)->getMapped());
    for (size_t home = 1; home < padding.size(); ++home)
    {
        ASSERT_EQ(Cell::getKey(table.cellAt(home)->getValue()), padding[home]);
        ASSERT_TRUE(table.cellAt(home)->getMapped());
    }
    auto & a_owner = table.cellAt(255)->getMapped();
    auto & b_owner = table.cellAt(0)->getMapped();
    a_owner = std::make_unique<CountedAsofLookup>(std::move(a_owner), a_destructions);
    b_owner = std::make_unique<CountedAsofLookup>(std::move(b_owner), b_destructions);
    const auto * b_lookup = b_owner.get();
    ASSERT_EQ(a_destructions, 0u);
    ASSERT_EQ(b_destructions, 0u);

    if (fail_overflow_allocation)
    {
        int exception_code = 0;
        const auto memory_exceptions = thread_status.performance_counters[ProfileEvents::QueryMemoryLimitExceeded];
        {
            auto & tracker = thread_status.memory_tracker;
            const auto saved_hard_limit = tracker.getHardLimit();
            const auto saved_untracked_limit = thread_status.untracked_memory_limit;
            const auto saved_min_allocation = CurrentMemoryTracker::getMinAllocationSizeBytesToThrow();
            SCOPE_EXIT_SAFE({
                tracker.setHardLimit(saved_hard_limit);
                thread_status.untracked_memory_limit = saved_untracked_limit;
                CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(saved_min_allocation);
            });
            thread_status.untracked_memory_limit = 0;
            CurrentThread::flushUntrackedMemory();

            /// Allow the outer overflow list, commitment flags and new cells, but not the first overflow entry.
            struct RehashEntry
            {
                UInt64 key;
                size_t hash;
                AsofRowRefs mapped;
            };
            Int64 list_bytes = 0;
            Int64 flag_bytes = 0;
            {
                const auto before = tracker.get();
                std::vector<std::vector<RehashEntry>> lists(1);
                list_bytes = tracker.get() - before;
            }
            {
                const auto before = tracker.get();
                std::vector<UInt8> flags(1, 0);
                flag_bytes = tracker.get() - before;
            }
            ASSERT_GT(list_bytes, 0);
            ASSERT_GT(flag_bytes, 0);
            tracker.setHardLimit(tracker.get() + list_bytes + flag_bytes + static_cast<Int64>(512 * sizeof(Cell)));
            CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(1);
            try
            {
                clause.finishSinglePartitionInsert();
            }
            catch (const Exception & exception)
            {
                exception_code = exception.code();
            }
        }
        ASSERT_EQ(exception_code, ErrorCodes::MEMORY_LIMIT_EXCEEDED);
        ASSERT_GT(thread_status.performance_counters[ProfileEvents::QueryMemoryLimitExceeded], memory_exceptions);
        ASSERT_EQ(table.sizeDegree(), 8u);
        ASSERT_EQ(table.cellCount(), 256u);
        ASSERT_EQ(table.newCellCount(), 512u);
        ASSERT_EQ(Cell::getKey(table.cellAt(0)->getValue()), b);
        ASSERT_EQ(Cell::getKey(table.cellAt(255)->getValue()), a);
        ASSERT_FALSE(table.cellAt(0)->getMapped());
        ASSERT_FALSE(table.cellAt(255)->getMapped());
        ASSERT_TRUE(table.newRangeIsCommitted(0));
        ASSERT_EQ(Cell::getKey(table.newCellAt(511)->getValue()), b);
        ASSERT_EQ(table.newCellAt(511)->getMapped().get(), b_lookup);
        for (size_t home = 1; home < padding.size(); ++home)
        {
            ASSERT_FALSE(table.cellAt(home)->getMapped());
            const auto * cell = table.newCellAt(table.newPlace(table.hash(padding[home])));
            ASSERT_EQ(Cell::getKey(cell->getValue()), padding[home]);
            ASSERT_TRUE(cell->getMapped());
        }
        size_t moved_keys = 0;
        for (size_t pos = 0; pos < table.newCellCount(); ++pos)
        {
            const auto * cell = table.newCellAt(pos);
            if (table.isEmptyCell(cell))
                continue;
            const UInt64 key = Cell::getKey(cell->getValue());
            ASSERT_NE(key, a);
            ASSERT_NE(std::find(keys.begin(), keys.end(), key), keys.end());
            ASSERT_TRUE(cell->getMapped());
            ++moved_keys;
        }
        ASSERT_EQ(moved_keys, 128u);
        ASSERT_TRUE(table.isEmptyCell(table.newCellAt(0)));
        ASSERT_EQ(a_destructions, 1u);
        ASSERT_EQ(b_destructions, 0u);
    }
    else
    {
        clause.finishSinglePartitionInsert();
        ASSERT_EQ(table.sizeDegree(), 9u);
        ASSERT_EQ(table.cellCount(), 512u);
        ASSERT_EQ(clause.buildStats().table_resizes, 1u);
        for (UInt64 key : keys)
        {
            const auto * cell = table.find(key);
            ASSERT_NE(cell, nullptr);
            ASSERT_TRUE(cell->getMapped());
        }
        ASSERT_EQ(a_destructions, 0u);
        ASSERT_EQ(b_destructions, 0u);
    }
    clause.releaseTable();
    EXPECT_EQ(a_destructions, 1u);
    EXPECT_EQ(b_destructions, 1u);
}

}

TEST(PartitionedHashJoin, SinglePartitionAsofGrowthFailureReleasesLookups)
{
    std::thread(
        []
        {
            EXPECT_NO_THROW(checkAsofGrowthCleanup(true));
        })
        .join();
}

TEST(PartitionedHashJoin, SinglePartitionAsofGrowthReleasesLookups)
{
    std::thread(
        []
        {
            EXPECT_NO_THROW(checkAsofGrowthCleanup(false));
        })
        .join();
}
