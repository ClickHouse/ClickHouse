#include <gtest/gtest.h>

#include <fmt/format.h>

#include <algorithm>
#include <atomic>
#include <tuple>
#include <vector>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/PartitionedHashJoin/DenseHyperLogLog.h>
#include <Interpreters/PartitionedHashJoin/JoinRouteHashing.h>
#include <Interpreters/PartitionedHashJoin/PartitionedHashJoin.h>
#include <Interpreters/PartitionedHashJoin/PartitionedJoinMaps.h>
#include <Interpreters/TableJoin.h>
#include <Common/assert_cast.h>

using namespace DB;

namespace
{

constexpr size_t block_rows = 65536;

/// The multiset of these tuples over a whole drain is an exact identity, so a dropped, duplicated or
/// cross-wired row changes it. A build row inserted into the wrong leaf can never be found by the
/// value-routed probe either, so mis-routing shows up as missing tuples.
using JoinedRow = std::tuple<UInt64, UInt64, UInt64, UInt64>;
using JoinedRows = std::vector<JoinedRow>;

Block twoColumnBlock(const String & key_name, const String & id_name, const std::vector<UInt64> & keys, const std::vector<UInt64> & ids)
{
    auto key_column = ColumnUInt64::create();
    auto id_column = ColumnUInt64::create();
    key_column->getData().assign(keys.begin(), keys.end());
    id_column->getData().assign(ids.begin(), ids.end());
    Block block;
    block.insert({std::move(key_column), std::make_shared<DataTypeUInt64>(), key_name});
    block.insert({std::move(id_column), std::make_shared<DataTypeUInt64>(), id_name});
    return block;
}

const UInt64 * columnData(const Block & block, const String & name, ColumnPtr & holder)
{
    holder = block.getByName(name).column->convertToFullColumnIfReplicated();
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

std::shared_ptr<TableJoin> makeTableJoin(const Block & left_header, const Block & right_header, JoinKind kind = JoinKind::Inner)
{
    Settings settings;
    auto table_join = std::make_shared<TableJoin>(settings, JoinAnalyzeMode::None, /*tmp_volume=*/nullptr, /*tmp_data=*/nullptr);
    table_join->setKind(kind);
    table_join->getTableJoin().strictness = JoinStrictness::All;
    table_join->addDisjunct();
    table_join->getClauses().back().addKey(
        left_header.getByPosition(0).name, right_header.getByPosition(0).name, /*null_safe_comparison=*/false);

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
    return table_join;
}

struct BuiltJoin
{
    std::shared_ptr<TableJoin> table_join;
    std::shared_ptr<PartitionedHashJoin> join;
};

/// Feeds the build through the real `IJoin` interface - fill, barrier, post-build. Blocks above
/// 65536 rows take the wide 8-byte locator encoding, smaller ones the packed 4-byte form.
BuiltJoin buildJoin(
    size_t distinct_keys,
    size_t duplicates,
    size_t num_threads,
    double reserve_safety_for_tests = 0,
    size_t build_block_rows = block_rows,
    JoinKind kind = JoinKind::Inner,
    bool disable_amac = false,
    size_t max_fanout_per_pass_for_tests = 0,
    const StatsCollectingParams & stats_collecting_params = {})
{
    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});

    BuiltJoin result;
    result.table_join = makeTableJoin(left_header, right_header, kind);
    result.join = std::make_shared<PartitionedHashJoin>(
        result.table_join,
        std::make_shared<const Block>(right_header),
        num_threads,
        /*any_take_last_row_=*/false,
        stats_collecting_params);
    if (reserve_safety_for_tests > 0)
        result.join->setReserveSafetyFactorForTests(reserve_safety_for_tests);
    /// These builds exceed the engagement threshold, so both AMAC paths run unless overridden.
    if (disable_amac)
        result.join->setAmacEnabledForTests(false);
    /// Forces a multi-pass scatter without a 500M-key build. Only the pass split changes; the
    /// partition count must not.
    if (max_fanout_per_pass_for_tests > 0)
        result.join->setMaxFanoutPerPassForTests(max_fanout_per_pass_for_tests);

    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    keys.reserve(build_block_rows);
    ids.reserve(build_block_rows);
    UInt64 id = 0;
    for (size_t i = 0; i < distinct_keys; ++i)
    {
        for (size_t d = 0; d < duplicates; ++d)
        {
            keys.push_back(i * 2654435761ULL + 1);
            ids.push_back(id++);
            if (keys.size() == build_block_rows)
            {
                EXPECT_TRUE(result.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
                keys.clear();
                ids.clear();
            }
        }
    }
    if (!keys.empty())
        EXPECT_TRUE(result.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));

    result.join->onBuildPhaseFinish();
    result.join->runPostBuildPhase();
    return result;
}

/// Probes every distinct key once, plus `misses` absent ones, and checks the exact joined multiset.
void probeAndCheck(BuiltJoin & built, size_t distinct_keys, size_t duplicates, size_t misses, bool use_lanes = false)
{
    JoinedRows expected;
    expected.reserve(distinct_keys * duplicates);
    for (size_t i = 0; i < distinct_keys; ++i)
    {
        const UInt64 key = i * 2654435761ULL + 1;
        for (size_t d = 0; d < duplicates; ++d)
            expected.emplace_back(key, i, key, i * duplicates + d);
    }
    std::sort(expected.begin(), expected.end());

    JoinedRows actual;
    actual.reserve(expected.size());
    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    size_t probe_block_index = 0;
    for (size_t i = 0; i < distinct_keys + misses; ++i)
    {
        /// The +2 offset cannot collide with a built key: `i * K + 2 == j * K + 1` would need
        /// `i - j` to be the modular inverse of `-K`, far outside these ranges.
        keys.push_back(i < distinct_keys ? i * 2654435761ULL + 1 : i * 2654435761ULL + 2);
        ids.push_back(i);
        if (keys.size() == block_rows || i + 1 == distinct_keys + misses)
        {
            Block probe_block = twoColumnBlock("k", "probe_id", keys, ids);
            /// `% 9` on an 8-slot table, so one lane per rotation is out of range and takes the
            /// pool fallback.
            auto result = use_lanes ? built.join->joinBlock(std::move(probe_block), probe_block_index++ % 9)
                                    : built.join->joinBlock(std::move(probe_block));
            drainResult(*result, actual);
            keys.clear();
            ids.clear();
        }
    }
    std::sort(actual.begin(), actual.end());
    ASSERT_EQ(actual.size(), expected.size());
    ASSERT_TRUE(actual == expected);
}

}

TEST(PartitionedHashJoin, DistinctEstimateCacheWarmRun)
{
    /// Unique per invocation, so `--gtest_repeat` cannot leave the cold build a warm entry.
    static std::atomic<UInt64> key_counter{0};
    const UInt64 key = 0xC1D15117C4C4E000ULL + key_counter.fetch_add(1);
    const StatsCollectingParams params(
        key, /*enable_=*/true, /*max_entries_for_hash_table_stats_=*/1024, /*max_size_to_preallocate_=*/1ULL << 40);

    constexpr size_t distinct_keys = 200000;
    constexpr size_t duplicates = 2;

    /// No cache entry: the sketch estimates and the post-build publishes exact counts.
    auto cold = buildJoin(
        distinct_keys, duplicates, /*num_threads=*/4, /*reserve_safety_for_tests=*/0, block_rows, JoinKind::Inner,
        /*disable_amac=*/false, /*max_fanout_per_pass_for_tests=*/0, params);
    const auto cold_stats = cold.join->getBuildStats();
    EXPECT_FALSE(cold_stats.distinct_estimate_reused);
    /// The sketch's standard error at precision 13 is around 1.15%.
    EXPECT_NEAR(cold_stats.hll_estimate, static_cast<double>(distinct_keys), 0.05 * distinct_keys);
    probeAndCheck(cold, distinct_keys, duplicates, /*misses=*/100);

    /// Same key and same thread count, so the same plan bits as the cache: the exact total replaces
    /// the estimate, `planHashTables` takes the cached counts unchanged, and the fill skips the
    /// sketch feed.
    auto warm = buildJoin(
        distinct_keys, duplicates, /*num_threads=*/4, /*reserve_safety_for_tests=*/0, block_rows, JoinKind::Inner,
        /*disable_amac=*/false, /*max_fanout_per_pass_for_tests=*/0, params);
    const auto warm_stats = warm.join->getBuildStats();
    EXPECT_TRUE(warm_stats.distinct_estimate_reused);
    EXPECT_EQ(warm_stats.hll_estimate, static_cast<double>(distinct_keys));
    EXPECT_EQ(warm_stats.bits, cold_stats.bits);
    EXPECT_EQ(warm_stats.leaf_growths, 0u);
    probeAndCheck(warm, distinct_keys, duplicates, /*misses=*/100);

    /// With statistics off nothing is reused, so the sketch path runs as before.
    auto disabled = buildJoin(distinct_keys, duplicates, /*num_threads=*/4);
    EXPECT_FALSE(disabled.join->getBuildStats().distinct_estimate_reused);
    probeAndCheck(disabled, distinct_keys, duplicates, /*misses=*/100);
}

TEST(PartitionedHashJoin, DistinctEstimateCachePerPartitionFoldAndSplit)
{
    /// Same query key but very different thread counts, so the parallelism floor moves the plan bits
    /// independently of the shared estimate and the breakdown is published at one bit count and
    /// consumed at another. That reaches the fold and split branches of `planHashTables`, which the
    /// sibling test above cannot.
    static std::atomic<UInt64> key_counter{0};
    const UInt64 key = 0xF01D5111C4C4E000ULL + key_counter.fetch_add(1);
    const StatsCollectingParams params(
        key, /*enable_=*/true, /*max_entries_for_hash_table_stats_=*/1024, /*max_size_to_preallocate_=*/1ULL << 40);

    constexpr size_t distinct_keys = 300000;
    constexpr size_t duplicates = 1;

    /// At 128 threads the parallelism floor is 7 bits, which very likely dominates the natural
    /// L2-driven bits for this key count, so the breakdown is published finer than a low-thread run
    /// would choose.
    auto cold = buildJoin(distinct_keys, duplicates, /*num_threads=*/128, /*reserve_safety_for_tests=*/0, block_rows, JoinKind::Inner,
        /*disable_amac=*/false, /*max_fanout_per_pass_for_tests=*/0, params);
    const auto cold_stats = cold.join->getBuildStats();
    EXPECT_FALSE(cold_stats.distinct_estimate_reused);
    probeAndCheck(cold, distinct_keys, duplicates, /*misses=*/100);

    /// Single-threaded, so no floor and the natural coarser bits: `planHashTables` has to fold
    /// contiguous cached leaves together.
    auto warm = buildJoin(distinct_keys, duplicates, /*num_threads=*/1, /*reserve_safety_for_tests=*/0, block_rows, JoinKind::Inner,
        /*disable_amac=*/false, /*max_fanout_per_pass_for_tests=*/0, params);
    const auto warm_stats = warm.join->getBuildStats();
    EXPECT_TRUE(warm_stats.distinct_estimate_reused);
    EXPECT_LT(warm_stats.bits, cold_stats.bits) << "test setup assumption: the thread-count-driven parallelism floor must actually "
                                                    "differ between the two builds for this test to exercise the fold/split paths";
    EXPECT_EQ(warm_stats.hll_estimate, static_cast<double>(distinct_keys));
    probeAndCheck(warm, distinct_keys, duplicates, /*misses=*/100);

    /// Re-consume at matching bits, then republish, then consume again single-threaded. The exact bit
    /// count depends on the machine's L2 size, so this asserts the mechanism stays self-consistent
    /// rather than pinning the numbers.
    auto rewarm_wide = buildJoin(distinct_keys, duplicates, /*num_threads=*/128, /*reserve_safety_for_tests=*/0, block_rows,
        JoinKind::Inner, /*disable_amac=*/false, /*max_fanout_per_pass_for_tests=*/0, params);
    EXPECT_TRUE(rewarm_wide.join->getBuildStats().distinct_estimate_reused);
    probeAndCheck(rewarm_wide, distinct_keys, duplicates, /*misses=*/100);
}

TEST(PartitionedHashJoin, LaneIdentityParity)
{
    /// Through the lane-carrying overloads, as the pipeline does, including out-of-range lanes.
    constexpr size_t distinct_keys = 100000;
    constexpr size_t duplicates = 2;

    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});
    BuiltJoin built;
    built.table_join = makeTableJoin(left_header, right_header, JoinKind::Inner);
    built.join = std::make_shared<PartitionedHashJoin>(built.table_join, std::make_shared<const Block>(right_header), 4);

    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    UInt64 id = 0;
    size_t build_block_index = 0;
    for (size_t i = 0; i < distinct_keys; ++i)
    {
        for (size_t d = 0; d < duplicates; ++d)
        {
            keys.push_back(i * 2654435761ULL + 1);
            ids.push_back(id++);
            if (keys.size() == block_rows)
            {
                const Block b = twoColumnBlock("rk", "build_id", keys, ids);
                /// The slot table holds 2 x num_threads = 8, so lane 8 falls back to the thread-id
                /// map in `getFillLane`.
                EXPECT_TRUE(built.join->addBlockToJoin(b, b.rows(), /*check_limits=*/true, build_block_index++ % 9));
                keys.clear();
                ids.clear();
            }
        }
    }
    if (!keys.empty())
    {
        const Block b = twoColumnBlock("rk", "build_id", keys, ids);
        EXPECT_TRUE(built.join->addBlockToJoin(b, b.rows(), /*check_limits=*/true, build_block_index++ % 9));
    }
    built.join->onBuildPhaseFinish();
    built.join->runPostBuildPhase();

    probeAndCheck(built, distinct_keys, duplicates, /*misses=*/100, /*use_lanes=*/true);
}

TEST(PartitionedHashJoin, PartitionedBuildExactReservesAndParity)
{
    constexpr size_t distinct_keys = 300000;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, /*num_threads=*/4);

    const auto stats = built.join->getBuildStats();
    EXPECT_GT(stats.partitions, 1u) << "a 300K-key build must partition";
    EXPECT_EQ(stats.leaf_growths, 0u) << "no leaf map may resize past its planned exact reserve";
    EXPECT_TRUE(stats.predictions_exact) << "predicted bucket bytes must equal the actual map buffer bytes";
    EXPECT_EQ(stats.leaf_rows, distinct_keys);
    EXPECT_GT(stats.ht_total_bytes, 0u);
    EXPECT_NEAR(stats.hll_estimate, static_cast<double>(distinct_keys), 0.05 * distinct_keys) << "the distinct estimate must be within 5%";

    probeAndCheck(built, distinct_keys, /*duplicates=*/1, /*misses=*/10000);
}

TEST(PartitionedHashJoin, PartitionedBuildWithDuplicates)
{
    constexpr size_t distinct_keys = 150000;
    constexpr size_t duplicates = 4;
    auto built = buildJoin(distinct_keys, duplicates, /*num_threads=*/4);

    const auto stats = built.join->getBuildStats();
    EXPECT_GT(stats.partitions, 1u);
    EXPECT_EQ(stats.leaf_growths, 0u);
    EXPECT_TRUE(stats.predictions_exact);
    EXPECT_EQ(stats.leaf_rows, distinct_keys * duplicates);

    probeAndCheck(built, distinct_keys, duplicates, /*misses=*/1000);
}

TEST(PartitionedHashJoin, DegenerateSingleLeaf)
{
    constexpr size_t distinct_keys = 1000;
    auto built = buildJoin(distinct_keys, /*duplicates=*/2, /*num_threads=*/4);

    const auto stats = built.join->getBuildStats();
    EXPECT_EQ(stats.partitions, 1u) << "a small build must degenerate to one leaf";
    EXPECT_EQ(stats.leaf_growths, 0u);
    EXPECT_TRUE(stats.predictions_exact);

    probeAndCheck(built, distinct_keys, /*duplicates=*/2, /*misses=*/100);
}

TEST(PartitionedHashJoin, WideLocatorsForLargeBlocks)
{
    /// Above 65536 rows the packed locator no longer fits, so this is the 8-byte path.
    constexpr size_t distinct_keys = 300000;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, /*num_threads=*/4, /*reserve_safety_for_tests=*/0, /*build_block_rows=*/100000);

    const auto stats = built.join->getBuildStats();
    EXPECT_GT(stats.partitions, 1u);
    EXPECT_EQ(stats.leaf_growths, 0u);
    EXPECT_TRUE(stats.predictions_exact);

    probeAndCheck(built, distinct_keys, /*duplicates=*/1, /*misses=*/1000);
}

TEST(PartitionedHashJoin, GrowthOnUnderestimate)
{
    /// A crippled safety factor makes every leaf reserve underestimate, so the maps must resize past
    /// their plan - correctly, and with the growths counted.
    constexpr size_t distinct_keys = 200000;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, /*num_threads=*/4, /*reserve_safety_for_tests=*/0.001);

    const auto stats = built.join->getBuildStats();
    EXPECT_GT(stats.leaf_growths, 0u) << "the crippled estimate must force leaf growths";

    probeAndCheck(built, distinct_keys, /*duplicates=*/1, /*misses=*/1000);
}

TEST(PartitionedHashJoin, RightJoinFlagBaseAndNonJoined)
{
    /// RIGHT ALL exercises the per-leaf flag bases and the non-joined iteration: a wrong base marks
    /// or reads the wrong cell, which shows up as missing or duplicated non-joined rows.
    constexpr size_t distinct_keys = 300000;
    constexpr size_t duplicates = 2;
    constexpr size_t probed_keys = distinct_keys / 2;
    auto built = buildJoin(distinct_keys, duplicates, /*num_threads=*/4, /*reserve_safety_for_tests=*/0, block_rows, JoinKind::Right);

    const auto stats = built.join->getBuildStats();
    EXPECT_GT(stats.partitions, 1u);
    EXPECT_EQ(stats.leaf_rows, distinct_keys * duplicates);

    /// Prefix sums with a nonempty span per leaf, covering a slot per build key plus the zero cells.
    ASSERT_EQ(stats.flag_base.size(), stats.partitions + 1);
    EXPECT_EQ(stats.flag_base.front(), 0u);
    for (size_t leaf = 0; leaf < stats.partitions; ++leaf)
        EXPECT_LT(stats.flag_base[leaf], stats.flag_base[leaf + 1]);
    EXPECT_GE(stats.flag_base.back(), distinct_keys + stats.partitions);

    /// RIGHT filters unmatched probe rows, so the output is exactly the probed keys' tuples.
    JoinedRows expected;
    expected.reserve(probed_keys * duplicates);
    for (size_t i = 0; i < probed_keys; ++i)
    {
        const UInt64 key = i * 2654435761ULL + 1;
        for (size_t d = 0; d < duplicates; ++d)
            expected.emplace_back(key, i, key, i * duplicates + d);
    }
    std::sort(expected.begin(), expected.end());

    JoinedRows actual;
    actual.reserve(expected.size());
    {
        std::vector<UInt64> keys;
        std::vector<UInt64> ids;
        for (size_t i = 0; i < probed_keys; ++i)
        {
            keys.push_back(i * 2654435761ULL + 1);
            ids.push_back(i);
            if (keys.size() == block_rows || i + 1 == probed_keys)
            {
                auto result = built.join->joinBlock(twoColumnBlock("k", "probe_id", keys, ids));
                drainResult(*result, actual);
                keys.clear();
                ids.clear();
            }
        }
    }
    std::sort(actual.begin(), actual.end());
    ASSERT_EQ(actual.size(), expected.size());
    ASSERT_TRUE(actual == expected);

    /// The non-joined stream must return exactly the build rows of the unprobed keys.
    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    Block result_sample = left_header.cloneEmpty();
    result_sample.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "build_id"});
    result_sample.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "rk"});

    std::vector<std::pair<UInt64, UInt64>> expected_non_joined;
    expected_non_joined.reserve((distinct_keys - probed_keys) * duplicates);
    for (size_t i = probed_keys; i < distinct_keys; ++i)
        for (size_t d = 0; d < duplicates; ++d)
            expected_non_joined.emplace_back(i * 2654435761ULL + 1, i * duplicates + d);
    std::sort(expected_non_joined.begin(), expected_non_joined.end());

    std::vector<std::pair<UInt64, UInt64>> actual_non_joined;
    actual_non_joined.reserve(expected_non_joined.size());
    auto non_joined = built.join->getNonJoinedBlocks(left_header, result_sample, /*max_block_size=*/65536);
    ASSERT_NE(non_joined, nullptr);
    while (true)
    {
        Block block = non_joined->next();
        if (block.empty())
            break;
        ColumnPtr rk_holder;
        ColumnPtr build_holder;
        const UInt64 * rk = columnData(block, "rk", rk_holder);
        const UInt64 * build_id = columnData(block, "build_id", build_holder);
        for (size_t i = 0; i < block.rows(); ++i)
            actual_non_joined.emplace_back(rk[i], build_id[i]);
    }
    std::sort(actual_non_joined.begin(), actual_non_joined.end());
    ASSERT_EQ(actual_non_joined.size(), expected_non_joined.size());
    ASSERT_TRUE(actual_non_joined == expected_non_joined);
}

TEST(PartitionedHashJoin, AmacRingGrowthResume)
{
    /// The safety factor is crippled to about a third of the real distinct count while the aggregate
    /// size stays well above the engagement threshold, so every leaf's ring hits the grower boundary
    /// mid-flight, drains, resizes and re-seeds - twice per leaf or so. The multiset check is
    /// count-exact, so a row lost to two in-flight rows claiming one cell, or duplicated by the
    /// re-seed, cannot pass.
    constexpr size_t distinct_keys = 3000000;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, /*num_threads=*/4, /*reserve_safety_for_tests=*/0.35);

    const auto stats = built.join->getBuildStats();
    EXPECT_GT(stats.partitions, 1u);
    EXPECT_TRUE(stats.amac_build_engaged) << "the tables must be far above the AMAC engagement threshold";
    EXPECT_GT(stats.amac_ring_growths, 0u) << "the crippled reserve must force growth inside the insert rings";
    EXPECT_GT(stats.leaf_growths, 0u) << "resizing past the planned reserves must be counted, never silent";
    EXPECT_EQ(stats.leaf_rows, distinct_keys);

    probeAndCheck(built, distinct_keys, /*duplicates=*/1, /*misses=*/10000);
}

TEST(PartitionedHashJoin, AmacDuplicateHeavyBuildParityVsSequential)
{
    /// 16 rows per key across several leaves, cross-checked against the same build and probe forced
    /// onto the sequential loops. Duplicates are adjacent in the scattered chunks, so same-key rows
    /// are permanently in flight together - which is exactly what the fused read-then-act step has to
    /// survive.
    constexpr size_t distinct_keys = 200000;
    constexpr size_t duplicates = 16;

    auto amac_built = buildJoin(distinct_keys, duplicates, /*num_threads=*/4);
    const auto amac_stats = amac_built.join->getBuildStats();
    EXPECT_GT(amac_stats.partitions, 1u);
    EXPECT_TRUE(amac_stats.amac_build_engaged);
    EXPECT_EQ(amac_stats.leaf_growths, 0u);
    EXPECT_EQ(amac_stats.amac_ring_growths, 0u);
    EXPECT_EQ(amac_stats.leaf_rows, distinct_keys * duplicates);

    auto sequential_built = buildJoin(
        distinct_keys, duplicates, /*num_threads=*/4, /*reserve_safety_for_tests=*/0, block_rows, JoinKind::Inner, /*disable_amac=*/true);
    const auto sequential_stats = sequential_built.join->getBuildStats();
    EXPECT_FALSE(sequential_stats.amac_build_engaged);
    EXPECT_EQ(sequential_stats.leaf_rows, distinct_keys * duplicates);

    /// Both must produce the expected multiset, and so each other's.
    probeAndCheck(amac_built, distinct_keys, duplicates, /*misses=*/1000);
    probeAndCheck(sequential_built, distinct_keys, duplicates, /*misses=*/1000);
}

TEST(PartitionedHashJoin, MultiPassForcedPlanLeafParity)
{
    /// The same build planned single-pass and multi-pass. The partition count must be identical - the
    /// ceiling splits the scatter, it must never cap the plan - and every leaf must receive the same
    /// rows, which the per-leaf counts and then the multiset check pin.
    constexpr size_t distinct_keys = 300000;

    auto single = buildJoin(distinct_keys, /*duplicates=*/1, /*num_threads=*/4);
    const auto single_stats = single.join->getBuildStats();
    ASSERT_GT(single_stats.partitions, 2u);
    ASSERT_EQ(single_stats.pass_bits.size(), 1u) << "the default ceiling must plan a single pass here";

    /// A 2-bit ceiling against a plan of at least 2 bits forces two passes.
    auto multi = buildJoin(
        distinct_keys,
        /*duplicates=*/1,
        /*num_threads=*/4,
        /*reserve_safety_for_tests=*/0,
        block_rows,
        JoinKind::Inner,
        /*disable_amac=*/false,
        /*max_fanout_per_pass_for_tests=*/4);
    const auto multi_stats = multi.join->getBuildStats();

    EXPECT_EQ(multi_stats.partitions, single_stats.partitions) << "the ceiling must split passes, never cap the plan";
    EXPECT_EQ(multi_stats.bits, single_stats.bits);
    ASSERT_GE(multi_stats.pass_bits.size(), 2u) << "the lowered ceiling must force a multi-pass plan";
    size_t total_bits = 0;
    for (const size_t pass : multi_stats.pass_bits)
    {
        EXPECT_GE(pass, 1u);
        EXPECT_LE(1uz << pass, 4u) << "no pass may exceed the forced ceiling";
        total_bits += pass;
    }
    EXPECT_EQ(total_bits, multi_stats.bits);

    /// The refine passes must land rows in exactly the leaves a single-pass plan produces.
    ASSERT_EQ(multi_stats.leaf_row_counts.size(), multi_stats.partitions);
    EXPECT_TRUE(multi_stats.leaf_row_counts == single_stats.leaf_row_counts);

    /// And must not disturb the exact-reserve behaviour.
    EXPECT_EQ(multi_stats.leaf_growths, 0u);
    EXPECT_TRUE(multi_stats.predictions_exact);
    EXPECT_EQ(multi_stats.leaf_rows, distinct_keys);

    probeAndCheck(multi, distinct_keys, /*duplicates=*/1, /*misses=*/10000);
}

TEST(PartitionedHashJoin, MultiPassWideLocatorsManyPassesWithDuplicates)
{
    /// Three or more single-bit refine passes over the wide locator encoding with duplicate keys:
    /// every pass has to carry the locators and route words forward exactly, keeping a key's
    /// duplicates adjacent within its leaf.
    constexpr size_t distinct_keys = 150000;
    constexpr size_t duplicates = 4;

    auto single = buildJoin(distinct_keys, duplicates, /*num_threads=*/4, /*reserve_safety_for_tests=*/0, /*build_block_rows=*/100000);
    const auto single_stats = single.join->getBuildStats();
    ASSERT_GT(single_stats.partitions, 2u);
    ASSERT_EQ(single_stats.pass_bits.size(), 1u);

    auto multi = buildJoin(
        distinct_keys,
        duplicates,
        /*num_threads=*/4,
        /*reserve_safety_for_tests=*/0,
        /*build_block_rows=*/100000,
        JoinKind::Inner,
        /*disable_amac=*/false,
        /*max_fanout_per_pass_for_tests=*/2);
    const auto multi_stats = multi.join->getBuildStats();

    EXPECT_EQ(multi_stats.partitions, single_stats.partitions);
    ASSERT_GE(multi_stats.pass_bits.size(), 3u) << "a 1-bit ceiling must force one pass per plan bit";
    EXPECT_TRUE(multi_stats.leaf_row_counts == single_stats.leaf_row_counts);
    EXPECT_EQ(multi_stats.leaf_growths, 0u);
    EXPECT_EQ(multi_stats.leaf_rows, distinct_keys * duplicates);

    probeAndCheck(multi, distinct_keys, duplicates, /*misses=*/1000);
}

TEST(PartitionedHashJoin, MultiPassRightJoinNonJoined)
{
    /// `RightJoinFlagBaseAndNonJoined`'s shape on a multi-pass plan: the flag bases and the
    /// non-joined iteration must be as exact over refined leaves as over single-pass ones.
    constexpr size_t distinct_keys = 300000;
    constexpr size_t probed_keys = distinct_keys / 2;

    auto built = buildJoin(
        distinct_keys,
        /*duplicates=*/1,
        /*num_threads=*/4,
        /*reserve_safety_for_tests=*/0,
        block_rows,
        JoinKind::Right,
        /*disable_amac=*/false,
        /*max_fanout_per_pass_for_tests=*/4);
    const auto stats = built.join->getBuildStats();
    ASSERT_GE(stats.pass_bits.size(), 2u);

    JoinedRows expected;
    expected.reserve(probed_keys);
    for (size_t i = 0; i < probed_keys; ++i)
    {
        const UInt64 key = i * 2654435761ULL + 1;
        expected.emplace_back(key, i, key, i);
    }
    std::sort(expected.begin(), expected.end());

    JoinedRows actual;
    actual.reserve(expected.size());
    {
        std::vector<UInt64> keys;
        std::vector<UInt64> ids;
        for (size_t i = 0; i < probed_keys; ++i)
        {
            keys.push_back(i * 2654435761ULL + 1);
            ids.push_back(i);
            if (keys.size() == block_rows || i + 1 == probed_keys)
            {
                auto result = built.join->joinBlock(twoColumnBlock("k", "probe_id", keys, ids));
                drainResult(*result, actual);
                keys.clear();
                ids.clear();
            }
        }
    }
    std::sort(actual.begin(), actual.end());
    ASSERT_EQ(actual.size(), expected.size());
    ASSERT_TRUE(actual == expected);

    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    Block result_sample = left_header.cloneEmpty();
    result_sample.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "build_id"});
    result_sample.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "rk"});

    std::vector<std::pair<UInt64, UInt64>> expected_non_joined;
    expected_non_joined.reserve(distinct_keys - probed_keys);
    for (size_t i = probed_keys; i < distinct_keys; ++i)
        expected_non_joined.emplace_back(i * 2654435761ULL + 1, i);
    std::sort(expected_non_joined.begin(), expected_non_joined.end());

    std::vector<std::pair<UInt64, UInt64>> actual_non_joined;
    actual_non_joined.reserve(expected_non_joined.size());
    auto non_joined = built.join->getNonJoinedBlocks(left_header, result_sample, /*max_block_size=*/65536);
    ASSERT_NE(non_joined, nullptr);
    while (true)
    {
        Block block = non_joined->next();
        if (block.empty())
            break;
        ColumnPtr rk_holder;
        ColumnPtr build_holder;
        const UInt64 * rk = columnData(block, "rk", rk_holder);
        const UInt64 * build_id = columnData(block, "build_id", build_holder);
        for (size_t i = 0; i < block.rows(); ++i)
            actual_non_joined.emplace_back(rk[i], build_id[i]);
    }
    std::sort(actual_non_joined.begin(), actual_non_joined.end());
    ASSERT_EQ(actual_non_joined.size(), expected_non_joined.size());
    ASSERT_TRUE(actual_non_joined == expected_non_joined);
}

namespace
{

Block stringKeyBlock(const String & key_name, const String & id_name, const std::vector<UInt64> & keys, const std::vector<UInt64> & ids)
{
    auto key_column = ColumnString::create();
    auto id_column = ColumnUInt64::create();
    for (const UInt64 k : keys)
    {
        const String value = fmt::format("key_{}", k);
        key_column->insertData(value.data(), value.size());
    }
    id_column->getData().assign(ids.begin(), ids.end());
    Block block;
    block.insert({std::move(key_column), std::make_shared<DataTypeString>(), key_name});
    block.insert({std::move(id_column), std::make_shared<DataTypeUInt64>(), id_name});
    return block;
}

using StringJoinedRow = std::tuple<String, UInt64, String, UInt64>;

/// String keys take the generic-mode scatter, so this covers its per-worker pieces and, on a
/// multi-pass plan, their refinement into per-leaf columns.
void buildAndCheckStringKeys(size_t distinct_keys, size_t max_fanout_per_pass_for_tests, std::vector<UInt64> & leaf_row_counts_out)
{
    const Block left_header = stringKeyBlock("k", "probe_id", {}, {});
    const Block right_header = stringKeyBlock("rk", "build_id", {}, {});

    auto table_join = makeTableJoin(left_header, right_header, JoinKind::Inner);
    auto join = std::make_shared<PartitionedHashJoin>(table_join, std::make_shared<const Block>(right_header), /*num_threads=*/4);
    if (max_fanout_per_pass_for_tests > 0)
        join->setMaxFanoutPerPassForTests(max_fanout_per_pass_for_tests);

    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    for (size_t i = 0; i < distinct_keys; ++i)
    {
        keys.push_back(i);
        ids.push_back(i);
        if (keys.size() == block_rows || i + 1 == distinct_keys)
        {
            EXPECT_TRUE(join->addBlockToJoin(stringKeyBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
            keys.clear();
            ids.clear();
        }
    }
    join->onBuildPhaseFinish();
    join->runPostBuildPhase();

    const auto stats = join->getBuildStats();
    ASSERT_GT(stats.partitions, 2u);
    if (max_fanout_per_pass_for_tests > 0)
        ASSERT_GE(stats.pass_bits.size(), 2u) << "the lowered ceiling must force a multi-pass plan";
    else
        ASSERT_EQ(stats.pass_bits.size(), 1u);
    EXPECT_EQ(stats.leaf_growths, 0u);
    EXPECT_EQ(stats.leaf_rows, distinct_keys);
    leaf_row_counts_out = stats.leaf_row_counts;

    std::vector<StringJoinedRow> expected;
    expected.reserve(distinct_keys);
    for (size_t i = 0; i < distinct_keys; ++i)
    {
        const String key = fmt::format("key_{}", i);
        expected.emplace_back(key, i, key, i);
    }
    std::sort(expected.begin(), expected.end());

    std::vector<StringJoinedRow> actual;
    actual.reserve(expected.size());
    constexpr size_t misses = 10000;
    for (size_t i = 0; i < distinct_keys + misses; ++i)
    {
        keys.push_back(i < distinct_keys ? i : i + (1uz << 40)); /// the offset cannot collide
        ids.push_back(i);
        if (keys.size() == block_rows || i + 1 == distinct_keys + misses)
        {
            auto result = join->joinBlock(stringKeyBlock("k", "probe_id", keys, ids));
            while (true)
            {
                auto r = result->next();
                if (r.block.rows())
                {
                    ColumnPtr k_holder = r.block.getByName("k").column->convertToFullColumnIfReplicated();
                    ColumnPtr probe_holder = r.block.getByName("probe_id").column->convertToFullColumnIfReplicated();
                    ColumnPtr rk_holder = r.block.getByName("rk").column->convertToFullColumnIfReplicated();
                    ColumnPtr build_holder = r.block.getByName("build_id").column->convertToFullColumnIfReplicated();
                    const auto & k_col = assert_cast<const ColumnString &>(*k_holder);
                    const auto & rk_col = assert_cast<const ColumnString &>(*rk_holder);
                    const auto & probe_col = assert_cast<const ColumnUInt64 &>(*probe_holder);
                    const auto & build_col = assert_cast<const ColumnUInt64 &>(*build_holder);
                    for (size_t row = 0; row < r.block.rows(); ++row)
                        actual.emplace_back(
                            String(k_col.getDataAt(row)),
                            probe_col.getData()[row],
                            String(rk_col.getDataAt(row)),
                            build_col.getData()[row]);
                }
                if (r.is_last)
                    break;
            }
            keys.clear();
            ids.clear();
        }
    }
    std::sort(actual.begin(), actual.end());
    ASSERT_EQ(actual.size(), expected.size());
    ASSERT_TRUE(actual == expected);
}

}

TEST(PartitionedHashJoin, MultiPassGenericStringKeys)
{
    /// Leaf row counts must match the single-pass plan of the same data exactly.
    constexpr size_t distinct_keys = 200000;

    std::vector<UInt64> single_pass_counts;
    buildAndCheckStringKeys(distinct_keys, /*max_fanout_per_pass_for_tests=*/0, single_pass_counts);

    std::vector<UInt64> multi_pass_counts;
    buildAndCheckStringKeys(distinct_keys, /*max_fanout_per_pass_for_tests=*/4, multi_pass_counts);

    EXPECT_TRUE(multi_pass_counts == single_pass_counts) << "refine passes must land every row in its single-pass leaf";
}

namespace
{

/// The two-pass consumption the fused fill entry replaces: full words from `computeJoinRouteWords`,
/// then the truncate and the skip-gated sketch feed. Comparing register arrays subsumes comparing
/// estimates. `routes` is poisoned first, so a row the fused path leaves unwritten fails the memcmp -
/// skipped rows' routes must be written too.
void expectFusedRoutingMatchesTwoPass(const ColumnRawPtrs & key_columns, size_t rows, const UInt8 * skip)
{
    PaddedPODArray<UInt32> words(rows);
    computeJoinRouteWords(key_columns, rows, words.data());
    PaddedPODArray<UInt16> expected_routes(rows);
    DenseHyperLogLog expected_hll;
    for (size_t i = 0; i < rows; ++i)
    {
        expected_routes[i] = static_cast<UInt16>(words[i] >> 16);
        if (!skip || !skip[i])
            expected_hll.add(words[i]);
    }

    PaddedPODArray<UInt16> routes;
    routes.assign(rows, static_cast<UInt16>(0xDEAD));
    DenseHyperLogLog hll;
    computeJoinRoutesForFill(key_columns, rows, skip, routes.data(), hll);

    ASSERT_EQ(0, memcmp(routes.data(), expected_routes.data(), rows * sizeof(UInt16)));
    ASSERT_TRUE(hll.registers == expected_hll.registers);
}

}

TEST(PartitionedHashJoin, FusedFillRoutingMatchesTwoPass)
{
    constexpr size_t rows = 10007;

    /// `FillBlock::skipData` hands the fill a null map under the same `const UInt8 *` contract, so
    /// the mask runs cover that shape too.
    std::vector<UInt8> skip_mask(rows);
    for (size_t i = 0; i < rows; ++i)
        skip_mask[i] = i % 3 == 0;

    /// The `routeSingleNumericColumn` hot path.
    auto uint64_key = ColumnUInt64::create();
    for (size_t i = 0; i < rows; ++i)
        uint64_key->insertValue(i * 2654435761ULL + 1);

    /// A second key column forces the general accumulator path over two fixed folds.
    auto uint16_key = ColumnUInt16::create();
    for (size_t i = 0; i < rows; ++i)
        uint16_key->insertValue(static_cast<UInt16>(i * 40503));

    /// Empty and over-8-byte values, so the String fold's tail dispatch runs.
    auto string_key = ColumnString::create();
    for (size_t i = 0; i < rows; ++i)
    {
        const std::string value = i % 7 == 0 ? "" : fmt::format("key-{}-{}", i, std::string(i % 19, 'x'));
        string_key->insertData(value.data(), value.size());
    }

    for (const UInt8 * skip : {static_cast<const UInt8 *>(nullptr), static_cast<const UInt8 *>(skip_mask.data())})
    {
        expectFusedRoutingMatchesTwoPass({uint64_key.get()}, rows, skip);
        expectFusedRoutingMatchesTwoPass({uint64_key.get(), uint16_key.get()}, rows, skip);
        expectFusedRoutingMatchesTwoPass({string_key.get()}, rows, skip);
        /// ASOF routes by the equi-key prefix only, so the fused entry must see exactly the prefix
        /// the caller sliced off.
        const ColumnRawPtrs asof_key_columns{uint64_key.get(), uint16_key.get()};
        expectFusedRoutingMatchesTwoPass(ColumnRawPtrs(asof_key_columns.begin(), asof_key_columns.end() - 1), rows, skip);
    }
}

TEST(PartitionedHashJoin, FusedProbeLeafIdsMatchTwoPass)
{
    constexpr size_t rows = 10007;

    auto uint64_key = ColumnUInt64::create();
    for (size_t i = 0; i < rows; ++i)
        uint64_key->insertValue(i * 2654435761ULL + 1);
    auto uint16_key = ColumnUInt16::create();
    for (size_t i = 0; i < rows; ++i)
        uint16_key->insertValue(static_cast<UInt16>(i * 40503));
    auto string_key = ColumnString::create();
    for (size_t i = 0; i < rows; ++i)
    {
        const std::string value = i % 7 == 0 ? "" : fmt::format("key-{}-{}", i, std::string(i % 19, 'x'));
        string_key->insertData(value.data(), value.size());
    }

    const std::vector<ColumnRawPtrs> key_sets
        = {{uint64_key.get()}, {uint64_key.get(), uint16_key.get()}, {string_key.get()}};
    /// Every plan the 16-bit stored routes cover, both edges included.
    for (const size_t bits : {1uz, 9uz, 16uz})
    {
        const auto shift = static_cast<UInt32>(32 - bits);
        for (const auto & key_columns : key_sets)
        {
            PaddedPODArray<UInt32> words(rows);
            computeJoinRouteWords(key_columns, rows, words.data());
            PaddedPODArray<UInt16> expected(rows);
            for (size_t i = 0; i < rows; ++i)
                expected[i] = static_cast<UInt16>(words[i] >> shift);

            PaddedPODArray<UInt16> leaf_ids;
            leaf_ids.assign(rows, static_cast<UInt16>(0xDEAD));
            computeJoinLeafIds(key_columns, rows, bits, leaf_ids.data());
            ASSERT_EQ(0, memcmp(leaf_ids.data(), expected.data(), rows * sizeof(UInt16)));
        }
    }
}
