#include <gtest/gtest.h>

#include <fmt/format.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <limits>
#include <optional>
#include <tuple>
#include <vector>
#include <unistd.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/PartitionedHashJoin/DenseHyperLogLog.h>
#include <Interpreters/PartitionedHashJoin/JoinRouteHashing.h>
#include <Interpreters/PartitionedHashJoin/PartitionedHashJoin.h>
#include <Interpreters/PartitionedHashJoin/SharedJoinTable.h>
#include <Interpreters/TableJoin.h>
#include <Common/assert_cast.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int LIMIT_EXCEEDED;
}

namespace
{

constexpr size_t block_rows = 65536;

/// The multiset of these tuples over a whole drain is an exact identity, so a dropped, duplicated or
/// cross-wired row changes it. A build row inserted outside its owner's range can never be found by the
/// probe's global walk from the key's home cell either, so mis-routing shows up as missing tuples.
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

std::shared_ptr<TableJoin> makeTableJoin(
    const Block & left_header, const Block & right_header, JoinKind kind = JoinKind::Inner, JoinStrictness strictness = JoinStrictness::All)
{
    Settings settings;
    auto table_join = std::make_shared<TableJoin>(settings, JoinAnalyzeMode::None, /*tmp_volume=*/nullptr, /*tmp_data=*/nullptr);
    table_join->setKind(kind);
    table_join->getTableJoin().strictness = strictness;
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
    PartitionedHashJoin::PostBuildPlan post_build_plan = PartitionedHashJoin::PostBuildPlan::Fits;
};

struct BuildOptions
{
    size_t num_threads = 4;
    double reserve_safety_for_tests = 0;
    size_t build_block_rows = block_rows;
    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
    bool disable_amac = false;
    size_t max_fanout_per_pass_for_tests = 0;
    std::optional<size_t> partition_bits_for_tests;
    bool cap_partitions_by_l1_descriptors = true;
    size_t l1_cache_bytes_for_tests = 0;
    const StatsCollectingParams * stats_collecting_params = nullptr;
    /// The post-build memory gate: zero disables grouping.
    size_t max_bytes_before_external_join = 0;
    /// `0` means grows are always allowed. Unset leaves the constructor budget (the grouping cap).
    std::optional<size_t> grow_budget_for_tests;
    bool lift_grow_budget_before_drain = false;
    std::optional<size_t> reserve_override_for_tests;
};

UInt64 keyOf(size_t i)
{
    return i * 2654435761ULL + 1;
}

using TestSharedTable
    = SharedJoinTable<UInt64, HashMapCell<UInt64, RowRefList, HashCRC32<UInt64>>, HashCRC32<UInt64>, HashTableGrowerWithPrecalculation<>>;

/// Feeds the build through the real `IJoin` interface - fill, barrier, post-build. Blocks above
/// 65536 rows take the wide 8-byte locator encoding, smaller ones the packed 4-byte form.
BuiltJoin buildJoin(size_t distinct_keys, size_t duplicates, const BuildOptions & options)
{
    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});

    BuiltJoin result;
    result.table_join = makeTableJoin(left_header, right_header, options.kind, options.strictness);
    result.join = std::make_shared<PartitionedHashJoin>(
        result.table_join,
        std::make_shared<const Block>(right_header),
        options.num_threads,
        /*any_take_last_row_=*/false,
        options.stats_collecting_params ? *options.stats_collecting_params : StatsCollectingParams{},
        options.max_bytes_before_external_join);
    if (options.reserve_safety_for_tests > 0)
        result.join->setReserveSafetyFactorForTests(options.reserve_safety_for_tests);
    if (options.reserve_override_for_tests)
        result.join->setReserveOverrideForTests(*options.reserve_override_for_tests);
    if (options.grow_budget_for_tests)
        result.join->setGrowBudgetForTests(*options.grow_budget_for_tests);
    if (options.lift_grow_budget_before_drain)
    {
        auto * join = result.join.get();
        result.join->setBeforeDrainHookForTests([join] { join->setGrowBudgetForTests(0); });
    }
    /// These builds exceed the engagement threshold, so both AMAC paths run unless overridden.
    if (options.disable_amac)
        result.join->setAmacEnabledForTests(false);
    /// Forces a multi-pass scatter without a 500M-key build. Only the pass split changes; the
    /// partition count must not.
    if (options.max_fanout_per_pass_for_tests > 0)
        result.join->setMaxFanoutPerPassForTests(options.max_fanout_per_pass_for_tests);
    if (options.partition_bits_for_tests)
        result.join->setPartitionBitsForTests(*options.partition_bits_for_tests);
    if (!options.cap_partitions_by_l1_descriptors)
        result.join->setCapPartitionsByL1DescriptorsForTests(false);
    if (options.l1_cache_bytes_for_tests > 0)
        result.join->setL1CacheSizeForTests(options.l1_cache_bytes_for_tests);

    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    keys.reserve(options.build_block_rows);
    ids.reserve(options.build_block_rows);
    UInt64 id = 0;
    for (size_t i = 0; i < distinct_keys; ++i)
    {
        for (size_t d = 0; d < duplicates; ++d)
        {
            keys.push_back(keyOf(i));
            ids.push_back(id++);
            if (keys.size() == options.build_block_rows)
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
    if (options.max_bytes_before_external_join)
        result.post_build_plan = result.join->planPostBuild();
    result.join->runPostBuildPhase();
    return result;
}

BuiltJoin buildJoin(size_t distinct_keys, size_t duplicates, size_t num_threads)
{
    BuildOptions options;
    options.num_threads = num_threads;
    return buildJoin(distinct_keys, duplicates, options);
}

/// One copy of every key per round of `ceil(distinct / block_rows)` blocks, so a grouped scatter
/// puts a key's rows in different groups and writes chains.
BuiltJoin buildDuplicateMajorGrouped(
    size_t distinct_keys,
    size_t duplicates,
    size_t budget,
    JoinKind kind = JoinKind::Inner,
    JoinStrictness strictness = JoinStrictness::All,
    bool run_post_build = true)
{
    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});
    BuiltJoin built;
    built.table_join = makeTableJoin(left_header, right_header, kind, strictness);
    built.join = std::make_shared<PartitionedHashJoin>(
        built.table_join,
        std::make_shared<const Block>(right_header),
        /*num_threads_=*/4,
        /*any_take_last_row_=*/false,
        StatsCollectingParams{},
        budget);
    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    for (size_t d = 0; d < duplicates; ++d)
    {
        for (size_t i = 0; i < distinct_keys; ++i)
        {
            keys.push_back(keyOf(i));
            ids.push_back(i * duplicates + d);
            if (keys.size() == block_rows || i + 1 == distinct_keys)
            {
                EXPECT_TRUE(built.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
                keys.clear();
                ids.clear();
            }
        }
    }
    built.join->onBuildPhaseFinish();
    if (budget)
        built.post_build_plan = built.join->planPostBuild();
    if (run_post_build)
        built.join->runPostBuildPhase();
    return built;
}

/// A budget that still splits the scatter: Grouped when that band exists, otherwise MustSpill just
/// under the ungrouped peak. Never Fits, so `scatter_groups > 1` is what the caller asserts.
size_t pickScatterBudget(const PartitionedHashJoin::PostBuildGateTerms & terms)
{
    const size_t peak = std::max(terms.peak_ungrouped, 2uz);
    size_t budget = terms.grouped_floor;
    if (budget == 0 || budget >= peak)
        budget = peak / 2;
    if (budget >= peak)
        budget = peak - 1;
    return std::max(budget, 1uz);
}

BuiltJoin
buildDuplicateMajorPickedBudget(size_t distinct_keys, size_t duplicates, JoinKind kind, JoinStrictness strictness = JoinStrictness::All)
{
    auto trial = buildDuplicateMajorGrouped(distinct_keys, duplicates, 256u << 20, kind, strictness, /*run_post_build=*/false);
    trial.join->planPostBuild();
    const size_t budget = pickScatterBudget(trial.join->getPostBuildGateTermsForTests());
    trial.join.reset();
    trial.table_join.reset();
    return buildDuplicateMajorGrouped(distinct_keys, duplicates, budget, kind, strictness);
}

/// Probes every distinct key once, plus `misses` absent ones, and checks the exact joined multiset.
void probeAndCheck(BuiltJoin & built, size_t distinct_keys, size_t duplicates, size_t misses, bool use_lanes = false)
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

    JoinedRows actual;
    actual.reserve(expected.size());
    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    size_t probe_block_index = 0;
    for (size_t i = 0; i < distinct_keys + misses; ++i)
    {
        /// The +2 offset cannot collide with a built key: `i * K + 2 == j * K + 1` would need
        /// `i - j` to be the modular inverse of `-K`, far outside these ranges.
        keys.push_back(i < distinct_keys ? keyOf(i) : i * 2654435761ULL + 2);
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

/// The invariants every build must publish: one table, its rows conserved, its distinct count exact.
void expectTableInvariants(const PartitionedHashJoin::BuildStats & stats, size_t distinct_keys, size_t rows)
{
    EXPECT_GT(stats.table_cells, 0u);
    EXPECT_EQ(stats.table_cells, 1uz << stats.table_size_degree);
    if (stats.load_factor_grow_skipped == 0)
        EXPECT_GE(stats.table_cells, 2 * distinct_keys) << "the table must keep at most 50% fill when a load-factor grow was affordable";
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

/// Reconstructs the duplicate-major block layout `GroupedScatter*` fills: one copy of every key per
/// round of `ceil(distinct / block_rows)` blocks. W6 counts headers from the groups the budget chose,
/// not from `scatter_groups` as a per-key segment count.
std::vector<std::vector<size_t>> duplicateMajorBlocks(size_t distinct, size_t duplicates, size_t rows_per_block)
{
    std::vector<std::vector<size_t>> blocks;
    std::vector<size_t> cur;
    cur.reserve(rows_per_block);
    for (size_t d = 0; d < duplicates; ++d)
    {
        for (size_t i = 0; i < distinct; ++i)
        {
            cur.push_back(i);
            if (cur.size() == rows_per_block || i + 1 == distinct)
            {
                blocks.push_back(std::move(cur));
                cur.clear();
                cur.reserve(rows_per_block);
            }
        }
    }
    return blocks;
}

size_t expectedHeadersFromGroups(
    size_t distinct,
    const std::vector<std::vector<size_t>> & blocks,
    const std::vector<PartitionedHashJoin::BuildStats::BlockRange> & groups)
{
    std::vector<size_t> cumulative(distinct, 0);
    size_t headers = 0;
    for (const auto & range : groups)
    {
        std::vector<size_t> in_group(distinct, 0);
        for (size_t b = range.begin; b < range.end && b < blocks.size(); ++b)
            for (size_t k : blocks[b])
                ++in_group[k];
        for (size_t k = 0; k < distinct; ++k)
        {
            const size_t before = cumulative[k];
            const size_t now = in_group[k];
            cumulative[k] += now;
            if (now >= 1 && before >= 2)
                ++headers;
        }
    }
    return headers;
}
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
            keys.push_back(keyOf(i));
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

    expectTableInvariants(built.join->getBuildStats(), distinct_keys, distinct_keys * duplicates);
    probeAndCheck(built, distinct_keys, duplicates, /*misses=*/100, /*use_lanes=*/true);
}

TEST(PartitionedHashJoin, PartitionedBuildExactReservesAndParity)
{
    constexpr size_t distinct_keys = 300000;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, /*num_threads=*/4);

    const auto stats = built.join->getBuildStats();
    EXPECT_GT(stats.partitions, 1u) << "a 300K-key build must partition";
    expectTableInvariants(stats, distinct_keys, distinct_keys);
    EXPECT_GT(stats.ht_total_bytes, 0u);
    EXPECT_NEAR(stats.hll_estimate, static_cast<double>(distinct_keys), 0.05 * distinct_keys) << "the distinct estimate must be within 5%";
    EXPECT_EQ(stats.owner_duplicates.arena_bytes + stats.drain_duplicates.arena_bytes, 0u) << "unique keys never touch the arena";

    probeAndCheck(built, distinct_keys, /*duplicates=*/1, /*misses=*/10000);
}

TEST(PartitionedHashJoin, PartitionedBuildWithDuplicates)
{
    constexpr size_t distinct_keys = 150000;
    constexpr size_t duplicates = 4;
    auto built = buildJoin(distinct_keys, duplicates, /*num_threads=*/4);

    const auto stats = built.join->getBuildStats();
    EXPECT_GT(stats.partitions, 1u);
    expectTableInvariants(stats, distinct_keys, distinct_keys * duplicates);
    /// Every key has four rows in one pass: one exact run, 8 bytes per row, no header.
    const auto dup = totalDuplicates(stats);
    EXPECT_EQ(dup.ranges, distinct_keys);
    EXPECT_EQ(dup.headers, 0u);
    EXPECT_EQ(dup.arena_bytes, distinct_keys * duplicates * sizeof(UInt64));

    probeAndCheck(built, distinct_keys, duplicates, /*misses=*/1000);
}

TEST(PartitionedHashJoin, DegenerateSinglePartition)
{
    constexpr size_t distinct_keys = 1000;
    auto built = buildJoin(distinct_keys, /*duplicates=*/2, /*num_threads=*/4);

    const auto stats = built.join->getBuildStats();
    EXPECT_EQ(stats.partitions, 1u) << "a small build must run as one partition";
    expectTableInvariants(stats, distinct_keys, distinct_keys * 2);
    EXPECT_EQ(stats.overflow_rows, 0u) << "the single-partition walk wraps instead of overflowing";
    EXPECT_EQ(stats.owner_duplicates.ranges, distinct_keys);
    EXPECT_EQ(stats.owner_duplicates.headers, 0u);
    EXPECT_EQ(stats.owner_duplicates.arena_bytes, distinct_keys * 16u);

    probeAndCheck(built, distinct_keys, /*duplicates=*/2, /*misses=*/100);
}

TEST(PartitionedHashJoin, WideLocatorsForLargeBlocks)
{
    /// Above 65536 rows the packed locator no longer fits, so this is the 8-byte path.
    constexpr size_t distinct_keys = 300000;
    BuildOptions options;
    options.build_block_rows = 100000;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, options);

    const auto stats = built.join->getBuildStats();
    EXPECT_GT(stats.partitions, 1u);
    expectTableInvariants(stats, distinct_keys, distinct_keys);

    probeAndCheck(built, distinct_keys, /*duplicates=*/1, /*misses=*/1000);
}

TEST(PartitionedHashJoin, UndersizedTableGrows)
{
    /// A crippled safety factor sizes the table far below the key count. Growth restores the fill;
    /// the build must not throw and the probe must still be an identity.
    constexpr size_t distinct_keys = 50000;
    BuildOptions ungrouped;
    ungrouped.reserve_safety_for_tests = 0.25;
    auto ungrouped_built = buildJoin(distinct_keys, /*duplicates=*/1, ungrouped);
    const auto ungrouped_stats = ungrouped_built.join->getBuildStats();
    EXPECT_GE(ungrouped_stats.table_resizes, 1u);
    expectTableInvariants(ungrouped_stats, distinct_keys, distinct_keys);
    probeAndCheck(ungrouped_built, distinct_keys, /*duplicates=*/1, /*misses=*/100);

    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});
    BuiltJoin grouped;
    grouped.table_join = makeTableJoin(left_header, right_header);
    constexpr size_t grouped_keys = 200000;
    constexpr size_t duplicates = 8;
    constexpr size_t budget = 55u << 20;
    grouped.join = std::make_shared<PartitionedHashJoin>(
        grouped.table_join,
        std::make_shared<const Block>(right_header),
        /*num_threads_=*/4,
        /*any_take_last_row_=*/false,
        StatsCollectingParams{},
        budget);
    grouped.join->setReserveSafetyFactorForTests(0.25);
    grouped.join->setGrowBudgetForTests(0);
    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    for (size_t d = 0; d < duplicates; ++d)
    {
        for (size_t i = 0; i < grouped_keys; ++i)
        {
            keys.push_back(keyOf(i));
            ids.push_back(i * duplicates + d);
            if (keys.size() == block_rows || i + 1 == grouped_keys)
            {
                EXPECT_TRUE(grouped.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
                keys.clear();
                ids.clear();
            }
        }
    }
    grouped.join->onBuildPhaseFinish();
    grouped.post_build_plan = grouped.join->planPostBuild();
    ASSERT_EQ(grouped.post_build_plan, PartitionedHashJoin::PostBuildPlan::Grouped);
    grouped.join->runPostBuildPhase();
    const auto grouped_stats = grouped.join->getBuildStats();
    EXPECT_GE(grouped_stats.table_resizes, 1u);
    EXPECT_GT(grouped_stats.scatter_groups, 1u);
    expectTableInvariants(grouped_stats, grouped_keys, grouped_keys * duplicates);
    probeAndCheck(grouped, grouped_keys, duplicates, /*misses=*/100);
}

TEST(PartitionedHashJoin, RightJoinSharedFlagsAndNonJoined)
{
    /// RIGHT ALL exercises the shared used-flag space and the non-joined iteration over cell
    /// positions: a wrong offset marks or reads the wrong cell, which shows up as missing or duplicated
    /// non-joined rows.
    constexpr size_t distinct_keys = 300000;
    constexpr size_t duplicates = 2;
    constexpr size_t probed_keys = distinct_keys / 2;
    BuildOptions options;
    options.kind = JoinKind::Right;
    auto built = buildJoin(distinct_keys, duplicates, options);

    const auto stats = built.join->getBuildStats();
    EXPECT_GT(stats.partitions, 1u);
    expectTableInvariants(stats, distinct_keys, distinct_keys * duplicates);

    /// RIGHT filters unmatched probe rows, so the output is exactly the probed keys' tuples.
    JoinedRows expected;
    expected.reserve(probed_keys * duplicates);
    for (size_t i = 0; i < probed_keys; ++i)
    {
        const UInt64 key = keyOf(i);
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
            keys.push_back(keyOf(i));
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

    /// The non-joined streams must return exactly the build rows of the unprobed keys, once, across
    /// several parallel streams.
    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    Block result_sample = left_header.cloneEmpty();
    result_sample.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "build_id"});
    result_sample.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "rk"});

    std::vector<std::pair<UInt64, UInt64>> expected_non_joined;
    expected_non_joined.reserve((distinct_keys - probed_keys) * duplicates);
    for (size_t i = probed_keys; i < distinct_keys; ++i)
        for (size_t d = 0; d < duplicates; ++d)
            expected_non_joined.emplace_back(keyOf(i), i * duplicates + d);
    std::sort(expected_non_joined.begin(), expected_non_joined.end());

    constexpr size_t num_streams = 3;
    std::vector<std::pair<UInt64, UInt64>> actual_non_joined;
    actual_non_joined.reserve(expected_non_joined.size());
    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        auto non_joined = built.join->getNonJoinedBlocks(left_header, result_sample, /*max_block_size=*/65536, stream, num_streams);
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
    }
    std::sort(actual_non_joined.begin(), actual_non_joined.end());
    ASSERT_EQ(actual_non_joined.size(), expected_non_joined.size());
    ASSERT_TRUE(actual_non_joined == expected_non_joined);
}

TEST(PartitionedHashJoin, AmacDuplicateHeavyBuildParityVsSequential)
{
    /// 16 rows per key across several partitions, cross-checked against the same build and probe forced
    /// onto the sequential loops. Duplicates are adjacent in the scattered chunks, so same-key rows
    /// are permanently in flight together - which is exactly what the fused read-then-act step has to
    /// survive.
    constexpr size_t distinct_keys = 200000;
    constexpr size_t duplicates = 16;

    auto amac_built = buildJoin(distinct_keys, duplicates, /*num_threads=*/4);
    const auto amac_stats = amac_built.join->getBuildStats();
    EXPECT_GT(amac_stats.partitions, 1u);
    EXPECT_TRUE(amac_stats.amac_build_engaged);
    expectTableInvariants(amac_stats, distinct_keys, distinct_keys * duplicates);

    BuildOptions sequential;
    sequential.disable_amac = true;
    auto sequential_built = buildJoin(distinct_keys, duplicates, sequential);
    const auto sequential_stats = sequential_built.join->getBuildStats();
    EXPECT_FALSE(sequential_stats.amac_build_engaged);
    expectTableInvariants(sequential_stats, distinct_keys, distinct_keys * duplicates);

    /// Both must produce the expected multiset, and so each other's.
    probeAndCheck(amac_built, distinct_keys, duplicates, /*misses=*/1000);
    probeAndCheck(sequential_built, distinct_keys, duplicates, /*misses=*/1000);
}

TEST(PartitionedHashJoin, MultiPassForcedPlanLeafParity)
{
    /// The same build planned single-pass and multi-pass. The partition count must be identical - the
    /// ceiling splits the scatter, it must never cap the plan - and every partition must receive the
    /// same rows, which the per-partition counts and then the multiset check pin.
    constexpr size_t distinct_keys = 300000;

    auto single = buildJoin(distinct_keys, /*duplicates=*/1, /*num_threads=*/4);
    const auto single_stats = single.join->getBuildStats();
    ASSERT_GT(single_stats.partitions, 2u);
    ASSERT_EQ(single_stats.pass_bits.size(), 1u) << "the default ceiling must plan a single pass here";

    /// A 2-bit ceiling against a plan of at least 2 bits forces two passes.
    BuildOptions options;
    options.max_fanout_per_pass_for_tests = 4;
    auto multi = buildJoin(distinct_keys, /*duplicates=*/1, options);
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

    /// The refine passes must land rows in exactly the partitions a single-pass plan produces.
    ASSERT_EQ(multi_stats.partition_row_counts.size(), multi_stats.partitions);
    EXPECT_TRUE(multi_stats.partition_row_counts == single_stats.partition_row_counts);
    expectTableInvariants(multi_stats, distinct_keys, distinct_keys);

    probeAndCheck(multi, distinct_keys, /*duplicates=*/1, /*misses=*/10000);
}

TEST(PartitionedHashJoin, DescriptorCapClampsPlan)
{
    /// A 256-byte L1 holds four 16-byte descriptors in its quarter, so the cap clamps a 2M-key build
    /// to four partitions; with the cap switched off the same L1 changes nothing. Results stay exact.
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
    EXPECT_EQ(stats.inserted_rows, distinct_keys);

    probeAndCheck(capped, distinct_keys, /*duplicates=*/1, /*misses=*/10000);
}

TEST(PartitionedHashJoin, MultiPassWideLocatorsManyPassesWithDuplicates)
{
    /// Three or more single-bit refine passes over the wide locator encoding with duplicate keys:
    /// every pass has to carry the locators and route words forward exactly, keeping a key's
    /// duplicates adjacent within its partition.
    constexpr size_t distinct_keys = 150000;
    constexpr size_t duplicates = 4;

    BuildOptions single_options;
    single_options.build_block_rows = 100000;
    auto single = buildJoin(distinct_keys, duplicates, single_options);
    const auto single_stats = single.join->getBuildStats();
    ASSERT_GT(single_stats.partitions, 2u);
    ASSERT_EQ(single_stats.pass_bits.size(), 1u);

    BuildOptions multi_options;
    multi_options.build_block_rows = 100000;
    multi_options.max_fanout_per_pass_for_tests = 2;
    auto multi = buildJoin(distinct_keys, duplicates, multi_options);
    const auto multi_stats = multi.join->getBuildStats();

    EXPECT_EQ(multi_stats.partitions, single_stats.partitions);
    ASSERT_GE(multi_stats.pass_bits.size(), 3u) << "a 1-bit ceiling must force one pass per plan bit";
    EXPECT_TRUE(multi_stats.partition_row_counts == single_stats.partition_row_counts);
    expectTableInvariants(multi_stats, distinct_keys, distinct_keys * duplicates);

    probeAndCheck(multi, distinct_keys, duplicates, /*misses=*/1000);
}

TEST(PartitionedHashJoin, MultiPassRightJoinNonJoined)
{
    /// `RightJoinSharedFlagsAndNonJoined`'s shape on a multi-pass plan: the flags and the non-joined
    /// iteration must be as exact over refined partitions as over single-pass ones.
    constexpr size_t distinct_keys = 300000;
    constexpr size_t probed_keys = distinct_keys / 2;

    BuildOptions options;
    options.kind = JoinKind::Right;
    options.max_fanout_per_pass_for_tests = 4;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, options);
    const auto stats = built.join->getBuildStats();
    ASSERT_GE(stats.pass_bits.size(), 2u);

    JoinedRows expected;
    expected.reserve(probed_keys);
    for (size_t i = 0; i < probed_keys; ++i)
    {
        const UInt64 key = keyOf(i);
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
            keys.push_back(keyOf(i));
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
        expected_non_joined.emplace_back(keyOf(i), i);
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
/// multi-pass plan, their refinement into per-partition columns; the keys are persisted into the
/// build arenas by the claims and by the overflow handoff.
void buildAndCheckStringKeys(size_t distinct_keys, size_t max_fanout_per_pass_for_tests, std::vector<UInt64> & partition_row_counts_out)
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
    expectTableInvariants(stats, distinct_keys, distinct_keys);
    partition_row_counts_out = stats.partition_row_counts;

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
    /// Partition row counts must match the single-pass plan of the same data exactly.
    constexpr size_t distinct_keys = 200000;

    std::vector<UInt64> single_pass_counts;
    buildAndCheckStringKeys(distinct_keys, /*max_fanout_per_pass_for_tests=*/0, single_pass_counts);

    std::vector<UInt64> multi_pass_counts;
    buildAndCheckStringKeys(distinct_keys, /*max_fanout_per_pass_for_tests=*/4, multi_pass_counts);

    EXPECT_TRUE(multi_pass_counts == single_pass_counts) << "refine passes must land every row in its single-pass partition";
}

TEST(PartitionedHashJoin, RoutesMatchTablePlacement)
{
    /// Invariant I1/I2 of the design: the route the fill saves for a key names the partition whose cell
    /// range contains the key's home cell, for every plan the 16-bit routes cover. Checked on the table
    /// type the `UInt64` keys use and on the string type, with the same hashes the build and probe use.
    constexpr size_t rows = 10007;
    auto uint64_key = ColumnUInt64::create();
    for (size_t i = 0; i < rows; ++i)
        uint64_key->insertValue(keyOf(i));
    uint64_key->insertValue(0); /// the zero key has a route too

    {
        using Table = typename decltype(SharedMapsAll::key64)::element_type;
        const ColumnRawPtrs key_columns{uint64_key.get()};
        const Sizes key_sizes{sizeof(UInt64)};
        PaddedPODArray<UInt16> routes(uint64_key->size());
        DenseHyperLogLog hll;
        computeJoinRoutesForFill(HashJoin::Type::key64, key_columns, key_sizes, uint64_key->size(), nullptr, routes.data(), hll);
        EXPECT_NEAR(hll.estimate(), static_cast<double>(uint64_key->size()), 0.05 * static_cast<double>(uint64_key->size()));

        for (const size_t bits : {1uz, 9uz, 15uz})
        {
            const size_t size_degree = std::max<size_t>(bits, 16);
            Table table(size_degree, bits);
            const auto & data = uint64_key->getData();
            for (size_t i = 0; i < data.size(); ++i)
            {
                const size_t hash = table.hash(data[i]);
                const size_t partition = routes[i] >> (16 - bits);
                ASSERT_EQ(table.partitionOf(hash), partition) << "bits " << bits << " row " << i;
                const size_t home = table.place(hash);
                ASSERT_GE(home, table.rangeBegin(partition));
                ASSERT_LT(home, table.rangeEnd(partition));
            }
        }
    }

    {
        auto string_key = ColumnString::create();
        for (size_t i = 0; i < rows; ++i)
        {
            const std::string value = i % 7 == 0 ? "" : fmt::format("key-{}-{}", i, std::string(i % 19, 'x'));
            string_key->insertData(value.data(), value.size());
        }
        using Table = typename decltype(SharedMapsAll::key_string)::element_type;
        const ColumnRawPtrs key_columns{string_key.get()};
        const Sizes key_sizes{0};
        PaddedPODArray<UInt16> routes(rows);
        DenseHyperLogLog hll;
        computeJoinRoutesForFill(HashJoin::Type::key_string, key_columns, key_sizes, rows, nullptr, routes.data(), hll);

        constexpr size_t bits = 7;
        Table table(/*size_degree_=*/16, bits);
        for (size_t i = 0; i < rows; ++i)
        {
            const std::string_view value = string_key->getDataAt(i);
            const size_t hash = table.hash(value);
            ASSERT_EQ(table.partitionOf(hash), routes[i] >> (16 - bits)) << "row " << i;
        }
    }
}

TEST(PartitionedHashJoin, TwoThousandLeavesTwoPasses)
{
    /// The production ceiling takes 2048 partitions in one pass, so the test forces 1024 per pass: two
    /// MSB-first passes of 6 + 5 bits, every one of the 2048 owner ranges filled by the wave, one table,
    /// exact row conservation, exact results.
    constexpr size_t distinct_keys = 1000000;
    BuildOptions options;
    options.partition_bits_for_tests = 11;
    options.max_fanout_per_pass_for_tests = 1024;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, options);

    const auto stats = built.join->getBuildStats();
    EXPECT_EQ(stats.bits, 11u);
    EXPECT_EQ(stats.partitions, 2048u);
    ASSERT_EQ(stats.pass_bits.size(), 2u) << "2048 partitions must take two scatter passes under a 1024-per-pass ceiling";
    EXPECT_EQ(stats.pass_bits[0] + stats.pass_bits[1], 11u);
    ASSERT_EQ(stats.partition_row_counts.size(), 2048u);
    UInt64 routed = 0;
    for (UInt64 rows : stats.partition_row_counts)
        routed += rows;
    EXPECT_EQ(routed, distinct_keys);
    expectTableInvariants(stats, distinct_keys, distinct_keys);
    EXPECT_GE(stats.table_size_degree, 11u);

    probeAndCheck(built, distinct_keys, /*duplicates=*/1, /*misses=*/10000);
}

TEST(PartitionedHashJoin, FourThousandLeavesThreePasses)
{
    /// 4096 partitions with a 16-way per-pass ceiling: three refine passes of 4 bits, duplicates kept
    /// adjacent through all of them, then 4096 owner ranges and the drain.
    constexpr size_t distinct_keys = 500000;
    constexpr size_t duplicates = 2;
    BuildOptions options;
    options.partition_bits_for_tests = 12;
    options.max_fanout_per_pass_for_tests = 16;
    auto built = buildJoin(distinct_keys, duplicates, options);

    const auto stats = built.join->getBuildStats();
    EXPECT_EQ(stats.bits, 12u);
    EXPECT_EQ(stats.partitions, 4096u);
    ASSERT_EQ(stats.pass_bits.size(), 3u) << "4096 partitions under a 16-way ceiling must take three passes";
    for (const size_t pass : stats.pass_bits)
        EXPECT_LE(1uz << pass, 16u);
    ASSERT_EQ(stats.partition_row_counts.size(), 4096u);
    UInt64 routed = 0;
    for (UInt64 rows : stats.partition_row_counts)
        routed += rows;
    EXPECT_EQ(routed, distinct_keys * duplicates);
    expectTableInvariants(stats, distinct_keys, distinct_keys * duplicates);
    EXPECT_EQ(totalDuplicates(stats).ranges, distinct_keys);
    EXPECT_EQ(totalDuplicates(stats).headers, 0u);
    EXPECT_EQ(totalDuplicates(stats).arena_bytes, distinct_keys * 16u) << "every key is a run of two";

    probeAndCheck(built, distinct_keys, duplicates, /*misses=*/10000);
}

namespace
{

/// A build whose special keys all have their home cell in the last `window` cells of the LAST partition
/// range: `fillers` unique keys arrive first and fill that window, then duplicate sets of 2, 3 and 9 rows
/// of three more such keys arrive in later blocks. Their owner walks must reach the range end, hand the
/// rows to the overflow, and the drain must place them past the buffer end - wrapping into partition 0 -
/// and build their exact runs there. The probe then has to wrap the same way to find them.
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
    using Table = typename decltype(SharedMapsAll::key64)::element_type;
    constexpr size_t random_keys = 100000;

    /// The geometry the real build will have: the same key count gives the same table degree (the
    /// estimate would have to be off by more than 20% to move it), and the partition bits are forced.
    BuildOptions pilot_options;
    pilot_options.num_threads = num_threads;
    pilot_options.partition_bits_for_tests = bits;
    const auto pilot = buildJoin(random_keys, /*duplicates=*/1, pilot_options).join->getBuildStats();
    const size_t size_degree = pilot.table_size_degree;
    EXPECT_EQ(pilot.bits, bits) << "the forced partition bits must be accepted";
    Table geometry(size_degree, bits);
    const size_t last = geometry.partitions() - 1;
    const size_t range_end = geometry.rangeEnd(last);
    EXPECT_EQ(range_end, geometry.cellCount()) << "the crossing window must sit at the buffer end";

    CrossingBuild result;
    result.window = 64;
    result.fillers = result.window + 192;
    const size_t window_begin = range_end - result.window;

    /// Keys whose home cell lies inside the window, found by scanning candidates that cannot collide
    /// with the random keys below (odd multiples of the random step are never `i * step + 2`).
    std::vector<UInt64> window_keys;
    for (UInt64 candidate = 2; window_keys.size() < result.fillers + 3; candidate += 2654435761ULL)
    {
        const size_t home = geometry.place(geometry.hash(candidate));
        if (home >= window_begin && home < range_end)
            window_keys.push_back(candidate);
    }

    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});
    result.built.table_join = makeTableJoin(left_header, right_header, JoinKind::Inner);
    result.built.join = std::make_shared<PartitionedHashJoin>(result.built.table_join, std::make_shared<const Block>(right_header), num_threads);
    result.built.join->setPartitionBitsForTests(bits);
    if (disable_amac)
        result.built.join->setAmacEnabledForTests(false);

    UInt64 build_id = 0;
    std::vector<std::pair<UInt64, UInt64>> build_rows; /// (key, build_id) in insertion order
    auto add_block = [&](const std::vector<UInt64> & keys)
    {
        std::vector<UInt64> ids;
        for (const UInt64 key : keys)
        {
            ids.push_back(build_id);
            build_rows.emplace_back(key, build_id);
            ++build_id;
        }
        EXPECT_TRUE(result.built.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
    };

    /// Block 0: the fillers, first in every worker's stripe order.
    add_block(std::vector<UInt64>(window_keys.begin(), window_keys.begin() + result.fillers));
    /// Then the random keys, in blocks of 65536 - the bulk of the build, spread over every partition.
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
    /// The last block: the duplicate sets of the three remaining window keys, interleaved so a key's
    /// rows are not adjacent.
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

    result.built.join->onBuildPhaseFinish();
    result.built.join->runPostBuildPhase();

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

void probeCrossing(CrossingBuild & crossing)
{
    JoinedRows actual;
    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    for (size_t i = 0; i < crossing.probe_keys.size(); ++i)
    {
        keys.push_back(crossing.probe_keys[i]);
        ids.push_back(i);
        if (keys.size() == block_rows || i + 1 == crossing.probe_keys.size())
        {
            auto result = crossing.built.join->joinBlock(twoColumnBlock("k", "probe_id", keys, ids));
            drainResult(*result, actual);
            keys.clear();
            ids.clear();
        }
    }
    std::sort(actual.begin(), actual.end());
    ASSERT_EQ(actual.size(), crossing.expected.size());
    ASSERT_TRUE(actual == crossing.expected);
}

void expectCrossingStats(const CrossingBuild & crossing)
{
    const auto stats = crossing.built.join->getBuildStats();
    /// The window holds `window` cells; every later window key - the extra fillers and all 14 duplicate
    /// rows - reached the range end and went through the drain, which wrapped past the buffer end.
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

TEST(PartitionedHashJoin, RangeCrossingWraparoundDuplicates)
{
    /// One owner range and one worker per pass, AMAC on and off, so the ring's fused visit and the
    /// sequential walk both hand the boundary rows off and the drain wraps them.
    for (const bool disable_amac : {false, true})
    {
        auto crossing = buildCrossing(/*num_threads=*/1, disable_amac, /*bits=*/2);
        expectCrossingStats(crossing);
        probeCrossing(crossing);
    }
}

TEST(PartitionedHashJoin, RangeCrossingWraparoundManyWorkers)
{
    /// Eight workers over sixteen partitions: the fillers and the duplicates still meet the last range's
    /// end, the overflow of that range is drained after the barrier, and the other owners never touch it.
    for (const bool disable_amac : {false, true})
    {
        auto crossing = buildCrossing(/*num_threads=*/8, disable_amac, /*bits=*/4);
        expectCrossingStats(crossing);
        probeCrossing(crossing);
    }
}

TEST(PartitionedHashJoin, GroupedScatterExactSpans)
{
    /// A memory budget between the grouped floor and the ungrouped peak makes the post-build scatter run in
    /// several block ranges; a key's eight duplicates are spread one per block so they arrive in different
    /// groups. Later groups append a headered range onto the headerless run the second group created.
    constexpr size_t distinct_keys = 200000;
    constexpr size_t duplicates = 8;

    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});
    BuiltJoin built;
    built.table_join = makeTableJoin(left_header, right_header, JoinKind::Inner);
    constexpr size_t budget = 55u << 20;
    built.join = std::make_shared<PartitionedHashJoin>(
        built.table_join, std::make_shared<const Block>(right_header), /*num_threads_=*/4, /*any_take_last_row_=*/false, StatsCollectingParams{}, budget);

    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    for (size_t d = 0; d < duplicates; ++d)
    {
        for (size_t i = 0; i < distinct_keys; ++i)
        {
            keys.push_back(keyOf(i));
            ids.push_back(i * duplicates + d);
            if (keys.size() == block_rows || i + 1 == distinct_keys)
            {
                EXPECT_TRUE(built.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
                keys.clear();
                ids.clear();
            }
        }
    }
    built.join->onBuildPhaseFinish();
    built.post_build_plan = built.join->planPostBuild();
    ASSERT_EQ(built.post_build_plan, PartitionedHashJoin::PostBuildPlan::Grouped)
        << "test setup assumption: the budget must fall between the grouped floor and the ungrouped peak";
    built.join->runPostBuildPhase();

    const auto stats = built.join->getBuildStats();
    ASSERT_GT(stats.scatter_groups, 1u);
    EXPECT_GT(stats.partitions, 1u);
    expectTableInvariants(stats, distinct_keys, distinct_keys * duplicates);

    const auto dup = totalDuplicates(stats);
    const auto blocks = duplicateMajorBlocks(distinct_keys, duplicates, block_rows);
    const size_t expected_headers = expectedHeadersFromGroups(distinct_keys, blocks, stats.scatter_group_ranges);
    EXPECT_EQ(dup.headers, expected_headers) << "W6 over the actual group block ranges";
    EXPECT_EQ(dup.headers, dup.spanning_keys) << "each spanning key has one later-pass range and no split";
    EXPECT_EQ(dup.ranges, distinct_keys + dup.headers) << "one headerless first range per key, then one range per header";
    EXPECT_EQ(dup.arena_bytes, 8 * distinct_keys * duplicates + 16 * dup.headers);

    probeAndCheck(built, distinct_keys, duplicates, /*misses=*/1000);
}

TEST(PartitionedHashJoin, GroupedChainsAcrossGroups)
{
    /// Keys with many rows, spread one row per block over the whole build, so every scatter group boundary
    /// that finds an existing range writes a header. The probe walks newest range first.
    constexpr size_t distinct_keys = 100000;
    constexpr size_t duplicates = 24;

    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});
    BuiltJoin built;
    built.table_join = makeTableJoin(left_header, right_header, JoinKind::Inner);
    constexpr size_t budget = 75u << 20;
    built.join = std::make_shared<PartitionedHashJoin>(
        built.table_join, std::make_shared<const Block>(right_header), /*num_threads_=*/4, /*any_take_last_row_=*/false, StatsCollectingParams{}, budget);

    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    for (size_t d = 0; d < duplicates; ++d)
    {
        for (size_t i = 0; i < distinct_keys; ++i)
        {
            keys.push_back(keyOf(i));
            ids.push_back(i * duplicates + d);
            if (keys.size() == block_rows || i + 1 == distinct_keys)
            {
                EXPECT_TRUE(built.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
                keys.clear();
                ids.clear();
            }
        }
    }
    built.join->onBuildPhaseFinish();
    built.post_build_plan = built.join->planPostBuild();
    ASSERT_EQ(built.post_build_plan, PartitionedHashJoin::PostBuildPlan::Grouped)
        << "test setup assumption: the budget must fall between the grouped floor and the ungrouped peak";
    built.join->runPostBuildPhase();

    const auto stats = built.join->getBuildStats();
    ASSERT_GE(stats.scatter_groups, 2u);
    EXPECT_GT(stats.partitions, 1u);
    expectTableInvariants(stats, distinct_keys, distinct_keys * duplicates);

    const auto dup = totalDuplicates(stats);
    const auto blocks = duplicateMajorBlocks(distinct_keys, duplicates, block_rows);
    const size_t expected_headers = expectedHeadersFromGroups(distinct_keys, blocks, stats.scatter_group_ranges);
    EXPECT_EQ(dup.headers, expected_headers) << "W6 over the actual group block ranges";
    EXPECT_EQ(dup.headers, dup.spanning_keys) << "no split, so every header is a spanning later pass";
    EXPECT_GT(dup.headers, 0u);
    EXPECT_EQ(dup.ranges, distinct_keys + dup.headers);
    EXPECT_EQ(dup.arena_bytes, 8u * distinct_keys * duplicates + 16u * dup.headers);

    probeAndCheck(built, distinct_keys, duplicates, /*misses=*/1000);
}

TEST(PartitionedHashJoin, AllPairsFormulas)
{
    /// Every key twice, one group: 16 arena bytes per key (F4), and the first-group scratch bound is
    /// 28 bytes per key. The predicted terms must cover the actuals.
    constexpr size_t distinct_keys = 2000;
    constexpr size_t duplicates = 2;
    auto built = buildJoin(distinct_keys, duplicates, /*num_threads=*/4);
    const auto stats = built.join->getBuildStats();
    expectTableInvariants(stats, distinct_keys, distinct_keys * duplicates);

    const auto dup = totalDuplicates(stats);
    EXPECT_EQ(dup.headers, 0u);
    EXPECT_EQ(dup.ranges, distinct_keys);
    EXPECT_EQ(dup.arena_bytes, 16u * distinct_keys);
    EXPECT_GE(built.join->predictedArenaBytesForTests(/*grouped=*/false), dup.arena_bytes);

    const size_t predicted_tail = built.join->predictedDuplicateScratchBytesForTests(distinct_keys * duplicates, /*first_group=*/true);
    EXPECT_GE(predicted_tail, stats.scratch_used_high_water);
    EXPECT_GE(predicted_tail, 28u * distinct_keys);
    if (stats.partitions == 1)
        EXPECT_EQ(stats.scratch_used_high_water, 28u * distinct_keys);
}

TEST(PartitionedHashJoin, PlanPostBuildHeaderTerm)
{
    /// `G_est` comes from the ungrouped floor; the grouped arena prediction then adds 16 bytes per
    /// estimated header. The difference of the two floors is exactly that term, not the old
    /// extra_per_key model.
    constexpr size_t distinct_keys = 200000;
    constexpr size_t duplicates = 8;
    constexpr size_t budget = 55u << 20;

    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});
    BuiltJoin built;
    built.table_join = makeTableJoin(left_header, right_header, JoinKind::Inner);
    built.join = std::make_shared<PartitionedHashJoin>(
        built.table_join,
        std::make_shared<const Block>(right_header),
        /*num_threads_=*/4,
        /*any_take_last_row_=*/false,
        StatsCollectingParams{},
        budget);

    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    for (size_t d = 0; d < duplicates; ++d)
    {
        for (size_t i = 0; i < distinct_keys; ++i)
        {
            keys.push_back(keyOf(i));
            ids.push_back(i * duplicates + d);
            if (keys.size() == block_rows || i + 1 == distinct_keys)
            {
                EXPECT_TRUE(built.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
                keys.clear();
                ids.clear();
            }
        }
    }
    built.join->onBuildPhaseFinish();
    built.post_build_plan = built.join->planPostBuild();
    ASSERT_EQ(built.post_build_plan, PartitionedHashJoin::PostBuildPlan::Grouped)
        << "test setup assumption: the budget must fall between the grouped floor and the ungrouped peak";

    const auto terms = built.join->getPostBuildGateTermsForTests();
    ASSERT_GE(terms.groups_est, 2u) << "a grouped plan must estimate more than one group";

    const auto stats = built.join->getBuildStats();
    const size_t distinct = std::max(static_cast<size_t>(std::llround(stats.hll_estimate)), 1uz);
    const size_t rows = distinct_keys * duplicates;
    const size_t dup_rows = rows > distinct ? rows - distinct : 0;
    const size_t dup_keys = std::min(distinct, dup_rows);
    const size_t header_term = 16 * std::min(dup_rows, dup_keys * (terms.groups_est - 1));
    EXPECT_EQ(terms.floor_bytes_grouped - terms.floor_bytes, header_term);

    const size_t headroom = terms.floor_bytes + terms.tables < budget ? budget - terms.floor_bytes - terms.tables : 1;
    const size_t recomputed = std::max(1uz, terms.chunk_all / headroom + (terms.chunk_all % headroom != 0));
    EXPECT_EQ(terms.groups_est, recomputed) << "G_est must be ceil(chunk_all / ungrouped-floor headroom)";
}

TEST(PartitionedHashJoin, SaturatedRunCount)
{
    /// A range holds at most 32766 refs. Exactly that many is a headerless run; one more splits and
    /// `rows` reads the exact total from the newest header. The boundary values are checked on both sides.
    for (const size_t rows : {static_cast<size_t>(RowRefList::MAX_RANGE_REFS), static_cast<size_t>(RowRefList::COUNT_SAT), 40000uz})
    {
        auto built = buildJoin(/*distinct_keys=*/1, rows, /*num_threads=*/2);
        const auto stats = built.join->getBuildStats();
        expectTableInvariants(stats, 1, rows);
        const auto dup = totalDuplicates(stats);
        const UInt64 headers = rows > RowRefList::MAX_RANGE_REFS ? 1u : 0u;
        EXPECT_EQ(dup.ranges, 1u + headers) << "rows " << rows;
        EXPECT_EQ(dup.headers, headers) << "rows " << rows;
        EXPECT_EQ(dup.arena_bytes, 8u * rows + 16u * headers) << "rows " << rows;
        probeAndCheck(built, /*distinct_keys=*/1, rows, /*misses=*/10);
    }
}

TEST(PartitionedHashJoin, ZeroKeyDuplicatesMultiPartition)
{
    /// The zero key lives in the zero-value cell outside every range. Its rows are all routed to one
    /// partition, whose owner claims that cell once and appends the rest; the probe finds them through the
    /// same cell, and the distinct count includes it.
    constexpr size_t distinct_keys = 300000;
    constexpr size_t zero_rows = 5;

    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});
    BuiltJoin built;
    built.table_join = makeTableJoin(left_header, right_header, JoinKind::Inner);
    built.join = std::make_shared<PartitionedHashJoin>(built.table_join, std::make_shared<const Block>(right_header), 4);

    JoinedRows expected;
    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    UInt64 id = 0;
    auto push = [&](UInt64 key)
    {
        keys.push_back(key);
        ids.push_back(id++);
        if (keys.size() == block_rows)
        {
            EXPECT_TRUE(built.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
            keys.clear();
            ids.clear();
        }
    };
    for (size_t i = 0; i < distinct_keys; ++i)
    {
        push(keyOf(i));
        expected.emplace_back(keyOf(i), i, keyOf(i), id - 1);
        /// Zero rows scattered through the build, not adjacent.
        if (i % (distinct_keys / zero_rows) == 0)
        {
            push(0);
            expected.emplace_back(0, distinct_keys, 0, id - 1);
        }
    }
    if (!keys.empty())
        EXPECT_TRUE(built.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
    built.join->onBuildPhaseFinish();
    built.join->runPostBuildPhase();

    const auto stats = built.join->getBuildStats();
    EXPECT_GT(stats.partitions, 1u);
    expectTableInvariants(stats, distinct_keys + 1, distinct_keys + zero_rows);
    EXPECT_EQ(stats.owner_duplicates.ranges + stats.drain_duplicates.ranges, 1u) << "the zero key is the only duplicated one";

    std::sort(expected.begin(), expected.end());
    JoinedRows actual;
    keys.clear();
    ids.clear();
    for (size_t i = 0; i <= distinct_keys; ++i)
    {
        keys.push_back(i < distinct_keys ? keyOf(i) : 0);
        ids.push_back(i);
        if (keys.size() == block_rows || i == distinct_keys)
        {
            auto result = built.join->joinBlock(twoColumnBlock("k", "probe_id", keys, ids));
            drainResult(*result, actual);
            keys.clear();
            ids.clear();
        }
    }
    std::sort(actual.begin(), actual.end());
    ASSERT_EQ(actual.size(), expected.size());
    ASSERT_TRUE(actual == expected);
}

TEST(PartitionedHashJoin, PublishesStatisticsWithoutConsuming)
{
    /// The statistics cache receives the exact distinct count of every build, for the planner's other
    /// consumers, but a later build under the same key never sizes from it: a five times larger build must
    /// still estimate its own count from the sketch and size the table for it.
    static std::atomic<UInt64> key_counter{0};
    const UInt64 key = 0xC1D15117C4C4E000ULL + key_counter.fetch_add(1);
    const StatsCollectingParams params(
        key, /*enable_=*/true, /*max_entries_for_hash_table_stats_=*/1024, /*max_size_to_preallocate_=*/1ULL << 40);

    constexpr size_t small_keys = 50000;
    constexpr size_t large_keys = 250000;

    BuildOptions options;
    options.stats_collecting_params = &params;
    auto small = buildJoin(small_keys, /*duplicates=*/1, options);
    expectTableInvariants(small.join->getBuildStats(), small_keys, small_keys);
    const auto published = getHashTablesStatistics<PartitionedHashJoinEntry>().getSizeHint(params);
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ(published->total_distinct, small_keys);

    auto large = buildJoin(large_keys, /*duplicates=*/1, options);
    const auto large_stats = large.join->getBuildStats();
    EXPECT_NEAR(large_stats.hll_estimate, static_cast<double>(large_keys), 0.05 * static_cast<double>(large_keys))
        << "the sketch must have run; a cached count would read 50000";
    expectTableInvariants(large_stats, large_keys, large_keys);
    probeAndCheck(large, large_keys, /*duplicates=*/1, /*misses=*/1000);

    const auto republished = getHashTablesStatistics<PartitionedHashJoinEntry>().getSizeHint(params);
    ASSERT_TRUE(republished.has_value());
    EXPECT_EQ(republished->total_distinct, large_keys);
}

TEST(PartitionedHashJoin, DrainCutGrowsAndContinues)
{
    /// Ungrouped, estimate off by more than 2x: refuse the pre-drain G2, then lift the budget so the
    /// drain's last empty cell can grow once and finish.
    constexpr size_t distinct_keys = 131072;
    BuildOptions options;
    options.reserve_safety_for_tests = 0.4;
    options.grow_budget_for_tests = 1;
    options.lift_grow_budget_before_drain = true;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, options);
    const auto stats = built.join->getBuildStats();
    EXPECT_EQ(stats.table_resizes, 1u);
    EXPECT_EQ(stats.table_size_degree, TestSharedTable::degreeFor(static_cast<size_t>(std::ceil(distinct_keys * 0.4))) + 1);
    expectTableInvariants(stats, distinct_keys, distinct_keys);
    EXPECT_EQ(stats.inserted_rows, distinct_keys);
    probeAndCheck(built, distinct_keys, /*duplicates=*/1, /*misses=*/100);
}

TEST(PartitionedHashJoin, GrowRefusedThrows)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    GTEST_SKIP() << "a refused G1 grow raises LOGICAL_ERROR, which aborts instead of throwing in debug and sanitizer builds";
#else
    constexpr size_t distinct_keys = 131072;
    BuildOptions options;
    options.reserve_safety_for_tests = 0.4;
    options.grow_budget_for_tests = 1;
    try
    {
        buildJoin(distinct_keys, /*duplicates=*/1, options);
        FAIL() << "a G1 grow refused by budget must throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
        EXPECT_TRUE(e.message().contains("cannot grow") || e.message().contains("distinct keys")) << e.message();
    }
#endif
}

TEST(PartitionedHashJoin, SinglePartitionGrowsMidPass)
{
    constexpr size_t more_than_2x_keys = 131072;
    BuildOptions g1;
    g1.reserve_safety_for_tests = 0.4;
    g1.partition_bits_for_tests = 0;
    auto g1_built = buildJoin(more_than_2x_keys, /*duplicates=*/1, g1);
    const auto g1_stats = g1_built.join->getBuildStats();
    EXPECT_EQ(g1_stats.partitions, 1u);
    EXPECT_FALSE(g1_stats.amac_build_engaged);
    EXPECT_GE(g1_stats.table_resizes, 1u);
    const size_t initial_degree = TestSharedTable::degreeFor(static_cast<size_t>(std::ceil(more_than_2x_keys * 0.4)));
    EXPECT_EQ(g1_stats.table_size_degree, initial_degree + g1_stats.table_resizes);
    expectTableInvariants(g1_stats, more_than_2x_keys, more_than_2x_keys);
    probeAndCheck(g1_built, more_than_2x_keys, /*duplicates=*/1, /*misses=*/100);

    constexpr size_t less_than_2x_keys = 200000;
    BuildOptions g2;
    g2.reserve_safety_for_tests = 0.6;
    g2.partition_bits_for_tests = 0;
    auto g2_built = buildJoin(less_than_2x_keys, /*duplicates=*/1, g2);
    const auto g2_stats = g2_built.join->getBuildStats();
    EXPECT_EQ(g2_stats.partitions, 1u);
    EXPECT_FALSE(g2_stats.amac_build_engaged);
    EXPECT_EQ(g2_stats.table_resizes, 1u);
    expectTableInvariants(g2_stats, less_than_2x_keys, less_than_2x_keys);
    EXPECT_LE(g2_stats.distinct_keys, g2_stats.table_cells / 2);
    probeAndCheck(g2_built, less_than_2x_keys, /*duplicates=*/1, /*misses=*/100);
}

TEST(PartitionedHashJoin, OwnerWaveFillsBufferThenDrainGrows)
{
    constexpr size_t distinct_keys = 131072;
    BuildOptions options;
    options.reserve_safety_for_tests = 0.4;
    options.partition_bits_for_tests = 1;
    options.grow_budget_for_tests = 1;
    options.lift_grow_budget_before_drain = true;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, options);
    const auto stats = built.join->getBuildStats();
    EXPECT_EQ(stats.partitions, 2u);
    EXPECT_EQ(stats.table_resizes, 1u);
    expectTableInvariants(stats, distinct_keys, distinct_keys);
    probeAndCheck(built, distinct_keys, /*duplicates=*/1, /*misses=*/100);
}

TEST(PartitionedHashJoin, OwnerWaveFillsBufferThenDrainGrowsRefused)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    GTEST_SKIP() << "a refused G1 grow raises LOGICAL_ERROR, which aborts instead of throwing in debug and sanitizer builds";
#else
    /// Two partitions, estimate off by more than 2x, owners fill every cell. G1 must fire before the
    /// drain's first walk and throw; a hang here is the v3.2 empty-cell-branch defect.
    constexpr size_t distinct_keys = 131072;
    BuildOptions options;
    options.reserve_safety_for_tests = 0.4;
    options.partition_bits_for_tests = 1;
    options.grow_budget_for_tests = 1;
    alarm(120);
    try
    {
        buildJoin(distinct_keys, /*duplicates=*/1, options);
        alarm(0);
        FAIL() << "a G1 grow refused by budget must throw; a hang means the occupancy check is still inside the empty-cell branch";
    }
    catch (const Exception & e)
    {
        alarm(0);
        EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
        EXPECT_TRUE(e.message().contains("cannot grow") || e.message().contains("distinct keys")) << e.message();
    }
#endif
}

TEST(PartitionedHashJoin, DrainFoldIsDelta)
{
    constexpr size_t distinct_keys = 131072;
    BuildOptions options;
    options.reserve_safety_for_tests = 0.4;
    options.partition_bits_for_tests = 1;
    options.grow_budget_for_tests = 1;
    options.lift_grow_budget_before_drain = true;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, options);
    const auto stats = built.join->getBuildStats();
    UInt64 claimed = 0;
    for (UInt64 c : stats.claimed_per_partition)
        claimed += c;
    EXPECT_EQ(claimed, distinct_keys);
    EXPECT_EQ(stats.distinct_keys, distinct_keys);
    EXPECT_GE(stats.table_resizes, 1u);
    probeAndCheck(built, distinct_keys, /*duplicates=*/1, /*misses=*/100);
}

TEST(PartitionedHashJoin, HeadRowOfChainIsFirstInserted)
{
    /// Enough keys that the plan is partitioned (`bits > 0`) so a picked budget can group and write
    /// `TAG_CHAIN`. Right/Full `RightAny` keep `MapsAll` and emit `firstWord` of each chain.
    constexpr size_t distinct_keys = 100000;
    constexpr size_t duplicates = 8;
    const auto run = [&](JoinKind kind, JoinStrictness strictness)
    {
        auto built = buildDuplicateMajorPickedBudget(distinct_keys, duplicates, kind, strictness);
        const auto stats = built.join->getBuildStats();
        ASSERT_GT(stats.scatter_groups, 1u);
        const auto dup = totalDuplicates(stats);
        ASSERT_GT(dup.headers, 0u) << "a zero budget never writes TAG_CHAIN; the picked budget must group";
        EXPECT_EQ(stats.distinct_keys, distinct_keys);

        JoinedRows actual;
        std::vector<UInt64> keys;
        std::vector<UInt64> ids;
        for (size_t i = 0; i < distinct_keys; ++i)
        {
            keys.push_back(keyOf(i));
            ids.push_back(i);
            if (keys.size() == block_rows || i + 1 == distinct_keys)
            {
                auto result = built.join->joinBlock(twoColumnBlock("k", "probe_id", keys, ids));
                drainResult(*result, actual);
                keys.clear();
                ids.clear();
            }
        }
        ASSERT_TRUE(actual.size() == distinct_keys || actual.size() == distinct_keys * duplicates);
        if (actual.size() == distinct_keys)
        {
            for (const auto & row : actual)
            {
                const UInt64 probe_id = std::get<1>(row);
                const UInt64 build_id = std::get<3>(row);
                EXPECT_EQ(build_id, probe_id * duplicates) << "first-inserted row of key " << keyOf(probe_id);
            }
        }
        else
        {
            std::vector<UInt64> min_build(distinct_keys, std::numeric_limits<UInt64>::max());
            for (const auto & row : actual)
            {
                const UInt64 probe_id = std::get<1>(row);
                const UInt64 build_id = std::get<3>(row);
                ASSERT_LT(probe_id, distinct_keys);
                min_build[probe_id] = std::min(min_build[probe_id], build_id);
            }
            for (size_t i = 0; i < distinct_keys; ++i)
                EXPECT_EQ(min_build[i], i * duplicates) << "first-inserted row of key " << keyOf(i);
        }
    };
    run(JoinKind::Right, JoinStrictness::RightAny);
    run(JoinKind::Full, JoinStrictness::RightAny);
}

TEST(PartitionedHashJoin, FirstGroupOfSkippedRowsOnly)
{
    constexpr size_t distinct_keys = 200000;
    constexpr size_t duplicates = 8;
    constexpr size_t budget = 55u << 20;
    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    Block right_header;
    right_header.insert(
        {ColumnNullable::create(ColumnUInt64::create(), ColumnUInt8::create()),
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
         "rk"});
    right_header.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "build_id"});

    BuiltJoin built;
    built.table_join = makeTableJoin(left_header, right_header);
    built.join = std::make_shared<PartitionedHashJoin>(
        built.table_join,
        std::make_shared<const Block>(right_header),
        /*num_threads_=*/4,
        /*any_take_last_row_=*/false,
        StatsCollectingParams{},
        budget);

    auto push_nullable = [&](const std::vector<UInt64> & keys, const std::vector<UInt64> & ids, bool is_null)
    {
        auto nested = ColumnUInt64::create();
        nested->getData().assign(keys.begin(), keys.end());
        auto null_map = ColumnUInt8::create();
        null_map->getData().assign(keys.size(), is_null ? UInt8(1) : UInt8(0));
        auto id_column = ColumnUInt64::create();
        id_column->getData().assign(ids.begin(), ids.end());
        Block block;
        block.insert(
            {ColumnNullable::create(std::move(nested), std::move(null_map)),
             std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
             "rk"});
        block.insert({std::move(id_column), std::make_shared<DataTypeUInt64>(), "build_id"});
        EXPECT_TRUE(built.join->addBlockToJoin(block, /*check_limits=*/true));
    };

    std::vector<UInt64> keys(block_rows, 0);
    std::vector<UInt64> ids(block_rows, 0);
    push_nullable(keys, ids, /*is_null=*/true);

    keys.clear();
    ids.clear();
    for (size_t d = 0; d < duplicates; ++d)
    {
        for (size_t i = 0; i < distinct_keys; ++i)
        {
            keys.push_back(keyOf(i));
            ids.push_back(i * duplicates + d);
            if (keys.size() == block_rows || i + 1 == distinct_keys)
            {
                push_nullable(keys, ids, /*is_null=*/false);
                keys.clear();
                ids.clear();
            }
        }
    }
    built.join->onBuildPhaseFinish();
    built.post_build_plan = built.join->planPostBuild();
    ASSERT_EQ(built.post_build_plan, PartitionedHashJoin::PostBuildPlan::Grouped);
    built.join->runPostBuildPhase();
    const auto stats = built.join->getBuildStats();
    EXPECT_GT(stats.scatter_groups, 1u);
    expectTableInvariants(stats, distinct_keys, distinct_keys * duplicates);
}

TEST(PartitionedHashJoin, BoundaryProjectionSkippedFirstGroup)
{
    /// A first group of only skipped rows inserts nothing: the G2 projection is the sketch term, not a
    /// division by zero. The nullable-block integration test above is the real case; this pins the helper.
    EXPECT_EQ(PartitionedHashJoin::boundaryProjection(0, 0, 1000, 10.0, 1.2), static_cast<UInt64>(std::ceil(12.0)));
    EXPECT_EQ(
        PartitionedHashJoin::boundaryProjection(50, 100, 200, 80.0, 1.2), std::max(static_cast<UInt64>(std::ceil(96.0)), UInt64{100}));
}

TEST(PartitionedHashJoin, OverflowDuplicatesDoNotForceGrowth)
{
    constexpr size_t distinct_keys = 1000;
    constexpr size_t overflow_duplicates = 100000;
    BuildOptions options;
    options.partition_bits_for_tests = 4;
    options.grow_budget_for_tests = 1;
    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});
    BuiltJoin built;
    built.table_join = makeTableJoin(left_header, right_header);
    built.join = std::make_shared<PartitionedHashJoin>(built.table_join, std::make_shared<const Block>(right_header), 4);
    built.join->setGrowBudgetForTests(1);
    built.join->setPartitionBitsForTests(4);

    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    UInt64 id = 0;
    for (size_t i = 0; i < distinct_keys; ++i)
    {
        keys.push_back(keyOf(i));
        ids.push_back(id++);
        if (keys.size() == block_rows)
        {
            EXPECT_TRUE(built.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
            keys.clear();
            ids.clear();
        }
    }
    for (size_t d = 0; d < overflow_duplicates; ++d)
    {
        keys.push_back(keyOf(0));
        ids.push_back(id++);
        if (keys.size() == block_rows)
        {
            EXPECT_TRUE(built.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
            keys.clear();
            ids.clear();
        }
    }
    if (!keys.empty())
        EXPECT_TRUE(built.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
    built.join->onBuildPhaseFinish();
    built.join->runPostBuildPhase();
    const auto stats = built.join->getBuildStats();
    EXPECT_EQ(stats.table_resizes, 0u);
    EXPECT_EQ(stats.distinct_keys, distinct_keys);
    EXPECT_EQ(stats.inserted_rows, distinct_keys + overflow_duplicates);
}

TEST(PartitionedHashJoin, LoadFactorGrowSkippedUnderBudget)
{
    constexpr size_t distinct_keys = 200000;
    BuildOptions options;
    options.reserve_safety_for_tests = 0.6;
    options.grow_budget_for_tests = 1;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, options);
    const auto stats = built.join->getBuildStats();
    EXPECT_GE(stats.load_factor_grow_skipped, 1u);
    EXPECT_EQ(stats.table_resizes, 0u);
    probeAndCheck(built, distinct_keys, /*duplicates=*/1, /*misses=*/100);
}

TEST(PartitionedHashJoin, GrowRefusedWhenResidentExceedsBudget)
{
    constexpr size_t distinct_keys = 200000;
    BuildOptions options;
    options.reserve_safety_for_tests = 0.6;
    options.grow_budget_for_tests = 1;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, options);
    const auto stats = built.join->getBuildStats();
    EXPECT_GE(stats.load_factor_grow_skipped, 1u);
    EXPECT_EQ(stats.table_resizes, 0u);
}

TEST(PartitionedHashJoin, GroupSizedAfterGrowth)
{
    constexpr size_t distinct_keys = 200000;
    constexpr size_t duplicates = 8;
    /// 55 MiB groups this layout but cannot pay for a 4 MiB G2 doubling on a ~53 MiB resident set.
    /// 80 MiB is already Fits. 62 MiB is still Grouped (peak is between 55 and 80) with headroom for
    /// the doubling R2.10 reserves.
    constexpr size_t budget = 62u << 20;
    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});
    BuiltJoin built;
    built.table_join = makeTableJoin(left_header, right_header);
    built.join = std::make_shared<PartitionedHashJoin>(
        built.table_join,
        std::make_shared<const Block>(right_header),
        /*num_threads_=*/4,
        /*any_take_last_row_=*/false,
        StatsCollectingParams{},
        budget);
    built.join->setReserveSafetyFactorForTests(0.25);
    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    for (size_t d = 0; d < duplicates; ++d)
    {
        for (size_t i = 0; i < distinct_keys; ++i)
        {
            keys.push_back(keyOf(i));
            ids.push_back(i * duplicates + d);
            if (keys.size() == block_rows || i + 1 == distinct_keys)
            {
                EXPECT_TRUE(built.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
                keys.clear();
                ids.clear();
            }
        }
    }
    built.join->onBuildPhaseFinish();
    built.post_build_plan = built.join->planPostBuild();
    ASSERT_EQ(built.post_build_plan, PartitionedHashJoin::PostBuildPlan::Grouped);
    built.join->runPostBuildPhase();
    const auto stats = built.join->getBuildStats();
    ASSERT_GE(stats.scatter_group_ranges.size(), 2u);
    EXPECT_GE(stats.table_resizes, 1u);
    for (size_t i = 1; i < stats.scatter_group_ranges.size(); ++i)
    {
        const auto & g = stats.scatter_group_ranges[i];
        /// R2.10: with headroom, the group stays within budget plus one block. Once arenas and a
        /// quality grow have pushed `residentBytes` past the grouping budget, the sizing loop takes
        /// exactly one block — the baseline overshoot, not a multi-block range.
        EXPECT_TRUE(g.resident_bytes + g.chunk_bytes <= budget + g.one_block_chunk_bytes || g.end == g.begin + 1)
            << "group " << i << " resident " << g.resident_bytes << " chunk " << g.chunk_bytes << " one_block " << g.one_block_chunk_bytes
            << " blocks [" << g.begin << ", " << g.end << ")";
    }
    probeAndCheck(built, distinct_keys, duplicates, /*misses=*/100);
}

TEST(PartitionedHashJoin, PublishedFillAtMostHalf)
{
    constexpr size_t distinct_keys = 50000;
    BuildOptions options;
    options.reserve_safety_for_tests = 0.25;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, options);
    const auto stats = built.join->getBuildStats();
    EXPECT_GE(stats.table_resizes, 1u);
    EXPECT_LE(stats.distinct_keys, stats.table_cells / 2);
}

TEST(PartitionedHashJoin, DegreeCapAtPlan)
{
    using Table = TestSharedTable;
#ifndef DEBUG_OR_SANITIZER_BUILD
    try
    {
        Table table(33, 0);
        FAIL() << "degree 33 must throw before allocating";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
    }
#endif
    const size_t reserve_for_33 = (1uz << 31) + 1;
    EXPECT_GE(Table::degreeFor(reserve_for_33), 33u);

    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});
    auto table_join = makeTableJoin(left_header, right_header);
    auto join = std::make_shared<PartitionedHashJoin>(table_join, std::make_shared<const Block>(right_header), 2);
    join->setReserveOverrideForTests(reserve_for_33);
    EXPECT_TRUE(join->addBlockToJoin(twoColumnBlock("rk", "build_id", {keyOf(0)}, {0}), /*check_limits=*/true));
    try
    {
        join->onBuildPhaseFinish();
        FAIL() << "a reserve that maps to degree 33 must throw LIMIT_EXCEEDED";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LIMIT_EXCEEDED);
    }
}

TEST(PartitionedHashJoin, ZeroKeyChain)
{
    constexpr size_t distinct_keys = 200000;
    constexpr size_t duplicates = 8;
    constexpr size_t budget = 55u << 20;
    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});
    BuiltJoin built;
    built.table_join = makeTableJoin(left_header, right_header);
    built.join = std::make_shared<PartitionedHashJoin>(
        built.table_join,
        std::make_shared<const Block>(right_header),
        /*num_threads_=*/4,
        /*any_take_last_row_=*/false,
        StatsCollectingParams{},
        budget);

    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    UInt64 id = 0;
    for (size_t d = 0; d < duplicates; ++d)
    {
        keys.push_back(0);
        ids.push_back(id++);
        for (size_t i = 0; i < distinct_keys; ++i)
        {
            keys.push_back(keyOf(i));
            ids.push_back(id++);
            if (keys.size() == block_rows || (d + 1 == duplicates && i + 1 == distinct_keys))
            {
                EXPECT_TRUE(built.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
                keys.clear();
                ids.clear();
            }
        }
    }
    if (!keys.empty())
        EXPECT_TRUE(built.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
    built.join->onBuildPhaseFinish();
    built.post_build_plan = built.join->planPostBuild();
    ASSERT_EQ(built.post_build_plan, PartitionedHashJoin::PostBuildPlan::Grouped);
    built.join->runPostBuildPhase();
    const auto stats = built.join->getBuildStats();
    EXPECT_GT(stats.scatter_groups, 1u);
    expectTableInvariants(stats, distinct_keys + 1, distinct_keys * duplicates + duplicates);
}

TEST(PartitionedHashJoin, AmacParityWithChains)
{
    constexpr size_t distinct_keys = 200000;
    constexpr size_t duplicates = 8;
    constexpr size_t budget = 55u << 20;
    const auto build = [&](bool disable_amac)
    {
        const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
        const Block right_header = twoColumnBlock("rk", "build_id", {}, {});
        BuiltJoin built;
        built.table_join = makeTableJoin(left_header, right_header);
        built.join = std::make_shared<PartitionedHashJoin>(
            built.table_join,
            std::make_shared<const Block>(right_header),
            /*num_threads_=*/4,
            /*any_take_last_row_=*/false,
            StatsCollectingParams{},
            budget);
        if (disable_amac)
            built.join->setAmacEnabledForTests(false);
        std::vector<UInt64> keys;
        std::vector<UInt64> ids;
        for (size_t d = 0; d < duplicates; ++d)
        {
            for (size_t i = 0; i < distinct_keys; ++i)
            {
                keys.push_back(keyOf(i));
                ids.push_back(i * duplicates + d);
                if (keys.size() == block_rows || i + 1 == distinct_keys)
                {
                    EXPECT_TRUE(built.join->addBlockToJoin(twoColumnBlock("rk", "build_id", keys, ids), /*check_limits=*/true));
                    keys.clear();
                    ids.clear();
                }
            }
        }
        built.join->onBuildPhaseFinish();
        built.post_build_plan = built.join->planPostBuild();
        built.join->runPostBuildPhase();
        return built;
    };
    auto amac = build(false);
    auto sequential = build(true);
    const auto amac_stats = amac.join->getBuildStats();
    const auto sequential_stats = sequential.join->getBuildStats();
    EXPECT_GT(amac_stats.scatter_groups, 1u);
    EXPECT_GT(sequential_stats.scatter_groups, 1u);
    EXPECT_TRUE(amac_stats.amac_build_engaged);
    EXPECT_FALSE(sequential_stats.amac_build_engaged);
    expectTableInvariants(amac_stats, distinct_keys, distinct_keys * duplicates);
    expectTableInvariants(sequential_stats, distinct_keys, distinct_keys * duplicates);
    probeAndCheck(amac, distinct_keys, duplicates, /*misses=*/100);
    probeAndCheck(sequential, distinct_keys, duplicates, /*misses=*/100);
}

TEST(PartitionedHashJoin, SinglePartitionAsofGrows)
{
    constexpr size_t distinct_keys = 131072;
    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    Block right_header;
    right_header.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "rk"});
    right_header.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "ts"});
    right_header.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "build_id"});

    auto make_asof_join = [&](double safety)
    {
        Settings settings;
        auto table_join = std::make_shared<TableJoin>(settings, JoinAnalyzeMode::None, /*tmp_volume=*/nullptr, /*tmp_data=*/nullptr);
        table_join->setKind(JoinKind::Inner);
        table_join->getTableJoin().strictness = JoinStrictness::Asof;
        table_join->setAsofInequality(ASOFJoinInequality::GreaterOrEquals);
        table_join->addDisjunct();
        table_join->getClauses().back().addKey("k", "rk", /*null_safe_comparison=*/false);
        table_join->getClauses().back().addKey("probe_ts", "ts", /*null_safe_comparison=*/false);
        NamesAndTypesList left_columns{
            {"k", std::make_shared<DataTypeUInt64>()},
            {"probe_id", std::make_shared<DataTypeUInt64>()},
            {"probe_ts", std::make_shared<DataTypeUInt64>()}};
        NamesAndTypesList right_columns{
            {"rk", std::make_shared<DataTypeUInt64>()},
            {"ts", std::make_shared<DataTypeUInt64>()},
            {"build_id", std::make_shared<DataTypeUInt64>()}};
        table_join->setInputColumns(left_columns, right_columns);
        table_join->setUsedColumns({"k", "probe_id", "probe_ts", "rk", "ts", "build_id"});
        auto join = std::make_shared<PartitionedHashJoin>(table_join, std::make_shared<const Block>(right_header), 2);
        join->setReserveSafetyFactorForTests(safety);
        std::vector<UInt64> keys;
        std::vector<UInt64> ts;
        std::vector<UInt64> ids;
        for (size_t i = 0; i < distinct_keys; ++i)
        {
            keys.push_back(keyOf(i));
            ts.push_back(i);
            ids.push_back(i);
            if (keys.size() == block_rows || i + 1 == distinct_keys)
            {
                auto key_column = ColumnUInt64::create();
                auto ts_column = ColumnUInt64::create();
                auto id_column = ColumnUInt64::create();
                key_column->getData().assign(keys.begin(), keys.end());
                ts_column->getData().assign(ts.begin(), ts.end());
                id_column->getData().assign(ids.begin(), ids.end());
                Block block;
                block.insert({std::move(key_column), std::make_shared<DataTypeUInt64>(), "rk"});
                block.insert({std::move(ts_column), std::make_shared<DataTypeUInt64>(), "ts"});
                block.insert({std::move(id_column), std::make_shared<DataTypeUInt64>(), "build_id"});
                EXPECT_TRUE(join->addBlockToJoin(block, /*check_limits=*/true));
                keys.clear();
                ts.clear();
                ids.clear();
            }
        }
        join->onBuildPhaseFinish();
        join->runPostBuildPhase();
        return join;
    };

    auto grown = make_asof_join(0.4);
    const auto stats = grown->getBuildStats();
    EXPECT_EQ(stats.partitions, 1u);
    EXPECT_GE(stats.table_resizes, 1u);
    EXPECT_EQ(stats.distinct_keys, distinct_keys);

    auto control = make_asof_join(1.2);
    EXPECT_EQ(control->getBuildStats().table_resizes, 0u);

    auto probe_one = [&](const std::shared_ptr<PartitionedHashJoin> & join, size_t i)
    {
        auto key_column = ColumnUInt64::create();
        auto id_column = ColumnUInt64::create();
        auto ts_column = ColumnUInt64::create();
        key_column->getData().push_back(keyOf(i));
        id_column->getData().push_back(i);
        ts_column->getData().push_back(i);
        Block probe;
        probe.insert({std::move(key_column), std::make_shared<DataTypeUInt64>(), "k"});
        probe.insert({std::move(id_column), std::make_shared<DataTypeUInt64>(), "probe_id"});
        probe.insert({std::move(ts_column), std::make_shared<DataTypeUInt64>(), "probe_ts"});
        auto result = join->joinBlock(std::move(probe));
        JoinedRows rows;
        /// ASOF output uses different column names; just count rows.
        size_t n = 0;
        while (true)
        {
            auto r = result->next();
            n += r.block.rows();
            if (r.is_last)
                break;
        }
        return n;
    };
    EXPECT_EQ(probe_one(grown, 0), probe_one(control, 0));
    EXPECT_EQ(probe_one(grown, distinct_keys / 2), probe_one(control, distinct_keys / 2));
}

TEST(PartitionedHashJoin, DrainCreatedKeyAppendedByLaterGroup)
{
    /// Unique padding does not create spans, so drain stats are only A and B. A and B hash into the
    /// last range's full window, so every row of them overflows. Group 1 drain-creates A with three
    /// rows and B with one; a later group appends more. W6: A gets one header; B becomes a `TAG_RUN`.
    using Table = typename decltype(SharedMapsAll::key64)::element_type;
    constexpr size_t bits = 2;
    constexpr size_t size_degree = 19;
    constexpr size_t padding_keys = 200000;
    constexpr size_t budget = 8u << 20;
    constexpr size_t window = 64;
    constexpr size_t fillers = window + 192;

    Table geometry(size_degree, bits);
    const size_t range_end = geometry.rangeEnd(geometry.partitions() - 1);
    const size_t window_begin = range_end - window;
    std::vector<UInt64> window_keys;
    for (UInt64 candidate = 2; window_keys.size() < fillers + 2; candidate += 2654435761ULL)
    {
        const size_t home = geometry.place(geometry.hash(candidate));
        if (home >= window_begin && home < range_end)
            window_keys.push_back(candidate);
    }
    const UInt64 key_a = window_keys[fillers];
    const UInt64 key_b = window_keys[fillers + 1];

    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});
    BuiltJoin built;
    built.table_join = makeTableJoin(left_header, right_header);
    built.join = std::make_shared<PartitionedHashJoin>(
        built.table_join, std::make_shared<const Block>(right_header), /*num_threads_=*/4, false, StatsCollectingParams{}, budget);
    built.join->setPartitionBitsForTests(bits);
    /// `reserve` is a 50% fill target, so `2^(degree-1)` keys produce a table of `2^degree` cells.
    built.join->setReserveOverrideForTests(1uz << (size_degree - 1));
    built.join->setGrowBudgetForTests(1);

    UInt64 build_id = 0;
    auto add_block = [&](const std::vector<UInt64> & block_keys)
    {
        std::vector<UInt64> ids;
        ids.reserve(block_keys.size());
        for (size_t i = 0; i < block_keys.size(); ++i)
            ids.push_back(build_id++);
        EXPECT_TRUE(built.join->addBlockToJoin(twoColumnBlock("rk", "build_id", block_keys, ids), /*check_limits=*/true));
    };

    add_block(std::vector<UInt64>(window_keys.begin(), window_keys.begin() + fillers));
    add_block({key_a, key_a, key_a, key_b});
    std::vector<UInt64> keys;
    for (size_t i = 0; i < padding_keys; ++i)
    {
        const UInt64 key = keyOf(i);
        const size_t home = geometry.place(geometry.hash(key));
        if (home >= window_begin && home < range_end)
            continue;
        keys.push_back(key);
        if (keys.size() == block_rows)
        {
            add_block(keys);
            keys.clear();
        }
    }
    if (!keys.empty())
        add_block(keys);
    add_block({key_a, key_a, key_b, key_b});

    built.join->onBuildPhaseFinish();
    built.post_build_plan = built.join->planPostBuild();
    ASSERT_NE(built.post_build_plan, PartitionedHashJoin::PostBuildPlan::Fits)
        << "test setup: the budget must split the unique padding across groups";
    built.join->runPostBuildPhase();

    const auto stats = built.join->getBuildStats();
    ASSERT_GT(stats.scatter_groups, 1u);
    EXPECT_EQ(stats.table_resizes, 0u) << "a grow would move later A/B rows onto the owner path";
    EXPECT_EQ(stats.drain_duplicates.headers, 1u) << "only A already had a range when the later group ran";
    EXPECT_EQ(stats.drain_duplicates.ranges, 3u) << "A's headerless run plus its later range, and B's run";
    EXPECT_EQ(stats.drain_duplicates.arena_bytes, 8u * (5 + 3) + 16u);
    EXPECT_EQ(stats.owner_duplicates.arena_bytes, 0u);

    auto probe_count = [&](UInt64 key)
    {
        JoinedRows rows;
        auto result = built.join->joinBlock(twoColumnBlock("k", "probe_id", {key}, {0}));
        drainResult(*result, rows);
        return rows.size();
    };
    EXPECT_EQ(probe_count(key_a), 5u);
    EXPECT_EQ(probe_count(key_b), 3u);
}

TEST(PartitionedHashJoin, RightJoinNonJoinedOverChains)
{
    constexpr size_t distinct_keys = 200000;
    constexpr size_t duplicates = 8;
    constexpr size_t probed_keys = distinct_keys / 2;
    auto built = buildDuplicateMajorPickedBudget(distinct_keys, duplicates, JoinKind::Right);
    const auto stats = built.join->getBuildStats();
    ASSERT_GT(stats.scatter_groups, 1u);
    EXPECT_GT(totalDuplicates(stats).headers, 0u);
    expectTableInvariants(stats, distinct_keys, distinct_keys * duplicates);

    JoinedRows expected;
    expected.reserve(probed_keys * duplicates);
    for (size_t i = 0; i < probed_keys; ++i)
    {
        const UInt64 key = keyOf(i);
        for (size_t d = 0; d < duplicates; ++d)
            expected.emplace_back(key, i, key, i * duplicates + d);
    }
    std::sort(expected.begin(), expected.end());

    JoinedRows actual;
    actual.reserve(expected.size());
    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    for (size_t i = 0; i < probed_keys; ++i)
    {
        keys.push_back(keyOf(i));
        ids.push_back(i);
        if (keys.size() == block_rows || i + 1 == probed_keys)
        {
            auto result = built.join->joinBlock(twoColumnBlock("k", "probe_id", keys, ids));
            drainResult(*result, actual);
            keys.clear();
            ids.clear();
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
    expected_non_joined.reserve((distinct_keys - probed_keys) * duplicates);
    for (size_t i = probed_keys; i < distinct_keys; ++i)
        for (size_t d = 0; d < duplicates; ++d)
            expected_non_joined.emplace_back(keyOf(i), i * duplicates + d);
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

TEST(PartitionedHashJoin, FullJoinOverChains)
{
    constexpr size_t distinct_keys = 200000;
    constexpr size_t duplicates = 8;
    constexpr size_t probed_keys = distinct_keys / 2;
    auto built = buildDuplicateMajorPickedBudget(distinct_keys, duplicates, JoinKind::Full);
    const auto stats = built.join->getBuildStats();
    ASSERT_GT(stats.scatter_groups, 1u);
    EXPECT_GT(totalDuplicates(stats).headers, 0u);
    expectTableInvariants(stats, distinct_keys, distinct_keys * duplicates);

    JoinedRows expected_matches;
    expected_matches.reserve(probed_keys * duplicates);
    for (size_t i = 0; i < probed_keys; ++i)
    {
        const UInt64 key = keyOf(i);
        for (size_t d = 0; d < duplicates; ++d)
            expected_matches.emplace_back(key, i, key, i * duplicates + d);
    }
    std::sort(expected_matches.begin(), expected_matches.end());

    JoinedRows actual;
    std::vector<UInt64> keys;
    std::vector<UInt64> ids;
    for (size_t i = 0; i < probed_keys; ++i)
    {
        keys.push_back(keyOf(i));
        ids.push_back(i);
        if (keys.size() == block_rows || i + 1 == probed_keys)
        {
            auto result = built.join->joinBlock(twoColumnBlock("k", "probe_id", keys, ids));
            drainResult(*result, actual);
            keys.clear();
            ids.clear();
        }
    }
    JoinedRows actual_matches;
    actual_matches.reserve(actual.size());
    for (const auto & row : actual)
        if (std::get<2>(row) != 0 || std::get<3>(row) != 0)
            actual_matches.push_back(row);
    std::sort(actual_matches.begin(), actual_matches.end());
    ASSERT_EQ(actual_matches.size(), expected_matches.size());
    ASSERT_TRUE(actual_matches == expected_matches);

    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    Block result_sample = left_header.cloneEmpty();
    result_sample.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "build_id"});
    result_sample.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "rk"});

    std::vector<std::pair<UInt64, UInt64>> expected_non_joined;
    expected_non_joined.reserve((distinct_keys - probed_keys) * duplicates);
    for (size_t i = probed_keys; i < distinct_keys; ++i)
        for (size_t d = 0; d < duplicates; ++d)
            expected_non_joined.emplace_back(keyOf(i), i * duplicates + d);
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
