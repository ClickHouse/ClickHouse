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
#include <Interpreters/PartitionedHashJoin/SharedJoinTable.h>
#include <Interpreters/TableJoin.h>
#include <Common/assert_cast.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
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
    PartitionedHashJoin::PostBuildPlan post_build_plan = PartitionedHashJoin::PostBuildPlan::Fits;
};

struct BuildOptions
{
    size_t num_threads = 4;
    double reserve_safety_for_tests = 0;
    size_t build_block_rows = block_rows;
    JoinKind kind = JoinKind::Inner;
    bool disable_amac = false;
    size_t max_fanout_per_pass_for_tests = 0;
    std::optional<size_t> partition_bits_for_tests;
    bool cap_partitions_by_l1_descriptors = true;
    size_t l1_cache_bytes_for_tests = 0;
    const StatsCollectingParams * stats_collecting_params = nullptr;
    /// The post-build memory gate: zero disables grouping.
    size_t max_bytes_before_external_join = 0;
};

UInt64 keyOf(size_t i)
{
    return i * 2654435761ULL + 1;
}

/// Feeds the build through the real `IJoin` interface - fill, barrier, post-build. Blocks above
/// 65536 rows take the wide 8-byte locator encoding, smaller ones the packed 4-byte form.
BuiltJoin buildJoin(size_t distinct_keys, size_t duplicates, const BuildOptions & options)
{
    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});

    BuiltJoin result;
    result.table_join = makeTableJoin(left_header, right_header, options.kind);
    result.join = std::make_shared<PartitionedHashJoin>(
        result.table_join,
        std::make_shared<const Block>(right_header),
        options.num_threads,
        /*any_take_last_row_=*/false,
        options.stats_collecting_params ? *options.stats_collecting_params : StatsCollectingParams{},
        options.max_bytes_before_external_join);
    if (options.reserve_safety_for_tests > 0)
        result.join->setReserveSafetyFactorForTests(options.reserve_safety_for_tests);
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
    EXPECT_GE(stats.table_cells, 2 * distinct_keys) << "the table must keep at most 50% fill";
    EXPECT_EQ(stats.inserted_rows, rows);
    EXPECT_EQ(stats.distinct_keys, distinct_keys);
    EXPECT_TRUE(stats.predictions_exact) << "the created buffer must match the plan's prediction";
    EXPECT_EQ(stats.partitions, 1uz << stats.bits);
    EXPECT_LE(stats.bits, stats.table_size_degree);
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
    /// Every key has four rows in one pass: one exact run of 32 bytes, nothing else.
    const auto & dup = stats.owner_duplicates;
    EXPECT_EQ(dup.runs + stats.drain_duplicates.runs, distinct_keys);
    EXPECT_EQ(dup.descriptors + stats.drain_duplicates.descriptors, 0u);
    EXPECT_EQ(dup.small_blocks + stats.drain_duplicates.small_blocks, 0u);
    EXPECT_EQ(dup.arena_bytes + stats.drain_duplicates.arena_bytes, distinct_keys * duplicates * sizeof(UInt64));

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
    EXPECT_EQ(stats.owner_duplicates.pairs, distinct_keys);

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

TEST(PartitionedHashJoin, UndersizedTableGuardThrows)
{
    /// A crippled safety factor sizes the table far below the key count. The table never grows, so
    /// the build must fail the capacity guard with an exception - and never hang in a walk that cannot
    /// find an empty cell.
#ifdef DEBUG_OR_SANITIZER_BUILD
    GTEST_SKIP() << "the capacity guard raises LOGICAL_ERROR, which aborts instead of throwing in debug and sanitizer builds";
#else
    constexpr size_t distinct_keys = 200000;
    const auto expect_capacity_guard = [](auto && build)
    {
        try
        {
            build();
            FAIL() << "the undersized table must trip the capacity guard";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
            EXPECT_TRUE(e.message().contains("distinct keys")) << e.message();
        }
    };
    BuildOptions options;
    options.reserve_safety_for_tests = 0.001;
    expect_capacity_guard([&] { buildJoin(distinct_keys, /*duplicates=*/1, options); });

    /// The single-partition path guards per claim.
    BuildOptions single;
    single.reserve_safety_for_tests = 0.001;
    single.partition_bits_for_tests = 0;
    expect_capacity_guard([&] { buildJoin(distinct_keys, /*duplicates=*/1, single); });
#endif
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
    /// 2048 partitions really executed: the plan is forced above the 1024-per-pass ceiling, so the
    /// scatter runs two MSB-first passes (6 + 5 bits), and every one of the 2048 owner ranges is filled by
    /// the wave. One table, exact row conservation, exact results.
    constexpr size_t distinct_keys = 1000000;
    BuildOptions options;
    options.partition_bits_for_tests = 11;
    auto built = buildJoin(distinct_keys, /*duplicates=*/1, options);

    const auto stats = built.join->getBuildStats();
    EXPECT_EQ(stats.bits, 11u);
    EXPECT_EQ(stats.partitions, 2048u);
    ASSERT_EQ(stats.pass_bits.size(), 2u) << "2048 partitions must take two scatter passes under the default ceiling";
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
    EXPECT_EQ(stats.owner_duplicates.pairs + stats.drain_duplicates.pairs, distinct_keys) << "every key is a pair";

    probeAndCheck(built, distinct_keys, duplicates, /*misses=*/10000);
}

namespace
{

/// A build whose special keys all have their home cell in the last `window` cells of the LAST partition
/// range: `fillers` unique keys arrive first and fill that window, then duplicate sets of 2, 3 and 9 rows
/// of three more such keys arrive in later blocks. Their owner walks must reach the range end, hand the
/// rows to the overflow, and the drain must place them past the buffer end - wrapping into partition 0 -
/// and build their pair and runs there. The probe then has to wrap the same way to find them.
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
    EXPECT_EQ(stats.drain_duplicates.pairs, 1u);
    EXPECT_EQ(stats.drain_duplicates.runs, 2u);
    EXPECT_EQ(stats.drain_duplicates.descriptors, 0u);
    EXPECT_EQ(stats.drain_duplicates.arena_bytes, 16u + 24u + 72u);
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

TEST(PartitionedHashJoin, GroupedScatterRunLists)
{
    /// A memory budget between the grouped floor and the ungrouped peak makes the post-build scatter run in
    /// several block ranges; a key's eight duplicates are spread one per block so they arrive in different
    /// groups, and the later groups must append to the runs the earlier groups finished: in place while a
    /// small block has room, as new nodes behind a descriptor afterwards.
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

    const auto & dup = stats.owner_duplicates;
    EXPECT_GT(dup.small_blocks, 0u) << "grouped contributions below 8 rows take 64-byte blocks";
    EXPECT_GT(dup.in_place_fills, 0u) << "later groups fill the free slots of earlier blocks";
    EXPECT_EQ(dup.moved_refs, 0u) << "no key needs more than one block: 8 rows fit the 8 slots";
    EXPECT_EQ(dup.descriptors, 0u);
    /// Every key ends in exactly one 64-byte block. A key whose first group brought two rows passed
    /// through a pair first; that pair went back to the free list, but its 16 bytes stay counted in the
    /// arena (the arena never shrinks), so the live bytes sit between one block per key and one block
    /// plus one parked pair per key. Referenced pairs are summed over all writers (a writer may free a
    /// pair another writer allocated, so its own count can be negative).
    const Int64 arena = dup.arena_bytes + stats.drain_duplicates.arena_bytes;
    const Int64 referenced_pairs = dup.pairs + stats.drain_duplicates.pairs;
    const Int64 live = arena - 16 * referenced_pairs;
    EXPECT_GE(live, static_cast<Int64>(64 * distinct_keys));
    EXPECT_LE(live, static_cast<Int64>(80 * distinct_keys));

    probeAndCheck(built, distinct_keys, duplicates, /*misses=*/1000);
}

TEST(PartitionedHashJoin, GroupedRunListsChainAcrossGroups)
{
    /// Keys with more rows than one 64-byte block holds, spread one row per block over the whole build, so
    /// every scatter group boundary splits every key: the first group's contribution is an exact run, each
    /// later group's contribution chains a node behind a descriptor, and the probe must walk the lists in
    /// insertion order across the nodes.
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

    /// A row per block per key and at least one group boundary: every key spans groups, so every key
    /// ends as a run list with at least one appended node.
    const Int64 descriptors = stats.owner_duplicates.descriptors + stats.drain_duplicates.descriptors;
    const Int64 appended = stats.owner_duplicates.appended_nodes + stats.drain_duplicates.appended_nodes;
    EXPECT_EQ(descriptors, static_cast<Int64>(distinct_keys)) << "every key chains exactly once";
    EXPECT_GE(appended, descriptors) << "each descriptor has at least one appended node";

    probeAndCheck(built, distinct_keys, duplicates, /*misses=*/1000);
}

TEST(PartitionedHashJoin, SaturatedRunCount)
{
    /// A key with more rows than the word's 15-bit count can hold: the run takes a descriptor from the
    /// start and `rows` reads the exact total there. The boundary values are checked on both sides.
    for (const size_t rows : {static_cast<size_t>(RowRefList::COUNT_SAT) - 1, static_cast<size_t>(RowRefList::COUNT_SAT), 40000uz})
    {
        auto built = buildJoin(/*distinct_keys=*/1, rows, /*num_threads=*/2);
        const auto stats = built.join->getBuildStats();
        expectTableInvariants(stats, 1, rows);
        EXPECT_EQ(stats.owner_duplicates.runs, 1u);
        EXPECT_EQ(stats.owner_duplicates.descriptors, rows >= RowRefList::COUNT_SAT ? 1u : 0u) << "rows " << rows;
        EXPECT_EQ(stats.owner_duplicates.arena_bytes, 8u * rows + (rows >= RowRefList::COUNT_SAT ? 24u : 0u)) << "rows " << rows;
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
    EXPECT_EQ(stats.owner_duplicates.runs + stats.drain_duplicates.runs, 1u) << "the zero key is the only duplicated one";

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
