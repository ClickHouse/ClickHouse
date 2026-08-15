#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <set>
#include <tuple>
#include <vector>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsScatter.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/RadixHashJoin/RadixHashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Common/assert_cast.h>

namespace DB::Setting
{
extern const SettingsUInt64 max_joined_block_size_rows;
}

using namespace DB;

namespace
{

/// One joined output row: (k, probe_id, rk, build_id). The multiset of these tuples over the whole
/// drain is an exact identity: any dropped, duplicated, or cross-wired row changes it.
using JoinedRow = std::tuple<UInt64, UInt64, UInt64, UInt64>;
using JoinedRows = std::multiset<JoinedRow>;

Block twoColumnBlock(const String & key_name, const String & id_name, const std::vector<UInt64> & keys, const std::vector<UInt64> & ids)
{
    auto key_column = ColumnUInt64::create();
    auto id_column = ColumnUInt64::create();
    for (size_t i = 0; i < keys.size(); ++i)
    {
        key_column->insertValue(keys[i]);
        id_column->insertValue(ids[i]);
    }
    Block block;
    block.insert({std::move(key_column), std::make_shared<DataTypeUInt64>(), key_name});
    block.insert({std::move(id_column), std::make_shared<DataTypeUInt64>(), id_name});
    return block;
}

/// Partition of a single-UInt64-key row for a 2-partition single-pass radix plan: computePassBits(2, ...)
/// yields one pass of 1 bit, consumed MSB-first, so partition = (routeWord(key) >> (32 - 1)) & 1.
/// If the routing ever changes, the two selected keys may collapse into one partition; that weakens
/// the two-worker shape but not the liveness property under test.
UInt32 partitionForKey(UInt64 key)
{
    return (ColumnsScatter::routeWord(key) >> 31) & 1;
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
        rows.emplace(k[i], probe_id[i], rk[i], build_id[i]);
}

/// Drains a result to completion, collecting every output row.
size_t drainResult(IJoinResult & result, JoinedRows & rows)
{
    size_t drained = 0;
    while (true)
    {
        auto r = result.next();
        drained += r.block.rows();
        accumulateRows(r.block, rows);
        if (r.is_last)
            return drained;
    }
}

std::shared_ptr<TableJoin> makeTableJoin(const Block & left_header, const Block & right_header)
{
    /// Constructed from query Settings like a real query, with one override: max_joined_block_size_rows = 1
    /// makes every leaf probe emit one output block per probe row (each probe row has more matches than
    /// the cap), so a wave produces enough blocks to overfill its bounded output queue.
    Settings settings;
    settings[Setting::max_joined_block_size_rows] = 1;
    auto table_join = std::make_shared<TableJoin>(settings, /*tmp_volume*/ nullptr, /*tmp_data*/ nullptr);
    table_join->setKind(JoinKind::Inner);
    table_join->getTableJoin().strictness = JoinStrictness::All;
    table_join->addDisjunct();
    table_join->getClauses().back().addKey(
        left_header.getByPosition(0).name, right_header.getByPosition(0).name, /*null_safe_comparison*/ false);

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

/// What the asynchronous lane-B call produced. On the abandoned (old-code) path the result is drained
/// and destroyed inside the lane-B thread, because there it owns a lock acquired on that thread.
struct LaneBOutcome
{
    JoinResultPtr result;
    Block first_block;
    bool first_is_last = false;
    size_t abandoned_rows = 0;
    JoinedRows abandoned_tuples;
};

}

/// Two probe lanes fill their windows back to back. Lane A triggers a wave and pops one block, leaving
/// its bounded output queue full and the wave's producers parked (8 output blocks per wave vs queue
/// capacity 2 * threads + 1 = 5). A concurrent joinBlock + next quantum on lane B must return control
/// promptly instead of waiting for lane A's wave to finish: parking every executor lane on the wave
/// admission while only the admitted result's owner can drain the queue is the deadlock under test.
TEST(RadixHashJoin, ConcurrentJoiningQuantumDoesNotWaitForPreviousWave)
{
    /// Two keys deliberately routed to different radix partitions, so both wave workers probe.
    UInt64 key_for_partition[2] = {0, 0};
    bool found[2] = {false, false};
    for (UInt64 v = 1; !(found[0] && found[1]); ++v)
    {
        const UInt32 p = partitionForKey(v);
        if (!found[p])
        {
            key_for_partition[p] = v;
            found[p] = true;
        }
    }
    const UInt64 k0 = key_for_partition[0];
    const UInt64 k1 = key_for_partition[1];

    /// Build side: each key duplicated 4 times (duplicates keep INNER ALL from promoting to RightAny),
    /// so every probe row joins to 4 output rows.
    const std::vector<UInt64> build_keys{k0, k0, k0, k0, k1, k1, k1, k1};
    const std::vector<UInt64> build_ids{100, 101, 102, 103, 104, 105, 106, 107};

    /// Probe lanes: 8 rows each (4 per key), distinct probe ids across lanes.
    const std::vector<UInt64> probe_keys{k0, k0, k0, k0, k1, k1, k1, k1};
    const std::vector<UInt64> probe_ids_a{0, 1, 2, 3, 4, 5, 6, 7};
    const std::vector<UInt64> probe_ids_b{8, 9, 10, 11, 12, 13, 14, 15};

    /// The exact expected output: every probe row of both lanes joined with every matching build row.
    JoinedRows expected;
    for (const auto * probe_ids : {&probe_ids_a, &probe_ids_b})
        for (size_t i = 0; i < probe_keys.size(); ++i)
            for (size_t j = 0; j < build_keys.size(); ++j)
                if (probe_keys[i] == build_keys[j])
                    expected.emplace(probe_keys[i], (*probe_ids)[i], build_keys[j], build_ids[j]);
    ASSERT_EQ(expected.size(), 64u);

    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});

    auto table_join = makeTableJoin(left_header, right_header);
    auto join = std::make_shared<RadixHashJoin>(
        table_join,
        std::make_shared<const Block>(right_header),
        /*max_threads*/ 2,
        /*rhs_size_estimation*/ std::nullopt,
        /*max_partitions_per_pass*/ 8,
        /*size_tables_by_distinct_estimate*/ false,
        /*probe_buffer_fraction*/ 0.0,
        /*probe_buffer_min_bytes*/ 1,
        /*probe_buffer_max_bytes*/ 1,
        StatsCollectingParams{});

    ASSERT_TRUE(join->addBlockToJoin(twoColumnBlock("rk", "build_id", build_keys, build_ids), /*check_limits*/ false));
    join->onBuildPhaseFinish();
    join->runPostBuildPhase();

    /// Lane A: the 1-byte window budget turns this single block into a full wave. One next() pop leaves
    /// 7 of its 8 output blocks undrained against a 5-slot queue: the wave is now mid-flight with parked
    /// producers, exactly the state every other lane must be able to pass through.
    auto result_a = join->joinBlock(twoColumnBlock("k", "probe_id", probe_keys, probe_ids_a), 0);
    ASSERT_NE(result_a, nullptr);
    JoinedRows drained_a;
    size_t rows_a = 0;
    {
        auto first = result_a->next();
        ASSERT_FALSE(first.is_last);
        rows_a += first.block.rows();
        accumulateRows(first.block, drained_a);
    }

    /// Lane B: one full JoiningTransform-style work quantum (joinBlock + immediate next) on another
    /// thread. It must give control back within the deadline whatever the state of lane A's wave.
    std::atomic<bool> abandoned{false};
    auto lane_b = std::async(
        std::launch::async,
        [&]() -> LaneBOutcome
        {
            LaneBOutcome outcome;
            auto result_b = join->joinBlock(twoColumnBlock("k", "probe_id", probe_keys, probe_ids_b), 1);
            auto r = result_b->next();
            if (abandoned.load())
            {
                /// Old-code path: this thread acquired the wave admission, so finish the result here.
                outcome.abandoned_rows += r.block.rows();
                accumulateRows(r.block, outcome.abandoned_tuples);
                if (!r.is_last)
                    outcome.abandoned_rows += drainResult(*result_b, outcome.abandoned_tuples);
                return outcome;
            }
            outcome.result = std::move(result_b);
            outcome.first_block = std::move(r.block);
            outcome.first_is_last = r.is_last;
            return outcome;
        });

    const bool lane_b_returned = lane_b.wait_for(std::chrono::seconds(10)) == std::future_status::ready;
    EXPECT_TRUE(lane_b_returned) << "lane B's joinBlock+next quantum did not return while lane A's wave was mid-flight";

    if (!lane_b_returned)
    {
        /// Controlled failure on the old code: release lane A's wave so lane B can finish, and let it
        /// drain its own result on its own thread. The test fails via the EXPECT above without hanging.
        abandoned.store(true);
        result_a.reset();
        auto outcome = lane_b.get();
        if (outcome.result)
        {
            /// Lane B finished its quantum in the gap between the deadline and the abandon flag.
            JoinedRows ignored;
            if (!outcome.first_is_last)
                drainResult(*outcome.result, ignored);
        }
        return;
    }

    /// Fixed code: drain both results concurrently and check the exact output identity.
    auto outcome = lane_b.get();
    ASSERT_NE(outcome.result, nullptr);

    auto drain_a = std::async(
        std::launch::async,
        [&]() -> size_t
        {
            return drainResult(*result_a, drained_a);
        });

    JoinedRows drained_b;
    size_t rows_b = outcome.first_block.rows();
    accumulateRows(outcome.first_block, drained_b);
    if (!outcome.first_is_last)
        rows_b += drainResult(*outcome.result, drained_b);

    rows_a += drain_a.get();
    result_a.reset();
    outcome.result.reset();

    /// Lanes drain the shared waves cooperatively, so the per-lane split is arbitrary; only the
    /// total and the exact multiset are invariant.
    EXPECT_EQ(rows_a + rows_b, 64u);

    JoinedRows drained_all = drained_a;
    drained_all.insert(drained_b.begin(), drained_b.end());
    EXPECT_EQ(drained_all.size(), 64u);
    EXPECT_TRUE(drained_all == expected) << "joined output multiset does not match the expected probe x build identity";
}

/// Cooperative-participation contract (WaveJoinProbe.tla, `ParticipationLive`): while one lane's
/// sealed wave is mid-drain with claimable leaf work, another lane's joinBlock + next() quantum must
/// CLAIM part of that drain and emit a nonempty output block of the sealed wave — not merely return
/// control with an empty result. Lane A seals a wave whose probe output is far larger than one
/// block and stalls after a single next(); lane B then joins with a block too small to trigger
/// anything on its own. A design with dedicated wave producers and a buffered window hands lane B
/// an empty result (its rows wait for a later window) while the sealed wave still holds unclaimed
/// leaves; the cooperative contract requires lane B's first quantum to yield sealed-wave output.
TEST(RadixHashJoin, SealedWaveDrainIsClaimableByOtherLanes)
{
    UInt64 key_for_partition[2] = {0, 0};
    bool found[2] = {false, false};
    for (UInt64 v = 1; !(found[0] && found[1]); ++v)
    {
        const UInt32 p = partitionForKey(v);
        if (!found[p])
        {
            key_for_partition[p] = v;
            found[p] = true;
        }
    }
    const UInt64 k0 = key_for_partition[0];
    const UInt64 k1 = key_for_partition[1];

    /// Build side: each key duplicated 4 times, so every probe row joins to 4 output rows.
    const std::vector<UInt64> build_keys{k0, k0, k0, k0, k1, k1, k1, k1};
    const std::vector<UInt64> build_ids{100, 101, 102, 103, 104, 105, 106, 107};

    /// Lane A: a large block (both partitions populated) that crosses the budget by itself.
    /// Lane B: a tiny block that stays below the budget on its own.
    std::vector<UInt64> probe_keys_a(4096);
    std::vector<UInt64> probe_ids_a(4096);
    for (size_t i = 0; i < probe_keys_a.size(); ++i)
    {
        probe_keys_a[i] = (i % 2 == 0) ? k0 : k1;
        probe_ids_a[i] = i;
    }
    const std::vector<UInt64> probe_keys_b{k0, k0, k0, k0, k1, k1, k1, k1};
    const std::vector<UInt64> probe_ids_b{5000, 5001, 5002, 5003, 5004, 5005, 5006, 5007};

    Block probe_a = twoColumnBlock("k", "probe_id", probe_keys_a, probe_ids_a);
    Block probe_b = twoColumnBlock("k", "probe_id", probe_keys_b, probe_ids_b);

    /// Budget between the two block sizes, measured (not guessed): B alone stays below it, A alone
    /// crosses it, whatever the columns' allocation granularity is.
    const size_t budget = 2 * probe_b.allocatedBytes();
    ASSERT_LT(probe_b.allocatedBytes(), budget);
    ASSERT_GE(probe_a.allocatedBytes(), 2 * budget);

    JoinedRows expected;
    auto add_expected = [&](const std::vector<UInt64> & keys, const std::vector<UInt64> & ids)
    {
        for (size_t i = 0; i < keys.size(); ++i)
            for (size_t j = 0; j < build_keys.size(); ++j)
                if (keys[i] == build_keys[j])
                    expected.emplace(keys[i], ids[i], build_keys[j], build_ids[j]);
    };
    add_expected(probe_keys_a, probe_ids_a);
    add_expected(probe_keys_b, probe_ids_b);
    ASSERT_EQ(expected.size(), 4096u * 4 + 8u * 4);

    const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
    const Block right_header = twoColumnBlock("rk", "build_id", {}, {});

    auto table_join = makeTableJoin(left_header, right_header);
    auto join = std::make_shared<RadixHashJoin>(
        table_join,
        std::make_shared<const Block>(right_header),
        /*max_threads*/ 2,
        /*rhs_size_estimation*/ std::nullopt,
        /*max_partitions_per_pass*/ 8,
        /*size_tables_by_distinct_estimate*/ false,
        /*probe_buffer_fraction*/ 0.0,
        /*probe_buffer_min_bytes*/ budget,
        /*probe_buffer_max_bytes*/ budget,
        StatsCollectingParams{});

    ASSERT_TRUE(join->addBlockToJoin(twoColumnBlock("rk", "build_id", build_keys, build_ids), /*check_limits*/ false));
    join->onBuildPhaseFinish();
    join->runPostBuildPhase();

    /// Lane A seals the wave and pops exactly one block, then stalls: the wave is mid-drain, with
    /// (with max_joined_block_size_rows = 1) thousands of output blocks still unproduced and at
    /// least one whole leaf unclaimed.
    auto result_a = join->joinBlock(std::move(probe_a), 0);
    ASSERT_NE(result_a, nullptr);
    JoinedRows drained_a;
    size_t rows_a = 0;
    {
        auto first = result_a->next();
        ASSERT_FALSE(first.is_last);
        ASSERT_GT(first.block.rows(), 0u);
        rows_a += first.block.rows();
        accumulateRows(first.block, drained_a);
    }

    /// Lane B: one joinBlock + next() quantum on another thread while lane A is stalled.
    struct LaneBFirstQuantum
    {
        JoinResultPtr result;
        Block first_block;
        bool first_is_last = false;
    };
    auto lane_b = std::async(
        std::launch::async,
        [&]() -> LaneBFirstQuantum
        {
            LaneBFirstQuantum outcome;
            outcome.result = join->joinBlock(std::move(probe_b), 1);
            auto r = outcome.result->next();
            outcome.first_block = std::move(r.block);
            outcome.first_is_last = r.is_last;
            return outcome;
        });

    const bool lane_b_returned = lane_b.wait_for(std::chrono::seconds(10)) == std::future_status::ready;
    EXPECT_TRUE(lane_b_returned) << "lane B's joinBlock+next quantum did not return while lane A's sealed wave was mid-drain";
    if (!lane_b_returned)
    {
        /// Unwedge without asserting anything further: complete the wave on lane A and let B finish.
        JoinedRows ignored;
        drainResult(*result_a, ignored);
        result_a.reset();
        auto outcome = lane_b.get();
        if (outcome.result && !outcome.first_is_last)
            drainResult(*outcome.result, ignored);
        return;
    }

    auto outcome = lane_b.get();
    ASSERT_NE(outcome.result, nullptr);

    /// THE contract assertion: lane B's first quantum must have claimed sealed-wave drain work.
    EXPECT_GT(outcome.first_block.rows(), 0u)
        << "lane B's first quantum produced no drain output while lane A's sealed wave held claimable leaves";

    if (outcome.first_block.rows() == 0)
    {
        /// Non-cooperative design: finish cleanly (the failure is already recorded above).
        JoinedRows ignored;
        if (!outcome.first_is_last)
            drainResult(*outcome.result, ignored);
        outcome.result.reset();
        drainResult(*result_a, ignored);
        result_a.reset();
        return;
    }

    /// Cooperative design: drain both lanes concurrently, flush the leftover input through the
    /// delayed-blocks stream, and check the exact output multiset.
    JoinedRows drained_b;
    size_t rows_b = outcome.first_block.rows();
    accumulateRows(outcome.first_block, drained_b);

    auto drain_a = std::async(std::launch::async, [&]() -> size_t { return drainResult(*result_a, drained_a); });
    if (!outcome.first_is_last)
        rows_b += drainResult(*outcome.result, drained_b);
    rows_a += drain_a.get();
    result_a.reset();
    outcome.result.reset();

    JoinedRows drained_delayed;
    size_t rows_delayed = 0;
    if (auto delayed = join->getDelayedBlocks())
    {
        while (true)
        {
            Block block = delayed->next();
            if (block.empty())
                break;
            rows_delayed += block.rows();
            accumulateRows(block, drained_delayed);
        }
    }

    EXPECT_EQ(rows_a + rows_b + rows_delayed, expected.size());
    JoinedRows drained_all = drained_a;
    drained_all.insert(drained_b.begin(), drained_b.end());
    drained_all.insert(drained_delayed.begin(), drained_delayed.end());
    EXPECT_EQ(drained_all.size(), expected.size());
    EXPECT_TRUE(drained_all == expected) << "joined output multiset does not match the expected probe x build identity";
}

namespace
{

/// Shared scaffolding for the multiset contract tests below: build 8 rows (each key duplicated
/// 4 times), probe blocks constructed by the caller, all output drained through the results and
/// the delayed-blocks stream, compared as an exact multiset.
struct WaveHarness
{
    UInt64 k0 = 0;
    UInt64 k1 = 0;
    std::vector<UInt64> build_keys;
    std::vector<UInt64> build_ids;
    std::shared_ptr<TableJoin> table_join;
    std::shared_ptr<RadixHashJoin> join;
    JoinedRows expected;

    WaveHarness(size_t max_threads, UInt64 max_partitions_per_pass, size_t probe_budget)
    {
        bool found[2] = {false, false};
        UInt64 key_for_partition[2] = {0, 0};
        for (UInt64 v = 1; !(found[0] && found[1]); ++v)
        {
            const UInt32 p = partitionForKey(v);
            if (!found[p])
            {
                key_for_partition[p] = v;
                found[p] = true;
            }
        }
        k0 = key_for_partition[0];
        k1 = key_for_partition[1];
        build_keys = {k0, k0, k0, k0, k1, k1, k1, k1};
        build_ids = {100, 101, 102, 103, 104, 105, 106, 107};

        const Block left_header = twoColumnBlock("k", "probe_id", {}, {});
        const Block right_header = twoColumnBlock("rk", "build_id", {}, {});
        table_join = makeTableJoin(left_header, right_header);
        join = std::make_shared<RadixHashJoin>(
            table_join,
            std::make_shared<const Block>(right_header),
            max_threads,
            /*rhs_size_estimation*/ std::nullopt,
            max_partitions_per_pass,
            /*size_tables_by_distinct_estimate*/ false,
            /*probe_buffer_fraction*/ 0.0,
            /*probe_buffer_min_bytes*/ probe_budget,
            /*probe_buffer_max_bytes*/ probe_budget,
            StatsCollectingParams{});
        EXPECT_TRUE(join->addBlockToJoin(twoColumnBlock("rk", "build_id", build_keys, build_ids), /*check_limits*/ false));
        join->onBuildPhaseFinish();
        join->runPostBuildPhase();
    }

    Block makeProbe(size_t rows, UInt64 first_id, std::vector<UInt64> * keys_out = nullptr)
    {
        std::vector<UInt64> keys(rows);
        std::vector<UInt64> ids(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            keys[i] = (i % 2 == 0) ? k0 : k1;
            ids[i] = first_id + i;
            for (size_t j = 0; j < build_keys.size(); ++j)
                if (keys[i] == build_keys[j])
                    expected.emplace(keys[i], ids[i], build_keys[j], build_ids[j]);
        }
        if (keys_out)
            *keys_out = keys;
        return twoColumnBlock("k", "probe_id", keys, ids);
    }

    /// Joins one block on one lane and drains its result to completion.
    size_t joinAndDrain(Block block, size_t lane, JoinedRows & rows)
    {
        auto result = join->joinBlock(std::move(block), lane);
        return drainResult(*result, rows);
    }

    size_t drainDelayed(JoinedRows & rows)
    {
        size_t drained = 0;
        if (auto delayed = join->getDelayedBlocks())
        {
            while (true)
            {
                Block block = delayed->next();
                if (block.empty())
                    break;
                drained += block.rows();
                accumulateRows(block, rows);
            }
        }
        return drained;
    }
};

}

/// Multiple budget-sealed waves followed by a final partial wave through the delayed-blocks
/// stream, all on the one shared machine: the total output multiset must be exact — every probe
/// row of every wave joined exactly once, none dropped at wave boundaries, none duplicated.
TEST(RadixHashJoin, MultipleWavesAndFinalPartialWaveExactMultiset)
{
    Block probe = twoColumnBlock("k", "probe_id", std::vector<UInt64>(64, 1), std::vector<UInt64>(64, 0));
    const size_t block_bytes = probe.allocatedBytes();
    WaveHarness h(/*max_threads*/ 2, /*max_partitions_per_pass*/ 8, /*probe_budget*/ 2 * block_bytes);

    JoinedRows drained;
    size_t rows = 0;
    /// 7 equal blocks: waves {b1,b2}, {b3,b4}, {b5,b6} seal on their second admission; b7 stays
    /// as the final partial wave for the delayed flush.
    for (size_t b = 0; b < 7; ++b)
        rows += h.joinAndDrain(h.makeProbe(64, 1000 * (b + 1)), b % 2, drained);
    rows += h.drainDelayed(drained);

    EXPECT_EQ(rows, h.expected.size());
    EXPECT_TRUE(drained == h.expected) << "multiset mismatch across waves and the delayed flush";
}

/// Multi-pass refinement: a 4-leaf plan with a per-pass fanout cap of 2 forces two radix passes
/// (scatter + one refine stage) through the same machine; the output multiset must be exact.
TEST(RadixHashJoin, MultiPassRefineExactMultiset)
{
    Block probe = twoColumnBlock("k", "probe_id", std::vector<UInt64>(64, 1), std::vector<UInt64>(64, 0));
    const size_t block_bytes = probe.allocatedBytes();
    WaveHarness h(/*max_threads*/ 4, /*max_partitions_per_pass*/ 2, /*probe_budget*/ block_bytes);

    JoinedRows drained;
    size_t rows = 0;
    for (size_t b = 0; b < 3; ++b)
        rows += h.joinAndDrain(h.makeProbe(64, 1000 * (b + 1)), b % 4, drained);
    rows += h.drainDelayed(drained);

    EXPECT_EQ(rows, h.expected.size());
    EXPECT_TRUE(drained == h.expected) << "multiset mismatch on the multi-pass plan";
}

/// Regression for a real deadlock found by the 04509 stateless test: concurrent delayed-blocks
/// pulls race on sealing the final partial wave, and one pull's failed seal CAS used to overwrite
/// its control-word snapshot with the CURRENT value — if the winning pull had already drained the
/// whole (tiny) wave to the terminal state, the loser then waited for a further transition that
/// never comes. Many rounds of tiny final waves pulled by many threads must always terminate; a
/// wedged round trips the deadline instead of hanging the suite.
TEST(RadixHashJoin, ConcurrentDelayedPullsTerminate)
{
    constexpr size_t rounds = 256;
    constexpr size_t pullers = 32;

    for (size_t round = 0; round < rounds; ++round)
    {
        /// A budget far above the probe bytes: nothing seals mid-stream, everything becomes the
        /// final partial wave, sealed by whichever delayed pull wins the race. A tiny wave makes
        /// the winner reach the terminal state almost instantly, inside the losers' race window.
        WaveHarness h(/*max_threads*/ pullers, /*max_partitions_per_pass*/ 8, /*probe_budget*/ 1ULL << 30);
        JoinedRows drained;
        size_t rows = h.joinAndDrain(h.makeProbe(8, 1000), 0, drained);
        ASSERT_EQ(rows, 0u); /// below budget: the lane's result owes nothing yet

        auto delayed = h.join->getDelayedBlocks();
        ASSERT_NE(delayed, nullptr);

        std::vector<std::future<std::pair<size_t, JoinedRows>>> futures;
        futures.reserve(pullers);
        for (size_t t = 0; t < pullers; ++t)
            futures.push_back(std::async(
                std::launch::async,
                [&]() -> std::pair<size_t, JoinedRows>
                {
                    size_t pulled = 0;
                    JoinedRows mine;
                    while (true)
                    {
                        Block block = delayed->next();
                        if (block.empty())
                            break;
                        pulled += block.rows();
                        accumulateRows(block, mine);
                    }
                    return {pulled, mine};
                }));

        for (auto & future : futures)
        {
            ASSERT_EQ(future.wait_for(std::chrono::seconds(30)), std::future_status::ready)
                << "delayed pull wedged in round " << round << " — cooperative EOF-seal deadlock";
            auto [pulled, mine] = future.get();
            rows += pulled;
            drained.insert(mine.begin(), mine.end());
        }

        ASSERT_EQ(rows, h.expected.size()) << "round " << round;
        ASSERT_TRUE(drained == h.expected) << "multiset mismatch in round " << round;
    }
}

/// Fail-close on early caller destruction: a result destroyed while it still owes work (here a
/// half-probed leaf) must poison the wave so no lane can silently lose rows — the next quantum on
/// any other lane observes the first error, claims no new work, and the failure propagates.
TEST(RadixHashJoin, AbandonedResultPoisonsJoin)
{
    Block tiny_probe = twoColumnBlock("k", "probe_id", std::vector<UInt64>(8, 1), std::vector<UInt64>(8, 0));
    const size_t tiny_bytes = tiny_probe.allocatedBytes();
    WaveHarness h(/*max_threads*/ 2, /*max_partitions_per_pass*/ 8, /*probe_budget*/ 2 * tiny_bytes);

    Block big = h.makeProbe(4096, 1000);
    ASSERT_GE(big.allocatedBytes(), 2 * 2 * tiny_bytes);

    /// Seal a wave and take exactly one output block: the result now owns a half-probed leaf.
    auto result = h.join->joinBlock(std::move(big), 0);
    {
        auto first = result->next();
        ASSERT_FALSE(first.is_last);
        ASSERT_GT(first.block.rows(), 0u);
    }
    result.reset();

    /// Any further quantum on any lane must observe the poisoned wave and throw.
    auto late = h.join->joinBlock(h.makeProbe(8, 100000), 1);
    EXPECT_THROW(
        {
            while (true)
            {
                auto r = late->next();
                if (r.is_last)
                    break;
            }
        },
        DB::Exception);
}
