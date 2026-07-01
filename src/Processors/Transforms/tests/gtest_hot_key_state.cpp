#include <gtest/gtest.h>

#include <optional>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Transforms/HotKeyState.h>
#include <Processors/Transforms/ShardedAggregationSelectors.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>

using namespace DB;

namespace
{

HotKeyStatePtr makeState(bool nullable = false)
{
    DataTypePtr type = std::make_shared<DataTypeUInt64>();
    if (nullable)
        type = std::make_shared<DataTypeNullable>(type);
    ColumnsWithTypeAndName key_header{ColumnWithTypeAndName(type->createColumn(), type, "k")};
    return std::make_shared<HotKeyState>(key_header);
}

/// Feeds one chunk of keys through the input shard selector, which runs a fresh per-stream warmup
/// detector and promotes any hot keys into `state`.
void observe(const HotKeyStatePtr & state, MutableColumnPtr key_col, size_t num_cold)
{
    auto builder = makeInputHotColdSelector(state, ColumnNumbers{0}, num_cold, /*merge_output=*/num_cold);
    Columns columns;
    columns.push_back(std::move(key_col));
    builder(columns);
}

bool isHot(const HotKeyStatePtr & state, UInt64 key)
{
    auto col = ColumnUInt64::create();
    col->insertValue(key);
    Columns columns;
    columns.push_back(std::move(col));
    auto mask = state->buildHotMask(columns);
    if (!mask)
        return false;
    return assert_cast<const ColumnUInt8 &>(*mask).getData()[0] != 0;
}

}

class HotKeyStateTest : public ::testing::Test
{
protected:
    void SetUp() override { MainThreadStatus::getInstance(); }
};

/// A key that reaches the `MIN_HOT_COUNT` floor of 512 is promoted, and one occurrence short of it is not.
TEST_F(HotKeyStateTest, PromotesAtThresholdFloor)
{
    {
        auto state = makeState();
        auto col = ColumnUInt64::create();
        for (size_t i = 0; i < 512; ++i)
            col->insertValue(42);
        observe(state, std::move(col), /*num_cold=*/8);
        EXPECT_TRUE(isHot(state, 42));
    }
    {
        auto state = makeState();
        auto col = ColumnUInt64::create();
        for (size_t i = 0; i < 511; ++i)
            col->insertValue(42);
        observe(state, std::move(col), /*num_cold=*/8);
        EXPECT_FALSE(isHot(state, 42));
    }
}

/// At a fixed shard count the imbalance ratio R = 1.5 puts the threshold at (R - 1) / (S - 1) of the
/// rows. For S = 8 that is about 7.1%, so a key at 10% of the rows is promoted and a key at 5% is not,
/// and both are well above the count floor.
TEST_F(HotKeyStateTest, PromotesAboveImbalanceRatio)
{
    auto state = makeState();
    auto col = ColumnUInt64::create();
    for (size_t i = 0; i < 200000; ++i)
    {
        if (i % 10 == 0)
            col->insertValue(1); // Key 1 is 10% of the rows.
        else if (i % 20 == 1)
            col->insertValue(2); // Key 2 is 5% of the rows.
        else
            col->insertValue(100 + i); // The rest are a high-cardinality cold tail.
    }
    observe(state, std::move(col), /*num_cold=*/8);

    EXPECT_TRUE(isHot(state, 1));
    EXPECT_FALSE(isHot(state, 2));
}

/// The threshold follows the per-shard fair share (R - 1) / (S - 1). The same key, at 25% of the rows,
/// is hot when num_cold is 8, where the threshold is about 7%, but not when num_cold is 2, where the
/// threshold is 50% and 25% never reaches it.
TEST_F(HotKeyStateTest, ThresholdScalesWithNumCold)
{
    auto make_data = []()
    {
        auto col = ColumnUInt64::create();
        for (size_t i = 0; i < 20000; ++i)
            col->insertValue(i % 4 == 0 ? 42 : 1000 + i);
        return col;
    };

    auto hot_state = makeState();
    observe(hot_state, make_data(), /*num_cold=*/8);
    EXPECT_TRUE(isHot(hot_state, 42));

    auto cold_state = makeState();
    observe(cold_state, make_data(), /*num_cold=*/2);
    EXPECT_FALSE(isHot(cold_state, 42));
}

/// A key that first appears only after many distinct cold keys is still promoted. The exact per-key
/// counter tracks every key seen during the window, so a key that becomes frequent late is counted like
/// any other and nothing crowds it out. This is the path the late-residue case depends on.
TEST_F(HotKeyStateTest, LateKeyStillPromoted)
{
    auto state = makeState();
    auto col = ColumnUInt64::create();
    for (size_t i = 0; i < 10000; ++i) // First come 10000 distinct cold keys.
        col->insertValue(100000 + i);
    for (size_t i = 0; i < 5000; ++i) // Then a key that suddenly becomes frequent.
        col->insertValue(999);
    observe(state, std::move(col), /*num_cold=*/8);

    EXPECT_TRUE(isHot(state, 999));
    EXPECT_FALSE(isHot(state, 100005));
}

/// The detector only counts keys during an initial warmup window and then stops. A key that becomes
/// frequent only after the window has closed is never promoted, even when it is frequent enough that it
/// would have been promoted had counting continued. Here key 888 is frequent but turns up only after
/// enough cold keys have closed the window, so the closed window is the only reason it stays cold, while
/// key 111 is frequent from the start, well inside the window, and is promoted.
TEST_F(HotKeyStateTest, NoPromotionAfterWarmupWindow)
{
    auto state = makeState();
    auto col = ColumnUInt64::create();
    for (size_t i = 0; i < 5000; ++i) // Key 111 is frequent from the start, inside the window.
        col->insertValue(111);
    for (size_t i = 0; i < 200000; ++i) // Enough distinct cold keys to run well past the warmup window.
        col->insertValue(200000 + i);
    for (size_t i = 0; i < 30000; ++i) // Key 888 is frequent enough to promote if it were still counted.
        col->insertValue(888);
    observe(state, std::move(col), /*num_cold=*/8);

    EXPECT_TRUE(isHot(state, 111));
    EXPECT_FALSE(isHot(state, 888));
}

/// `HotKeyState` membership is exact and aware of NULLs, because the `Set` is built with
/// `transform_null_in` set to true: a promoted NULL key matches NULL, and only promoted keys are members.
TEST_F(HotKeyStateTest, MembershipExactAndNullAware)
{
    auto state = makeState(/*nullable=*/true);

    auto null_key = ColumnNullable::create(ColumnUInt64::create(), ColumnUInt8::create());
    null_key->insertDefault(); // A single NULL row.
    Columns null_cols;
    null_cols.push_back(std::move(null_key));
    state->promote(null_cols, 0, /*hash=*/1);

    auto value_key = ColumnNullable::create(ColumnUInt64::create(), ColumnUInt8::create());
    value_key->insert(Field(UInt64(5))); // A single non-NULL row holding 5.
    Columns value_cols;
    value_cols.push_back(std::move(value_key));
    state->promote(value_cols, 0, /*hash=*/2);

    auto probe_nested = ColumnUInt64::create();
    auto probe_null = ColumnUInt8::create();
    auto add = [&](std::optional<UInt64> v)
    {
        probe_nested->insertValue(v.value_or(0));
        probe_null->insertValue(v ? 0 : 1);
    };
    add(std::nullopt); // NULL is expected to be hot.
    add(5); // 5 is expected to be hot.
    add(7); // 7 is expected to be cold.
    add(std::nullopt); // NULL is expected to be hot.

    Columns probe;
    probe.push_back(ColumnNullable::create(std::move(probe_nested), std::move(probe_null)));
    auto mask = state->buildHotMask(probe);

    ASSERT_TRUE(mask != nullptr);
    const auto & m = assert_cast<const ColumnUInt8 &>(*mask).getData();
    ASSERT_EQ(m.size(), 4u);
    EXPECT_EQ(m[0], 1);
    EXPECT_EQ(m[1], 1);
    EXPECT_EQ(m[2], 0);
    EXPECT_EQ(m[3], 1);
}

namespace
{

/// Drives a fresh input selector through a full warmup window of rows drawn from `distinct` keys, then
/// probes it with a one-row chunk and returns that probe's routing. By the time the window closes the
/// selector has decided whether to fall back to the merge path, so the probe reveals the decision.
ChunkRouting routeAfterWarmup(size_t distinct, size_t num_cold)
{
    auto state = makeState();
    auto builder = makeInputHotColdSelector(state, ColumnNumbers{0}, num_cold, /*merge_output=*/num_cold);

    const size_t n = 200000; // Past the warmup window (min 131072 rows), so it closes within this chunk.
    auto warmup = ColumnUInt64::create();
    for (size_t i = 0; i < n; ++i)
        warmup->insertValue(i % distinct);
    Columns warmup_cols;
    warmup_cols.push_back(std::move(warmup));
    builder(warmup_cols);

    auto probe = ColumnUInt64::create();
    probe->insertValue(0);
    Columns probe_cols;
    probe_cols.push_back(std::move(probe));
    return builder(probe_cols);
}

}

/// With only 16 distinct keys, the warmup's distinct count stays far below the fallback's distinct-key
/// threshold (about 24000 for these 8 shards), so the selector falls back and routes the whole chunk to the
/// merge path (the last port, numbered `num_cold`) with no per-row selector.
TEST_F(HotKeyStateTest, LowCardinalityRoutesWholeChunkToMergePath)
{
    const size_t num_cold = 8;
    auto routing = routeAfterWarmup(/*distinct=*/16, num_cold);
    ASSERT_TRUE(routing.whole_chunk_output.has_value());
    EXPECT_EQ(*routing.whole_chunk_output, num_cold);
    EXPECT_TRUE(routing.selector.empty());
}

/// With every row a new key, the distinct count fills the warmup window and stays above that threshold, so
/// the selector does not fall back: it keeps scattering and returns a per-row selector rather than a
/// whole-chunk route.
TEST_F(HotKeyStateTest, HighCardinalityKeepsScattering)
{
    const size_t num_cold = 8;
    auto routing = routeAfterWarmup(/*distinct=*/200000, num_cold);
    EXPECT_FALSE(routing.whole_chunk_output.has_value());
}
