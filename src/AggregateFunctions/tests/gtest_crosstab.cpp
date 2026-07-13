#include <AggregateFunctions/CrossTab.h>
#include <AggregateFunctions/AggregateFunctionTheilsU.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>


using namespace DB;

/// Helper: add N observations from a simple pattern
static void addObservations(CrossTabPhiSquaredWindowData & data, size_t n, size_t mod_a, size_t mod_b)
{
    for (size_t i = 0; i < n; ++i)
        data.add(i % mod_a, i % mod_b);
}

/// Helper: serialize src, then deserialize into dst
static void roundTrip(const CrossTabPhiSquaredWindowData & src, CrossTabPhiSquaredWindowData & dst)
{
    WriteBufferFromOwnString wb;
    src.serialize(wb);

    ReadBufferFromString rb(wb.str());
    dst.deserialize(rb);
}

TEST(CrossTabPhiSquaredWindowData, SerializeDeserializeRoundTrip)
{
    CrossTabPhiSquaredWindowData data;
    addObservations(data, 100, 10, 6);

    CrossTabPhiSquaredWindowData restored;
    roundTrip(data, restored);

    EXPECT_EQ(restored.count, data.count);
    /// Incremental phi_term_sum may differ slightly from batch rebuild in deserialize
    EXPECT_NEAR(restored.getPhiSquared(), data.getPhiSquared(), 1e-12);
}

TEST(CrossTabPhiSquaredWindowData, SerializeDeserializeEmpty)
{
    CrossTabPhiSquaredWindowData data;

    CrossTabPhiSquaredWindowData restored;
    roundTrip(data, restored);

    EXPECT_EQ(restored.count, 0u);
    EXPECT_DOUBLE_EQ(restored.getPhiSquared(), 0.0);
}

TEST(CrossTabPhiSquaredWindowData, GetPhiSquaredEmpty)
{
    CrossTabPhiSquaredWindowData data;
    EXPECT_DOUBLE_EQ(data.getPhiSquared(), 0.0);
}

TEST(CrossTabPhiSquaredWindowData, GetPhiSquaredSingleDistinctColumn)
{
    /// All rows have the same value for column A -> q = min(1, |B|) = 1 -> return 0
    CrossTabPhiSquaredWindowData data;
    for (size_t i = 0; i < 20; ++i)
        data.add(42, i % 5);

    EXPECT_DOUBLE_EQ(data.getPhiSquared(), 0.0);
}

TEST(CrossTabPhiSquaredWindowData, MergeWithEmptyOther)
{
    CrossTabPhiSquaredWindowData data;
    addObservations(data, 50, 5, 3);

    Float64 phi_before = data.getPhiSquared();
    UInt64 count_before = data.count;

    CrossTabPhiSquaredWindowData empty;
    data.merge(empty);

    EXPECT_EQ(data.count, count_before);
    EXPECT_DOUBLE_EQ(data.getPhiSquared(), phi_before);
}

TEST(CrossTabPhiSquaredWindowData, MergeIntoEmpty)
{
    CrossTabPhiSquaredWindowData other;
    addObservations(other, 50, 5, 3);

    CrossTabPhiSquaredWindowData data;
    data.merge(other);

    EXPECT_EQ(data.count, other.count);
    EXPECT_NEAR(data.getPhiSquared(), other.getPhiSquared(), 1e-12);
}

TEST(CrossTabPhiSquaredWindowData, MergeOverlappingEdges)
{
    /// Both states have the same (a, b) pairs -- merge should combine edge counts.
    /// Use explicit pairs so both states have identical observations.
    CrossTabPhiSquaredWindowData s1;
    s1.add(1, 10);
    s1.add(1, 20);
    s1.add(2, 10);
    s1.add(2, 20);

    CrossTabPhiSquaredWindowData s2;
    s2.add(1, 10);
    s2.add(1, 20);
    s2.add(2, 10);
    s2.add(2, 20);

    /// Build expected by adding all observations to one state
    CrossTabPhiSquaredWindowData expected;
    for (int rep = 0; rep < 2; ++rep)
    {
        expected.add(1, 10);
        expected.add(1, 20);
        expected.add(2, 10);
        expected.add(2, 20);
    }

    s1.merge(s2);

    EXPECT_EQ(s1.count, expected.count);
    EXPECT_NEAR(s1.getPhiSquared(), expected.getPhiSquared(), 1e-9);
}

TEST(CrossTabPhiSquaredWindowData, MergeDisjointEdges)
{
    /// Two states with completely different hash values
    CrossTabPhiSquaredWindowData s1;
    s1.add(1, 10);
    s1.add(2, 20);

    CrossTabPhiSquaredWindowData s2;
    s2.add(3, 30);
    s2.add(4, 40);

    /// Build expected by adding all observations to one state
    CrossTabPhiSquaredWindowData expected;
    expected.add(1, 10);
    expected.add(2, 20);
    expected.add(3, 30);
    expected.add(4, 40);

    s1.merge(s2);

    EXPECT_EQ(s1.count, expected.count);
    EXPECT_NEAR(s1.getPhiSquared(), expected.getPhiSquared(), 1e-9);
}

TEST(CrossTabPhiSquaredWindowData, MergeAggregateDataWithEmptyOther)
{
    CrossTabPhiSquaredWindowData data;
    addObservations(data, 50, 5, 3);

    Float64 phi_before = data.getPhiSquared();
    UInt64 count_before = data.count;

    CrossTabAggregateData empty;
    data.merge(empty);

    EXPECT_EQ(data.count, count_before);
    EXPECT_DOUBLE_EQ(data.getPhiSquared(), phi_before);
}

TEST(CrossTabPhiSquaredWindowData, SerializeDeserializePreservesPhiSquared)
{
    /// Verify that deserialize correctly rebuilds phi_term_sum from the maps
    CrossTabPhiSquaredWindowData data;
    addObservations(data, 200, 10, 6);

    Float64 original_phi = data.getPhiSquared();
    EXPECT_GT(original_phi, 0.0);

    CrossTabPhiSquaredWindowData restored;
    roundTrip(data, restored);

    EXPECT_EQ(restored.count, data.count);
    EXPECT_NEAR(restored.getPhiSquared(), original_phi, 1e-12);
}

TEST(CrossTabPhiSquaredWindowData, ClearResetsState)
{
    CrossTabPhiSquaredWindowData data;
    addObservations(data, 100, 10, 6);
    EXPECT_GT(data.count, 0u);

    /// Deserialize an empty state to trigger clear()
    CrossTabPhiSquaredWindowData empty;
    WriteBufferFromOwnString wb;
    empty.serialize(wb);

    ReadBufferFromString rb(wb.str());
    data.deserialize(rb);

    EXPECT_EQ(data.count, 0u);
    EXPECT_DOUBLE_EQ(data.getPhiSquared(), 0.0);
}


/// ---- TheilsUWindowData tests ----

static void addObservations(TheilsUWindowData & data, size_t n, size_t mod_a, size_t mod_b)
{
    for (size_t i = 0; i < n; ++i)
        data.add(i % mod_a, i % mod_b);
}

static void roundTrip(const TheilsUWindowData & src, TheilsUWindowData & dst)
{
    WriteBufferFromOwnString wb;
    src.serialize(wb);

    ReadBufferFromString rb(wb.str());
    dst.deserialize(rb);
}

TEST(TheilsUWindowData, SerializeDeserializeRoundTrip)
{
    TheilsUWindowData data;
    addObservations(data, 100, 10, 6);

    TheilsUWindowData restored;
    roundTrip(data, restored);

    EXPECT_EQ(restored.count, data.count);
    EXPECT_NEAR(restored.getResult(), data.getResult(), 1e-12);
}

TEST(TheilsUWindowData, SerializeDeserializeEmpty)
{
    TheilsUWindowData data;

    TheilsUWindowData restored;
    roundTrip(data, restored);

    EXPECT_EQ(restored.count, 0u);
    EXPECT_TRUE(std::isnan(restored.getResult()));
}

TEST(TheilsUWindowData, GetResultCountLessThanTwo)
{
    /// count == 0
    TheilsUWindowData empty;
    EXPECT_TRUE(std::isnan(empty.getResult()));

    /// count == 1
    TheilsUWindowData one;
    one.add(1, 2);
    EXPECT_TRUE(std::isnan(one.getResult()));
}

TEST(TheilsUWindowData, MergeWithEmptyOther)
{
    TheilsUWindowData data;
    addObservations(data, 50, 5, 3);

    Float64 result_before = data.getResult();
    UInt64 count_before = data.count;

    TheilsUWindowData empty;
    data.merge(empty);

    EXPECT_EQ(data.count, count_before);
    EXPECT_DOUBLE_EQ(data.getResult(), result_before);
}

TEST(TheilsUWindowData, MergeIntoEmpty)
{
    TheilsUWindowData other;
    addObservations(other, 50, 5, 3);

    TheilsUWindowData data;
    data.merge(other);

    EXPECT_EQ(data.count, other.count);
    EXPECT_NEAR(data.getResult(), other.getResult(), 1e-12);
}

TEST(TheilsUWindowData, MergeOverlapping)
{
    TheilsUWindowData s1;
    s1.add(1, 10);
    s1.add(1, 20);
    s1.add(2, 10);
    s1.add(2, 20);

    TheilsUWindowData s2;
    s2.add(1, 10);
    s2.add(1, 20);
    s2.add(2, 10);
    s2.add(2, 20);

    TheilsUWindowData expected;
    for (int rep = 0; rep < 2; ++rep)
    {
        expected.add(1, 10);
        expected.add(1, 20);
        expected.add(2, 10);
        expected.add(2, 20);
    }

    s1.merge(s2);

    EXPECT_EQ(s1.count, expected.count);
    EXPECT_NEAR(s1.getResult(), expected.getResult(), 1e-9);
}

TEST(TheilsUWindowData, MergeAggregateDataWithEmptyOther)
{
    TheilsUWindowData data;
    addObservations(data, 50, 5, 3);

    Float64 result_before = data.getResult();
    UInt64 count_before = data.count;

    CrossTabAggregateData empty;
    data.merge(empty);

    EXPECT_EQ(data.count, count_before);
    EXPECT_DOUBLE_EQ(data.getResult(), result_before);
}

TEST(TheilsUWindowData, ClearResetsState)
{
    TheilsUWindowData data;
    addObservations(data, 100, 10, 6);
    EXPECT_GT(data.count, 0u);

    /// Deserialize an empty state to trigger clear()
    TheilsUWindowData empty;
    WriteBufferFromOwnString wb;
    empty.serialize(wb);

    ReadBufferFromString rb(wb.str());
    data.deserialize(rb);

    EXPECT_EQ(data.count, 0u);
    EXPECT_TRUE(std::isnan(data.getResult()));
}

TEST(TheilsUWindowData, GetResultSingleDistinctColumnA)
{
    /// All rows have the same value for column A -> H(A) = 0 -> return 0
    TheilsUWindowData data;
    for (size_t i = 0; i < 20; ++i)
        data.add(42, i % 5);

    EXPECT_DOUBLE_EQ(data.getResult(), 0.0);
}
