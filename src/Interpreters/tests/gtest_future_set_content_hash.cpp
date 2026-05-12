#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/PreparedSets.h>
#include <QueryPipeline/SizeLimits.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

/// Build a single-column ColumnsWithTypeAndName from a list of string values.
ColumnsWithTypeAndName makeStringBlock(std::vector<String> values)
{
    auto col = ColumnString::create();
    for (const auto & v : values)
        col->insert(v);
    return {{std::move(col), std::make_shared<DataTypeString>(), "s"}};
}

/// Build a single-column ColumnsWithTypeAndName from a list of UInt64 values.
ColumnsWithTypeAndName makeUInt64Block(std::vector<UInt64> values)
{
    auto col = ColumnVector<UInt64>::create();
    for (auto v : values)
        col->insert(v);
    return {{std::move(col), std::make_shared<DataTypeUInt64>(), "n"}};
}

/// Build a two-column ColumnsWithTypeAndName (String, UInt64) from paired values.
ColumnsWithTypeAndName makeTwoColumnBlock(std::vector<std::pair<String, UInt64>> rows)
{
    auto col_s = ColumnString::create();
    auto col_n = ColumnVector<UInt64>::create();
    for (const auto & [s, n] : rows)
    {
        col_s->insert(s);
        col_n->insert(n);
    }
    return {
        {std::move(col_s), std::make_shared<DataTypeString>(), "s"},
        {std::move(col_n), std::make_shared<DataTypeUInt64>(), "n"},
    };
}

FutureSet::Hash contentHash(ColumnsWithTypeAndName block)
{
    /// The AST-based hash_ and ast_ fields do not affect getContentHash().
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, std::move(block), false, SizeLimits{});
    return future_set->getContentHash();
}

}

/// Same single element always produces the same hash (degenerate case).
TEST(FutureSetContentHash, SingleElement)
{
    EXPECT_EQ(contentHash(makeStringBlock({"x"})),
              contentHash(makeStringBlock({"x"})));
}

/// Two elements in opposite orders must hash identically.
TEST(FutureSetContentHash, TwoElementsOrderIndependent)
{
    EXPECT_EQ(contentHash(makeStringBlock({"a", "b"})),
              contentHash(makeStringBlock({"b", "a"})));
}

/// All six permutations of three elements must produce the same hash.
TEST(FutureSetContentHash, ThreeElementsAllPermutations)
{
    const auto ref = contentHash(makeStringBlock({"a", "b", "c"}));
    EXPECT_EQ(ref, contentHash(makeStringBlock({"a", "c", "b"})));
    EXPECT_EQ(ref, contentHash(makeStringBlock({"b", "a", "c"})));
    EXPECT_EQ(ref, contentHash(makeStringBlock({"b", "c", "a"})));
    EXPECT_EQ(ref, contentHash(makeStringBlock({"c", "a", "b"})));
    EXPECT_EQ(ref, contentHash(makeStringBlock({"c", "b", "a"})));
}

/// Numeric set: reversed order must hash the same.
TEST(FutureSetContentHash, NumericOrderIndependent)
{
    EXPECT_EQ(contentHash(makeUInt64Block({1, 2, 3})),
              contentHash(makeUInt64Block({3, 1, 2})));
    EXPECT_EQ(contentHash(makeUInt64Block({1, 2, 3})),
              contentHash(makeUInt64Block({3, 2, 1})));
}

/// Multi-column set: row order must not affect the hash.
TEST(FutureSetContentHash, MultiColumnOrderIndependent)
{
    auto h1 = contentHash(makeTwoColumnBlock({{"a", 1}, {"b", 2}, {"c", 3}}));
    auto h2 = contentHash(makeTwoColumnBlock({{"c", 3}, {"a", 1}, {"b", 2}}));
    auto h3 = contentHash(makeTwoColumnBlock({{"b", 2}, {"c", 3}, {"a", 1}}));
    EXPECT_EQ(h1, h2);
    EXPECT_EQ(h1, h3);
}

/// Different sets must produce different hashes.
TEST(FutureSetContentHash, DifferentSetsCollide)
{
    EXPECT_NE(contentHash(makeStringBlock({"a", "b"})),
              contentHash(makeStringBlock({"a", "c"})));
    EXPECT_NE(contentHash(makeStringBlock({"a"})),
              contentHash(makeStringBlock({"b"})));
    EXPECT_NE(contentHash(makeUInt64Block({1, 2})),
              contentHash(makeUInt64Block({1, 3})));
}

/// A subset must not collide with a superset.
TEST(FutureSetContentHash, SubsetVsSuperset)
{
    EXPECT_NE(contentHash(makeStringBlock({"a", "b"})),
              contentHash(makeStringBlock({"a", "b", "c"})));
}

/// Same values but different column types must produce different hashes.
TEST(FutureSetContentHash, TypeSensitivity)
{
    /// "1" as a string vs 1 as a UInt64 — different type names, different hash.
    EXPECT_NE(contentHash(makeStringBlock({"1"})),
              contentHash(makeUInt64Block({1})));
}
