#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/PreparedSets.h>
#include <QueryPipeline/SizeLimits.h>

#include <gtest/gtest.h>
#include <thread>

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

/// Multi-column set with repeated element types (String, String): rows that tie on the
TEST(FutureSetContentHash, MultiColumnRepeatedTypeOrderIndependent)
{
    auto makeBlock = [](std::vector<std::pair<String, String>> rows)
    {
        auto col1 = ColumnString::create();
        auto col2 = ColumnString::create();
        for (const auto & [a, b] : rows) { col1->insert(a); col2->insert(b); }
        auto type = std::make_shared<DataTypeString>();
        return ColumnsWithTypeAndName{{std::move(col1), type, "s1"}, {std::move(col2), type, "s2"}};
    };

    /// ('a','b') and ('a','c') — tie on first column, differ on second
    auto h1 = contentHash(makeBlock({{"a", "b"}, {"a", "c"}}));
    auto h2 = contentHash(makeBlock({{"a", "c"}, {"a", "b"}}));
    EXPECT_EQ(h1, h2);

    /// Must differ from a set that has different second-column values
    auto h3 = contentHash(makeBlock({{"a", "b"}, {"a", "d"}}));
    EXPECT_NE(h1, h3);
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

/// Duplicate elements are removed after Set::insertFromColumns, so IN ('a','a')
/// and IN ('a') must hash identically.
TEST(FutureSetContentHash, DuplicatesNormalized)
{
    EXPECT_EQ(contentHash(makeStringBlock({"a"})),
              contentHash(makeStringBlock({"a", "a"})));
    EXPECT_EQ(contentHash(makeStringBlock({"a", "b"})),
              contentHash(makeStringBlock({"a", "b", "a"})));
}

/// Empty IN list must not crash and must produce a stable hash.
TEST(FutureSetContentHash, EmptySet)
{
    auto h1 = contentHash(makeStringBlock({}));
    auto h2 = contentHash(makeStringBlock({}));
    EXPECT_EQ(h1, h2);
}

/// Calling getContentHash() twice on the same instance must return the same value
/// (the lazy callOnce path is idempotent).
TEST(FutureSetContentHash, IdempotentOnSameInstance)
{
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, makeStringBlock({"b", "a", "c"}), false, SizeLimits{});
    EXPECT_EQ(future_set->getContentHash(), future_set->getContentHash());
}

/// Calling getKeyColumns() before getContentHash() pre-fires fill_set_elements_once;
/// the content hash must still be computed correctly afterwards.
TEST(FutureSetContentHash, ContentHashAfterGetKeyColumns)
{
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, makeStringBlock({"b", "a"}), false, SizeLimits{});
    future_set->getKeyColumns(); // fires fill_set_elements_once early
    auto h1 = future_set->getContentHash();

    auto h2 = contentHash(makeStringBlock({"a", "b"})); // fresh instance, same logical set
    EXPECT_EQ(h1, h2);
}

/// Concurrent first calls to getContentHash() on the same instance must all return
/// the same value (callOnce thread-safety).
TEST(FutureSetContentHash, ThreadSafe)
{
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, makeStringBlock({"c", "a", "b"}), false, SizeLimits{});

    constexpr int num_threads = 16;
    std::vector<FutureSet::Hash> results(num_threads);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i)
        threads.emplace_back([&, i] { results[i] = future_set->getContentHash(); });
    for (auto & t : threads)
        t.join();

    for (int i = 1; i < num_threads; ++i)
        EXPECT_EQ(results[0], results[i]);
}

/// With transform_null_in = false (the default), NULL-containing rows are skipped
/// during insertion, so IN ('a', NULL) and IN ('a') must hash identically.
TEST(FutureSetContentHash, NullsFilteredWhenTransformNullInDisabled)
{
    auto makeBlockWithNull = [](std::vector<std::optional<String>> values)
    {
        auto col = ColumnString::create();
        auto null_map = ColumnVector<UInt8>::create();
        for (const auto & v : values)
        {
            col->insert(v.value_or(""));
            null_map->insert(v.has_value() ? 0 : 1);
        }
        auto nullable = ColumnNullable::create(std::move(col), std::move(null_map));
        return ColumnsWithTypeAndName{
            {std::move(nullable), std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "s"}};
    };

    /// transform_null_in = false: NULL rows are dropped, so {'a', NULL} == {'a'}
    EXPECT_EQ(contentHash(makeBlockWithNull({"a", std::nullopt})),
              contentHash(makeBlockWithNull({"a"})));
}
