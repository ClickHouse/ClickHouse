#include <Interpreters/QueryStatisticsCache.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

size_t hashOf(const Names & part_names, const Names & required_columns)
{
    QueryStatisticsCache::Key key;
    key.part_names = part_names;
    key.required_columns = required_columns;
    return QueryStatisticsCache::KeyHash{}(key);
}

}

/// A name vector is hashed as bytes, so without a length prefix per name two vectors whose names
/// concatenate to the same string are indistinguishable. Such a pair is reachable: part names and
/// column names are both arbitrary strings, and the resulting keys share a bucket while comparing
/// unequal, which is the worst case for lookup.
TEST(QueryStatisticsCacheKey, DifferentSplitsOfTheSameConcatenationHashApart)
{
    EXPECT_NE(hashOf({"p"}, {"a", "bc"}), hashOf({"p"}, {"ab", "c"}));
    EXPECT_NE(hashOf({"a", "bc"}, {"c1"}), hashOf({"ab", "c"}, {"c1"}));
}

/// An empty name is a split too: {"", "ab"} and {"ab"} differ in length, {"", "ab"} and {"ab", ""}
/// do not.
TEST(QueryStatisticsCacheKey, EmptyNamesTakePart)
{
    EXPECT_NE(hashOf({"p"}, {"", "ab"}), hashOf({"p"}, {"ab", ""}));
}

/// The pairs above must differ because of their splits, not because the hash is unstable.
TEST(QueryStatisticsCacheKey, EqualKeysHashEqual)
{
    EXPECT_EQ(hashOf({"p"}, {"a", "bc"}), hashOf({"p"}, {"a", "bc"}));
    EXPECT_EQ(hashOf({"a", "bc"}, {"c1"}), hashOf({"a", "bc"}, {"c1"}));
}
