#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/FixedHashSet.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/HashTable/StringHashSet.h>
#include <Common/HashTable/TwoLevelHashMap.h>

#include <utility>

#include <gtest/gtest.h>

namespace
{

template <typename Table>
class HashTableMemoryUsage : public ::testing::Test
{
};

using Tables = ::testing::Types<HashSet<UInt64>, HashMap<UInt64, UInt64>, FixedHashSet<UInt8>, FixedHashMap<UInt8, UInt64>>;
TYPED_TEST_SUITE(HashTableMemoryUsage, Tables);

TYPED_TEST(HashTableMemoryUsage, ClearRetainsBufferAndShrinkReleasesIt)
{
    TypeParam table;
    const size_t initial_bytes = table.getBufferSizeInBytes();
    ASSERT_GT(initial_bytes, 0);

    typename TypeParam::LookupResult it;
    bool inserted = false;
    table.emplace(1, it, inserted);
    ASSERT_TRUE(inserted);

    table.clear();
    EXPECT_EQ(table.getBufferSizeInBytes(), initial_bytes);
    table.clearAndShrink();
    EXPECT_EQ(table.getBufferSizeInBytes(), 0);
}

TYPED_TEST(HashTableMemoryUsage, MoveTransfersBuffer)
{
    TypeParam source;
    const size_t bytes = source.getBufferSizeInBytes();
    ASSERT_GT(bytes, 0);

    TypeParam destination(std::move(source));
    EXPECT_EQ(destination.getBufferSizeInBytes(), bytes);
    EXPECT_EQ(source.getBufferSizeInBytes(), 0);

    TypeParam assigned;
    assigned = std::move(destination);
    EXPECT_EQ(assigned.getBufferSizeInBytes(), bytes);
    EXPECT_EQ(destination.getBufferSizeInBytes(), 0);
}

template <typename Table>
class StringHashTableMemoryUsage : public ::testing::Test
{
};

using StringTables = ::testing::Types<StringHashMap<UInt64>, StringHashSet<>>;
TYPED_TEST_SUITE(StringHashTableMemoryUsage, StringTables);

TYPED_TEST(StringHashTableMemoryUsage, ShrinkRetainsOnlyInlineStorage)
{
    TypeParam table;
    const size_t inline_bytes = table.emptyStringSlot().getBufferSizeInBytes();
    ASSERT_GT(table.getBufferSizeInBytes(), inline_bytes);

    table.clearAndShrink();
    EXPECT_EQ(table.getBufferSizeInBytes(), inline_bytes);
}

TEST(HashTableMemoryUsage, TwoLevelBucketsReleaseIndependently)
{
    TwoLevelHashMap<UInt64, UInt64> table;
    size_t bytes = table.getBufferSizeInBytes();
    ASSERT_GT(bytes, 0);

    for (auto & bucket : table.impls)
    {
        bytes -= bucket.getBufferSizeInBytes();
        bucket.clearAndShrink();
        EXPECT_EQ(table.getBufferSizeInBytes(), bytes);
    }
    EXPECT_EQ(bytes, 0);
}

}
