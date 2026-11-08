#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/TwoLevelHashTable.h>
#include <Interpreters/HashJoin/DeferredBuild.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

/// The size that a table has after the streaming build inserted `keys` keys.
template <typename Grower>
size_t grownCells(size_t keys)
{
    Grower grower;
    while (grower.overflow(keys))
        grower.increaseSize();
    return grower.bufSize();
}

/// The size that `HashTable::reserve` gives a table for `keys` keys.
template <typename Grower>
size_t reservedCells(size_t keys)
{
    Grower grower;
    if (keys)
        grower.set(keys);
    return grower.bufSize();
}

template <typename Grower>
void checkSmallTablesGrowLikeStreaming()
{
    for (size_t keys = 0; keys <= (1ULL << 15); ++keys)
        ASSERT_EQ(reservedCells<Grower>(deferredReserveKeys<Grower>(keys)), grownCells<Grower>(keys)) << keys << " keys";
}

}

TEST(DeferredBuildReserve, SmallBucketsOfATwoLevelMapGrowLikeStreaming)
{
    checkSmallTablesGrowLikeStreaming<TwoLevelHashTableGrower<>>();
}

TEST(DeferredBuildReserve, SmallSingleTableGrowsLikeStreaming)
{
    checkSmallTablesGrowLikeStreaming<HashTableGrowerWithPrecalculation<>>();
}

TEST(DeferredBuildReserve, ExactReserveIsDenserThanStreamingForSomeKeyCounts)
{
    /// 195 keys per bucket: `HashTable::reserve` takes 512 cells, the streaming build grows 1024.
    using Grower = TwoLevelHashTableGrower<>;
    EXPECT_EQ(reservedCells<Grower>(195), 512);
    EXPECT_EQ(grownCells<Grower>(195), 1024);
    EXPECT_EQ(reservedCells<Grower>(deferredReserveKeys<Grower>(195)), 1024);
}

TEST(DeferredBuildReserve, LargeTablesKeepTheExactSize)
{
    using Grower = TwoLevelHashTableGrower<>;
    for (size_t keys : {40000UL, 58982UL, 58983UL, 100000UL, 1UL << 20, 123456789UL})
        EXPECT_EQ(deferredReserveKeys<Grower>(keys), keys);

    for (size_t keys : {40000UL, 1UL << 20, 123456789UL})
        EXPECT_EQ(deferredReserveKeys<HashTableGrowerWithPrecalculation<>>(keys), keys);
}

TEST(DeferredBuildReserve, TableHoldsItsKeysWithoutResize)
{
    using Grower = TwoLevelHashTableGrower<>;
    for (size_t keys = 1; keys <= (1ULL << 17); keys = keys * 3 / 2 + 1)
    {
        Grower grower;
        grower.set(deferredReserveKeys<Grower>(keys));
        EXPECT_FALSE(grower.overflow(keys)) << keys << " keys";
    }
}
