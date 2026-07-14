#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>

using namespace DB;

TEST(PuffinFilesCacheKey, SamePuffinSliceDifferentReferencedDataFile)
{
    const auto key1 = PuffinFilesCache::tryCreateKey("puffin.bin", "etag-1", 100, 200, "data/file-a.parquet");
    const auto key2 = PuffinFilesCache::tryCreateKey("puffin.bin", "etag-1", 100, 200, "data/file-b.parquet");

    ASSERT_TRUE(key1.has_value());
    ASSERT_TRUE(key2.has_value());
    EXPECT_NE(*key1, *key2);
    EXPECT_NE(PuffinFilesCacheKeyHash{}(*key1), PuffinFilesCacheKeyHash{}(*key2));
}

TEST(PuffinFilesCacheKey, SameReferencedDataFileProducesEqualKeys)
{
    const auto key1 = PuffinFilesCache::tryCreateKey("puffin.bin", "etag-1", 100, 200, "data/file-a.parquet");
    const auto key2 = PuffinFilesCache::tryCreateKey("puffin.bin", "etag-1", 100, 200, "data/file-a.parquet");

    ASSERT_TRUE(key1.has_value());
    ASSERT_TRUE(key2.has_value());
    EXPECT_EQ(*key1, *key2);
    EXPECT_EQ(PuffinFilesCacheKeyHash{}(*key1), PuffinFilesCacheKeyHash{}(*key2));
}

TEST(PuffinFilesCacheKey, EmptyEtagReturnsNullopt)
{
    const auto key = PuffinFilesCache::tryCreateKey("puffin.bin", "", 100, 200, "data/file-a.parquet");
    EXPECT_FALSE(key.has_value());
}
