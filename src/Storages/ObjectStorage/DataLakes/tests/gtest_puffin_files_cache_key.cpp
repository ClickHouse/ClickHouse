#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>

using namespace DB;

namespace
{

std::optional<PuffinFilesCacheKey> makeKey(
    const String & referenced_data_file,
    UInt64 expected_cardinality = 2,
    UInt64 data_file_record_count = 100)
{
    return PuffinFilesCache::tryCreateKey(
        "puffin.bin",
        "etag-1",
        100,
        200,
        referenced_data_file,
        expected_cardinality,
        data_file_record_count);
}

}

TEST(PuffinFilesCacheKey, SamePuffinSliceDifferentReferencedDataFile)
{
    const auto key1 = makeKey("data/file-a.parquet");
    const auto key2 = makeKey("data/file-b.parquet");

    ASSERT_TRUE(key1.has_value());
    ASSERT_TRUE(key2.has_value());
    EXPECT_NE(*key1, *key2);
    EXPECT_NE(PuffinFilesCacheKeyHash{}(*key1), PuffinFilesCacheKeyHash{}(*key2));
}

TEST(PuffinFilesCacheKey, SameReferencedDataFileProducesEqualKeys)
{
    const auto key1 = makeKey("data/file-a.parquet");
    const auto key2 = makeKey("data/file-a.parquet");

    ASSERT_TRUE(key1.has_value());
    ASSERT_TRUE(key2.has_value());
    EXPECT_EQ(*key1, *key2);
    EXPECT_EQ(PuffinFilesCacheKeyHash{}(*key1), PuffinFilesCacheKeyHash{}(*key2));
}

TEST(PuffinFilesCacheKey, DifferentExpectedCardinalityProducesUnequalKeys)
{
    const auto key1 = makeKey("data/file-a.parquet", /*expected_cardinality=*/2);
    const auto key2 = makeKey("data/file-a.parquet", /*expected_cardinality=*/3);

    ASSERT_TRUE(key1.has_value());
    ASSERT_TRUE(key2.has_value());
    EXPECT_NE(*key1, *key2);
    EXPECT_NE(PuffinFilesCacheKeyHash{}(*key1), PuffinFilesCacheKeyHash{}(*key2));
}

TEST(PuffinFilesCacheKey, DifferentDataFileRecordCountProducesUnequalKeys)
{
    const auto key1 = makeKey("data/file-a.parquet", /*expected_cardinality=*/2, /*data_file_record_count=*/100);
    const auto key2 = makeKey("data/file-a.parquet", /*expected_cardinality=*/2, /*data_file_record_count=*/50);

    ASSERT_TRUE(key1.has_value());
    ASSERT_TRUE(key2.has_value());
    EXPECT_NE(*key1, *key2);
    EXPECT_NE(PuffinFilesCacheKeyHash{}(*key1), PuffinFilesCacheKeyHash{}(*key2));
}

TEST(PuffinFilesCacheKey, EmptyEtagReturnsNullopt)
{
    const auto key = PuffinFilesCache::tryCreateKey("puffin.bin", "", 100, 200, "data/file-a.parquet", 2, 100);
    EXPECT_FALSE(key.has_value());
}
