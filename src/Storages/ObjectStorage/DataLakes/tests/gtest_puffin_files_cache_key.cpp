#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>

using namespace DB;

namespace
{

constexpr const char * kDefaultStorageIdentity = "Local:///test-prefix";

std::optional<PuffinFilesCacheKey> makeKey(
    const String & referenced_data_file,
    UInt64 expected_cardinality = 2,
    UInt64 data_file_record_count = 100,
    const String & storage_identity = kDefaultStorageIdentity)
{
    return PuffinFilesCache::tryCreateKey(
        storage_identity,
        "puffin.bin",
        "etag-1",
        100,
        200,
        referenced_data_file,
        expected_cardinality,
        data_file_record_count);
}

DataLakeObjectMetadata::ExcludedRowsPtr makeExcludedRows(const std::vector<size_t> & positions)
{
    auto excluded_rows = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
    for (size_t position : positions)
        excluded_rows->add(position);
    return excluded_rows;
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

TEST(PuffinFilesCacheKey, DifferentStorageIdentityProducesUnequalKeys)
{
    const auto key1 = makeKey("data/file-a.parquet", 2, 100, "S3://bucket-a/warehouse");
    const auto key2 = makeKey("data/file-a.parquet", 2, 100, "S3://bucket-b/warehouse");

    ASSERT_TRUE(key1.has_value());
    ASSERT_TRUE(key2.has_value());
    EXPECT_NE(*key1, *key2);
    EXPECT_NE(PuffinFilesCacheKeyHash{}(*key1), PuffinFilesCacheKeyHash{}(*key2));
}

TEST(PuffinFilesCacheKey, EmptyEtagReturnsNullopt)
{
    const auto key = PuffinFilesCache::tryCreateKey(
        kDefaultStorageIdentity, "puffin.bin", "", 100, 200, "data/file-a.parquet", 2, 100);
    EXPECT_FALSE(key.has_value());
}

TEST(PuffinFilesCacheKey, DifferentStorageIdentityDoesNotHitShare)
{
    PuffinFilesCache cache("SLRU", 1'000'000, 100, 0.5);

    const auto key_a = makeKey("data/file-a.parquet", 2, 100, "S3://bucket-a/warehouse");
    const auto key_b = makeKey("data/file-a.parquet", 2, 100, "S3://bucket-b/warehouse");
    ASSERT_TRUE(key_a.has_value());
    ASSERT_TRUE(key_b.has_value());

    size_t load_a_calls = 0;
    size_t load_b_calls = 0;

    auto first = cache.getOrSetDeletionVector(*key_a, [&]()
    {
        ++load_a_calls;
        return makeExcludedRows({1, 2});
    });

    auto second = cache.getOrSetDeletionVector(*key_b, [&]()
    {
        ++load_b_calls;
        return makeExcludedRows({10, 20});
    });

    ASSERT_EQ(load_a_calls, 1);
    ASSERT_EQ(load_b_calls, 1);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_TRUE(first->rb_contains(1));
    EXPECT_FALSE(first->rb_contains(10));
    EXPECT_TRUE(second->rb_contains(10));
    EXPECT_FALSE(second->rb_contains(1));

    auto third = cache.getOrSetDeletionVector(*key_b, [&]()
    {
        ++load_b_calls;
        return makeExcludedRows({99});
    });

    ASSERT_EQ(load_b_calls, 1);
    ASSERT_TRUE(third);
    EXPECT_TRUE(third->rb_contains(10));
    EXPECT_FALSE(third->rb_contains(99));
}
