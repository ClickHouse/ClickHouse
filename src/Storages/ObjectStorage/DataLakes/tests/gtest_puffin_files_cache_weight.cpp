#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Common/CurrentMetrics.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>
#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>
#include <roaring/roaring64map.hh>

using namespace DB;

namespace CurrentMetrics
{
extern const Metric PuffinFilesCacheBytes;
extern const Metric PuffinFilesCacheFiles;
}

namespace
{

DataLakeObjectMetadata::ExcludedRowsPtr makeLargeSparseExcludedRows(size_t keys = 33)
{
    auto excluded_rows = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
    for (size_t i = 0; i < keys; ++i)
        excluded_rows->add((static_cast<UInt64>(i) + 1) << 32);
    return excluded_rows;
}

roaring::Roaring64Map makeLargeSparseRoaring64(size_t keys = 33)
{
    roaring::Roaring64Map bitmap;
    for (size_t i = 0; i < keys; ++i)
        bitmap.add((static_cast<UInt64>(i) + 1) << 32);
    return bitmap;
}

PuffinFilesCacheKey makeUniqueKey(size_t index, const String & long_suffix = "")
{
    auto key = PuffinFilesCache::tryCreateKey(
        "Local:////test-prefix",
        "puffin.bin",
        "etag-" + std::to_string(index) + long_suffix,
        100,
        200,
        "data/file-" + std::to_string(index) + ".parquet" + long_suffix,
        /*expected_cardinality=*/0,
        /*data_file_record_count=*/1000);
    EXPECT_TRUE(key.has_value());
    return *key;
}

}

TEST(RoaringBitmapWithSmallSetMemory, LargeSparseBitmapAllocatedBytesExceedCardinalityEstimate)
{
    const auto excluded_rows = makeLargeSparseExcludedRows();

    ASSERT_TRUE(excluded_rows->isLarge());
    EXPECT_GT(excluded_rows->getAllocatedBytes(), excluded_rows->size() * sizeof(size_t));
}

TEST(RoaringBitmapWithSmallSetMemory, LargeSparseBitmapAllocatedBytesExceedSerializedSize)
{
    const auto excluded_rows = makeLargeSparseExcludedRows();
    const auto serialized = makeLargeSparseRoaring64().getSizeInBytes(/*portable=*/true);

    ASSERT_TRUE(excluded_rows->isLarge());
    EXPECT_GT(excluded_rows->getAllocatedBytes(), serialized);
}

TEST(PuffinFilesCacheWeight, UsesRoaringAllocatedBytesForWeight)
{
    const auto excluded_rows = makeLargeSparseExcludedRows();
    const auto key = PuffinFilesCache::tryCreateKey(
        "Local:////test-prefix", "puffin.bin", "etag-1", 100, 200, "data/file-a.parquet", 33, 1000);
    ASSERT_TRUE(key.has_value());

    PuffinFilesCache cache("SLRU", 1'000'000, 100, 0.5);
    cache.getOrSetDeletionVector(*key, [&]() { return excluded_rows; });

    EXPECT_GE(CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheBytes), excluded_rows->getAllocatedBytes());
}

TEST(PuffinFilesCacheWeight, EvictsSparseBitmapsUnderSmallByteLimit)
{
    /// Each sparse entry weighs far more than cardinality × sizeof(size_t); a tiny byte
    /// limit must not retain many of them if weight uses allocated bytes.
    const auto sample_key = makeUniqueKey(0);
    const auto sample_rows = makeLargeSparseExcludedRows(16);
    const auto entry_weight = PuffinFilesCacheCell::calculateMemorySize(
        /*is_empty_deletion_vector_=*/false, sample_rows, sample_key.approximateMemoryBytes());
    ASSERT_GT(entry_weight, 0u);

    const size_t max_bytes = static_cast<size_t>(entry_weight) + 64; // roughly one entry
    PuffinFilesCache cache("SLRU", max_bytes, /*max_count=*/100, /*size_ratio=*/0.5);

    for (size_t i = 0; i < 8; ++i)
    {
        const auto key = makeUniqueKey(i);
        cache.getOrSetDeletionVector(key, [&]() { return makeLargeSparseExcludedRows(16); });
    }

    EXPECT_LE(CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheBytes), static_cast<Int64>(max_bytes));
    EXPECT_LT(CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheFiles), 8);
}

TEST(PuffinFilesCacheWeight, LongKeyEmptyEntriesEvictAtByteLimit)
{
    /// Empty DVs used to weigh 1 byte and ignore key strings. With an unlimited entry
    /// count, long unique keys must still be bounded by the configured byte limit.
    const String long_suffix(8 * 1024, 'x');
    const auto sample_key = makeUniqueKey(0, long_suffix);
    const auto entry_weight = PuffinFilesCacheCell::calculateMemorySize(
        /*is_empty_deletion_vector_=*/true, nullptr, sample_key.approximateMemoryBytes());
    ASSERT_GT(entry_weight, sample_key.approximateMemoryBytes());
    ASSERT_GT(entry_weight, 8 * 1024u);

    /// Allow roughly two long-key empty entries; inserting many more must evict.
    const size_t max_bytes = static_cast<size_t>(entry_weight) * 2 + 128;
    PuffinFilesCache cache("SLRU", max_bytes, /*max_count=*/0, /*size_ratio=*/0.5);

    for (size_t i = 0; i < 32; ++i)
    {
        const auto key = makeUniqueKey(i, long_suffix);
        cache.getOrSetDeletionVector(key, []() -> DataLakeObjectMetadata::ExcludedRowsPtr { return nullptr; });
    }

    EXPECT_LE(CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheBytes), static_cast<Int64>(max_bytes));
    EXPECT_LE(CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheFiles), 3);
}

TEST(PuffinFilesCacheWeight, EmptyEntryChargesKeyNotOneByte)
{
    const String long_path(4096, 'p');
    const auto key = PuffinFilesCache::tryCreateKey(
        "Local:////test-prefix", long_path, "etag-empty", 0, 0, long_path, 0, 0);
    ASSERT_TRUE(key.has_value());

    const auto weight = PuffinFilesCacheCell::calculateMemorySize(
        /*is_empty_deletion_vector_=*/true, nullptr, key->approximateMemoryBytes());
    EXPECT_GT(weight, key->approximateMemoryBytes());
    EXPECT_GE(weight, 2 * 4096u);
}

TEST(PuffinFilesCacheWeight, EstimateMinimumMatchesEmptyEntryWeight)
{
    const auto key = PuffinFilesCache::tryCreateKey(
        "Local:////test-prefix", "data/file.parquet", "etag-1", 4, 44, "/data/file.parquet", 2, 100);
    ASSERT_TRUE(key.has_value());

    const UInt64 key_bytes = key->approximateMemoryBytes();
    EXPECT_EQ(
        PuffinFilesCacheCell::estimateMinimumMemorySize(key_bytes),
        PuffinFilesCacheCell::calculateMemorySize(/*is_empty_deletion_vector_=*/true, nullptr, key_bytes));

    /// A 1-byte cache cannot hold the key/overhead lower bound for a realistic path.
    EXPECT_GT(PuffinFilesCacheCell::estimateMinimumMemorySize(key_bytes), 1u);
}
