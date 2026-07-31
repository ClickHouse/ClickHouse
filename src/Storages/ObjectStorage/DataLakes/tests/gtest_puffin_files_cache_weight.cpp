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
    const auto key = PuffinFilesCache::tryCreateKey("puffin.bin", "etag-1", 100, 200, "data/file-a.parquet", 33, 1000);
    ASSERT_TRUE(key.has_value());

    PuffinFilesCache cache("SLRU", 1'000'000, 100, 0.5);
    cache.getOrSetDeletionVector(*key, [&]() { return excluded_rows; });

    EXPECT_GE(CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheBytes), excluded_rows->getAllocatedBytes());
}

TEST(PuffinFilesCacheWeight, EvictsSparseBitmapsUnderSmallByteLimit)
{
    /// Each sparse entry weighs far more than cardinality × sizeof(size_t); a tiny byte
    /// limit must not retain many of them if weight uses allocated bytes.
    const auto entry_weight = makeLargeSparseExcludedRows(16)->getAllocatedBytes();
    ASSERT_GT(entry_weight, 0u);

    const size_t max_bytes = static_cast<size_t>(entry_weight) + 200; // roughly one entry + overhead
    PuffinFilesCache cache("SLRU", max_bytes, /*max_count=*/100, /*size_ratio=*/0.5);

    for (size_t i = 0; i < 8; ++i)
    {
        const auto key = PuffinFilesCache::tryCreateKey(
            "puffin.bin",
            "etag-" + std::to_string(i),
            100,
            200,
            "data/file-" + std::to_string(i) + ".parquet",
            16,
            1000);
        ASSERT_TRUE(key.has_value());
        cache.getOrSetDeletionVector(*key, [&]() { return makeLargeSparseExcludedRows(16); });
    }

    EXPECT_LE(CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheBytes), static_cast<Int64>(max_bytes));
    EXPECT_LT(CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheFiles), 8);
}
