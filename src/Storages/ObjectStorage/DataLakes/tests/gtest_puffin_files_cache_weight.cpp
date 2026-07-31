#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Common/CurrentMetrics.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>
#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>

using namespace DB;

namespace CurrentMetrics
{
extern const Metric PuffinFilesCacheBytes;
}

namespace
{

DataLakeObjectMetadata::ExcludedRowsPtr makeLargeSparseExcludedRows()
{
    auto excluded_rows = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
    for (size_t i = 0; i < 33; ++i)
        excluded_rows->add((static_cast<UInt64>(i) + 1) << 32);
    return excluded_rows;
}

}

TEST(RoaringBitmapWithSmallSetMemory, LargeSparseBitmapAllocatedBytesExceedCardinalityEstimate)
{
    const auto excluded_rows = makeLargeSparseExcludedRows();

    ASSERT_TRUE(excluded_rows->isLarge());
    EXPECT_GT(excluded_rows->getAllocatedBytes(), excluded_rows->size() * sizeof(size_t));
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
