#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <IO/ReadBufferFromMemory.h>
#include <Storages/ObjectStorage/DataLakes/PuffinDeletionVectorReader.h>
#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>

using namespace DB;

namespace ProfileEvents
{
extern const Event PuffinFilesRead;
}

namespace CurrentMetrics
{
extern const Metric PuffinFilesCacheBytes;
extern const Metric PuffinFilesCacheFiles;
}

namespace
{

/// Two equal-cardinality deletion-vector-v1 blobs for different data files.
constexpr UInt8 two_equal_cardinality_dvs_puffin[] = {
    0x50, 0x46, 0x41, 0x31, 0x00, 0x00, 0x00, 0x24, 0xD1, 0xD3, 0x39, 0x64, 0x01, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x10, 0x00, 0x00, 0x00, 0x02, 0x00, 0x05, 0x00, 0x2C, 0xDB, 0x9F, 0xC1,
    0x00, 0x00, 0x00, 0x24, 0xD1, 0xD3, 0x39, 0x64, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x3A, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x10, 0x00, 0x00, 0x00, 0x07, 0x00, 0x09, 0x00, 0xB7, 0xB0, 0x20, 0xFF, 0x50, 0x46, 0x41, 0x31,
    0x7B, 0x22, 0x62, 0x6C, 0x6F, 0x62, 0x73, 0x22, 0x3A, 0x20, 0x5B, 0x7B, 0x22, 0x74, 0x79, 0x70,
    0x65, 0x22, 0x3A, 0x20, 0x22, 0x64, 0x65, 0x6C, 0x65, 0x74, 0x69, 0x6F, 0x6E, 0x2D, 0x76, 0x65,
    0x63, 0x74, 0x6F, 0x72, 0x2D, 0x76, 0x31, 0x22, 0x2C, 0x20, 0x22, 0x66, 0x69, 0x65, 0x6C, 0x64,
    0x73, 0x22, 0x3A, 0x20, 0x5B, 0x5D, 0x2C, 0x20, 0x22, 0x73, 0x6E, 0x61, 0x70, 0x73, 0x68, 0x6F,
    0x74, 0x2D, 0x69, 0x64, 0x22, 0x3A, 0x20, 0x2D, 0x31, 0x2C, 0x20, 0x22, 0x73, 0x65, 0x71, 0x75,
    0x65, 0x6E, 0x63, 0x65, 0x2D, 0x6E, 0x75, 0x6D, 0x62, 0x65, 0x72, 0x22, 0x3A, 0x20, 0x2D, 0x31,
    0x2C, 0x20, 0x22, 0x6F, 0x66, 0x66, 0x73, 0x65, 0x74, 0x22, 0x3A, 0x20, 0x34, 0x2C, 0x20, 0x22,
    0x6C, 0x65, 0x6E, 0x67, 0x74, 0x68, 0x22, 0x3A, 0x20, 0x34, 0x34, 0x2C, 0x20, 0x22, 0x70, 0x72,
    0x6F, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x22, 0x3A, 0x20, 0x7B, 0x22, 0x72, 0x65, 0x66,
    0x65, 0x72, 0x65, 0x6E, 0x63, 0x65, 0x64, 0x2D, 0x64, 0x61, 0x74, 0x61, 0x2D, 0x66, 0x69, 0x6C,
    0x65, 0x22, 0x3A, 0x20, 0x22, 0x2F, 0x64, 0x61, 0x74, 0x61, 0x2F, 0x66, 0x69, 0x6C, 0x65, 0x5F,
    0x61, 0x2E, 0x70, 0x61, 0x72, 0x71, 0x75, 0x65, 0x74, 0x22, 0x2C, 0x20, 0x22, 0x63, 0x61, 0x72,
    0x64, 0x69, 0x6E, 0x61, 0x6C, 0x69, 0x74, 0x79, 0x22, 0x3A, 0x20, 0x22, 0x32, 0x22, 0x7D, 0x7D,
    0x2C, 0x20, 0x7B, 0x22, 0x74, 0x79, 0x70, 0x65, 0x22, 0x3A, 0x20, 0x22, 0x64, 0x65, 0x6C, 0x65,
    0x74, 0x69, 0x6F, 0x6E, 0x2D, 0x76, 0x65, 0x63, 0x74, 0x6F, 0x72, 0x2D, 0x76, 0x31, 0x22, 0x2C,
    0x20, 0x22, 0x66, 0x69, 0x65, 0x6C, 0x64, 0x73, 0x22, 0x3A, 0x20, 0x5B, 0x5D, 0x2C, 0x20, 0x22,
    0x73, 0x6E, 0x61, 0x70, 0x73, 0x68, 0x6F, 0x74, 0x2D, 0x69, 0x64, 0x22, 0x3A, 0x20, 0x2D, 0x31,
    0x2C, 0x20, 0x22, 0x73, 0x65, 0x71, 0x75, 0x65, 0x6E, 0x63, 0x65, 0x2D, 0x6E, 0x75, 0x6D, 0x62,
    0x65, 0x72, 0x22, 0x3A, 0x20, 0x2D, 0x31, 0x2C, 0x20, 0x22, 0x6F, 0x66, 0x66, 0x73, 0x65, 0x74,
    0x22, 0x3A, 0x20, 0x34, 0x38, 0x2C, 0x20, 0x22, 0x6C, 0x65, 0x6E, 0x67, 0x74, 0x68, 0x22, 0x3A,
    0x20, 0x34, 0x34, 0x2C, 0x20, 0x22, 0x70, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73,
    0x22, 0x3A, 0x20, 0x7B, 0x22, 0x72, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6E, 0x63, 0x65, 0x64, 0x2D,
    0x64, 0x61, 0x74, 0x61, 0x2D, 0x66, 0x69, 0x6C, 0x65, 0x22, 0x3A, 0x20, 0x22, 0x2F, 0x64, 0x61,
    0x74, 0x61, 0x2F, 0x66, 0x69, 0x6C, 0x65, 0x5F, 0x62, 0x2E, 0x70, 0x61, 0x72, 0x71, 0x75, 0x65,
    0x74, 0x22, 0x2C, 0x20, 0x22, 0x63, 0x61, 0x72, 0x64, 0x69, 0x6E, 0x61, 0x6C, 0x69, 0x74, 0x79,
    0x22, 0x3A, 0x20, 0x22, 0x32, 0x22, 0x7D, 0x7D, 0x5D, 0x7D, 0x9A, 0x01, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x50, 0x46, 0x41, 0x31,
};

PuffinFilesCache::FooterBlobsPtr loadFixtureFooter()
{
    ReadBufferFromMemory file(two_equal_cardinality_dvs_puffin, sizeof(two_equal_cardinality_dvs_puffin));
    return std::make_shared<const std::vector<PuffinBlob>>(
        readPuffinFooterBlobsFromSeekable(file, sizeof(two_equal_cardinality_dvs_puffin)));
}

}

TEST(PuffinFooterMemo, CoalescedSlicesShareOneFooterParse)
{
    PuffinFilesCache cache("SLRU", 1'000'000, 100, 0.5);

    const auto footer_key = PuffinFilesCache::tryCreateFooterKey("Local:////test", "coalesced.puffin", "etag-1");
    ASSERT_TRUE(footer_key.has_value());

    const auto key_a = PuffinFilesCache::tryCreateKey(
        "Local:////test", "coalesced.puffin", "etag-1", 4, 44, "/data/file_a.parquet", 2, 100);
    const auto key_b = PuffinFilesCache::tryCreateKey(
        "Local:////test", "coalesced.puffin", "etag-1", 48, 44, "/data/file_b.parquet", 2, 100);
    ASSERT_TRUE(key_a.has_value());
    ASSERT_TRUE(key_b.has_value());

    auto & counters = ProfileEvents::global_counters;
    const auto files_read_before = counters[ProfileEvents::PuffinFilesRead].load();

    size_t footer_loads = 0;
    auto load_footer = [&]()
    {
        ++footer_loads;
        return loadFixtureFooter();
    };

    /// Cold scan of two DV slices from one coalesced Puffin: one footer parse, two bitmap loads.
    cache.getOrSetDeletionVector(*key_a, [&]()
    {
        auto footer = cache.getOrSetFooter(*footer_key, load_footer);
        EXPECT_EQ(footer->size(), 2u);
        bindDeletionVectorBlob(*footer, 4, 44, "/data/file_a.parquet", 2);
        ReadBufferFromMemory file(two_equal_cardinality_dvs_puffin, sizeof(two_equal_cardinality_dvs_puffin));
        const auto positions = readDeletionVectorFromPuffin(file, 4, 44, 2);
        auto excluded = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
        for (UInt64 position : positions)
            excluded->add(static_cast<size_t>(position));
        return excluded;
    });

    cache.getOrSetDeletionVector(*key_b, [&]()
    {
        auto footer = cache.getOrSetFooter(*footer_key, load_footer);
        EXPECT_EQ(footer->size(), 2u);
        bindDeletionVectorBlob(*footer, 48, 44, "/data/file_b.parquet", 2);
        ReadBufferFromMemory file(two_equal_cardinality_dvs_puffin, sizeof(two_equal_cardinality_dvs_puffin));
        const auto positions = readDeletionVectorFromPuffin(file, 48, 44, 2);
        auto excluded = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
        for (UInt64 position : positions)
            excluded->add(static_cast<size_t>(position));
        return excluded;
    });

    EXPECT_EQ(footer_loads, 1u);
    /// One footer parse (`PuffinFilesRead` in readPuffinFooter) plus two blob reads.
    EXPECT_EQ(counters[ProfileEvents::PuffinFilesRead].load() - files_read_before, 3u);
}

TEST(PuffinFooterMemo, ClearDropsFooterEntries)
{
    PuffinFilesCache cache("SLRU", 1'000'000, 100, 0.5);
    const auto footer_key = PuffinFilesCache::tryCreateFooterKey("Local:////test", "coalesced.puffin", "etag-1");
    ASSERT_TRUE(footer_key.has_value());

    size_t footer_loads = 0;
    auto load_footer = [&]()
    {
        ++footer_loads;
        return loadFixtureFooter();
    };

    ASSERT_NE(cache.getOrSetFooter(*footer_key, load_footer), nullptr);
    cache.clear();
    ASSERT_NE(cache.getOrSetFooter(*footer_key, load_footer), nullptr);
    EXPECT_EQ(footer_loads, 2u);
}

TEST(PuffinFooterMemo, SizeZeroDoesNotInsert)
{
    PuffinFilesCache cache("SLRU", /*max_size_in_bytes=*/0, 100, 0.5);
    const auto footer_key = PuffinFilesCache::tryCreateFooterKey("Local:////test", "coalesced.puffin", "etag-1");
    ASSERT_TRUE(footer_key.has_value());

    size_t footer_loads = 0;
    auto load_footer = [&]()
    {
        ++footer_loads;
        return loadFixtureFooter();
    };

    ASSERT_NE(cache.getOrSetFooter(*footer_key, load_footer), nullptr);
    ASSERT_NE(cache.getOrSetFooter(*footer_key, load_footer), nullptr);
    EXPECT_EQ(footer_loads, 2u);
    EXPECT_EQ(cache.footerMemoEntries(), 0u);
    EXPECT_EQ(cache.footerMemoBytes(), 0u);
}

TEST(PuffinFooterMemo, SizeZeroClearsMemo)
{
    PuffinFilesCache cache("SLRU", 1'000'000, 100, 0.5);
    const auto footer_key = PuffinFilesCache::tryCreateFooterKey("Local:////test", "coalesced.puffin", "etag-1");
    ASSERT_TRUE(footer_key.has_value());

    size_t footer_loads = 0;
    auto load_footer = [&]()
    {
        ++footer_loads;
        return loadFixtureFooter();
    };

    ASSERT_NE(cache.getOrSetFooter(*footer_key, load_footer), nullptr);
    EXPECT_EQ(footer_loads, 1u);
    EXPECT_EQ(cache.footerMemoEntries(), 1u);
    EXPECT_GT(cache.footerMemoBytes(), 0u);

    cache.setMaxSizeInBytes(0);
    EXPECT_EQ(cache.footerMemoEntries(), 0u);
    EXPECT_EQ(cache.footerMemoBytes(), 0u);

    ASSERT_NE(cache.getOrSetFooter(*footer_key, load_footer), nullptr);
    EXPECT_EQ(footer_loads, 2u);
    EXPECT_EQ(cache.footerMemoEntries(), 0u);

    cache.setMaxSizeInBytes(1'000'000);
    ASSERT_NE(cache.getOrSetFooter(*footer_key, load_footer), nullptr);
    ASSERT_NE(cache.getOrSetFooter(*footer_key, load_footer), nullptr);
    EXPECT_EQ(footer_loads, 3u);
    EXPECT_EQ(cache.footerMemoEntries(), 1u);
}

TEST(PuffinFooterMemo, ByteBudgetEvictsOnShrink)
{
    PuffinFilesCache cache("SLRU", 1'000'000, 100, 0.5);
    const auto footer_key = PuffinFilesCache::tryCreateFooterKey("Local:////test", "coalesced.puffin", "etag-1");
    ASSERT_TRUE(footer_key.has_value());

    size_t footer_loads = 0;
    auto load_footer = [&]()
    {
        ++footer_loads;
        return loadFixtureFooter();
    };

    ASSERT_NE(cache.getOrSetFooter(*footer_key, load_footer), nullptr);
    const UInt64 memo_bytes = cache.footerMemoBytes();
    ASSERT_GT(memo_bytes, 1u);

    /// Shrinking below current memo weight must drop retained footers.
    cache.setMaxSizeInBytes(1);
    EXPECT_EQ(cache.footerMemoEntries(), 0u);
    EXPECT_EQ(cache.footerMemoBytes(), 0u);

    ASSERT_NE(cache.getOrSetFooter(*footer_key, load_footer), nullptr);
    EXPECT_EQ(footer_loads, 2u);
    /// Single fixture footer exceeds a 1-byte budget, so it must not be re-inserted.
    EXPECT_EQ(cache.footerMemoEntries(), 0u);
}

TEST(PuffinFooterMemo, CountLimitEvictsOneEntryNotAll)
{
    PuffinFilesCache cache("SLRU", 1'000'000, /*max_count=*/1, 0.5);
    const auto key_a = PuffinFilesCache::tryCreateFooterKey("Local:////test", "a.puffin", "etag-a");
    const auto key_b = PuffinFilesCache::tryCreateFooterKey("Local:////test", "b.puffin", "etag-b");
    ASSERT_TRUE(key_a.has_value());
    ASSERT_TRUE(key_b.has_value());

    size_t footer_loads = 0;
    auto load_footer = [&]()
    {
        ++footer_loads;
        return loadFixtureFooter();
    };

    ASSERT_NE(cache.getOrSetFooter(*key_a, load_footer), nullptr);
    EXPECT_EQ(cache.footerMemoEntries(), 1u);

    ASSERT_NE(cache.getOrSetFooter(*key_b, load_footer), nullptr);
    /// Evict one victim for room — do not wipe the memo to empty before insert.
    EXPECT_EQ(cache.footerMemoEntries(), 1u);
    EXPECT_EQ(footer_loads, 2u);

    /// The retained entry must be key_b (key_a was the only victim).
    ASSERT_NE(cache.getOrSetFooter(*key_b, load_footer), nullptr);
    EXPECT_EQ(footer_loads, 2u);
}

TEST(PuffinFooterMemo, SharesBudgetAndMetricsWithDeletionVectors)
{
    /// Tiny shared budget: a resident DV must leave no room for a second full budget of footers.
    PuffinFilesCache cache("SLRU", /*max_size_in_bytes=*/50'000, /*max_count=*/100, 0.5);

    const auto footer_key = PuffinFilesCache::tryCreateFooterKey("Local:////test", "coalesced.puffin", "etag-1");
    ASSERT_TRUE(footer_key.has_value());

    const auto bytes_before = CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheBytes);
    const auto files_before = CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheFiles);

    ASSERT_NE(cache.getOrSetFooter(*footer_key, loadFixtureFooter), nullptr);
    const UInt64 memo_bytes = cache.footerMemoBytes();
    ASSERT_GT(memo_bytes, 0u);
    EXPECT_EQ(cache.footerMemoEntries(), 1u);
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheBytes), bytes_before + static_cast<Int64>(memo_bytes));
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheFiles), files_before + 1);

    /// Fill almost all remaining shared budget with a DV so the memo must be trimmed.
    auto excluded = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
    for (size_t i = 0; i < 2000; ++i)
        excluded->add(i);

    const auto dv_key = PuffinFilesCache::tryCreateKey(
        "Local:////test", "coalesced.puffin", "etag-1", 4, 44, "/data/file_a.parquet", 2, 100);
    ASSERT_TRUE(dv_key.has_value());

    cache.getOrSetDeletionVector(*dv_key, [&]() { return excluded; });

    /// After DV insert, footer memo is trimmed so DV + memo stay within configured max.
    EXPECT_LE(
        static_cast<size_t>(CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheBytes) - bytes_before),
        50'000u);
    EXPECT_LE(cache.footerMemoBytes() + cache.sizeInBytes(), 50'000u);

    cache.clear();
    EXPECT_EQ(cache.footerMemoEntries(), 0u);
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheBytes), bytes_before);
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::PuffinFilesCacheFiles), files_before);
}
