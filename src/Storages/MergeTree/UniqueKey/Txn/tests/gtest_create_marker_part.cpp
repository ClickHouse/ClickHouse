#include <gtest/gtest.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyMarkerPart.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyManifest.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyTxnTypes.h>
#include <Storages/MergeTree/UniqueKey/Txn/tests/gtest_uk_storage_harness.h>
#include <Storages/StorageMergeTree.h>

using namespace DB;
using namespace DB::UniqueKeyTxn;
using namespace DB::UniqueKeyTxn::tests;

/// Smoke gate: a marker part is named `all_<bn>_<bn>_0` and carries a
/// `unique_key.txt` manifest with `is_marker = true` and the supplied
/// `creation_csn` + `bitmaps_created`.
TEST(CreateMarkerPart, ZeroRowPartWithMarkerManifest)
{
    UKStorageHarness h({.with_unique_key = false, .table_name = "test_marker_part", .relative_path = "store/test_marker_part/"});
    constexpr Int64 bn = 7;

    UniqueKeyManifest meta;
    meta.creation_csn = INVALID_CSN;   /// commit driver rewrites with real csn at publish
    meta.is_marker = true;
    meta.bitmaps_created = {
        {"all_1_1_0", 42},
        {"all_3_3_0", 42},
    };

    auto handle = createMarkerPart(*h.storage, /*partition_id=*/"all", bn, MergeTreePartition{}, meta);
    ASSERT_NE(handle.data_part, nullptr);

    /// Part shape: `all_<bn>_<bn>_0`, 0 rows.
    EXPECT_EQ(handle.data_part->name, "all_7_7_0");
    EXPECT_EQ(handle.data_part->rows_count, 0u);
    EXPECT_EQ(handle.data_part->info.getPartitionId(), "all");
    EXPECT_EQ(handle.data_part->info.min_block, bn);
    EXPECT_EQ(handle.data_part->info.max_block, bn);
    EXPECT_EQ(handle.data_part->info.level, 0u);

    /// Manifest written into the tmp dir before publish — the commit driver
    /// owns the rename, so `getFullPath()` is still under `tmp_empty_<name>/`.
    auto & part_storage = handle.data_part->getDataPartStorage();
    ASSERT_TRUE(UniqueKeyManifest::exists(part_storage))
        << "unique_key.txt missing under " << part_storage.getFullPath();

    auto loaded = UniqueKeyManifest::read(part_storage);
    EXPECT_EQ(loaded.creation_csn, INVALID_CSN);
    EXPECT_TRUE(loaded.is_marker);
    ASSERT_EQ(loaded.bitmaps_created.size(), 2u);
    EXPECT_EQ(loaded.bitmaps_created[0].first, "all_1_1_0");
    EXPECT_EQ(loaded.bitmaps_created[0].second, 42u);
    EXPECT_EQ(loaded.bitmaps_created[1].first, "all_3_3_0");
    EXPECT_EQ(loaded.bitmaps_created[1].second, 42u);

    /// In-memory lightweight cache must mirror the manifest scalars: a marker
    /// published in the same process must not appear legacy to
    /// CSN-seed-collection (which reads `getUniqueKeyMeta`). The lazy disk read
    /// only runs at attach; the primitive itself seeds the in-memory cache, so
    /// `getUniqueKeyMeta` returns a value without touching disk. The full
    /// `bitmaps_created` list is no longer retained in RAM — it is asserted on
    /// disk above.
    auto cached = handle.data_part->getUniqueKeyMeta();
    ASSERT_TRUE(cached.has_value());
    EXPECT_TRUE(cached->is_marker);
    EXPECT_EQ(cached->creation_csn, INVALID_CSN);
}

