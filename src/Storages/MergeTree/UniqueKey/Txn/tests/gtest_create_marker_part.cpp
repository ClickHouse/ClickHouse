#include <gtest/gtest.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyMarkerPart.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyManifest.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyTxnTypes.h>
#include <Storages/StorageMergeTree.h>

#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;
using namespace DB::UniqueKeyTxn;

namespace
{

/// Minimal `StorageMergeTree` harness. Same shape as
/// `gtest_storage_merge_tree_committing_blocks.cpp::StorageHarness` —
/// `LoadingStrictnessLevel::ATTACH` skips sanity / DDL checks the marker-part
/// primitive doesn't exercise. The harness is enough to drive
/// `MergeTreeData::createEmptyPart`, which is what `createMarkerPart`
/// delegates to.
struct StorageHarness
{
    ContextMutablePtr context;
    std::shared_ptr<StorageMergeTree> storage;
    StorageInMemoryMetadata metadata;

    StorageHarness()
    {
        MainThreadStatus::getInstance();
        tryRegisterFunctions();
        tryRegisterAggregateFunctions();

        getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getUnexpectedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();

        const auto & context_holder = getContext();
        context = Context::createCopy(context_holder.context);

        ColumnsDescription columns;
        columns.add(ColumnDescription("a", std::make_shared<DataTypeUInt64>()));
        metadata.setColumns(columns);

        auto order_by_ast = makeASTFunction("tuple");
        metadata.sorting_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
        metadata.primary_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
        metadata.primary_key.definition_ast = nullptr;
        metadata.partition_key = KeyDescription::getKeyFromAST(nullptr, metadata.columns, {}, context);

        auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
        auto partition_key = metadata.partition_key.expression_list_ast->clone();
        metadata.minmax_count_projection.emplace(
            ProjectionDescription::getMinMaxCountProjection(
                columns, partition_key, minmax_columns, metadata.primary_key, &metadata.partition_key, context));

        auto storage_settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());

        storage = std::make_shared<StorageMergeTree>(
            StorageID("test_db", "test_marker_part"),
            "store/test_marker_part/",
            metadata,
            LoadingStrictnessLevel::ATTACH,
            context,
            /*date_column_name=*/"",
            MergeTreeData::MergingParams{},
            std::move(storage_settings));
    }

    ~StorageHarness()
    {
        if (storage)
            storage->flushAndShutdown();
    }
};

}

/// Smoke gate: a marker part is named `all_<bn>_<bn>_0` and carries a
/// `unique_key.txt` manifest with `is_marker = true` and the supplied
/// `creation_csn` + `bitmaps_created`.
TEST(CreateMarkerPart, ZeroRowPartWithMarkerManifest)
{
    StorageHarness h;
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

