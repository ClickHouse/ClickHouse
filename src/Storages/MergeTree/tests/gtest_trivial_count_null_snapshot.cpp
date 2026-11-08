#include <gtest/gtest.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/SharedThreadPools.h>
#include <Parsers/ASTFunction.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageSnapshot.h>
#include <Common/CurrentThread.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

namespace DB::Setting
{
extern const SettingsBool apply_mutations_on_fly;
extern const SettingsBool apply_patch_parts;
}

/// Test that MergeTreeData::supportsTrivialCountOptimization does not crash
/// when the storage snapshot has a SnapshotData with null mutations_snapshot.
///
/// This is a regression test for a SEGFAULT (Cloud incident #1317) where
/// `createStorageSnapshot(metadata, context, /*without_data=*/true)` produces
/// a SnapshotData with `mutations_snapshot = nullptr`, and a code path in the
/// Planner (subquery building for sets) could pass such a snapshot to
/// `supportsTrivialCountOptimization`, which dereferenced it unconditionally.

TEST(SupportsTrivialCountOptimization, NullMutationsSnapshot)
{
    using namespace DB;

    MainThreadStatus::getInstance();
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getUnexpectedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();

    const auto & context_holder = getContext();
    auto context = Context::createCopy(context_holder.context);

    /// Build minimal StorageInMemoryMetadata for a MergeTree with one UInt64 column and tuple() ORDER BY.
    StorageInMemoryMetadata metadata;

    ColumnsDescription columns;
    columns.add(ColumnDescription("a", std::make_shared<DataTypeUInt64>()));
    metadata.setColumns(columns);

    /// ORDER BY tuple() — empty sorting key.
    auto order_by_ast = makeASTFunction("tuple");
    metadata.sorting_key = KeyDescription::getSortingKeyFromAST(order_by_ast, metadata.columns, context, {});
    metadata.primary_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, context);
    metadata.primary_key.definition_ast = nullptr;

    /// PARTITION BY — empty partition key.
    metadata.partition_key = KeyDescription::getKeyFromAST(nullptr, metadata.columns, context);

    auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
    auto partition_key = metadata.partition_key.expression_list_ast->clone();
    metadata.minmax_count_projection.emplace(
        ProjectionDescription::getMinMaxCountProjection(columns, partition_key, minmax_columns, metadata.primary_key, context));

    auto storage_settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());

    /// Create the StorageMergeTree with ATTACH mode to skip sanity checks.
    auto storage = std::make_shared<StorageMergeTree>(
        StorageID("test_db", "test_table"),
        "store/test_trivial_count/",
        metadata,
        LoadingStrictnessLevel::ATTACH,
        context,
        /*date_column_name=*/"",
        MergeTreeData::MergingParams{},
        std::move(storage_settings));

    auto metadata_snapshot = storage->getInMemoryMetadataPtr();

    /// Case 1: Null StorageSnapshot entirely (already handled by existing code before our fix).
    {
        StorageSnapshotPtr null_snapshot = nullptr;
        EXPECT_NO_THROW(storage->supportsTrivialCountOptimization(null_snapshot, context));
    }

    /// Case 2: SnapshotData with null mutations_snapshot — this is the crash scenario.
    /// createStorageSnapshot(metadata, context, /*without_data=*/true) produces exactly this.
    {
        auto snapshot_data = std::make_unique<MergeTreeData::SnapshotData>();
        /// mutations_snapshot is default-constructed to nullptr.
        ASSERT_EQ(snapshot_data->mutations_snapshot, nullptr);

        auto snapshot = std::make_shared<StorageSnapshot>(*storage, metadata_snapshot, std::move(snapshot_data));
        EXPECT_NO_THROW(storage->supportsTrivialCountOptimization(snapshot, context));

        /// With default settings (apply_mutations_on_fly=false, apply_patch_parts=true),
        /// the function should return false because !true == false.
        auto snapshot_data2 = std::make_unique<MergeTreeData::SnapshotData>();
        auto snapshot2 = std::make_shared<StorageSnapshot>(*storage, metadata_snapshot, std::move(snapshot_data2));
        EXPECT_FALSE(storage->supportsTrivialCountOptimization(snapshot2, context));
    }

    /// Case 3: SnapshotData with null mutations_snapshot and apply_patch_parts=false.
    {
        auto modified_context = Context::createCopy(context);
        modified_context->setSetting("apply_patch_parts", Field(false));
        modified_context->setSetting("apply_mutations_on_fly", Field(false));

        auto snapshot_data = std::make_unique<MergeTreeData::SnapshotData>();
        auto snapshot = std::make_shared<StorageSnapshot>(*storage, metadata_snapshot, std::move(snapshot_data));
        EXPECT_TRUE(storage->supportsTrivialCountOptimization(snapshot, modified_context));
    }

    /// Case 4: Verify the real getStorageSnapshotWithoutData path produces the same.
    {
        auto without_data_snapshot = storage->getStorageSnapshotWithoutData(metadata_snapshot, context);
        ASSERT_NE(without_data_snapshot, nullptr);
        ASSERT_NE(without_data_snapshot->data, nullptr);

        const auto & data = assert_cast<const MergeTreeData::SnapshotData &>(*without_data_snapshot->data);
        EXPECT_EQ(data.mutations_snapshot, nullptr);

        EXPECT_NO_THROW(storage->supportsTrivialCountOptimization(without_data_snapshot, context));
    }

    storage->flushAndShutdown();
}
