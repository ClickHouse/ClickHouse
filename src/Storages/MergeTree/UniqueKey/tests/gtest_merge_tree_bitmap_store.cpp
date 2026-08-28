#include <gtest/gtest.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/SharedThreadPools.h>
#include <Parsers/ASTFunction.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <Storages/MergeTree/UniqueKey/MergeTreeBitmapStore.h>
#include <Storages/MergeTree/UniqueKey/tests/gtest_part_storage_fixture.h>
#include <Storages/StorageMergeTree.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <algorithm>
#include <filesystem>
#include <memory>
#include <string>

using namespace DB;

namespace
{

/// The store asks the table three questions -- resolve a part, the format version, whether the
/// outdated load has finished -- so it needs a real `MergeTreeData`. Attached, never started: no
/// background task runs and the part set stays empty.
struct TableFixture
{
    ContextMutablePtr context;
    std::shared_ptr<StorageMergeTree> table;

    TableFixture()
    {
        MainThreadStatus::getInstance();
        tryRegisterFunctions();
        tryRegisterAggregateFunctions();
        getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getUnexpectedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();

        context = Context::createCopy(getContext().context);

        StorageInMemoryMetadata metadata;
        ColumnsDescription columns;
        columns.add(ColumnDescription("id", std::make_shared<DataTypeUInt64>()));
        metadata.setColumns(columns);

        auto order_by_ast = makeASTFunction("tuple");
        metadata.sorting_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
        metadata.primary_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
        metadata.primary_key.definition_ast = nullptr;
        metadata.partition_key = KeyDescription::getKeyFromAST(nullptr, metadata.columns, {}, context);

        auto partition_key = metadata.partition_key.expression_list_ast->clone();
        metadata.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
            columns, partition_key, metadata.getColumnsRequiredForPartitionKey(),
            metadata.primary_key, &metadata.partition_key, context));

        table = std::make_shared<StorageMergeTree>(
            StorageID("test_db", "uk_bitmap_store"),
            "store/test_uk_bitmap_store/",
            metadata,
            LoadingStrictnessLevel::ATTACH,
            context,
            /*date_column_name=*/"",
            MergeTreeData::MergingParams{},
            std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings()));
    }

    MergeTreePartInfo partInfo(const std::string & name) const
    {
        return MergeTreePartInfo::fromPartName(name, table->format_version);
    }
};

DeleteBitmap bitmapWithRow(UInt64 row)
{
    DeleteBitmap bitmap;
    bitmap.add(row);
    return bitmap;
}

}

TEST(MergeTreeBitmapStoreTest, UnknownPartReadsEmptyAtVersionZero)
{
    TableFixture tbl;
    MergeTreeBitmapStore store{*tbl.table, /*cache=*/nullptr};

    /// No index entry means no version, and no version really is "no rows deleted".
    const auto [bitmap, csn] = store.readBitmap(tbl.partInfo("all_1_1_0"), UNBOUNDED_CSN);
    ASSERT_NE(bitmap, nullptr);
    EXPECT_TRUE(bitmap->empty());
    EXPECT_EQ(csn, 0u);
}

TEST(MergeTreeBitmapStoreTest, LoadPartIndexesEverySettledVersionOnce)
{
    TableFixture tbl;
    PartStorageFixture fx("settled");
    MergeTreeBitmapStore store{*tbl.table, /*cache=*/nullptr};

    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, /*version=*/12, bitmapWithRow(1));
    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, /*version=*/7, bitmapWithRow(2));

    const auto info = tbl.partInfo("all_1_1_0");
    auto added = store.loadPart(info, *fx.storage);
    std::sort(added.begin(), added.end());
    EXPECT_EQ(added, std::vector<CSN>({7, 12}));

    /// Idempotent: a second announce of the same directory adds nothing.
    EXPECT_TRUE(store.loadPart(info, *fx.storage).empty());
}

TEST(MergeTreeBitmapStoreTest, LoadPartRegistersStagedTargets)
{
    TableFixture tbl;
    PartStorageFixture fx("staged");
    MergeTreeBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto owner = tbl.partInfo("all_9_9_0");
    const auto target = tbl.partInfo("all_1_1_0");
    DeleteBitmapFileOps::writeStagedBitmapToStorage(*fx.storage, target.getPartNameV1(), bitmapWithRow(3));

    /// A staged file is a version of the target, not of the part holding it.
    EXPECT_TRUE(store.loadPart(owner, *fx.storage).empty());
    EXPECT_EQ(store.stagedTargetsOf(owner), std::vector<MergeTreePartInfo>({target}));
}

TEST(MergeTreeBitmapStoreTest, ForgettingStagedBitmapsClearsBothDirections)
{
    TableFixture tbl;
    MergeTreeBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto owner = tbl.partInfo("all_9_9_0");
    const auto target = tbl.partInfo("all_1_1_0");
    store.registerStagedBitmaps(owner, {target});
    EXPECT_EQ(store.stagedTargetsOf(owner), std::vector<MergeTreePartInfo>({target}));

    store.forgetStagedBitmaps(owner, {target});
    EXPECT_TRUE(store.stagedTargetsOf(owner).empty());

    /// The target's side is gone too, so resolving it no longer reaches the owner at all.
    const auto [bitmap, csn] = store.readBitmap(target, UNBOUNDED_CSN);
    EXPECT_TRUE(bitmap->empty());
    EXPECT_EQ(csn, 0u);
}

/// A LOGICAL_ERROR aborts rather than throwing under debug and sanitizer builds, so the two
/// rejections below are only assertable where it stays an exception.
#ifndef DEBUG_OR_SANITIZER_BUILD

TEST(MergeTreeBitmapStoreTest, IndexedVersionWhosePartIsGoneIsRejected)
{
    TableFixture tbl;
    PartStorageFixture fx("orphan_version");
    MergeTreeBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto info = tbl.partInfo("all_1_1_0");
    DeleteBitmapFileOps::writeBitmapToStorage(*fx.storage, /*version=*/7, bitmapWithRow(1));
    ASSERT_EQ(store.loadPart(info, *fx.storage).size(), 1u);

    /// The index holds version 7 and the part set holds nothing. An empty bitmap here would report
    /// "no rows deleted" for a version that kills one, so INV-LOCATION makes it an error.
    EXPECT_THROW(store.readBitmap(info, UNBOUNDED_CSN), Exception);
}

TEST(MergeTreeBitmapStoreTest, StagedOwnerThatLeftThePartSetIsRejected)
{
    TableFixture tbl;
    MergeTreeBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto owner = tbl.partInfo("all_9_9_0");
    const auto target = tbl.partInfo("all_1_1_0");
    store.registerStagedBitmaps(owner, {target});

    /// INV-SETTLE is what keeps a staging owner in the part set, so an owner that is not there is a
    /// divergence -- and skipping it would silently drop whatever the staged bitmap kills.
    EXPECT_THROW(store.readBitmap(target, UNBOUNDED_CSN), Exception);
}

#endif
