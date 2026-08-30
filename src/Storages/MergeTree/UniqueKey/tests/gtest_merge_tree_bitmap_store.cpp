#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/InsertBlockInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <Storages/MergeTree/UniqueKey/MergeTreeBitmapStore.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyTxn.h>
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
/// background task runs, and the part set holds only what `addPart` puts there.
struct TableFixture
{
    /// A unique key gives the table its own `UniqueKeyTxnManager`, and so the store that
    /// `MergeTreeData` itself consults. The store tests build a standalone store instead.
    ContextMutablePtr context;
    std::shared_ptr<StorageMergeTree> table;
    std::string relative_path;

    explicit TableFixture(bool with_unique_key = false)
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

        /// A UNIQUE KEY has to sit on a real sorting key; nothing else here cares what it is.
        auto order_by_ast = with_unique_key
            ? makeASTFunction("tuple", make_intrusive<ASTIdentifier>("id"))
            : makeASTFunction("tuple");
        metadata.sorting_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
        metadata.primary_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
        metadata.primary_key.definition_ast = nullptr;
        metadata.partition_key = KeyDescription::getKeyFromAST(nullptr, metadata.columns, {}, context);

        if (with_unique_key)
            metadata.unique_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);

        auto partition_key = metadata.partition_key.expression_list_ast->clone();
        metadata.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
            columns, partition_key, metadata.getColumnsRequiredForPartitionKey(),
            metadata.primary_key, &metadata.partition_key, context));

        /// Per instance, not shared: `addPart` writes real files, and a fixed path would let one
        /// test's parts be loaded by the next test's `loadDataParts`.
        const auto unique_id
            = std::to_string(::getpid()) + "_" + std::to_string(reinterpret_cast<uintptr_t>(this));
        relative_path = "store/test_uk_bitmap_store_" + unique_id + "/";

        table = std::make_shared<StorageMergeTree>(
            StorageID("test_db", "uk_bitmap_store_" + unique_id),
            relative_path,
            metadata,
            LoadingStrictnessLevel::ATTACH,
            context,
            /*date_column_name=*/"",
            MergeTreeData::MergingParams{},
            std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings()));
    }

    ~TableFixture()
    {
        table.reset();
        std::error_code ec;
        std::filesystem::remove_all(std::filesystem::path(context->getPath()) / relative_path, ec);
    }

    MergeTreePartInfo partInfo(const std::string & name) const
    {
        return MergeTreePartInfo::fromPartName(name, table->format_version);
    }

    /// A real part in the Active set, under the name the caller asks for. Every settle and every
    /// sweep resolves its parts through `getPartIfExists`, so those paths cannot be reached from a
    /// fixture whose part set is empty -- unlike the index-only tests, which never leave the store.
    DataPartPtr addPart(const std::string & part_name, UInt64 first_id, size_t rows)
    {
        auto id_column = ColumnUInt64::create();
        for (size_t i = 0; i < rows; ++i)
            id_column->insertValue(first_id + i);

        auto block = std::make_shared<Block>(Block{
            ColumnWithTypeAndName(std::move(id_column), std::make_shared<DataTypeUInt64>(), "id")});
        BlockWithPartition block_with_partition(std::move(block), Row{});

        /// Bound to a named lvalue: converting an rvalue handle to `StorageMetadataPtr` is deleted.
        auto metadata_handle = table->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
        const StorageMetadataPtr metadata_snapshot = metadata_handle;

        MergeTreeDataWriter writer(*table);
        auto temporary = writer.writeTempPart(block_with_partition, metadata_snapshot, context);
        temporary->finalize();

        /// `fillNewPartName` is private to StorageMergeTree, so the test names the part itself --
        /// which is what lets the assertions below talk about `all_1_1_0` rather than whatever
        /// block number the insert increment happened to hand out.
        auto part = temporary->part;
        part->info = partInfo(part_name);
        part->setName(part_name);

        MergeTreeData::Transaction transaction(*table, nullptr);
        {
            auto lock = table->lockParts();
            table->renameTempPartAndAdd(part, transaction, lock, /*rename_in_transaction=*/ false);
            transaction.commit(lock);
        }
        return table->getPartIfExists(part->info, {MergeTreeData::DataPartState::Active});
    }
};

/// The sidecar writers take a mutable storage, and a part in the set hands out a const one. Same
/// `const_cast` the store itself does, and for the same reason: the rows are immutable, the
/// directory is not.
IDataPartStorage & partStorage(const IMergeTreeDataPart & part)
{
    return const_cast<IDataPartStorage &>(part.getDataPartStorage());
}

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

TEST(MergeTreeBitmapStoreTest, SettleBelowTheNewestVersionIsVisibleAtItsOwnSnapshot)
{
    TableFixture tbl;
    MergeTreeBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/0, /*rows=*/4);
    const auto owner = tbl.addPart("all_9_9_0", /*first_id=*/100, /*rows=*/1);

    /// The target already carries a later version, so the settle below has to land in the middle of
    /// the index rather than append to it.
    DeleteBitmapFileOps::writeBitmapToStorage(partStorage(*target), /*version=*/9, bitmapWithRow(3));
    ASSERT_EQ(store.loadPart(target->info, target->getDataPartStorage()), std::vector<CSN>({9}));

    DeleteBitmapFileOps::writeStagedBitmapToStorage(
        partStorage(*owner), target->info.getPartNameV1(), bitmapWithRow(1));
    ASSERT_TRUE(store.loadPart(owner->info, owner->getDataPartStorage()).empty());

    const auto report = store.settleStagedBitmaps(owner->info, {target->info}, /*csn=*/5);
    EXPECT_EQ(report.settled, 1u);
    EXPECT_FALSE(report.anyOutstanding());

    /// DISCRIMINATING: place the insert with `upper_bound` instead of `lower_bound`, or guard it on
    /// `pos == end()`, and csn 5 never enters the index -- this read goes empty while the file it
    /// should resolve to sits in the target's directory.
    const auto [at_five, five_csn] = store.readBitmap(target->info, /*snapshot_csn=*/5);
    EXPECT_EQ(five_csn, 5u);
    EXPECT_TRUE(at_five->contains(1));

    /// And the version above it still wins at its own snapshot.
    const auto [at_nine, nine_csn] = store.readBitmap(target->info, /*snapshot_csn=*/9);
    EXPECT_EQ(nine_csn, 9u);
    EXPECT_TRUE(at_nine->contains(3));
}

TEST(MergeTreeBitmapStoreTest, SettleForAMissingTargetUnlinksTheStagedFile)
{
    TableFixture tbl;
    MergeTreeBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto owner = tbl.addPart("all_9_9_0", /*first_id=*/100, /*rows=*/1);
    const auto missing_target = tbl.partInfo("all_1_1_0");

    const auto staged_name = DeleteBitmap::fileNameForStagedTarget(missing_target.getPartNameV1());
    DeleteBitmapFileOps::writeStagedBitmapToStorage(
        partStorage(*owner), missing_target.getPartNameV1(), bitmapWithRow(1));
    ASSERT_TRUE(store.loadPart(owner->info, owner->getDataPartStorage()).empty());
    ASSERT_EQ(store.stagedTargetsOf(owner->info), std::vector<MergeTreePartInfo>({missing_target}));

    /// The target is in no state at all and the part set is complete, so "reclaimed" is the only
    /// reading left: the bitmap is unlinked rather than carried by an owner nothing can resolve.
    ASSERT_TRUE(tbl.table->outdatedPartsSetIsComplete());
    const auto report = store.settleStagedBitmaps(owner->info, {missing_target}, /*csn=*/5);
    EXPECT_EQ(report.settled, 1u);
    EXPECT_FALSE(report.anyOutstanding());

    EXPECT_FALSE(owner->getDataPartStorage().existsFile(staged_name));
    EXPECT_TRUE(store.stagedTargetsOf(owner->info).empty());
}

TEST(MergeTreeBitmapStoreTest, ObsoleteSweepKeepsTheFloorVersionAndEverythingAbove)
{
    TableFixture tbl;
    MergeTreeBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto part = tbl.addPart("all_1_1_0", /*first_id=*/0, /*rows=*/4);
    for (const CSN version : {3, 7, 12})
        DeleteBitmapFileOps::writeBitmapToStorage(partStorage(*part), version, bitmapWithRow(version));
    ASSERT_EQ(store.loadPart(part->info, part->getDataPartStorage()).size(), 3u);

    /// A snapshot at 10 resolves to version 7, so 7 is the floor and only what is strictly below it
    /// is unreachable. Reclaiming 7 as well would change what that snapshot reads.
    EXPECT_EQ(store.removeObsoleteBitmaps(part->info, /*oldest_snapshot_csn=*/10), 1u);

    std::vector<CSN> left;
    for (const auto & file : store.listBitmaps(part->info))
        left.push_back(file.version);
    EXPECT_EQ(left, std::vector<CSN>({7, 12}));

    const auto [at_ten, ten_csn] = store.readBitmap(part->info, /*snapshot_csn=*/10);
    EXPECT_EQ(ten_csn, 7u);
    EXPECT_TRUE(at_ten->contains(7));
}

TEST(MergeTreeBitmapStoreTest, ObsoleteSweepKeepsOnlyTheNewestWhenNothingIsPinned)
{
    TableFixture tbl;
    MergeTreeBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto part = tbl.addPart("all_1_1_0", /*first_id=*/0, /*rows=*/4);
    for (const CSN version : {3, 7, 12})
        DeleteBitmapFileOps::writeBitmapToStorage(partStorage(*part), version, bitmapWithRow(version));
    ASSERT_EQ(store.loadPart(part->info, part->getDataPartStorage()).size(), 3u);

    /// No open snapshot means the floor is the newest version, and the newest always stays --
    /// it is the current state of the part, not a reclaimable predecessor.
    EXPECT_EQ(store.removeObsoleteBitmaps(part->info, UNBOUNDED_CSN), 2u);

    std::vector<CSN> left;
    for (const auto & file : store.listBitmaps(part->info))
        left.push_back(file.version);
    EXPECT_EQ(left, std::vector<CSN>({12}));

    /// A second round has nothing left to reclaim, and says so rather than double-counting.
    EXPECT_EQ(store.removeObsoleteBitmaps(part->info, UNBOUNDED_CSN), 0u);
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

/// Goes red if the `hasUnsettledStagedBitmaps` guard in `grabOldParts` is dropped. No retire path
/// reaches this state today -- they all settle first or refuse -- so the test builds it directly.
TEST(MergeTreeBitmapStoreTest, GrabOldPartsHoldsBackAPartWithUnsettledStagedBitmaps)
{
    TableFixture tbl{/*with_unique_key=*/ true};

    auto owner = tbl.addPart("all_2_2_0", /*first_id=*/ 100, /*rows=*/ 4);
    ASSERT_NE(owner, nullptr);
    const auto owner_info = owner->info;
    tbl.table->uniqueKeyTxnManager().bitmapStore().registerStagedBitmaps(
        owner_info, {tbl.partInfo("all_1_1_0")});

    {
        auto lock = tbl.table->lockParts();
        tbl.table->removePartsFromWorkingSet(
            NO_TRANSACTION_RAW, {owner}, /*clear_without_timeout=*/ true, lock);
    }

    /// `grabOldParts` skips a part any second holder can still see, so the fixture's own reference
    /// has to go -- otherwise the part is held back for the wrong reason and the test proves nothing.
    owner.reset();

    EXPECT_TRUE(tbl.table->grabOldParts(/*force=*/ true).empty());

    const auto held = tbl.table->getPartIfExists(owner_info, {MergeTreeData::DataPartState::Outdated});
    ASSERT_NE(held, nullptr);
    EXPECT_EQ(held->removal_state.load(), DataPartRemovalState::HAS_UNSETTLED_STAGED_BITMAPS);
}

/// Goes red if the guard over-triggers. It does NOT catch the guard being dropped -- read it with
/// the test above, not instead of it.
TEST(MergeTreeBitmapStoreTest, GrabOldPartsTakesAPartWithNoStagedBitmaps)
{
    TableFixture tbl{/*with_unique_key=*/ true};

    auto owner = tbl.addPart("all_2_2_0", /*first_id=*/ 100, /*rows=*/ 4);
    ASSERT_NE(owner, nullptr);

    {
        auto lock = tbl.table->lockParts();
        tbl.table->removePartsFromWorkingSet(
            NO_TRANSACTION_RAW, {owner}, /*clear_without_timeout=*/ true, lock);
    }
    owner.reset();

    EXPECT_EQ(tbl.table->grabOldParts(/*force=*/ true).size(), 1u);
}

/// Goes red if the `RolledBackCSN` exemption is dropped: the part would be pinned forever, because
/// its staged bitmaps can never publish.
TEST(MergeTreeBitmapStoreTest, GrabOldPartsTakesARolledBackPartWithStagedBitmaps)
{
    TableFixture tbl{/*with_unique_key=*/ true};

    auto owner = tbl.addPart("all_2_2_0", /*first_id=*/ 100, /*rows=*/ 4);
    ASSERT_NE(owner, nullptr);
    tbl.table->uniqueKeyTxnManager().bitmapStore().registerStagedBitmaps(
        owner->info, {tbl.partInfo("all_1_1_0")});
    owner->version->setAndStoreCreationCSN(Tx::RolledBackCSN);

    {
        auto lock = tbl.table->lockParts();
        tbl.table->removePartsFromWorkingSet(
            NO_TRANSACTION_RAW, {owner}, /*clear_without_timeout=*/ true, lock);
    }
    owner.reset();

    EXPECT_EQ(tbl.table->grabOldParts(/*force=*/ true).size(), 1u);
}

/// Goes red if the unresolvable-owner branch reports a guessed count instead of the whole list.
TEST(MergeTreeBitmapStoreTest, SettleCountsEveryTargetFailedWhenTheOwnerIsGone)
{
    TableFixture tbl;
    MergeTreeBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const std::vector<MergeTreePartInfo> targets{
        tbl.partInfo("all_1_1_0"), tbl.partInfo("all_2_2_0")};

    const auto report = store.settleStagedBitmaps(tbl.partInfo("all_9_9_0"), targets, /*csn=*/7);
    EXPECT_EQ(report.failed, targets.size());
    EXPECT_EQ(report.settled, 0u);
    EXPECT_TRUE(report.anyOutstanding());
}
