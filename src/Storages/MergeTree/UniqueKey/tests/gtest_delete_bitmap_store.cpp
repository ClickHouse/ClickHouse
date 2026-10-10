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
#include <Storages/MergeTree/UniqueKey/DeleteBitmapCache.h>
#include <Common/CurrentMetrics.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapStore.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyTxn.h>
#include <Storages/MergeTree/UniqueKey/tests/gtest_part_storage_fixture.h>
#include <Storages/StorageMergeTree.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

using namespace DB;

namespace
{

bool isPinned(const DeleteBitmapStore & store, StorageMergeTree & table, const IMergeTreeDataPart & part)
{
    const auto lock = table.readLockParts();
    return store.isPinned(part, lock);
}
struct TableFixture
{
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

        /// Per instance, not shared: `addPart` writes real files under this path.
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

    /// A real part in the Active set, under the name the caller asks for.
    DataPartPtr addPart(const std::string & part_name, UInt64 first_id, size_t rows) const
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

        /// `fillNewPartName` is private to StorageMergeTree, so the test names the part itself.
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

TEST(DeleteBitmapStoreTest, LoadPartIndexesEveryVersionOneHolderHas)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 0, /*rows=*/ 4);
    const auto holder = tbl.addPart("all_9_9_0", /*first_id=*/ 100, /*rows=*/ 1);
    const auto target_name = target->info.getPartNameV1();

    writeCarried(
        partStorage(*holder), /*version=*/12, target_name, bitmapWithRow(1));
    writeCarried(
        partStorage(*holder), /*version=*/7, target_name, bitmapWithRow(2));

    store.loadPart(holder->info, holder->getDataPartStorage());

    EXPECT_TRUE(isPinned(store, *tbl.table, *holder));

    EXPECT_EQ(store.readBitmap(target->info, /*snapshot_csn=*/ 12).second, 12u);
    EXPECT_EQ(store.readBitmap(target->info, /*snapshot_csn=*/ 7).second, 7u);
}

TEST(DeleteBitmapStoreTest, LoadPartRegistersStagedTargets)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto holder = tbl.addPart("all_9_9_0", /*first_id=*/ 100, /*rows=*/ 1);
    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 200, /*rows=*/ 1);
    DeleteBitmapFileOps::stageBitmap(partStorage(*holder), {0, target->name}, bitmapWithRow(3));

    store.loadPart(holder->info, holder->getDataPartStorage());
    EXPECT_TRUE(isPinned(store, *tbl.table, *holder));
}

TEST(DeleteBitmapStoreTest, RemovingStagedBitmapsClearsBothDirections)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto holder = tbl.addPart("all_9_9_0", /*first_id=*/ 100, /*rows=*/ 1);
    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 200, /*rows=*/ 1);
    store.registerStagedBitmaps(holder->info, {target->info});
    EXPECT_TRUE(isPinned(store, *tbl.table, *holder));

    store.removeStagedBitmaps(holder->info, {target->info});
    EXPECT_FALSE(isPinned(store, *tbl.table, *holder));

    const auto [bitmap, csn] = store.readBitmap(target->info, UNBOUNDED_CSN);
    EXPECT_TRUE(bitmap->empty());
    EXPECT_EQ(csn, 0u);
}

TEST(DeleteBitmapStoreTest, AStagedVersionTakesItsPlaceAmongTheCarriedOnes)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 0, /*rows=*/ 8);
    const auto stager = tbl.addPart("all_5_5_0", /*first_id=*/ 100, /*rows=*/ 1);
    const auto carrier = tbl.addPart("all_9_9_0", /*first_id=*/ 200, /*rows=*/ 1);

    writeCarried(
        partStorage(*carrier), /*version=*/ 7, target->info.getPartNameV1(), bitmapWithRow(5));
    store.loadPart(carrier->info, carrier->getDataPartStorage());

    /// Indexed after the higher carried one, so only the order decides which a snapshot reads.
    DeleteBitmapFileOps::stageBitmap(partStorage(*stager), {0, target->name}, bitmapWithRow(3));
    store.loadPart(stager->info, stager->getDataPartStorage());

    const auto [newest, newest_csn] = store.readBitmap(target->info, UNBOUNDED_CSN);
    EXPECT_EQ(newest_csn, 7u);
    EXPECT_TRUE(newest->contains(5));

    /// ... and a snapshot below it still reads the staged one.
    const auto [older, older_csn] = store.readBitmap(target->info, /*snapshot_csn=*/ 6);
    ASSERT_LT(older_csn, 7u);
    ASSERT_GT(older_csn, 0u);
    EXPECT_TRUE(older->contains(3));
}

TEST(DeleteBitmapStoreTest, ABitmapNamingAnUnparseableTargetRefusesTheLoad)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto holder = tbl.addPart("all_9_9_0", /*first_id=*/ 100, /*rows=*/ 1);

    /// The kills in this file apply to a part nobody can name, so indexing it and carrying on
    /// would serve rows it says are deleted. `CORRUPTED_DATA` rather than `LOGICAL_ERROR`: the
    /// caller detaches the part on it, which an aborting code would never let it do.
    DeleteBitmapFileOps::stageBitmap(partStorage(*holder), {0, "not_a_part_name"}, bitmapWithRow(1));

    EXPECT_THROW(
        store.loadPart(holder->info, holder->getDataPartStorage()), Exception);
}

TEST(DeleteBitmapStoreTest, AStagedVersionResolvesIntoTheMiddleOfTheOrder)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 0, /*rows=*/ 8);
    const auto stager = tbl.addPart("all_5_5_0", /*first_id=*/ 100, /*rows=*/ 1);
    const auto carrier = tbl.addPart("all_9_9_0", /*first_id=*/ 200, /*rows=*/ 1);
    const auto target_name = target->info.getPartNameV1();

    /// Three carried versions straddling the staged one, so the resolve lands between two
    /// existing entries rather than at either end -- the case two entries cannot show.
    writeCarried(partStorage(*carrier), /*version=*/ 3, target_name, bitmapWithRow(3));
    writeCarried(partStorage(*carrier), /*version=*/ 7, target_name, bitmapWithRow(7));
    writeCarried(partStorage(*carrier), /*version=*/ 12, target_name, bitmapWithRow(12));
    store.loadPart(carrier->info, carrier->getDataPartStorage());

    DeleteBitmapFileOps::stageBitmap(partStorage(*stager), {0, target->name}, bitmapWithRow(5));
    store.loadPart(stager->info, stager->getDataPartStorage());

    /// The staged holder is prehistoric, so its version sorts below 3 and every carried read
    /// has to be unaffected by the insertion.
    EXPECT_EQ(store.readBitmap(target->info, /*snapshot_csn=*/ 3).second, 3u);
    EXPECT_EQ(store.readBitmap(target->info, /*snapshot_csn=*/ 7).second, 7u);
    EXPECT_EQ(store.readBitmap(target->info, UNBOUNDED_CSN).second, 12u);
    EXPECT_TRUE(store.readBitmap(target->info, UNBOUNDED_CSN).first->contains(12));
}

TEST(DeleteBitmapStoreTest, AStagedVersionJoinsTheUnionOnceItResolves)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 0, /*rows=*/ 8);
    const auto stager = tbl.addPart("all_5_5_0", /*first_id=*/ 100, /*rows=*/ 1);
    const auto carrier = tbl.addPart("all_9_9_0", /*first_id=*/ 200, /*rows=*/ 1);
    const auto target_name = target->info.getPartNameV1();

    writeCarried(partStorage(*carrier), /*version=*/ 7, target_name, bitmapWithRow(7));
    writeCarried(partStorage(*carrier), /*version=*/ 12, target_name, bitmapWithRow(12));
    store.loadPart(carrier->info, carrier->getDataPartStorage());

    /// Staged and never read, so it is still unordered when the read starts. The read resolves
    /// it into place, and the union has to pick it up there rather than at the top of the order.
    DeleteBitmapFileOps::stageBitmap(partStorage(*stager), {0, target->name}, bitmapWithRow(5));
    store.loadPart(stager->info, stager->getDataPartStorage());

    const auto [all, newest] = store.readBitmap(target->info, UNBOUNDED_CSN);
    EXPECT_EQ(newest, 12u);
    EXPECT_TRUE(all->contains(7));
    EXPECT_TRUE(all->contains(12));
    /// The staged one, whose version is the stager's own `creation_csn`.
    EXPECT_TRUE(all->contains(5));
    EXPECT_EQ(all->cardinality(), 3u);
}

TEST(DeleteBitmapStoreTest, VersionsOfOnePartInterleaveAcrossTheHoldersOfThem)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/0, /*rows=*/4);
    const auto older = tbl.addPart("all_8_8_0", /*first_id=*/100, /*rows=*/1);
    const auto newer = tbl.addPart("all_9_9_0", /*first_id=*/200, /*rows=*/1);

    writeCarried(
        partStorage(*newer), /*version=*/9, target->info.getPartNameV1(), bitmapWithRow(3));
    store.loadPart(newer->info, newer->getDataPartStorage());

    writeCarried(
        partStorage(*older), /*version=*/5, target->info.getPartNameV1(), bitmapWithRow(1));
    store.loadPart(older->info, older->getDataPartStorage());

    const auto [at_five, five_csn] = store.readBitmap(target->info, /*snapshot_csn=*/5);
    EXPECT_EQ(five_csn, 5u);
    EXPECT_TRUE(at_five->contains(1));

    const auto [at_nine, nine_csn] = store.readBitmap(target->info, /*snapshot_csn=*/9);
    EXPECT_EQ(nine_csn, 9u);
    EXPECT_TRUE(at_nine->contains(3));
}

namespace
{
DataPartPtr carryVersionsOfOneTarget(
    TableFixture & tbl, DeleteBitmapStore & store, const std::vector<CSN> & versions)
{
    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 0, /*rows=*/ 4);
    UInt64 block = 20;
    for (const CSN version : versions)
    {
        const auto holder = tbl.addPart(
            fmt::format("all_{}_{}_0", block, block), /*first_id=*/ 100 * block, /*rows=*/ 1);
        writeCarried(
        partStorage(*holder), version, target->info.getPartNameV1(), bitmapWithRow(version));
        store.loadPart(holder->info, holder->getDataPartStorage());
        ++block;
    }
    return target;
}
}

/// `carryVersionsOfOneTarget` files version V killing row V, so the rows a read returns name
/// exactly the versions it consulted.
TEST(DeleteBitmapStoreTest, AReadUnionsEveryVersionAtOrBelowTheSnapshot)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = carryVersionsOfOneTarget(tbl, store, {3, 7, 12});

    const auto [at_ten, ten_csn] = store.readBitmap(target->info, /*snapshot_csn=*/10);
    /// The version reported is still the newest one consulted -- it names the union.
    EXPECT_EQ(ten_csn, 7u);
    EXPECT_TRUE(at_ten->contains(3));
    EXPECT_TRUE(at_ten->contains(7));
    /// Above the snapshot, so not yet visible.
    EXPECT_FALSE(at_ten->contains(12));
    EXPECT_EQ(at_ten->cardinality(), 2u);

    const auto [at_twelve, twelve_csn] = store.readBitmap(target->info, /*snapshot_csn=*/12);
    EXPECT_EQ(twelve_csn, 12u);
    EXPECT_EQ(at_twelve->cardinality(), 3u);
}

/// Every other test here runs with no cache, so the incremental path below is theirs alone.
TEST(DeleteBitmapStoreTest, AReadExtendsTheUnionItFindsCached)
{
    TableFixture tbl;
    auto cache = std::make_shared<DeleteBitmapCache>(
        "SLRU", CurrentMetrics::end(), CurrentMetrics::end(), /*max_size_in_bytes=*/ 64 << 20,
        /*size_ratio=*/ 0.5);
    DeleteBitmapStore store{*tbl.table, cache};

    const auto target = carryVersionsOfOneTarget(tbl, store, {3, 7, 12});

    /// Leaves the union of 3 and 7 in the cache, keyed by 7.
    EXPECT_EQ(store.readBitmap(target->info, /*snapshot_csn=*/7).first->cardinality(), 2u);

    /// Has to EXTEND that union rather than stand in for it: reading only the newest delta on
    /// top of a cache hit would answer with 12 alone.
    const auto [all, csn] = store.readBitmap(target->info, /*snapshot_csn=*/12);
    EXPECT_EQ(csn, 12u);
    EXPECT_EQ(all->cardinality(), 3u);
    EXPECT_TRUE(all->contains(3));
    EXPECT_TRUE(all->contains(7));
    EXPECT_TRUE(all->contains(12));
}

/// The incremental read trusts the entry cached under the second-newest csn to be the union of
/// everything at or below it. This asks what happens when a version lands BELOW a csn already
/// cached, which would make that entry a union of the wrong set.
TEST(DeleteBitmapStoreTest, AVersionLandingBelowACachedUnionIsStillRead)
{
    TableFixture tbl;
    auto cache = std::make_shared<DeleteBitmapCache>(
        "SLRU", CurrentMetrics::end(), CurrentMetrics::end(), /*max_size_in_bytes=*/ 64 << 20,
        /*size_ratio=*/ 0.5);
    DeleteBitmapStore store{*tbl.table, cache};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 0, /*rows=*/ 16);
    const auto target_name = target->info.getPartNameV1();

    auto hold = [&](const std::string & name, UInt64 version)
    {
        const auto holder = tbl.addPart(name, /*first_id=*/ 100 * version, /*rows=*/ 1);
        writeCarried(partStorage(*holder), version, target_name, bitmapWithRow(version));
        store.loadPart(holder->info, holder->getDataPartStorage());
    };

    hold("all_30_30_0", 3);
    hold("all_70_70_0", 7);

    /// Caches the union of 3 and 7 under key 7.
    ASSERT_EQ(store.readBitmap(target->info, UNBOUNDED_CSN).first->cardinality(), 2u);

    /// 5 arrives after that entry was written, so the entry under 7 no longer covers everything
    /// at or below 7. 9 then makes 7 the second-newest, which is the key the read consults.
    hold("all_50_50_0", 5);
    hold("all_90_90_0", 9);

    const auto [all, newest] = store.readBitmap(target->info, UNBOUNDED_CSN);
    EXPECT_EQ(newest, 9u);
    EXPECT_TRUE(all->contains(3));
    EXPECT_TRUE(all->contains(7));
    EXPECT_TRUE(all->contains(9));
    /// The one the stale entry would drop.
    EXPECT_TRUE(all->contains(5));
    EXPECT_EQ(all->cardinality(), 4u);
}

TEST(DeleteBitmapStoreTest, AReadBelowEveryVersionSeesNoKills)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = carryVersionsOfOneTarget(tbl, store, {3, 7, 12});

    const auto [none, none_csn] = store.readBitmap(target->info, /*snapshot_csn=*/2);
    EXPECT_EQ(none_csn, 0u);
    EXPECT_TRUE(none->empty());

    /// The oldest version alone, not the newest and not the whole set.
    const auto [first, first_csn] = store.readBitmap(target->info, /*snapshot_csn=*/3);
    EXPECT_EQ(first_csn, 3u);
    EXPECT_EQ(first->cardinality(), 1u);
    EXPECT_TRUE(first->contains(3));
}

#ifndef DEBUG_OR_SANITIZER_BUILD

TEST(DeleteBitmapStoreTest, ABitmapWhoseTargetLeftThePartSetIsRejected)
{
    TableFixture tbl;
    PartStorageFixture fx("holder");
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto holder = tbl.addPart("all_9_9_0", /*first_id=*/ 100, /*rows=*/ 1);
    const auto vanished = tbl.partInfo("all_1_1_0");
    store.registerStagedBitmaps(holder->info, {vanished});

    EXPECT_THROW(store.readBitmap(vanished, UNBOUNDED_CSN), Exception);
}

TEST(DeleteBitmapStoreTest, AHolderThatLeftThePartSetIsRejected)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto holder = tbl.partInfo("all_9_9_0");
    const auto target = tbl.partInfo("all_1_1_0");
    store.registerStagedBitmaps(holder, {target});

    EXPECT_THROW(store.readBitmap(target, UNBOUNDED_CSN), Exception);
}

#endif

TEST(DeleteBitmapStoreTest, GrabOldPartsHoldsBackAPartHoldingAnotherPartsBitmap)
{
    TableFixture tbl{/*with_unique_key=*/ true};

    auto holder = tbl.addPart("all_2_2_0", /*first_id=*/ 100, /*rows=*/ 4);
    ASSERT_NE(holder, nullptr);
    const auto holder_info = holder->info;
    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 0, /*rows=*/ 4);
    tbl.table->uniqueKeyTxnManager().deleteBitmapStore().registerStagedBitmaps(holder_info, {target->info});

    {
        auto lock = tbl.table->lockParts();
        tbl.table->removePartsFromWorkingSet(
            NO_TRANSACTION_RAW, {holder}, /*clear_without_timeout=*/ true, lock);
    }

    /// The fixture's own reference has to go, otherwise the part is held back for the wrong
    /// reason and the test proves nothing.
    holder.reset();

    EXPECT_TRUE(tbl.table->grabOldParts(/*force=*/ true).empty());

    const auto held = tbl.table->getPartIfExists(holder_info, {MergeTreeData::DataPartState::Outdated});
    ASSERT_NE(held, nullptr);
    EXPECT_EQ(held->removal_state.load(), DataPartRemovalState::PINNED_BY_DELETE_BITMAP);
}

TEST(DeleteBitmapStoreTest, GrabOldPartsTakesARolledBackPartWithStagedBitmaps)
{
    TableFixture tbl{/*with_unique_key=*/ true};

    auto holder = tbl.addPart("all_2_2_0", /*first_id=*/ 100, /*rows=*/ 4);
    ASSERT_NE(holder, nullptr);
    /// A real target, in the set: against a vanished one the pin lets go for another reason and
    /// the rolled-back exemption goes untested.
    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 0, /*rows=*/ 4);
    tbl.table->uniqueKeyTxnManager().deleteBitmapStore().registerStagedBitmaps(holder->info, {target->info});
    holder->version->setAndStoreCreationCSN(Tx::RolledBackCSN);

    {
        auto lock = tbl.table->lockParts();
        tbl.table->removePartsFromWorkingSet(
            NO_TRANSACTION_RAW, {holder}, /*clear_without_timeout=*/ true, lock);
    }
    holder.reset();

    EXPECT_EQ(tbl.table->grabOldParts(/*force=*/ true).size(), 1u);
}

namespace
{
void carryInto(
    DeleteBitmapStore & store,
    const IMergeTreeDataPart & merged,
    const std::vector<DeleteBitmapStore::CarriedBitmap> & carried)
{
    std::vector<DeleteBitmapStore::BitmapLink> links;
    for (const auto & [link, held_in, file] : carried)
    {
        DeleteBitmapFileOps::carryBitmap(
            held_in->getDataPartStorage(), file,
            partStorage(merged), {link.csn, link.target.getPartNameV1()});
        links.push_back(link);
    }
    store.registerLinks(merged.info, links);
}

void outdate(TableFixture & tbl, const DataPartPtr & part)
{
    auto lock = tbl.table->lockParts();
    tbl.table->removePartsFromWorkingSet(
        NO_TRANSACTION_RAW, {part}, /*clear_without_timeout=*/ true, lock);
}

void retire(TableFixture & tbl, DeleteBitmapStore & store, DataPartPtr & part)
{
    {
        auto lock = tbl.table->lockParts();
        tbl.table->removePartsFromWorkingSet(
            NO_TRANSACTION_RAW, {part}, /*clear_without_timeout=*/ true, lock);
    }
    store.dropPart(*part);
    part.reset();
    tbl.table->grabOldParts(/*force=*/ true);
}
}

TEST(DeleteBitmapStoreTest, ACarriedBitmapOutlivesTheOwnerThatWroteIt)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 0, /*rows=*/ 4);
    auto holder = tbl.addPart("all_9_9_0", /*first_id=*/ 100, /*rows=*/ 1);
    const auto co_source = tbl.addPart("all_8_8_0", /*first_id=*/ 200, /*rows=*/ 1);
    /// Stands in for the merge result. Not named `all_8_9_1`: the store only cares which part
    /// holds the file, not whether its name covers the sources.
    const auto merged = tbl.addPart("all_20_20_0", /*first_id=*/ 300, /*rows=*/ 2);

    DeleteBitmapFileOps::stageBitmap(
        partStorage(*holder), {0, target->info.getPartNameV1()}, bitmapWithRow(1));
    store.loadPart(holder->info, holder->getDataPartStorage());
    ASSERT_TRUE(store.readBitmap(target->info, UNBOUNDED_CSN).first->contains(1));

    const auto carried = store.selectCarriedBitmaps({holder->info, co_source->info});
    ASSERT_EQ(carried.size(), 1u);
    EXPECT_EQ(carried[0].link.target, target->info);
    EXPECT_EQ(carried[0].link.csn, Tx::NonTransactionalCSN);

    ASSERT_NE(carried[0].held_in, nullptr);
    EXPECT_EQ(carried[0].held_in->info, holder->info);
    const auto at_source = DeleteBitmapFileOps::tryReadBitmap(
        carried[0].held_in->getDataPartStorage(), carried[0].file);
    ASSERT_NE(at_source, nullptr);
    EXPECT_TRUE(at_source->contains(1));

    carryInto(store, *merged, carried);
    retire(tbl, store, holder);

    const auto [bitmap, csn] = store.readBitmap(target->info, UNBOUNDED_CSN);
    EXPECT_EQ(csn, Tx::NonTransactionalCSN);
    EXPECT_TRUE(bitmap->contains(1));
}

TEST(DeleteBitmapStoreTest, AReadSurvivesAHolderThatLeftTheSetWhenAnotherHoldsTheVersion)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 0, /*rows=*/ 4);
    const auto merged = tbl.addPart("all_20_20_0", /*first_id=*/ 300, /*rows=*/ 2);
    const auto target_name = target->info.getPartNameV1();

    /// Models the handoff window `versionsUpTo` tolerates (see its comment) directly, by
    /// registering the link and never adding the part: going through the real lifecycle leaves
    /// it Outdated, which still resolves and wouldn't exercise the window at all.
    writeCarried(partStorage(*merged), /*version=*/ 7, target_name, bitmapWithRow(7));
    store.loadPart(merged->info, merged->getDataPartStorage());
    store.registerLinks(tbl.partInfo("all_9_9_0"), {{target->info, /*csn=*/ 7}});

    /// Both holders are the same bytes, so the read owes an answer, not an error.
    const auto [bitmap, csn] = store.readBitmap(target->info, UNBOUNDED_CSN);
    EXPECT_EQ(csn, 7u);
    EXPECT_TRUE(bitmap->contains(7));
    EXPECT_EQ(bitmap->cardinality(), 1u);
}

TEST(DeleteBitmapStoreTest, ACarriedBitmapKeepsTheVersionItWasCreatedWith)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 0, /*rows=*/ 4)->info;
    const auto carrier = tbl.addPart("all_9_9_0", /*first_id=*/ 100, /*rows=*/ 1);
    ASSERT_EQ(carrier->version->getInfo().creation_csn, Tx::NonTransactionalCSN);

    writeCarried(
        partStorage(*carrier), /*version=*/ 5, target.getPartNameV1(), bitmapWithRow(1));
    store.loadPart(carrier->info, carrier->getDataPartStorage());

    EXPECT_EQ(store.readBitmap(target, /*snapshot_csn=*/ 4).second, 0u);

    const auto [bitmap, csn] = store.readBitmap(target, /*snapshot_csn=*/ 5);
    EXPECT_EQ(csn, 5u);
    EXPECT_TRUE(bitmap->contains(1));
}

/// Two sources, two versions of one outside target. Dropping the older would resurrect the row
/// only it killed, so the carry has to take both.
TEST(DeleteBitmapStoreTest, TheCarryTakesEveryVersionAcrossTheSources)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 0, /*rows=*/ 4);
    const auto older = tbl.addPart("all_8_8_0", /*first_id=*/ 100, /*rows=*/ 1);
    const auto newer = tbl.addPart("all_9_9_0", /*first_id=*/ 200, /*rows=*/ 1);
    const auto target_name = target->info.getPartNameV1();

    writeCarried(partStorage(*older), /*version=*/ 5, target_name, bitmapWithRow(1));
    store.loadPart(older->info, older->getDataPartStorage());
    writeCarried(partStorage(*newer), /*version=*/ 9, target_name, bitmapWithRow(3));
    store.loadPart(newer->info, newer->getDataPartStorage());

    /// Both orders: a delta is not subsumed by a newer one, so both come back, and they come
    /// back oldest first whichever order the merge hands its sources over in. Taking only the
    /// newest would drop row 1.
    for (const auto & sources : {std::vector{older->info, newer->info}, std::vector{newer->info, older->info}})
    {
        const auto carried = store.selectCarriedBitmaps(sources);
        ASSERT_EQ(carried.size(), 2u);
        EXPECT_EQ(carried[0].link.csn, 5u);
        EXPECT_EQ(carried[0].held_in->info, older->info);
        EXPECT_EQ(carried[1].link.csn, 9u);
        EXPECT_EQ(carried[1].held_in->info, newer->info);
    }
}

TEST(DeleteBitmapStoreTest, AKillAgainstAnotherSourceIsAbsorbedNotCarried)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 0, /*rows=*/ 4);
    const auto holder = tbl.addPart("all_9_9_0", /*first_id=*/ 100, /*rows=*/ 1);

    DeleteBitmapFileOps::stageBitmap(
        partStorage(*holder), {0, target->info.getPartNameV1()}, bitmapWithRow(1));
    store.loadPart(holder->info, holder->getDataPartStorage());

    EXPECT_TRUE(store.selectCarriedBitmaps({holder->info, target->info}).empty());
}

TEST(DeleteBitmapStoreTest, ABitmapForAVanishedTargetIsNotCarried)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto holder = tbl.addPart("all_9_9_0", /*first_id=*/ 100, /*rows=*/ 1);
    const auto vanished = tbl.partInfo("all_1_1_0");

    DeleteBitmapFileOps::stageBitmap(
        partStorage(*holder), {0, vanished.getPartNameV1()}, bitmapWithRow(1));
    store.loadPart(holder->info, holder->getDataPartStorage());

    EXPECT_FALSE(isPinned(store, *tbl.table, *holder));
    EXPECT_TRUE(store.selectCarriedBitmaps({holder->info}).empty());
}

TEST(DeleteBitmapStoreTest, TheCarryFailsOnASourceThatIsNotInThePartSet)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    EXPECT_THROW(store.selectCarriedBitmaps({tbl.partInfo("all_9_9_0")}), Exception);
}

TEST(DeleteBitmapStoreTest, ALateKillAgainstTheMergeResultDoesNotPinIt)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto merged = tbl.addPart("all_1_5_1", /*first_id=*/ 100, /*rows=*/ 1);
    store.registerStagedBitmaps(merged->info, {merged->info});

    EXPECT_FALSE(isPinned(store, *tbl.table, *merged));
}

TEST(DeleteBitmapStoreTest, ACarriedBitmapUnpinsTheSourceItCameFrom)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 100, /*rows=*/ 1);
    const auto source = tbl.addPart("all_4_4_0", /*first_id=*/ 200, /*rows=*/ 1);

    store.registerStagedBitmaps(source->info, {target->info});
    EXPECT_TRUE(isPinned(store, *tbl.table, *source));

    outdate(tbl, source);
    const auto merged = tbl.addPart("all_4_9_1", /*first_id=*/ 300, /*rows=*/ 1);
    ASSERT_TRUE(merged->info.contains(source->info));
    store.registerLinks(merged->info, {{target->info, source->version->getInfo().creation_csn}});

    EXPECT_FALSE(isPinned(store, *tbl.table, *source));

    EXPECT_TRUE(isPinned(store, *tbl.table, *merged));
}

TEST(DeleteBitmapStoreTest, ACarrierThatDoesNotCoverTheSourceDoesNotUnpinIt)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 100, /*rows=*/ 1);
    const auto source = tbl.addPart("all_4_4_0", /*first_id=*/ 200, /*rows=*/ 1);
    const auto sibling = tbl.addPart("all_7_7_0", /*first_id=*/ 300, /*rows=*/ 1);
    ASSERT_FALSE(sibling->info.contains(source->info));

    store.registerStagedBitmaps(source->info, {target->info});
    store.registerLinks(sibling->info, {{target->info, source->version->getInfo().creation_csn}});

    EXPECT_TRUE(isPinned(store, *tbl.table, *source));
}

TEST(DeleteBitmapStoreTest, ANewerCarriedVersionDoesNotUnpinTheOlderOne)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 100, /*rows=*/ 1);
    const auto source = tbl.addPart("all_4_4_0", /*first_id=*/ 200, /*rows=*/ 1);

    store.registerStagedBitmaps(source->info, {target->info});

    outdate(tbl, source);
    const auto merged = tbl.addPart("all_4_9_1", /*first_id=*/ 300, /*rows=*/ 1);
    store.registerLinks(merged->info, {{target->info, source->version->getInfo().creation_csn + 1}});

    EXPECT_TRUE(isPinned(store, *tbl.table, *source));
}

TEST(DeleteBitmapStoreTest, AnUnpublishedCarrierDoesNotUnpinTheSource)
{
    TableFixture tbl;
    DeleteBitmapStore store{*tbl.table, /*cache=*/nullptr};

    const auto target = tbl.addPart("all_1_1_0", /*first_id=*/ 100, /*rows=*/ 1);
    const auto source = tbl.addPart("all_4_4_0", /*first_id=*/ 200, /*rows=*/ 1);
    const CSN version = source->version->getInfo().creation_csn;
    store.registerStagedBitmaps(source->info, {target->info});

    /// In the set and covering the source, so everything but the commit says it carried -- which
    /// is the state a merge is in between linking its copies and reaching its commit point.
    outdate(tbl, source);
    const auto merged = tbl.addPart("all_4_9_1", /*first_id=*/ 300, /*rows=*/ 1);
    ASSERT_TRUE(merged->info.contains(source->info));
    merged->version->setAndStoreCreationCSN(Tx::RolledBackCSN);
    store.registerLinks(merged->info, {{target->info, version}});

    EXPECT_TRUE(isPinned(store, *tbl.table, *source));
}
