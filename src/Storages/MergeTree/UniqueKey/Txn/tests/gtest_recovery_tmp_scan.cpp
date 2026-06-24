/// Recovery tmp-scan.
///
/// `PartitionTxnController::recover()` walks each `tmp_<op>/` directory passed
/// in, reads its `unique_key.txt` manifest, unlinks the listed
/// `(target, csn)` sidecars (via `IBitmapStore::unlink`), and removes
/// the tmp dir.
///
/// Tested behaviour:
///   1. Manifest with one third-party entry → unlink called with
///      that (target, csn) pair, tmp dir removed.
///   2. Self-targeting entry → unlink called with the tmp dir's
///      basename as the resolved target.
///   3. Missing manifest → CORRUPTED_DATA thrown (fail-closed).
///   4. Multiple tmp dirs drain in one call.
///   5. `forwarded` entries are NOT unlinked — they point at live
///      third-party bitmaps that other parts still attest.
#include <gtest/gtest.h>

#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/IBitmapStore.h>
#include <Storages/MergeTree/UniqueKey/Txn/ICommitCoordinator.h>
#include <Storages/MergeTree/UniqueKey/Txn/SnapshotPinning.h>
#include <Storages/MergeTree/UniqueKey/Txn/PartitionTxnController.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyManifest.h>

#include <Common/Exception.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <random>
#include <utility>
#include <vector>

using namespace DB;
using namespace DB::UniqueKeyTxn;

namespace
{

/// Records every `unlink(target, csn)` invocation.
class RecordingBitmapStore : public IBitmapStore
{
public:
    std::vector<std::pair<PartName, CSN>> unlinks;

    std::pair<ConstDeleteBitmapPtr, CSN>
    readBitmap(const PartName & /*part*/, CSN /*csn*/) override { return {std::make_shared<DeleteBitmap>(), 0}; }
    void installBitmap(const PartName & /*target*/, CSN /*csn*/, const DeleteBitmap & /*bitmap*/) override {}
    void removeBitmap(const PartName & target, CSN csn) override { unlinks.emplace_back(target, csn); }
};

/// Coordinator + pin-registry stubs — recovery doesn't touch either, but
/// `PartitionTxnController`'s ctor requires non-null strategies.
class StubCoordinator : public ICommitCoordinator
{
public:
    PreparedCommitSnapshot readSnapshot() override { return {}; }
    /// Never invoked by recovery; stubs to satisfy the interface.
    CSN attemptCommit(PublishAction) override { return INVALID_CSN; }
    void withinSnapshotRegion(std::function<void(CSN)>) override {}
};

class StubPinRegistry : public IPinRegistry
{
public:
    std::shared_ptr<PinHandle> acquire(CSN /*csn*/) override { return nullptr; }
    CSN clusterFloor() override { return MAX_CSN; }
};

struct RecoveryFixture
{
    RecordingBitmapStore * store = nullptr;
    std::unique_ptr<PartitionTxnController> state;

    /// One sandbox per fixture, fronted by a real `DiskLocal`; tmp dirs are
    /// part directories underneath it.
    std::filesystem::path sandbox;
    DiskPtr disk;
    VolumePtr volume;

    /// Wrap a tmp-dir basename in a `DataPartStorageOnDiskFull` rooted at the
    /// sandbox — the shape `recover()` consumes in production.
    MutableDataPartStoragePtr storageFor(const std::string & name) const
    {
        return std::make_shared<DataPartStorageOnDiskFull>(volume, /*root_path=*/"", name);
    }
};

RecoveryFixture makeFixture()
{
    auto store_owned = std::make_unique<RecordingBitmapStore>();
    auto coord_owned = std::make_unique<StubCoordinator>();
    auto pin_owned = std::make_unique<StubPinRegistry>();
    RecoveryFixture fx;
    fx.store = store_owned.get();
    fx.state = std::make_unique<PartitionTxnController>(
        std::move(coord_owned),
        std::move(store_owned),
        std::move(pin_owned));

    std::random_device rd;
    const auto pid = static_cast<unsigned>(::getpid());
    const auto suffix = rd();
    const std::string unique_id = std::to_string(pid) + "_" + std::to_string(suffix);
    fx.sandbox = std::filesystem::temp_directory_path()
        / ("ck_uk_txn_recov_" + unique_id);
    std::filesystem::create_directories(fx.sandbox);
    fx.disk = std::make_shared<DiskLocal>("test_disk_" + unique_id, fx.sandbox.string());
    fx.volume = std::make_shared<SingleDiskVolume>("test_volume", fx.disk);
    return fx;
}

/// Create the tmp-dir part directory on disk and return its part storage.
MutableDataPartStoragePtr makeTmpDir(const RecoveryFixture & fx, const std::string & name)
{
    std::filesystem::create_directories(fx.sandbox / name);
    return fx.storageFor(name);
}

void plantBitmapStub(const IDataPartStorage & storage, CSN csn)
{
    /// Recovery doesn't read the bitmap content; the unlink path is what
    /// we verify. Plant a marker file so the directory has something
    /// concrete to remove.
    std::ofstream out(std::filesystem::path(storage.getFullPath())
        / ("delete_bitmap_" + std::to_string(csn) + ".rbm"));
    out << "stub-bitmap";
}

}

TEST(RecoveryTmpScan, ThirdPartyEntryUnlinksAndRemovesTmpDir)
{
    auto fx = makeFixture();
    auto tmp_dir = makeTmpDir(fx, "tmp_insert_all_5_5_0");
    const std::filesystem::path tmp_path{tmp_dir->getFullPath()};

    UniqueKeyManifest meta;
    meta.creation_csn = 42;
    meta.is_marker = false;
    meta.bitmaps_created.emplace_back("all_1_1_0", 42);
    UniqueKeyManifest::write(*tmp_dir, meta);
    plantBitmapStub(*tmp_dir, 42);

    ASSERT_TRUE(std::filesystem::exists(tmp_path));

    fx.state->recover({tmp_dir});

    ASSERT_EQ(fx.store->unlinks.size(), 1u);
    EXPECT_EQ(fx.store->unlinks[0].first, "all_1_1_0");
    EXPECT_EQ(fx.store->unlinks[0].second, 42u);

    EXPECT_FALSE(std::filesystem::exists(tmp_path));

    std::filesystem::remove_all(fx.sandbox);
}

TEST(RecoveryTmpScan, SelfTargetingEntryResolvesToTmpDirBasename)
{
    auto fx = makeFixture();
    auto tmp_dir = makeTmpDir(fx, "tmp_merge_all_3_5_1");
    const std::filesystem::path tmp_path{tmp_dir->getFullPath()};

    UniqueKeyManifest meta;
    meta.creation_csn = 7;
    meta.is_marker = false;
    /// `"self"` resolves to the tmp dir basename ("tmp_merge_all_3_5_1").
    meta.bitmaps_created.emplace_back("self", 7);
    /// Plus one third-party entry — covers the mixed case.
    meta.bitmaps_created.emplace_back("all_1_2_0", 7);
    UniqueKeyManifest::write(*tmp_dir, meta);
    plantBitmapStub(*tmp_dir, 7);

    fx.state->recover({tmp_dir});

    ASSERT_EQ(fx.store->unlinks.size(), 2u);
    /// Order matches the manifest's `bitmaps_created` order.
    EXPECT_EQ(fx.store->unlinks[0].first, "tmp_merge_all_3_5_1");
    EXPECT_EQ(fx.store->unlinks[0].second, 7u);
    EXPECT_EQ(fx.store->unlinks[1].first, "all_1_2_0");
    EXPECT_EQ(fx.store->unlinks[1].second, 7u);

    EXPECT_FALSE(std::filesystem::exists(tmp_path));

    std::filesystem::remove_all(fx.sandbox);
}

TEST(RecoveryTmpScan, MissingManifestThrowsFailClosed)
{
    auto fx = makeFixture();
    auto tmp_dir = makeTmpDir(fx, "tmp_insert_all_9_9_0");
    const std::filesystem::path tmp_path{tmp_dir->getFullPath()};
    /// Deliberately do NOT write unique_key.txt.

    EXPECT_THROW(fx.state->recover({tmp_dir}), DB::Exception);
    /// Tmp dir remains on disk — operator inspects.
    EXPECT_TRUE(std::filesystem::exists(tmp_path));
    EXPECT_TRUE(fx.store->unlinks.empty());

    std::filesystem::remove_all(fx.sandbox);
}

TEST(RecoveryTmpScan, MultipleTmpDirsDrainInOneCall)
{
    auto fx = makeFixture();
    auto tmp_a = makeTmpDir(fx, "tmp_insert_all_1_1_0");
    auto tmp_b = makeTmpDir(fx, "tmp_delete_all_2_2_0");
    const std::filesystem::path tmp_a_path{tmp_a->getFullPath()};
    const std::filesystem::path tmp_b_path{tmp_b->getFullPath()};

    UniqueKeyManifest meta_a;
    meta_a.creation_csn = 10;
    meta_a.is_marker = false;
    meta_a.bitmaps_created.emplace_back("all_0_0_0", 10);
    UniqueKeyManifest::write(*tmp_a, meta_a);

    UniqueKeyManifest meta_b;
    meta_b.creation_csn = 11;
    meta_b.is_marker = true;
    meta_b.bitmaps_created.emplace_back("all_0_0_0", 11);
    UniqueKeyManifest::write(*tmp_b, meta_b);

    fx.state->recover({tmp_a, tmp_b});

    EXPECT_EQ(fx.store->unlinks.size(), 2u);
    EXPECT_FALSE(std::filesystem::exists(tmp_a_path));
    EXPECT_FALSE(std::filesystem::exists(tmp_b_path));

    std::filesystem::remove_all(fx.sandbox);
}

TEST(RecoveryTmpScan, ForwardedEntriesAreNotUnlinked)
{
    /// A merge writes its tmp manifest BEFORE the rename. If the crash falls
    /// between the write and the rename, recovery sees the tmp dir and must
    /// unlink only the merge's OWN `bitmaps_created` (here a `self` bitmap for
    /// late kills, owned), leaving the `forwarded` entries alone — those belong
    /// to live third-party parts still using them. One manifest carrying both
    /// kinds proves the create-vs-forward split in a single pass.
    auto fx = makeFixture();
    auto tmp_dir = makeTmpDir(fx, "tmp_merge_all_3_5_1");
    const std::filesystem::path tmp_path{tmp_dir->getFullPath()};

    UniqueKeyManifest meta;
    meta.creation_csn = 50;
    meta.is_marker = false;
    meta.bitmaps_created = {{"self", 50}};
    meta.forwarded = {{"third_X", 10}, {"third_Y", 20}};
    UniqueKeyManifest::write(*tmp_dir, meta);
    plantBitmapStub(*tmp_dir, 50);

    fx.state->recover({tmp_dir});

    /// Only the self-bitmap was unlinked (resolved to the tmp dir basename);
    /// neither forwarded ref was touched.
    ASSERT_EQ(fx.store->unlinks.size(), 1u);
    EXPECT_EQ(fx.store->unlinks[0].first, "tmp_merge_all_3_5_1");
    EXPECT_EQ(fx.store->unlinks[0].second, 50u);
    EXPECT_FALSE(std::filesystem::exists(tmp_path));

    std::filesystem::remove_all(fx.sandbox);
}
