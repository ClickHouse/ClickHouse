#include <gtest/gtest.h>

#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/MergeTreeBitmapStore.h>
#include <Storages/MergeTree/UniqueKey/IBitmapStore.h>
#include <Storages/MergeTree/UniqueKey/Txn/SnapshotPinning.h>
#include <Storages/MergeTree/UniqueKey/Txn/LocalStrategies.h>
#include <Storages/MergeTree/UniqueKey/Txn/PartitionTxnController.h>
#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyManifest.h>
#include <Storages/MergeTree/UniqueKey/Txn/tests/gtest_txn_fakes.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace DB;
using namespace DB::UniqueKeyTxn;
using namespace DB::UniqueKeyTxn::tests;

namespace
{

std::unique_ptr<PartitionTxnController> makeFixture(
    RecordingBitmapStore *& out_store, FakeCoordinator *& out_coord)
{
    auto fx = tests::makeFixture();
    out_store = fx.store;
    out_coord = fx.coord;
    return std::move(fx.state);
}

}

/// The bitmap PUT must use the coordinator's freshly-assigned csn, not the
/// tentative `snap.csn + 1`: when the drift sampler bumps csn under the publish
/// lock, the sidecar (PUT inside the staging callable) and the manifest must
/// still agree on that assigned csn.
TEST(UniqueKeyDeleteViaTxnController, BitmapPutUsesAssignedCsnEvenWhenSamplerBumpsHigher)
{
    RecordingBitmapStore * store = nullptr;
    FakeCoordinator * coord = nullptr;
    auto state = makeFixture(store, coord);

    /// Simulate a concurrent INSERT having advanced `max_block` to 7 while
    /// this DELETE held `snap.csn = 0`. The drift sampler bumps `csn` to
    /// 7 under the publish lock, so `assigned_csn = 8` (not 1).
    coord->drift_sampler = [] { return static_cast<CSN>(7); };

    CommitRequest req;
    req.touched.push_back({"all_5_5_0", makeBitmap({1, 2, 3})});

    std::atomic<CSN> finalize_csn{INVALID_CSN};
    std::atomic<CSN> publish_csn{INVALID_CSN};
    req.staged.finalize_manifest = [&finalize_csn](CSN c)
    {
        finalize_csn.store(c, std::memory_order_release);
    };
    req.staged.publish = [&publish_csn](CSN c)
    {
        publish_csn.store(c, std::memory_order_release);
    };

    auto result = state->commit(std::move(req));

    EXPECT_EQ(result.csn, 8u);
    EXPECT_EQ(finalize_csn.load(), 8u);
    EXPECT_EQ(publish_csn.load(), 8u);
    ASSERT_EQ(store->puts.size(), 1u);
    EXPECT_EQ(store->puts[0].csn, 8u);
    EXPECT_EQ(store->puts[0].target, "all_5_5_0");
    EXPECT_EQ(store->puts[0].bytes->cardinality(), 3u);
}

/// Writer order is `finalize_manifest → bitmap PUT → publish`: the manifest
/// reaches disk before any sidecar (so recovery finds every (target, csn) this
/// commit claims), and the rename runs after all sidecars are durable (so a
/// successful publish has no missing predecessors).
TEST(UniqueKeyDeleteViaTxnController, PublishActionOrderIsManifestThenBitmapThenPublish)
{
    /// Store + finalize/publish callbacks all push labels into one shared event
    /// log, yielding a single totally-ordered sequence per commit.
    class OrderingBitmapStore : public IBitmapStore
    {
    public:
        std::vector<std::string> & events;
        explicit OrderingBitmapStore(std::vector<std::string> & ev) : events(ev) {}
        std::pair<ConstDeleteBitmapPtr, CSN>
        readBitmap(const PartName &, CSN) override { return {std::make_shared<DeleteBitmap>(), 0}; }
        void installBitmap(const PartName & target, CSN csn, const DeleteBitmap &) override
        {
            events.push_back("put:" + target + "@" + std::to_string(csn));
        }
        void removeBitmap(const PartName &, CSN) override {}
    };

    std::vector<std::string> events;
    auto store_owned = std::make_unique<OrderingBitmapStore>(events);
    auto coord_owned = std::make_unique<FakeCoordinator>();
    auto state = std::make_unique<PartitionTxnController>(
        std::move(coord_owned),
        std::move(store_owned),
        std::make_unique<CountingPinRegistry>());

    CommitRequest req;
    req.touched.push_back({"all_1_1_0", makeBitmap({42})});

    req.staged.finalize_manifest = [&events](CSN c)
    {
        events.push_back("finalize@" + std::to_string(c));
    };
    req.staged.publish = [&events](CSN c)
    {
        events.push_back("publish@" + std::to_string(c));
    };

    auto result = state->commit(std::move(req));

    EXPECT_EQ(result.csn, 1u);
    ASSERT_EQ(events.size(), 3u);
    EXPECT_EQ(events[0], "finalize@1");
    EXPECT_EQ(events[1], "put:all_1_1_0@1");
    EXPECT_EQ(events[2], "publish@1");
}

/// A publish-throw after PUT but before rename would leave an orphan sidecar
/// readers could observe as committed — making the aborted DELETE visible. The
/// fix wraps the PUTs + publish in a try/catch inside the staging callable and
/// `unlink`s every successful PUT on the way out.
TEST(UniqueKeyDeleteViaTxnController, PublishThrowRollsBackBitmapPutsAndLeavesNoOrphan)
{
    RecordingBitmapStore * store = nullptr;
    FakeCoordinator * coord = nullptr;
    auto state = makeFixture(store, coord);

    /// Seed a prior committed bitmap on the target so we can prove a
    /// subsequent read sees it after rollback. `coord->csn = 5` is the
    /// pre-commit baseline (the seeded csn=5 dominates `snap.csn = 5`).
    auto prior_bitmap = makeBitmap({100, 101});
    store->seed("all_3_3_0", /*csn=*/5, prior_bitmap);
    coord->csn = 5;

    /// Pre-commit cached_version sanity: seed bumped it to 5.
    EXPECT_EQ(store->cached_version["all_3_3_0"], 5u);

    CommitRequest req;
    req.touched.push_back({"all_3_3_0", makeBitmap({11, 12})});
    req.staged.finalize_manifest = [](CSN) {};
    req.staged.publish = [](CSN)
    {
        throw std::runtime_error("simulated rename failure");
    };

    EXPECT_THROW(state->commit(std::move(req)), std::runtime_error);

    EXPECT_EQ(coord->csn, 5u);

    /// The PUT happened at the assigned csn (6) but rollback `unlink`ed it, so
    /// the store map has no entry — no orphan delete_bitmap_6.rbm.
    ASSERT_EQ(store->puts.size(), 1u);
    EXPECT_EQ(store->puts[0].csn, 6u);
    EXPECT_EQ(store->puts[0].target, "all_3_3_0");
    EXPECT_EQ(store->store.count({"all_3_3_0", 6u}), 0u)
        << "publish-throw left an orphan delete_bitmap_6.rbm on disk";

    EXPECT_EQ(store->cached_version["all_3_3_0"], 5u);

    /// A read at a high snapshot still returns the seeded pre-DELETE bitmap —
    /// the aborted DELETE's rows {11, 12} are invisible.
    auto [visible, visible_csn] = store->readBitmap("all_3_3_0", /*snapshot_csn=*/1'000);
    ASSERT_NE(visible, nullptr);
    EXPECT_EQ(visible_csn, 5u);
    EXPECT_EQ(visible->cardinality(), 2u);
    EXPECT_TRUE(visible->contains(100));
    EXPECT_TRUE(visible->contains(101));
    EXPECT_FALSE(visible->contains(11));
    EXPECT_FALSE(visible->contains(12));

    /// Proves rollback ran (vs. the PUT never happening): one unlink at the
    /// same (target, csn) the PUT used.
    ASSERT_EQ(store->unlinks.size(), 1u);
    EXPECT_EQ(store->unlinks[0].target, "all_3_3_0");
    EXPECT_EQ(store->unlinks[0].csn, 6u);
}

namespace
{

/// Tempdir-backed real-storage fixture for `MergeTreeBitmapStore` (pattern
/// mirrors `gtest_merge_tree_bitmap_store.cpp`'s `PartStorageFixture`). The
/// store is exercised through its storage-level methods on this one part, so
/// install/remove hit a real `.rbm` file on disk and the monotonicity check
/// reads the on-disk version set.
struct MergeTreeBitmapStoreFixture
{
    std::filesystem::path base_path;
    std::string part_dir;
    DiskPtr disk;
    VolumePtr volume;
    MutableDataPartStoragePtr storage;
    std::unique_ptr<DB::MergeTreeBitmapStore> store;

    MergeTreeBitmapStoreFixture()
    {
        auto base = std::filesystem::temp_directory_path();
        auto unique_id = std::to_string(::getpid()) + "_"
            + std::to_string(reinterpret_cast<uintptr_t>(this));
        base_path = base / ("merge_tree_bitmap_store_gtest_" + unique_id);
        std::filesystem::create_directories(base_path);
        part_dir = "part";
        std::filesystem::create_directories(base_path / part_dir);

        disk = std::make_shared<DiskLocal>("test_disk_" + unique_id, base_path.string());
        volume = std::make_shared<SingleDiskVolume>("test_volume", disk);
        storage = std::make_shared<DataPartStorageOnDiskFull>(volume, /*root_path=*/"", part_dir);

        store = std::make_unique<DB::MergeTreeBitmapStore>(/*cache=*/nullptr);
    }

    ~MergeTreeBitmapStoreFixture()
    {
        std::error_code ec;
        std::filesystem::remove_all(base_path, ec);
    }
};

}

/// `MergeTreeBitmapStore::installBitmap` derives monotonicity from the versions
/// ON DISK (not a sticky in-memory watermark), so a rollback's `removeBitmap`
/// frees the csn for the retry the coordinator reissues at the same value.
///
/// We assert success-after-rollback rather than EXPECT_THROW on a genuine
/// violation: `LOGICAL_ERROR` aborts under DEBUG_OR_SANITIZER_BUILD, so a
/// throw-test would abort the binary.
TEST(MergeTreeBitmapStore, RollbackThenRetryAtSameCsnSucceeds)
{
    MergeTreeBitmapStoreFixture fx;
    const std::string part_id = "all_3_3_0";
    const std::string part_name = "all_3_3_0";
    const BitmapVersion csn = 6;

    DeleteBitmap bm;
    bm.add(11);
    bm.add(12);

    fx.store->installBitmap(*fx.storage, part_id, part_name, csn, bm);
    EXPECT_TRUE(std::filesystem::exists(fx.base_path / fx.part_dir / "delete_bitmap_6.rbm"));

    fx.store->removeBitmap(*fx.storage, part_id, csn);
    EXPECT_FALSE(std::filesystem::exists(fx.base_path / fx.part_dir / "delete_bitmap_6.rbm"));

    /// Retry at the SAME csn must NOT throw — disk no longer holds csn=6.
    fx.store->installBitmap(*fx.storage, part_id, part_name, csn, bm);
    EXPECT_TRUE(std::filesystem::exists(fx.base_path / fx.part_dir / "delete_bitmap_6.rbm"));

    auto [reread, reread_csn] = fx.store->readBitmap(*fx.storage, /*snapshot_csn=*/1'000, part_id);
    ASSERT_NE(reread, nullptr);
    EXPECT_EQ(reread_csn, 6u);
    EXPECT_EQ(reread->cardinality(), 2u);
    EXPECT_TRUE(reread->contains(11));
    EXPECT_TRUE(reread->contains(12));
}
