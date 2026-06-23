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
    return makeRecordingFixture(out_store, out_coord);
}

}

/// BLOCKER coverage: drift case.
///
/// `LocalCommitCoordinator::attemptCommit` may bump `csn` past the
/// tentative `snap.csn + 1` when a concurrent INSERT pushed
/// `max(info.max_block)` higher between this commit's `readSnapshot` and
/// the publish lock (sampler runs INSIDE the lock — see canonical §3
/// Writer). Before the fix, `PartitionTxnController::commit` called
/// `IBitmapStore::put(target, snap.csn + 1, ...)` BEFORE `attemptCommit`,
/// so the on-disk sidecar carried the tentative csn while the manifest
/// (rewritten in the publish callback) carried the higher assigned csn —
/// mismatched. After the fix, the PUT runs INSIDE the staging callable
/// with the freshly-allocated `assigned_csn`, so manifest + sidecar agree.
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
    req.is_marker = true;
    req.touched.push_back({"all_5_5_0", bitmapOf({1, 2, 3})});

    /// Capture both csns the callbacks see — they must match `assigned_csn`,
    /// not `snap.csn + 1`.
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

    /// Without the fix the PUT would have been recorded at csn=1 (the
    /// pre-drift `snap.csn + 1`); with the fix it lands at csn=8 — same
    /// csn both callbacks saw, same csn the commit result reports.
    EXPECT_EQ(result.csn, 8u);
    EXPECT_EQ(finalize_csn.load(), 8u);
    EXPECT_EQ(publish_csn.load(), 8u);
    ASSERT_EQ(store->puts.size(), 1u);
    EXPECT_EQ(store->puts[0].csn, 8u);
    EXPECT_EQ(store->puts[0].target, "all_5_5_0");
    EXPECT_EQ(store->puts[0].bytes->cardinality(), 3u);
}

/// BLOCKER coverage: callback ordering under the publish lock.
///
/// The canonical §3 Writer order is `finalize_manifest → bitmap PUT →
/// publish`. The manifest must reach disk BEFORE any sidecar so the
/// recovery sweep finds every (target, csn) pair this commit claims; the
/// rename must run AFTER all sidecars are durable so a successful publish
/// has no missing predecessors. The fake here records each callback's
/// invocation index against the same shared counter as the bitmap PUT;
/// the recorded order must be (finalize, put, publish).
TEST(UniqueKeyDeleteViaTxnController, PublishActionOrderIsManifestThenBitmapThenPublish)
{
    /// Specialized recording store that ALSO records ordering by pushing
    /// into a shared event log on each `put`. Combined with finalize /
    /// publish callbacks pushing their own labels, we get a single
    /// totally-ordered event sequence per commit.
    class OrderingBitmapStore : public IBitmapStore
    {
    public:
        std::vector<std::string> & events;
        explicit OrderingBitmapStore(std::vector<std::string> & ev) : events(ev) {}
        std::pair<std::shared_ptr<const DeleteBitmap>, CSN>
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
        std::make_unique<FakePinRegistry>());

    CommitRequest req;
    req.is_marker = true;
    req.touched.push_back({"all_1_1_0", bitmapOf({42})});

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

/// BLOCKER coverage: publish-throw rolls back bitmap PUTs.
///
/// A publish-throw after PUT but before rename would leave an orphan
/// sidecar that readers could observe as committed — making the aborted
/// DELETE visible. The fix wraps the PUTs + publish in a try/catch inside
/// the staging callable and `unlink`s every successful PUT on the way out.
///
/// Asserts the full post-rollback contract:
///   (1) `partition.csn` unchanged (coordinator never advances on throw).
///   (2) The store has NO entry for `(target, assigned_csn)` — file gone.
///   (3) The fake's `cached_version[target]` is back at its pre-commit
///       value (no stale fetch-max sticking at `assigned_csn`).
///   (4) A subsequent `getAt(target, high_snap)` returns the pre-DELETE
///       state (the seeded prior commit's bitmap), proving the aborted
///       DELETE is truly invisible.
///   (5) `unlinks` records the rollback PUTs were undone — verifies the
///       new rollback path actually ran (vs. the file happening to be
///       absent for another reason).
TEST(UniqueKeyDeleteViaTxnController, PublishThrowRollsBackBitmapPutsAndLeavesNoOrphan)
{
    RecordingBitmapStore * store = nullptr;
    FakeCoordinator * coord = nullptr;
    auto state = makeFixture(store, coord);

    /// Seed a prior committed bitmap on the target so we can prove a
    /// subsequent read sees it after rollback. `coord->csn = 5` is the
    /// pre-commit baseline (the seeded csn=5 dominates `snap.csn = 5`).
    auto prior_bitmap = bitmapOf({100, 101});
    store->seed("all_3_3_0", /*csn=*/5, prior_bitmap);
    coord->csn = 5;

    /// Pre-commit cached_version sanity: seed bumped it to 5.
    EXPECT_EQ(store->cached_version["all_3_3_0"], 5u);

    CommitRequest req;
    req.is_marker = true;
    req.touched.push_back({"all_3_3_0", bitmapOf({11, 12})});
    req.staged.finalize_manifest = [](CSN) {};
    req.staged.publish = [](CSN)
    {
        throw std::runtime_error("simulated rename failure");
    };

    EXPECT_THROW(state->commit(std::move(req)), std::runtime_error);

    /// (1) partition.csn unchanged.
    EXPECT_EQ(coord->csn, 5u);

    /// (2) The bitmap PUT did happen at the assigned csn (6) — recorded in
    ///     `puts` — but the store map has no entry for it: rollback
    ///     `unlink`ed it.
    ASSERT_EQ(store->puts.size(), 1u);
    EXPECT_EQ(store->puts[0].csn, 6u);
    EXPECT_EQ(store->puts[0].target, "all_3_3_0");
    EXPECT_EQ(store->store.count({"all_3_3_0", 6u}), 0u)
        << "publish-throw left an orphan delete_bitmap_6.rbm on disk";

    /// (3) Cached version reverted to the pre-commit value (5).
    EXPECT_EQ(store->cached_version["all_3_3_0"], 5u);

    /// (4) Subsequent read at a high snapshot returns the pre-DELETE
    ///     state — the seeded prior bitmap, with no row {11, 12}.
    auto [visible, visible_csn] = store->readBitmap("all_3_3_0", /*snapshot_csn=*/1'000);
    ASSERT_NE(visible, nullptr);
    EXPECT_EQ(visible_csn, 5u);
    EXPECT_EQ(visible->cardinality(), 2u);
    EXPECT_TRUE(visible->contains(100));
    EXPECT_TRUE(visible->contains(101));
    EXPECT_FALSE(visible->contains(11));
    EXPECT_FALSE(visible->contains(12));

    /// (5) Rollback `unlink` actually ran (vs. a never-PUT scenario): one
    ///     unlink for the same `(target, csn)` the PUT used.
    ASSERT_EQ(store->unlinks.size(), 1u);
    EXPECT_EQ(store->unlinks[0].target, "all_3_3_0");
    EXPECT_EQ(store->unlinks[0].csn, 6u);
}

/// Multi-target rollback. When a publish throws after multiple targets
/// have been PUT, EACH target gets unlinked (reverse order). Confirms
/// the rollback isn't truncated by partial success.
TEST(UniqueKeyDeleteViaTxnController, PublishThrowRollsBackAllMultiTargetPuts)
{
    RecordingBitmapStore * store = nullptr;
    FakeCoordinator * coord = nullptr;
    auto state = makeFixture(store, coord);

    coord->csn = 0;

    CommitRequest req;
    req.is_marker = true;
    req.touched.push_back({"all_1_1_0", bitmapOf({1})});
    req.touched.push_back({"all_2_2_0", bitmapOf({2})});
    req.touched.push_back({"all_3_3_0", bitmapOf({3})});
    req.staged.finalize_manifest = [](CSN) {};
    req.staged.publish = [](CSN)
    {
        throw std::runtime_error("simulated rename failure");
    };

    EXPECT_THROW(state->commit(std::move(req)), std::runtime_error);

    /// All three PUTs were unlinked; no orphan entries in the store.
    EXPECT_EQ(coord->csn, 0u);
    EXPECT_EQ(store->puts.size(), 3u);
    ASSERT_EQ(store->unlinks.size(), 3u);
    EXPECT_EQ(store->store.size(), 0u);

    /// Reverse-order rollback: unlinks visit (all_3, all_2, all_1).
    EXPECT_EQ(store->unlinks[0].target, "all_3_3_0");
    EXPECT_EQ(store->unlinks[1].target, "all_2_2_0");
    EXPECT_EQ(store->unlinks[2].target, "all_1_1_0");
    EXPECT_EQ(store->unlinks[0].csn, 1u);
    EXPECT_EQ(store->unlinks[1].csn, 1u);
    EXPECT_EQ(store->unlinks[2].csn, 1u);
}

/// MAJOR coverage: manifest-throw leaves no bitmap on disk.
///
/// Symmetric to the publish-throw case. If `finalize_manifest` throws (no
/// manifest on disk for this tmp), the staging callable aborts before any
/// bitmap PUT — so the target part's `delete_bitmap_<csn>.rbm` was never
/// written. Both `partition.csn` and the on-disk bitmap inventory are
/// unchanged.
TEST(UniqueKeyDeleteViaTxnController, ManifestThrowLeavesNoBitmapBehind)
{
    RecordingBitmapStore * store = nullptr;
    FakeCoordinator * coord = nullptr;
    auto state = makeFixture(store, coord);

    coord->csn = 5;

    CommitRequest req;
    req.is_marker = true;
    req.touched.push_back({"all_4_4_0", bitmapOf({13, 14, 15})});
    req.staged.finalize_manifest = [](CSN)
    {
        throw std::runtime_error("simulated manifest write failure");
    };
    /// publish must not be invoked.
    bool publish_called = false;
    req.staged.publish = [&publish_called](CSN) { publish_called = true; };

    EXPECT_THROW(state->commit(std::move(req)), std::runtime_error);

    EXPECT_EQ(coord->csn, 5u);
    EXPECT_TRUE(store->puts.empty());
    EXPECT_FALSE(publish_called);
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

/// BLOCKER coverage: rollback-then-retry at the same csn must succeed.
///
/// `MergeTreeBitmapStore::installBitmap` derives monotonicity from the versions
/// ON DISK (not a sticky in-memory watermark). After a publish-throw,
/// rollback calls `removeBitmap(target, csn)` which deletes the `.rbm` file;
/// the coordinator leaves `partition.csn` unchanged, so the next attempt
/// reuses the same `assigned_csn`. With the disk-derived check the retry
/// install at the same csn succeeds — the just-removed version is gone from
/// disk, so it is not a monotonicity violation.
///
/// (We assert the success-after-rollback property rather than EXPECT_THROW
/// on a genuine violation: `LOGICAL_ERROR` aborts under
/// DEBUG_OR_SANITIZER_BUILD, so a throw-test would abort the binary.)
TEST(MergeTreeBitmapStore, RollbackThenRetryAtSameCsnSucceeds)
{
    MergeTreeBitmapStoreFixture fx;
    const std::string part_id = "all_3_3_0";
    const std::string part_name = "all_3_3_0";
    const BitmapVersion csn = 6;

    DeleteBitmap bm;
    bm.add(11);
    bm.add(12);

    /// First install lands the file on disk.
    fx.store->installBitmap(*fx.storage, part_id, part_name, csn, bm);
    EXPECT_TRUE(std::filesystem::exists(fx.base_path / fx.part_dir / "delete_bitmap_6.rbm"));

    /// Rollback: remove the just-installed version (file + cache gone).
    fx.store->removeBitmap(*fx.storage, part_id, csn);
    EXPECT_FALSE(std::filesystem::exists(fx.base_path / fx.part_dir / "delete_bitmap_6.rbm"));

    /// Retry at the SAME csn — must NOT throw (would abort under DEBUG if the
    /// old sticky-watermark check survived). The disk no longer holds csn=6,
    /// so the install is monotone again.
    fx.store->installBitmap(*fx.storage, part_id, part_name, csn, bm);
    EXPECT_TRUE(std::filesystem::exists(fx.base_path / fx.part_dir / "delete_bitmap_6.rbm"));

    /// And the retried bitmap reads back at csn=6.
    auto [reread, reread_csn] = fx.store->readBitmap(*fx.storage, /*snapshot_csn=*/1'000, part_id);
    ASSERT_NE(reread, nullptr);
    EXPECT_EQ(reread_csn, 6u);
    EXPECT_EQ(reread->cardinality(), 2u);
    EXPECT_TRUE(reread->contains(11));
    EXPECT_TRUE(reread->contains(12));
}
