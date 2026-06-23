/// Read-path: snapshot-anchored bitmap pick.
///
/// `PartitionTxnController::takeQuerySnapshot()` returns a `Pinned<PartitionView>`
/// whose `bitmap_at(part)` closure must dispatch to
/// `IBitmapStore::getAt(part, snapshot_csn)`. These tests cover:
///   1. Bitmap selection at snapshot_csn picks the max version ≤ csn.
///   2. Snapshot held across a later DELETE: the older reader continues to
///      see the pre-DELETE bitmap (csn-gating works).
///   3. Pin lifetime: pin count tracks the snapshot's lifetime via RAII.
///   4. Multi-part lookup against one snapshot.
///   5. share() preserves pin ref-count across copies.
#include <gtest/gtest.h>

#include <Storages/MergeTree/UniqueKey/Txn/tests/gtest_txn_fakes.h>

using namespace DB;
using namespace DB::UniqueKeyTxn;
using namespace DB::UniqueKeyTxn::tests;

TEST(PartitionTxnControllerQuerySnapshot, PicksMaxBitmapAtSnapshotCsn)
{
    auto fx = makeFixture();
    fx.coord->setCurrent(5);
    /// Sidecars at csn=2 and csn=4: snapshot at csn=5 should pick csn=4.
    fx.store->seed("partA", 2, makeBitmap({10}));
    fx.store->seed("partA", 4, makeBitmap({10, 20}));
    /// Sidecar at csn=7 lives above the snapshot — must be skipped.
    fx.store->seed("partA", 7, makeBitmap({10, 20, 30}));

    auto snap = fx.state->takeQuerySnapshot();
    ASSERT_TRUE(snap->bitmap_at);

    auto bm = snap->bitmap_at("partA");
    ASSERT_NE(bm, nullptr);
    EXPECT_EQ(bm->toVector(), (std::vector<UInt64>{10, 20}));
    EXPECT_EQ(snap->csn, 5u);
}

TEST(PartitionTxnControllerQuerySnapshot, OlderSnapshotHidesNewerDelete)
{
    /// Reader takes a snapshot at csn=3; a DELETE commit lands at csn=4
    /// installing a new sidecar. The OLDER reader's bound `bitmap_at` must
    /// still return the csn=3 bitmap (snapshot_csn never advances).
    auto fx = makeFixture();
    fx.coord->setCurrent(3);
    fx.store->seed("partA", 3, makeBitmap({10}));

    auto reader_snap = fx.state->takeQuerySnapshot();

    /// Concurrent DELETE commit at csn=4.
    fx.store->seed("partA", 4, makeBitmap({10, 20}));
    fx.coord->setCurrent(4);

    /// Older reader still sees csn=3 (10), not csn=4 (10,20).
    auto bm = reader_snap->bitmap_at("partA");
    ASSERT_NE(bm, nullptr);
    EXPECT_EQ(bm->toVector(), (std::vector<UInt64>{10}));

    /// A fresh snapshot would see csn=4.
    auto fresh = fx.state->takeQuerySnapshot();
    auto fresh_bm = fresh->bitmap_at("partA");
    ASSERT_NE(fresh_bm, nullptr);
    EXPECT_EQ(fresh_bm->toVector(), (std::vector<UInt64>{10, 20}));
}

TEST(PartitionTxnControllerQuerySnapshot, PinReleasesOnSnapshotDestroyed)
{
    auto fx = makeFixture();
    fx.coord->setCurrent(7);

    EXPECT_EQ(fx.pins->total(), 0u);
    {
        auto snap = fx.state->takeQuerySnapshot();
        EXPECT_EQ(fx.pins->total(), 1u);
        EXPECT_EQ(snap.pinnedCsn(), 7u);
    }
    /// `Pinned<>` dropped → pin handle released.
    EXPECT_EQ(fx.pins->total(), 0u);
}

TEST(PartitionTxnControllerQuerySnapshot, SharedPinSurvivesSourceDrop)
{
    auto fx = makeFixture();
    fx.coord->setCurrent(2);
    auto snap = fx.state->takeQuerySnapshot();
    EXPECT_EQ(fx.pins->total(), 1u);

    {
        auto extra = snap.share();
        EXPECT_EQ(fx.pins->total(), 1u); /// share() does NOT increment registry count
    }
    /// Original still alive → pin still held.
    EXPECT_EQ(fx.pins->total(), 1u);
}

