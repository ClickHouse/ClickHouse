#include <gtest/gtest.h>

#include <Storages/MergeTree/UniqueKey/IBitmapStore.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/Txn/LocalStrategies.h>
#include <Storages/MergeTree/UniqueKey/Txn/PartitionTxnController.h>

#include <atomic>
#include <memory>
#include <thread>

using namespace DB;
using namespace DB::UniqueKeyTxn;

namespace
{

/// Minimal store — `takeQuerySnapshot` binds `bitmap_at` to it but this test
/// never reads a bitmap; the floor/pin coupling is what's under test.
class EmptyBitmapStore : public IBitmapStore
{
public:
    std::pair<ConstDeleteBitmapPtr, CSN>
    readBitmap(const PartName &, CSN) override { return {std::make_shared<DeleteBitmap>(), 0}; }
    void installBitmap(const PartName &, CSN, const DeleteBitmap &) override {}
    void removeBitmap(const PartName &, CSN) override {}
};

}

/// The snapshot region must capture `(observed-state, csn)` as ONE
/// observation under the coordinator's `commit_lock`. We drive an atomic
/// counter (`tick`) that the writer bumps INSIDE `attemptCommit`'s staging
/// callable — which the coordinator runs under that same mutex, in lock-step
/// with the csn bump. A reader running its observation through
/// `withinSnapshotRegion` (the same lock `takeQuerySnapshot` uses) must always
/// see `tick == csn`; if the two reads were not lock-coupled it could observe
/// `tick = N` paired with `csn = N+1` (writer interleaved between them).
TEST(LocalSnapshotRegion, WithinSnapshotRegionIsAtomicWithCommit)
{
    LocalCommitCoordinator coord;
    std::atomic<UInt64> tick{0};

    constexpr int N_COMMITS = 200;

    /// Reader thread — repeatedly observes `(tick, csn)` THROUGH the snapshot
    /// region (the same `commit_lock` the writer commits under). The pair must
    /// always be consistent: if the csn bump and the tick bump were not
    /// lock-coupled, the reader could observe `tick = N` paired with `csn = N+1`.
    std::atomic<bool> stop{false};
    std::atomic<size_t> reads{0};
    std::thread reader([&]
    {
        while (!stop.load(std::memory_order_acquire))
        {
            coord.withinSnapshotRegion([&](CSN current_csn)
            {
                /// Read `tick` under the snapshot lock, paired with `current_csn`.
                const UInt64 t = tick.load(std::memory_order_acquire);
                EXPECT_EQ(t, static_cast<UInt64>(current_csn))
                    << "snapshot region violated: tick=" << t << " csn=" << current_csn;
            });
            reads.fetch_add(1, std::memory_order_release);
        }
    });

    /// Writer thread — commits N_COMMITS times, bumping `tick` inside
    /// `attemptCommit`'s staging callable (run under the same `commit_lock` the
    /// snapshot region locks), so the csn bump and the tick bump are observed
    /// together by the reader. Wait until the reader has taken at least one
    /// observation first: this makes the commits provably overlap a live reader
    /// and `reads > 0` hold WITHOUT depending on thread scheduling — under slow
    /// instrumentation (msan / coverage) the reader could otherwise be starved
    /// until after the writer drained, a flake unrelated to the lock-coupling
    /// invariant under test.
    std::thread writer([&]
    {
        while (reads.load(std::memory_order_acquire) == 0)
            std::this_thread::yield();
        for (int i = 0; i < N_COMMITS; ++i)
        {
            coord.attemptCommit([&](CSN /*tentative*/)
            {
                tick.fetch_add(1, std::memory_order_release);
            });
            std::this_thread::yield();
        }
    });

    writer.join();
    stop.store(true, std::memory_order_release);
    reader.join();

    /// After the writer is done, the final coordinator csn equals the commit
    /// count; a final observation must show tick = csn.
    UInt64 final_tick = 0;
    coord.withinSnapshotRegion([&](CSN c) { final_tick = static_cast<UInt64>(c); });
    EXPECT_EQ(final_tick, static_cast<UInt64>(N_COMMITS));
    EXPECT_GT(reads.load(), 0u);
}

/// `takeQuerySnapshot` installs the pin at the captured `view->csn` — same
/// observation, no skew — and the pin is visible to the controller's cluster
/// floor immediately (no deferred-publish window). Driven through the real
/// `PartitionTxnController` over a real `LocalCommitCoordinator` +
/// `LocalPinRegistry`; the pin/csn coupling is what's under test.
TEST(LocalSnapshotRegion, TakeQuerySnapshotInstallsPinAtomically)
{
    auto coord_owned = std::make_unique<LocalCommitCoordinator>();
    auto * coord = coord_owned.get();
    PartitionTxnController controller(
        std::move(coord_owned),
        std::make_unique<EmptyBitmapStore>(),
        std::make_unique<LocalPinRegistry>());

    /// Pre-commit two rounds so csn > 0.
    for (int i = 0; i < 2; ++i)
        coord->attemptCommit([](CSN) {});

    auto snap = controller.takeQuerySnapshot();
    ASSERT_TRUE(snap);

    /// The pin must be at the captured `view->csn` — same observation, no skew.
    EXPECT_EQ(snap.pinnedCsn(), snap->csn);
    EXPECT_EQ(snap->csn, 2u);

    /// The pin is visible to the controller's cluster floor while the snapshot
    /// is held.
    EXPECT_EQ(controller.clusterFloor(), snap->csn);
}
