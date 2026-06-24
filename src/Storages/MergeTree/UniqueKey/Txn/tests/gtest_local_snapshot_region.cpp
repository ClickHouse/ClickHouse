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

    /// Writer thread — commits N_COMMITS times. Inside `attemptCommit`'s
    /// staging callable (which runs under the same `commit_lock` the
    /// snapshot region locks), bump `tick` so it advances by one per commit.
    /// The csn bump and the tick bump are therefore observed together by the
    /// snapshot region.
    std::thread writer([&]
    {
        for (int i = 0; i < N_COMMITS; ++i)
        {
            coord.attemptCommit([&](CSN /*tentative*/)
            {
                tick.fetch_add(1, std::memory_order_release);
            });
            /// Yield so the reader interleaves even on a fully-loaded host;
            /// without it the writer can drain all commits before the reader
            /// is scheduled (`reads == 0`), a CPU-starvation flake unrelated to
            /// the lock-coupling invariant under test.
            std::this_thread::yield();
        }
    });

    std::atomic<bool> stop{false};
    size_t reads = 0;
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
            ++reads;
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
    EXPECT_GT(reads, 0u);
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
