#include <gtest/gtest.h>

#include <Storages/MergeTree/UniqueKey/IBitmapStore.h>
#include <Storages/MergeTree/UniqueKey/Txn/SnapshotPinning.h>
#include <Storages/MergeTree/UniqueKey/Txn/PartitionTxnController.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/Txn/tests/gtest_txn_fakes.h>

#include <Common/Exception.h>

#include <memory>
#include <optional>
#include <utility>
#include <vector>

using namespace DB;
using namespace DB::UniqueKeyTxn;
using namespace DB::UniqueKeyTxn::tests;

namespace DB::ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
}

namespace
{

/// The cumulative-publish tests assert the per-part bitmap chain stays
/// monotone, so the shared recording store runs with monotonicity enforcement on.
std::unique_ptr<PartitionTxnController> makeFixture(RecordingBitmapStore *& out_store)
{
    FakeCoordinator * coord_unused = nullptr;
    auto fixture = makeRecordingFixture(out_store, coord_unused);
    out_store->enforce_monotonicity = true;
    return fixture;
}

CommitRequest oneTouchedCommit(const PartName & target, std::shared_ptr<DeleteBitmap> delta)
{
    CommitRequest req;
    TouchedPartKills tk;
    tk.target = target;
    tk.new_kills = std::move(delta);
    req.touched.push_back(std::move(tk));
    return req;
}

}

/// Cumulative matrix over the (prev, delta) emptiness corners that change the
/// PUT shape: the cumulative published is `prev ∪ delta`, and the put is
/// SKIPPED entirely only when both are empty (no empty cumulative manufactured
/// that would falsely advance the per-part bitmap chain). The non-empty ∪
/// non-empty corner has its own test (`...PublishesUnion`) since it also pins
/// the no-in-place-mutation guard.
TEST(PartitionTxnControllerCommitCumulative, CumulativePutShapeMatrix)
{
    struct Case
    {
        const char * name;
        std::vector<UInt64> prev;          /// seeded at csn=0; empty → no seed
        std::vector<UInt64> delta;         /// empty → empty DeleteBitmap delta
        std::optional<std::vector<UInt64>> expected_put;  /// nullopt → put skipped
    };

    const Case cases[] = {
        {"EmptyPrevNonEmptyDelta", {}, {1, 2, 3}, std::vector<UInt64>{1, 2, 3}},
        {"EmptyPrevEmptyDelta", {}, {}, std::nullopt},
        {"NonEmptyPrevEmptyDelta", {5, 6}, {}, std::vector<UInt64>{5, 6}},
    };

    for (const auto & c : cases)
    {
        RecordingBitmapStore * store = nullptr;
        auto state = makeFixture(store);

        if (!c.prev.empty())
        {
            auto prev_bitmap = std::make_shared<DeleteBitmap>();
            prev_bitmap->addMany(c.prev);
            store->seed("partA", 0, std::move(prev_bitmap));
        }

        auto delta = std::make_shared<DeleteBitmap>();
        delta->addMany(c.delta);

        auto result = state->commit(oneTouchedCommit("partA", delta));
        EXPECT_EQ(result.csn, 1u) << c.name;

        if (!c.expected_put)
        {
            EXPECT_TRUE(store->puts.empty()) << c.name;
            continue;
        }

        ASSERT_EQ(store->puts.size(), 1u) << c.name;
        EXPECT_EQ(store->puts[0].target, "partA") << c.name;
        EXPECT_EQ(store->puts[0].csn, 1u) << c.name;
        ASSERT_NE(store->puts[0].bytes, nullptr) << c.name;
        EXPECT_EQ(store->puts[0].bytes->toVector(), *c.expected_put) << c.name;
    }
}

TEST(PartitionTxnControllerCommitCumulative, NonEmptyPrevNonEmptyDeltaPublishesUnion)
{
    RecordingBitmapStore * store = nullptr;
    auto state = makeFixture(store);

    /// Seed prev bitmap at csn=0 (the initial snap.csn). getAt(part, 0)
    /// will pick this up because 0 ≤ 0.
    store->seed("partA", 0, makeBitmap({10, 20}));

    auto delta = makeBitmap({30});
    auto result = state->commit(oneTouchedCommit("partA", delta));
    EXPECT_EQ(result.csn, 1u);

    /// The PUT must carry prev ∪ delta = {10, 20, 30}, NOT just the delta.
    /// (This is the BLOCKER 1 regression guard.)
    ASSERT_EQ(store->puts.size(), 1u);
    EXPECT_EQ(store->puts[0].csn, 1u);
    ASSERT_NE(store->puts[0].bytes, nullptr);
    EXPECT_EQ(store->puts[0].bytes->toVector(), (std::vector<UInt64>{10, 20, 30}));

    /// Delta payload must NOT have been mutated in place.
    EXPECT_EQ(delta->toVector(), (std::vector<UInt64>{30}));
}

TEST(PartitionTxnControllerCommitCumulative, MultipleTouchedPartsCumulateIndependently)
{
    RecordingBitmapStore * store = nullptr;
    auto state = makeFixture(store);

    store->seed("partA", 0, makeBitmap({1}));
    store->seed("partB", 0, makeBitmap({100, 200}));

    CommitRequest req;
    {
        TouchedPartKills tA;
        tA.target = "partA";
        tA.new_kills = makeBitmap({2, 3});
        req.touched.push_back(std::move(tA));
    }
    {
        TouchedPartKills tB;
        tB.target = "partB";
        tB.new_kills = makeBitmap({300});
        req.touched.push_back(std::move(tB));
    }

    auto result = state->commit(std::move(req));
    EXPECT_EQ(result.csn, 1u);

    ASSERT_EQ(store->puts.size(), 2u);
    /// Each part's put must carry that part's cumulative — no
    /// cross-contamination between targets.
    for (const auto & p : store->puts)
    {
        EXPECT_EQ(p.csn, 1u);
        ASSERT_NE(p.bytes, nullptr);
        if (p.target == "partA")
            EXPECT_EQ(p.bytes->toVector(), (std::vector<UInt64>{1, 2, 3}));
        else if (p.target == "partB")
            EXPECT_EQ(p.bytes->toVector(), (std::vector<UInt64>{100, 200, 300}));
        else
            FAIL() << "Unexpected target: " << p.target;
    }
}

/// A publish-throw whose rollback also throws (removeBitmap fails) latches the
/// partition fail-closed: the orphaned install survives on disk and csn-seed
/// could later surface it as committed, so subsequent commit / takeQuerySnapshot
/// must reject until the table reloads and recovery reconciles the sidecar.
TEST(PartitionTxnControllerCommitCumulative, RollbackFailureLatchesPartitionFailClosed)
{
    auto controller = std::make_unique<PartitionTxnController>(
        std::make_unique<FakeCoordinator>(),
        std::make_unique<ThrowOnRemoveBitmapStore>(),
        std::make_unique<FakePinRegistry>());

    CommitRequest req = oneTouchedCommit("partA", makeBitmap({1, 2, 3}));
    /// Install succeeds, then publish throws → rollback hits the throwing removeBitmap.
    req.staged.publish = [](CSN) { throw DB::Exception(DB::ErrorCodes::CANNOT_OPEN_FILE, "injected publish failure"); };
    EXPECT_THROW(controller->commit(std::move(req)), DB::Exception);

    /// Latched: further commit and snapshot fail closed.
    EXPECT_THROW(controller->commit(oneTouchedCommit("partB", makeBitmap({4}))), DB::Exception);
    EXPECT_THROW(controller->takeQuerySnapshot(), DB::Exception);
}

/// A publish-throw whose rollback SUCCEEDS (removeBitmap undoes the install) does
/// NOT latch — no orphan survives, so the partition stays usable.
TEST(PartitionTxnControllerCommitCumulative, SuccessfulRollbackDoesNotLatch)
{
    RecordingBitmapStore * store = nullptr;
    auto controller = makeFixture(store);

    CommitRequest req = oneTouchedCommit("partA", makeBitmap({1, 2, 3}));
    req.staged.publish = [](CSN) { throw DB::Exception(DB::ErrorCodes::CANNOT_OPEN_FILE, "injected publish failure"); };
    EXPECT_THROW(controller->commit(std::move(req)), DB::Exception);

    /// Rollback erased the install → not latched; partition still serves.
    EXPECT_NO_THROW(controller->takeQuerySnapshot());
    auto ok = controller->commit(oneTouchedCommit("partB", makeBitmap({4})));
    EXPECT_GT(ok.csn, 0u);
}

/// installBitmap that throws AFTER a durable write is still rolled back: commit
/// records each target BEFORE the install, so the (idempotent) rollback removeBitmap
/// erases the orphan; with rollback succeeding the partition stays usable.
TEST(PartitionTxnControllerCommitCumulative, InstallThrowAfterDurableWriteIsRolledBack)
{
    auto store_owned = std::make_unique<InstallThrowsAfterWriteStore>();
    auto * store = store_owned.get();
    auto controller = std::make_unique<PartitionTxnController>(
        std::make_unique<FakeCoordinator>(),
        std::move(store_owned),
        std::make_unique<FakePinRegistry>());

    EXPECT_THROW(controller->commit(oneTouchedCommit("partA", makeBitmap({1, 2, 3}))), DB::Exception);

    /// The durably-written orphan at the assigned csn (=1) was removed by rollback.
    auto [bm, found_csn] = store->readBitmap("partA", 1);
    EXPECT_EQ(found_csn, 0u);
    EXPECT_TRUE(bm->empty());
    /// removeBitmap succeeded → not latched; partition still serves.
    EXPECT_NO_THROW(controller->takeQuerySnapshot());
}
