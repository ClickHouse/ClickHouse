#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobMeta.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>

#include <chrono>
#include <variant>
#include <future>
#include <thread>
#include <utility>

using namespace DB::Cas;
using DB::Cas::tests::MetaWriteLatchBackend;
using DB::Cas::tests::awaitLatchEntered;
using DB::Cas::tests::openRequestsForTest;

namespace
{
constexpr auto kGcId = "0000000000000000000000000000002a";

static_assert(noexcept(std::declval<GcMetaWriter &>().drainOnExitNoThrow()),
    "round-exit meta-pool cleanup must not throw from the scope guard");
}

/// A real condemn-marker job may be in flight when its `Gc` is destroyed. The job holds everything it
/// touches, so the pool's join completes it correctly rather than racing member teardown -- and the
/// marker it was writing is durable afterwards.
///
/// This asserts function, not ordering: the release may land before, during or after destruction
/// begins, and all three are sound. Nothing here detects a job that wrongly captured its owner --
/// that is prevented by there being no API to write one.
TEST(CASGcMetaWriter, RealCondemnMarkerJobCompletesAcrossOwnerDestruction)
{
    auto backend = std::make_shared<MetaWriteLatchBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});

    const BlobRef ref = DB::Cas::tests::idOf("1");
    const PersistedEtag token{"emulated", "tok-1"};

    auto gc = std::make_unique<Gc>(store, DB::Cas::tests::u128Of(kGcId));
    backend->arm();
    gc->metaWriterForTest().scheduleCondemnMarkerWrite(ref, token, /*condemn_round=*/1, /*size=*/128);

    awaitLatchEntered(*backend);

    std::thread releaser([&] { backend->release(); });
    gc.reset();
    releaser.join();

    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const auto meta = loadMeta(op, store->layout(), ref);
    ASSERT_TRUE(meta) << "the condemn marker was lost across owner destruction";
    EXPECT_EQ(meta->meta.state, MetaState::Condemned);
    EXPECT_EQ(meta->meta.condemn_round, 1u);
}

/// The confirmation registry is written by the pool thread and read by the graduation gate. Assert it
/// on a `Gc` that is still alive, so the read is possible at all: after destruction there is no
/// registry left to consult, which is the documented behaviour a fresh leader relies on.
TEST(CASGcMetaWriter, CondemnMarkerConfirmationIsVisibleAfterDrain)
{
    auto backend = std::make_shared<MetaWriteLatchBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});

    const BlobRef ref = DB::Cas::tests::idOf("1");
    const PersistedEtag token{"emulated", "tok-1"};

    Gc gc(store, DB::Cas::tests::u128Of(kGcId));
    EXPECT_FALSE(gc.metaWriterForTest().condemnMarkerConfirmedInProcess(ref, token));

    gc.metaWriterForTest().scheduleCondemnMarkerWrite(ref, token, /*condemn_round=*/1, /*size=*/128);
    gc.metaWriterForTest().drain();

    EXPECT_TRUE(gc.metaWriterForTest().condemnMarkerConfirmedInProcess(ref, token));
    EXPECT_EQ(gc.metaWriterForTest().scheduled(), gc.metaWriterForTest().completed());
}

/// Same lifetime property for the other production job. `deleteConfirmedMeta` RETURNS IMMEDIATELY when
/// no meta object exists (`Gc/CasGcMetaWriter.cpp`), so the meta must be seeded first -- otherwise the
/// job never reaches the latch and the wait above is waiting for something that will never happen.
TEST(CASGcMetaWriter, RealConfirmedMetaDeleteCompletesAcrossOwnerDestruction)
{
    auto backend = std::make_shared<MetaWriteLatchBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});

    const BlobRef ref = DB::Cas::tests::idOf("2");
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(putMetaIfAbsent(
        op, store->layout(), ref, BlobMeta{.state = MetaState::Condemned, .condemn_round = 1, .size = 64})));
    ASSERT_TRUE(loadMeta(op, store->layout(), ref));

    auto gc = std::make_unique<Gc>(store, DB::Cas::tests::u128Of(kGcId));
    backend->arm();
    gc->metaWriterForTest().scheduleConfirmedMetaDelete(ref);

    awaitLatchEntered(*backend);

    std::thread releaser([&] { backend->release(); });
    gc.reset();
    releaser.join();

    EXPECT_FALSE(loadMeta(op, store->layout(), ref))
        << "the confirmed-meta delete was lost across owner destruction";
}

/// A round that throws must not leave its meta jobs running into the next round: their effects would
/// land in the registry the next round's graduation gate reads, and inside its counter deltas.
///
/// The round is made to throw at its outcome-log write, with the confirmed-meta delete it scheduled a
/// few lines earlier held inside the backend. The round must then BLOCK, draining, until that job is
/// released -- so the test asserts the round has NOT returned while the job is still held, releases,
/// and only then joins.
TEST(CASGcMetaWriter, ThrowingRoundDrainsBeforeReturning)
{
    auto backend = std::make_shared<DB::Cas::tests::OutcomeLogFaultBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});

    /// Fixture: one part written and dropped, then rounds driven until the NEXT round is the one that
    /// deletes -- the round that both schedules a confirmed-meta delete and writes an outcome log.
    const RootNamespace ns{"test/tbl"};
    const String ref_name = "all_0_0_0";
    const String payload = "round-drain-payload";
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref_name;
    auto build = store->beginPartWrite(info);
    ManifestEntry entry;
    entry.path = "data.bin";
    entry.placement = EntryPlacement::Blob;
    entry.ref = DB::Cas::tests::idOf(payload);
    entry.blob_size = payload.size();
    const ManifestId manifest_id = build->stageManifest({entry});
    build->precommitAdd(ns, ref_name, manifest_id);
    build->putBlob(entry.ref, BlobSource::fromString(payload));
    build->promote(ns, ref_name, build->buildId(), manifest_id);
    store->dropRef(ns, ref_name);
    store->renewWatermarkOnce();

    Gc gc(store, DB::Cas::tests::u128Of(kGcId));

    size_t rounds = 0;
    while (true)
    {
        bool delete_pending = false;
        for (const auto & entry_to_delete : gc.previewDeletes())
            delete_pending |= entry_to_delete.reason == "delete_pending";
        if (delete_pending)
            break;

        ASSERT_LT(++rounds, 16u) << "no round ever reached a pending delete -- fixture is wrong";
        ASSERT_NO_THROW(gc.runRegularRound());
        store->renewWatermarkOnce();
    }

    const uint64_t scheduled_before = gc.metaWriterForTest().scheduled();

    backend->arm();
    backend->fail_outcome_logs.store(true);

    /// Return the outcome instead of asserting on the worker thread: a gtest assertion raised off the
    /// main thread is not reliably reported, and this one distinguishes the two ways the test can go
    /// wrong, so it must be visible.
    auto round = std::async(std::launch::async, [&]
    {
        try
        {
            gc.runRegularRound();
            return false;
        }
        catch (...)
        {
            return true;
        }
    });

    awaitLatchEntered(*backend);
    EXPECT_GT(gc.metaWriterForTest().scheduled(), scheduled_before)
        << "the faulted round scheduled no meta job -- it cannot be the deleting round";

    EXPECT_EQ(round.wait_for(std::chrono::seconds(2)), std::future_status::timeout)
        << "the round returned while a meta job was still in flight -- it did not drain on its "
           "throwing exit";

    backend->release();
    EXPECT_TRUE(round.get())
        << "the round completed normally -- the outcome-log fault never fired, so the timeout above "
           "was the round blocking in its own `meta_pool_wait`, not in the drain under test";

    EXPECT_EQ(gc.metaWriterForTest().scheduled(), gc.metaWriterForTest().completed());
}
