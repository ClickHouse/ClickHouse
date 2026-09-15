#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

/// The manifest_deletes phase sends owner-removed manifest bodies to the store in chunks of
/// write-once keys, one request per chunk, and records every chunk that succeeded before a later
/// one can fail.

namespace ProfileEvents
{
    extern const Event CASBulkDeleteRequests;
}

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

const UInt128 kGc = hexToU128("00000000000000000000000000000001");
const RootNamespace kNs{"00/aa@cas@"};

ManifestRef ref(uint64_t seq)
{
    return ManifestRef{.writer_epoch = 1, .build_sequence = seq, .manifest_ordinal = 1};
}

/// `count` tables, each with one manifest, published committed and then dropped, so the fold sees
/// `count` owner removals and `mf_cleanup` carries `count` bodies.
std::vector<ManifestId> seedDroppedManifests(Backend & backend, const Layout & layout, uint64_t count)
{
    std::vector<ManifestId> ids;
    for (uint64_t i = 1; i <= count; ++i)
    {
        const ManifestRef r = ref(i);
        writeBlobBody(backend, layout, DB::UInt128(0x1000 + i));
        writeManifestRaw(backend, layout, kNs, r, {blobEntryFor("a", DB::UInt128(0x1000 + i))});
        const String table = "t" + std::to_string(i);
        publishCommittedTransition(backend, layout, kNs, table, std::nullopt, r);
        dropRefTransition(backend, layout, kNs, table, r);
        ids.push_back(ManifestId{kNs, r});
    }
    return ids;
}

/// Runs rounds until every listed manifest is gone or `max_rounds` passed; returns the sum of
/// `manifests_deleted` over the rounds that led.
uint64_t reclaim(Gc & gc, PoolPtr store, Backend & backend, const std::vector<ManifestId> & ids, size_t max_rounds)
{
    uint64_t total = 0;
    for (size_t round = 0; round < max_rounds; ++round)
    {
        const RoundReport rep = runRegularRoundReclaiming(gc);
        if (rep.acquired_lease)
            total += rep.manifests_deleted;
        store->renewWatermarkOnce();
        bool any_left = false;
        OperationForTest op(backend);
        for (const ManifestId & id : ids)
            any_left |= (*op).head(store->layout().manifestKey(id), Retry::once()).has_value();
        if (!any_left)
            break;
    }
    return total;
}

}

TEST(CASGCManifestBulkDelete, FiveBodiesInChunksOfTwoAreThreeRequests)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                                                .gc_bulk_delete_chunk_keys = 2, .gc_fold_max_defer_rounds = 0});
    const auto ids = seedDroppedManifests(*backend, store->layout(), 5);
    const auto requests_before = ProfileEvents::global_counters[ProfileEvents::CASBulkDeleteRequests].load();

    Gc gc(store, kGc);
    const uint64_t deleted = reclaim(gc, store, *backend, ids, 16);

    EXPECT_EQ(deleted, 5u);
    EXPECT_EQ(backend->bulkRemoveCalls(), 3u) << "2 + 2 + 1";
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASBulkDeleteRequests].load() - requests_before, 3u);
    OperationForTest op(*backend);
    for (const ManifestId & id : ids)
        EXPECT_FALSE((*op).head(store->layout().manifestKey(id), Retry::once()).has_value());
}

/// The object storage rejects the chunk's one bulk `removeManyWriteOnce` as NOT_IMPLEMENTED (a
/// GCS-backed pool): the phase's `flush()` falls back to one admitted request per key
/// (`removeChunkWriteOnceOrOneByOne`, CasGc.h), and every manifest in the chunk is still recorded
/// deleted -- the per-key event emission this phase does is unaffected by how the deletes were sent.
TEST(CASGCManifestBulkDelete, NotImplementedFallsBackToOneRequestPerKeyAndStillRecordsAllOfThem)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                                                .gc_fold_max_defer_rounds = 0});
    const auto ids = seedDroppedManifests(*backend, store->layout(), 5);

    /// One armed failure: the chunk's own bulk attempt (all 5 land in one chunk under the default
    /// chunk size) fails as "batch delete not supported"; the 5 single-key fallback calls that follow
    /// are not armed and succeed.
    backend->failNextBulkRemoveWith(std::make_exception_ptr(
        DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "no batch delete")));

    Gc gc(store, kGc);
    const uint64_t deleted = reclaim(gc, store, *backend, ids, 16);

    EXPECT_EQ(deleted, 5u) << "the per-key fallback must still record every manifest as deleted";
    EXPECT_EQ(backend->bulkRemoveCalls(), 6u) << "1 failed bulk attempt + 5 single-key fallback requests";
    OperationForTest op(*backend);
    for (const ManifestId & id : ids)
        EXPECT_FALSE((*op).head(store->layout().manifestKey(id), Retry::once()).has_value());
}

TEST(CASGCManifestBulkDelete, AThrowInTheSecondChunkKeepsTheFirstChunksAuditAndAbortsTheRound)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                                                .gc_bulk_delete_chunk_keys = 2, .gc_fold_max_defer_rounds = 0});
    const auto ids = seedDroppedManifests(*backend, store->layout(), 5);

    std::vector<std::map<String, UInt64>> manifest_phase_rows;
    Gc gc(store, kGc);
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "manifest_deletes")
            manifest_phase_rows.push_back(rec.metrics);
    });
    /// Rounds until the fold has adopted the removals; the first round whose manifest_deletes phase
    /// has work is the one the fault is armed for.
    size_t calls = 0;
    /// The hook throws on EVERY attempt of the second chunk, so the engine's policy is exhausted and
    /// the round aborts; the counter keeps climbing across the reissues, which is why the arm is
    /// "second call and later" rather than "exactly the second call".
    backend->onBeforeBulkRemove([&]
    {
        if (++calls >= 2)
            throw Poco::TimeoutException("injected into the second chunk, every attempt");
    });

    bool aborted = false;
    for (size_t round = 0; round < 16 && !aborted; ++round)
    {
        try
        {
            static_cast<void>(runRegularRoundReclaiming(gc));
        }
        catch (const Poco::Exception &)
        {
            aborted = true;
        }
        /// A round that exhausted the full retry window (up to `Retry::standard()`'s 90s) may have
        /// outlasted the mount lease itself, so the aborted round's own lease-renewal attempt can
        /// throw too -- irrelevant to what this test asserts, so skip it once aborted.
        if (!aborted)
            store->renewWatermarkOnce();
    }
    ASSERT_TRUE(aborted);
    ASSERT_FALSE(manifest_phase_rows.empty());
    /// The aborted round's row was never emitted (the phase threw), so the last emitted row belongs
    /// to an earlier, empty round; what proves the first chunk's audit survived is the store: exactly
    /// the first chunk's two bodies are gone.
    OperationForTest op(*backend);
    size_t gone = 0;
    for (const ManifestId & id : ids)
        gone += !(*op).head(store->layout().manifestKey(id), Retry::once()).has_value();
    EXPECT_EQ(gone, 2u);
}

TEST(CASGCManifestBulkDelete, ASuppressedRoundMakesNoRequest)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                                                .gc_fold_max_defer_rounds = 0});
    const auto ids = seedDroppedManifests(*backend, store->layout(), 3);
    Gc gc(store, kGc);
    for (size_t round = 0; round < 4; ++round)
    {
        static_cast<void>(gc.runRegularRound({}, /*allow_steal*/ true, UniversePolicy::StageA_Suppressed));
        store->renewWatermarkOnce();
    }
    EXPECT_EQ(backend->bulkRemoveCalls(), 0u);
    OperationForTest op(*backend);
    for (const ManifestId & id : ids)
        EXPECT_TRUE((*op).head(store->layout().manifestKey(id), Retry::once()).has_value());
}
