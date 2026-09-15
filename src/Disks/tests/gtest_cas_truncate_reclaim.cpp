#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>

#include <string>
#include <vector>

/// B140 regression guard. The soak's Phase-1 sync run did a `TRUNCATE TABLE` at op 450 and then
/// observed fsck `unreachable` STUCK above zero (1751) while the incremental GC reported
/// `candidates=0` — i.e. the GC believed it was done while orphaned blobs remained. This file
/// reproduces the soak shape at the CORE level (no server, no docker): publish many parts that
/// SHARE blobs (dedup), interleave regular GC rounds with the publishes (so trees get expanded
/// into the durable snap exactly as they would during a steady-state insert workload), then
/// perform the SAME removal a Replicated TRUNCATE issues — a per-ref `dropRef` for every part —
/// and drive the GC to a fixpoint. The invariant under test: after the drops are folded and the
/// cascade runs, `runFsck().unreachable` reaches 0 (every shared blob is reclaimed).
///
/// A `dropNamespace` variant is included as well (the path `removeRecursive` takes for a whole
/// table dir, e.g. DROP TABLE): it journals one Remove per former ref, so the cascade should fold
/// it identically.

using namespace DB::Cas;
using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;

namespace
{

PoolPtr openTestPool(std::shared_ptr<InMemoryBackend> & out_backend)
{
    out_backend = std::make_shared<InMemoryBackend>();
    return Pool::open(out_backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

/// Publish one part `ref` with TWO content files whose payloads are passed in. Identical payloads
/// across parts dedup to the SAME blob object (the soak's dedup_ratio ~3.8 comes from exactly this
/// sharing). Returns the manifest id.
ManifestId publishPart2(
    const PoolPtr & s, const String & ns, const String & ref,
    const String & payload_a, const String & payload_b)
{
    const RootNamespace nsr{ns};
    PartWriteInfo info;
    info.intended_ref = ns + "/" + ref;
    auto build = s->beginPartWrite(info);

    ManifestEntry ea;
    ea.path = "data.bin";
    ea.placement = EntryPlacement::Blob;
    ea.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(payload_a))};

    ea.blob_size = payload_a.size();

    ManifestEntry eb;
    eb.path = "data.cmrk3";
    eb.placement = EntryPlacement::Blob;
    eb.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(payload_b))};

    eb.blob_size = payload_b.size();

    /// Wiring order (EDGE-BEFORE-OBSERVE): stageManifest -> precommitAdd -> putBlob -> promote.
    const ManifestId id = build->stageManifest({ea, eb});
    build->precommitAdd(nsr, ref, id);
    build->putBlob(idOf(payload_a), BlobSource::fromString(payload_a));
    build->putBlob(idOf(payload_b), BlobSource::fromString(payload_b));
    build->promote(nsr, ref, build->buildId(), id);
    return id;
}

/// Whether the CURRENT retired list (any gc-shard) still holds an entry — the ack-floor deletion pipeline
/// (condemn -> graduate -> delete) is in flight while this is true.
bool anyRetiredPending(const PoolPtr & s)
{
    /// Condemned state rides the adopted fold seal's RunMarker::Condemned rows, not a
    /// separate retired list — reconstruct the in-flight set from the seal.
    return DB::Cas::tests::anyCondemnedInSeal(*s->poolBackendPtr(), s->layout());
}

/// Run regular GC rounds until a fixpoint over the ACK-FLOOR round. A condemned blob is deleted only a
/// few rounds after its removal folds (condemn -> graduate once the ack floor passes it -> delete), so the
/// loop advances the store's own mount ack after each round (`renewWatermarkOnce` runs the beat) and stays
/// alive while ANY work counter is nonzero OR the current retired list still holds an in-flight entry.
size_t runGcToFixpoint(const PoolPtr & s, Gc & gc, size_t max_rounds = 64)
{
    size_t rounds = 0;
    for (; rounds < max_rounds; ++rounds)
    {
        const RoundReport rep = DB::Cas::tests::runRegularRoundReclaiming(gc);
        if (!rep.acquired_lease)
            continue;
        s->renewWatermarkOnce();
        const bool no_work = rep.candidates == 0 && rep.deleted == 0 && rep.absent == 0
            && rep.replaced == 0 && rep.spared == 0;
        if (no_work && !anyRetiredPending(s))
            break;
    }
    return rounds;
}

}

/// The faithful soak repro: many parts sharing blobs, GC interleaved with the publishes, then a
/// per-ref drop of EVERY ref (Replicated TRUNCATE), then GC to a fixpoint. fsck.unreachable must
/// reach 0 — no orphaned blob may survive.
TEST(CASTruncateReclaim, PerRefDropOfSharedBlobsReclaimsToZero)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    const RootNamespace ns{"srv1/tbl"};

    constexpr int N = 32;

    /// Publish N parts. Payloads are chosen so blobs are SHARED across parts: data.bin cycles
    /// through 8 distinct contents, data.cmrk3 through 4 — heavy dedup, like the soak.
    std::vector<String> refs;
    for (int i = 0; i < N; ++i)
    {
        const String ref = "all_" + std::to_string(i) + "_" + std::to_string(i) + "_0";
        refs.push_back(ref);
        const String pa = "data-" + std::to_string(i % 8);
        const String pb = "mark-" + std::to_string(i % 4);
        publishPart2(s, ns.string(), ref, pa, pb);

        /// Interleave a GC round every few publishes, so the live trees get EXPANDED into the
        /// durable snap during the insert phase (steady-state GC, as in the soak).
        if (i % 5 == 4)
        {
            Gc gc(s, hexToU128("00000000000000000000000000000001"));
            DB::Cas::tests::runRegularRoundReclaiming(gc);
        }
    }

    /// Steady-state GC has nothing to reclaim while the refs are live.
    {
        Gc gc(s, hexToU128("00000000000000000000000000000001"));
        runGcToFixpoint(s, gc);
        const FsckReport before = runFsck(*s, /*detail=*/false);
        EXPECT_EQ(before.unreachable, 0u) << "live pool must have no unreachable debris";
        EXPECT_EQ(before.dangling, 0u);
        EXPECT_GT(before.reachable, 0u);
    }

    /// TRUNCATE: a Replicated TRUNCATE removes each part dir, which routes to dropRef per ref.
    for (const String & ref : refs)
        s->dropRef(ns, ref);

    /// Every publishing build finished; advance the durable watermark floor past their seqs so the
    /// Task 10 build-watermark guard no longer spares the now-dropped objects (production does this
    /// via the background renewer ~2s; here the renewer is off, so drive it explicitly).
    s->renewWatermarkOnce();

    /// Drive GC to a fixpoint and require full reclamation — this is the B140 assertion.
    {
        Gc gc(s, hexToU128("00000000000000000000000000000001"));
        const size_t rounds = runGcToFixpoint(s, gc);
        const FsckReport after = runFsck(*s, /*detail=*/false);
        EXPECT_EQ(after.dangling, 0u) << "TRUNCATE must never lose a reachable object";
        EXPECT_EQ(after.unreachable, 0u)
            << "B140: orphaned blobs survived TRUNCATE after " << rounds
            << " GC rounds (reachable=" << after.reachable
            << ", unreachable=" << after.unreachable << ")";
        EXPECT_EQ(after.reachable, 0u) << "no refs remain, so nothing should be reachable";
    }
}

/// Mirrors the soak exactly: TRUNCATE at "op 450" (drop every live ref), then CONTINUE inserting
/// (the soak's ops 451..599 had min_op=451) while the GC keeps running, then a final drive to a
/// fixpoint. The post-truncate inserts must not stall reclamation of the pre-truncate orphans.
/// Also asserts a TIGHT bound on the number of rounds reclamation needs (the soak's 180s budget at
/// gc_interval=30s only buys ~6 rounds, so the core must reach a fixpoint well inside that).
TEST(CASTruncateReclaim, TruncateThenKeepInsertingStillReclaims)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    const RootNamespace ns{"srv1/tbl"};

    /// Pre-truncate generation (the soak's ops < 451).
    std::vector<String> pre_refs;
    for (int i = 0; i < 24; ++i)
    {
        const String ref = "pre_" + std::to_string(i);
        pre_refs.push_back(ref);
        publishPart2(s, ns.string(), ref, "p-data-" + std::to_string(i % 6), "p-mark-" + std::to_string(i % 3));
        if (i % 5 == 4)
        {
            Gc gc(s, hexToU128("00000000000000000000000000000001"));
            DB::Cas::tests::runRegularRoundReclaiming(gc);
        }
    }

    /// TRUNCATE: drop every pre-truncate ref (per-ref dropRef).
    for (const String & ref : pre_refs)
        s->dropRef(ns, ref);

    /// Continue inserting AFTER the truncate (the soak's ops 451..599), interleaving GC rounds.
    for (int i = 0; i < 24; ++i)
    {
        publishPart2(s, ns.string(), "post_" + std::to_string(i),
                     "q-data-" + std::to_string(i % 6), "q-mark-" + std::to_string(i % 3));
        if (i % 5 == 4)
        {
            Gc gc(s, hexToU128("00000000000000000000000000000001"));
            DB::Cas::tests::runRegularRoundReclaiming(gc);
        }
    }

    /// All publishing builds finished; advance the durable watermark floor past their seqs so the
    /// Task 10 build-watermark guard no longer spares the dropped objects (the background renewer is
    /// off in this test, so drive it explicitly — production renews ~2s off the write path).
    s->renewWatermarkOnce();

    /// Drive to a fixpoint. unreachable must reach 0 (the pre-truncate orphans are gone) while the
    /// post-truncate refs stay reachable.
    Gc gc(s, hexToU128("00000000000000000000000000000001"));
    const size_t rounds = runGcToFixpoint(s, gc);
    const FsckReport after = runFsck(*s, /*detail=*/false);
    EXPECT_EQ(after.dangling, 0u);
    EXPECT_EQ(after.unreachable, 0u)
        << "B140: pre-truncate orphans survived after " << rounds << " GC rounds";
    EXPECT_GT(after.reachable, 0u) << "post-truncate refs must stay reachable";
    /// Round bound: the ack-floor pipeline adds a bounded, constant number of rounds over the old
    /// fold+delete (condemn -> graduate once the ack floor passes -> delete, with the ack kept current
    /// each round). The dead subgraph still drains in a small, constant number of rounds — not O(orphans).
    EXPECT_LE(rounds, 8u) << "reclamation took too many rounds (ack-floor pipeline is a small constant)";
}

/// The DROP TABLE path: removeRecursive of a table dir calls dropNamespace, which journals one
/// Remove per former ref. Same reclamation invariant.
TEST(CASTruncateReclaim, DropNamespaceLeavesSharedBlobDebrisForPerpetualSweep)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    const RootNamespace ns{"srv1/tbl"};

    constexpr int N = 32;
    for (int i = 0; i < N; ++i)
    {
        const String ref = "all_" + std::to_string(i) + "_" + std::to_string(i) + "_0";
        const String pa = "data-" + std::to_string(i % 8);
        const String pb = "mark-" + std::to_string(i % 4);
        publishPart2(s, ns.string(), ref, pa, pb);
        if (i % 5 == 4)
        {
            Gc gc(s, hexToU128("00000000000000000000000000000001"));
            DB::Cas::tests::runRegularRoundReclaiming(gc);
        }
    }

    {
        Gc gc(s, hexToU128("00000000000000000000000000000001"));
        runGcToFixpoint(s, gc);
    }

    /// DROP TABLE: the whole namespace is tombstoned at once (one Remove per ref in the journal).
    s->dropNamespace(ns);

    /// Every publishing build finished; advance the durable watermark floor past their seqs so the
    /// Task 10 build-watermark guard no longer spares the dropped objects (renewer off here).
    s->renewWatermarkOnce();

    {
        Gc gc(s, hexToU128("00000000000000000000000000000001"));
        const size_t rounds = runGcToFixpoint(s, gc);
        const FsckReport after = runFsck(*s, /*detail=*/false);
        EXPECT_EQ(after.dangling, 0u);
        /// Removal still performs no lifecycle-specific physical cleanup -- the perpetual sweep and the
        /// janitor own the orphaned bytes. What changed is that they can now FINISH: dropping the last
        /// namespace leaves an authoritative catalog that decodes to zero entries, which is a positive
        /// proof of no live edge rather than the vacuous 0 == 0, so the round's frontier completes and
        /// the sweep is no longer suppressed on an emptied pool.
        EXPECT_EQ(after.unreachable, 0u)
            << "an emptied pool must drain instead of standing still; the sweep owned these blobs and "
               "reclaimed them within " << rounds << " GC rounds";
        EXPECT_EQ(after.reachable, 0u);
        DB::Cas::CasRequests catalog_requests = DB::Cas::tests::openRequestsForTest(s->poolBackendPtr());
        DB::Cas::CasOperation catalog_op = catalog_requests.admit();
        EXPECT_FALSE(CasRefCatalog::lifeIfCataloged(catalog_op, s->layout(), ns))
            << "physical debris must not keep the logical namespace life cataloged";
    }
}
