#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include "cas_test_helpers.h"

#include <Common/ProfileEvents.h>

#include <mutex>
#include <set>

/// Task 12 required GC tests over the snapshot+log ref model (spec 2026-07-11-cas-ref-table-snapshot-log-design).
/// Every fixture produces REAL wire-format ref logs (via the writer or `writeRefLogTxnRaw`, never hand-rolled
/// bytes), and every test proves the fold actually consumed them (cursor advanced / nonzero in-degree), so a
/// silent no-op fold cannot pass vacuously.

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int NOT_IMPLEMENTED;
}

namespace ProfileEvents
{
extern const Event CASRefGlobalListPages;
extern const Event CASRefLogBodyGets;
extern const Event CASRefManifestBodyFoldGets;
extern const Event CASRefEmittedEdges;
extern const Event CASRefCleanupObjectsDeleted;
}

namespace
{
const UInt128 kGc = hexToU128("00000000000000000000000000000001");
const UInt128 kGc2 = hexToU128("00000000000000000000000000000002");

ManifestRef mref(uint64_t seq, uint32_t ord = 1)
{
    return ManifestRef{.writer_epoch = 1, .build_sequence = seq, .manifest_ordinal = ord};
}

/// Append a committed-ref log at an EXPLICIT sequence (no per-call LIST) -- fast bulk seeding of a
/// >1000-key stream. The ops are replay-valid (birth on the first, then add-precommit + promote).
void seedCommittedAt(
    Backend & backend, const Layout & layout, const RootNamespace & ns, uint64_t seq,
    const String & ref_name, const ManifestRef & mr, bool birth)
{
    std::vector<RefOp> ops;
    if (birth)
        ops.push_back(namespaceBirthOp());
    const std::vector<RefOp> commit_ops = publishCommittedOps(ref_name, mr);
    ops.insert(ops.end(), commit_ops.begin(), commit_ops.end());
    RefLogTxn txn;
    txn.ns = ns.string();
    txn.txn_id = RefTxnId{1, seq};
    txn.ops = std::move(ops);
    fixture::writeRefLogRaw(backend, layout, txn);
}

/// Drive regular rounds, renewing the mount ack after each, until quiescent or `max_rounds`.
size_t runToFixpoint(const PoolPtr & s, Gc & gc, size_t max_rounds = 64)
{
    size_t rounds = 0;
    for (; rounds < max_rounds; ++rounds)
    {
        const RoundReport rep = runRegularRoundReclaiming(gc);
        if (!rep.acquired_lease)
            continue;
        s->renewWatermarkOnce();
        const bool no_work = rep.candidates == 0 && rep.deleted == 0 && rep.absent == 0
            && rep.replaced == 0 && rep.spared == 0;
        if (no_work && !anyCondemnedInSeal(*s->poolBackendPtr(), s->layout()))
            break;
    }
    return rounds;
}

bool blobPresent(Backend & b, const Layout & layout, const UInt128 & hash)
{
    OperationForTest op(b);
    return (*op).head(layout.blobKey(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hash)}), Retry::once()).has_value();
}

/// Denies ONCE the single round-commit `gc/state` CAS that advances `snap_generation` (the losing
/// leader deposed mid-round). The denied round leaves only never-adopted attempt-scoped debris.
class DeposeRoundCommitBackend : public InMemoryBackend
{
public:
    /// The fault sits on the WRITE PRIMITIVE, not the legacy `casPut` verb: `Gc::runRegularRound`
    /// speaks the primitive directly, and `casPut`'s forwarding is one-way -- overriding it here would
    /// intercept nothing.
    std::expected<String, DB::Cas::Backend::RawConflict> write(const String & key, const String & bytes,
                                                               const std::optional<String> & expected_value,
                                                               DB::Cas::TransportAccess & access) override
    {
        if (arm && key == "p/gc/state")
        {
            const auto stored = read(key, access);
            const uint64_t stored_gen = stored ? decodeGcState(stored->bytes).snap_generation : 0;
            if (decodeGcState(bytes).snap_generation > stored_gen)
            {
                arm = false;
                throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
                    "test-injected: round-commit gc/state CAS denied (losing leader deposed mid-round)");
            }
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }
    bool arm = false;
};

/// Moves one of the two authorities `cleanupRefObjects` must revalidate at a precise ref-log delete
/// boundary. The target object's own token is untouched, so only an authority check can refuse it.
class RefCleanupAuthorityRaceBackend : public CountingBackend
{
public:
    enum class Authority : uint8_t
    {
        Catalog,
        GcFence,
        CatalogRebirth,
    };

    enum class Timing : uint8_t
    {
        AfterFirstDelete,
        DuringChunk,
    };

    void arm(
        Authority authority_, Timing timing_, const Layout & layout,
        const String & first_cleanup_key_)
    {
        arm(authority_, timing_, layout, first_cleanup_key_, std::nullopt);
    }

    void arm(
        Authority authority_, Timing timing_, const Layout & layout,
        const String & first_cleanup_key_, const RootNamespace & reborn_ns_)
    {
        arm(authority_, timing_, layout, first_cleanup_key_, std::optional<RootNamespace>{reborn_ns_});
    }

    /// "Before the first chunk" has no backend-request seam of its own to hang off: `cleanupRefObjects`
    /// issues no `HEAD`, and the chunk's own catalog/`gc/state` reads only happen ONCE, back to back,
    /// immediately before the delete they license -- by the time either is observable from a backend
    /// override, the chunk's catalog snapshot is already cached in `authorityHolds`'s local, and moving
    /// the authority no longer changes what THIS chunk decides. The window that must be hit instead is
    /// the round's own hot-scan catalog cut: `Gc::setPostHotScanCatalogReadHookForTest` (`CasGc.h`)
    /// fires the instant that cut is taken, before the round -- and later `authorityHolds` -- does
    /// anything else with it, so a move landed there is exactly "before the first chunk starts" and the
    /// chunk's later fresh reads observe it.
    ///
    /// `Authority::Catalog` moves the catalog's token directly, right there in the hook: the round's
    /// own `round_commit` CAS (phase 13) never touches the catalog, so nothing downstream collides.
    /// `Authority::GcFence` cannot do the same for `gc/state`: bumping its lease THERE lands strictly
    /// BEFORE `round_commit`'s own `gc/state` replace (which still holds the etag from lease adoption,
    /// phase 1), so that replace loses its own CAS and the round throws before `cleanupRefObjects`
    /// (phase 17) ever runs -- the "nothing deleted" assertions would pass vacuously, not because
    /// cleanup refused. Instead, the hook only ARMS `armGcFenceMoveOnAuthorityHoldsRevalidationForTest`: the
    /// actual lease bump is deferred to a LATER read of `gc/state`. Not the next one -- namespace
    /// janitor / orphan-sweep bookkeeping between `round_commit` and `cleanupRefObjects` also touches
    /// the catalog and `gc/state`, just never the two BACK TO BACK the way `authorityHolds` does
    /// (catalog, then `gc/state`, nothing in between): that adjacency is the one place in a round only
    /// `authorityHolds`'s own revalidation produces, so gating on it -- rather than on the catalog key
    /// alone -- is what actually lands the move inside that SAME call, well after `round_commit`.
    static void moveRefCleanupAuthorityBeforeFirstChunk(Authority authority_, CasOperation & op, const Layout & layout)
    {
        if (authority_ == Authority::GcFence)
            throw std::logic_error(
                "moveRefCleanupAuthorityBeforeFirstChunk is for Authority::Catalog/CatalogRebirth only -- "
                "use armGcFenceMoveOnAuthorityHoldsRevalidationForTest for Authority::GcFence");
        /// Same-content rewrite: only the catalog's TOKEN moves (mints a fresh etag), never its
        /// parsed content -- the pure "someone else touched this row" race `Authority::Catalog`
        /// models, as opposed to `Authority::CatalogRebirth`'s actual incarnation bump.
        const CasRefCatalog::Snapshot snap = CasRefCatalog::read(op, layout);
        op.replace(layout.refCatalogKey(), encodeRefCatalog(snap.catalog), *snap.etag, Retry::standard());
    }

    /// Arms the seam `read` below fires on: see the doc comment above. Called from the test body
    /// before the round (and so before any read-ahead worker exists), but still under the mutex, so
    /// this method and `read`'s critical sections never race even under future reordering.
    void armGcFenceMoveOnAuthorityHoldsRevalidationForTest(const Layout & layout)
    {
        catalog_key = layout.refCatalogKey();
        gc_state_key = layout.gcStateKey();
        std::lock_guard lock(seam_mutex);
        catalog_seam_armed = true;
    }

    /// `read` also runs on the GC read-ahead pool's threads (`CasGcReadAhead.cpp` schedules
    /// `CasOperation::read` there; `gc_read_concurrency` defaults to 16), concurrently with the round
    /// thread's own reads -- `catalog_seam_armed` and `last_control_key_read` below are shared mutable
    /// state a pool thread's read can land between `authorityHolds`'s two reads, so both are read AND
    /// written only under `seam_mutex`. `last_control_key_read` tracks only the catalog and `gc/state`
    /// keys, never any other key a read-ahead worker fetches: those workers never touch either control
    /// key (they fetch ref-log/manifest bodies), so an unrelated concurrent read can never perturb the
    /// adjacency signal even though it runs lock-free between this method's two critical sections.
    std::optional<DB::Cas::Backend::Raw> read(const String & key, DB::Cas::TransportAccess & access) override
    {
        bool fires_here = false;
        {
            std::lock_guard lock(seam_mutex);
            fires_here = catalog_seam_armed && key == gc_state_key && last_control_key_read == catalog_key;
            if (fires_here)
                catalog_seam_armed = false;
            if (key == catalog_key || key == gc_state_key)
                last_control_key_read = key;
        }
        if (fires_here)
        {
            const auto current = CountingBackend::read(key, access);
            if (!current)
                throw std::runtime_error("test-injected cleanup authority object is absent");
            GcState moved = decodeGcState(current->bytes);
            ++moved.lease.seq;
            if (!write(key, encodeGcState(moved), current->value, access).has_value())
                throw std::runtime_error("test-injected cleanup authority move lost its CAS");
            return CountingBackend::read(key, access);   /// the FRESH, post-move bytes, for THIS read
        }
        return CountingBackend::read(key, access);
    }

    /// The `AfterFirstDelete` seam moves the authority once the first chunk's batch delete has
    /// landed (so that chunk keeps whatever it already observed and only the NEXT chunk's
    /// revalidation refuses); `DuringChunk` moves it after the chunk's revalidation but before its
    /// batch delete lands, so the chunk in flight still completes under the authority it observed.
    void removeManyWriteOnce(const std::vector<DB::Cas::WriteOnceKey> & keys, DB::Cas::TransportAccess & access) override
    {
        const bool names_first = std::any_of(keys.begin(), keys.end(),
            [&](const DB::Cas::WriteOnceKey & key) { return key.str() == first_cleanup_key; });
        if (armed && timing == Timing::DuringChunk && names_first)
            moveAuthority(access);   /// after the revalidation, before the deletes land
        CountingBackend::removeManyWriteOnce(keys, access);
        if (armed && timing == Timing::AfterFirstDelete && names_first)
            moveAuthority(access);
    }

private:
    void arm(
        Authority authority_, Timing timing_, const Layout & layout,
        const String & first_cleanup_key_, std::optional<RootNamespace> reborn_ns_)
    {
        authority = authority_;
        timing = timing_;
        catalog_key = layout.refCatalogKey();
        gc_state_key = layout.gcStateKey();
        first_cleanup_key = first_cleanup_key_;
        reborn_ns = std::move(reborn_ns_);
        layout_for_rebirth_seed = &layout;
        armed = true;
    }

    /// `access` is the token the caller's own primitive override already holds for its in-flight
    /// request; reused here for this method's extra read+write rather than minting a new CasRequests,
    /// exactly as `Backend::probeSentinelRaw`'s default implementation reuses one `access` across its
    /// own head-then-more sequence.
    void moveAuthority(TransportAccess & access)
    {
        armed = false;
        const String & key = authority == Authority::GcFence ? gc_state_key : catalog_key;
        const auto got = read(key, access);
        if (!got)
            throw std::runtime_error("test-injected cleanup authority object is absent");

        String bytes = got->bytes;
        if (authority == Authority::GcFence)
        {
            GcState moved = decodeGcState(bytes);
            ++moved.lease.seq;
            bytes = encodeGcState(moved);
        }
        UInt128 reborn_incarnation = 0;
        if (authority == Authority::CatalogRebirth)
        {
            RefCatalog catalog = decodeRefCatalog(bytes);
            for (CatalogEntry & entry : catalog.entries)
                if (reborn_ns && entry.ns == *reborn_ns)
                {
                    entry.incarnation = entry.incarnation + 1;
                    reborn_incarnation = entry.incarnation;
                }
            bytes = encodeRefCatalog(catalog);
        }
        if (!write(key, bytes, got->value, access).has_value())
            throw std::runtime_error("test-injected cleanup authority move lost its CAS");

        /// Give the reborn life SOMETHING of its own, landed right after its catalog row exists (any
        /// earlier and an "unknown incarnation" sweep elsewhere in the SAME round can claim it, since
        /// no catalog entry yet names that incarnation) -- so the "reborn life untouched" assertions
        /// below test something real instead of an empty listing.
        if (authority == Authority::CatalogRebirth && reborn_ns && reborn_incarnation != 0 && layout_for_rebirth_seed)
        {
            const Layout & layout = *layout_for_rebirth_seed;
            const NamespaceLifeId reborn_life = NamespaceLifeId::fromCatalogEntry(*reborn_ns, reborn_incarnation);
            const RefTxnId reborn_log_id{1, 1};
            const RefLogTxn reborn_birth{
                .ns = reborn_ns->string(), .txn_id = reborn_log_id, .ops = {namespaceBirthOp()},
                .prev_epoch_seal = std::nullopt};
            if (!write(layout.refLogKey(reborn_life, reborn_log_id),
                    sealObject(FormatId::RefLog, encodeRefLogTxn(reborn_birth)), std::nullopt, access).has_value())
                throw std::runtime_error("test-injected reborn-life log seed lost its CAS");
            const RefTableSnapshot reborn_snap = minimalLiveSnapshot(reborn_ns->string(), reborn_log_id);
            if (!write(layout.refSnapshotKey(reborn_life, reborn_log_id),
                    sealObject(FormatId::RefSnapshot, encodeRefTableSnapshot(reborn_snap)), std::nullopt, access).has_value())
                throw std::runtime_error("test-injected reborn-life snapshot seed lost its CAS");
            /// A checkpoint too, naming the seeded log/snapshot: without one, the NEXT round's recovery
            /// grounding for this namespace finds "no usable checkpoint", which SUPPRESSES that round's
            /// destructive work ENTIRELY (every namespace, not just this one) -- a test relying on the
            /// old cohort surviving round 2 would then be observing a no-op round, not the plan moving
            /// to the reborn life. `writeRecoverableCkptForRawFixture` resolves its own fresh catalog
            /// read, which already sees the incarnation bump the write just above landed.
            writeRecoverableCkptForRawFixture(*this, layout, *reborn_ns, RefCkpt{
                .life_epoch = 1,
                .committed_through = reborn_log_id,
                .checkpoint_snapshot_id = reborn_log_id,
                .last_epoch_seal = std::nullopt,
            });
        }
    }

    Authority authority = Authority::Catalog;
    Timing timing = Timing::AfterFirstDelete;
    String catalog_key;
    String gc_state_key;
    String first_cleanup_key;
    std::optional<RootNamespace> reborn_ns;
    const Layout * layout_for_rebirth_seed = nullptr;
    bool armed = false;
    /// Guards both members below: `read` runs concurrently on the GC read-ahead pool's threads, see
    /// the doc comment on `read` itself.
    std::mutex seam_mutex;
    /// Independent of `armed`/`timing`/`authority` above: `armGcFenceMoveOnAuthorityHoldsRevalidationForTest`
    /// arms this, and the `read` override consumes it once.
    bool catalog_seam_armed = false;
    /// The most recent CONTROL key (catalog or `gc/state`) read -- every other key a read-ahead
    /// worker reads is ignored, so `read` can recognize the catalog-then-`gc/state` ADJACENCY
    /// `authorityHolds` alone produces -- see the doc comment above `moveRefCleanupAuthorityBeforeFirstChunk`.
    String last_control_key_read;
};

struct RefCleanupFixture
{
    String first_log_key;
    String second_log_key;
};

RefCleanupFixture seedTwoCoveredLogs(
    RefCleanupAuthorityRaceBackend & backend, const Layout & layout,
    const RootNamespace & ns)
{
    fixture::admitLive(backend, layout, ns);
    const ManifestRef r1 = mref(1);
    const ManifestRef r2 = mref(2);
    const ManifestRef r3 = mref(3);
    writeManifestRaw(backend, layout, ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    writeManifestRaw(backend, layout, ns, r2, {blobEntryFor("b", DB::UInt128(2))});
    writeManifestRaw(backend, layout, ns, r3, {blobEntryFor("c", DB::UInt128(3))});
    const uint64_t v1 = publishCommittedTransition(backend, layout, ns, "t1", std::nullopt, r1);
    const uint64_t v2 = publishCommittedTransition(backend, layout, ns, "t2", std::nullopt, r2);
    const uint64_t v3 = publishCommittedTransition(backend, layout, ns, "t3", std::nullopt, r3);
    writeRefSnapshotRaw(backend, layout,
        minimalLiveSnapshot(ns.string(), RefTxnId{1, v3},
            {committedRow("t1", r1), committedRow("t2", r2), committedRow("t3", r3)}));
    replaceRecoverableCkptForRawFixture(backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, v3},
        .checkpoint_snapshot_id = RefTxnId{1, v3},
        .last_epoch_seal = std::nullopt,
    });
    const NamespaceLifeId life = fixture::fixtureLife(ns);
    return {
        .first_log_key = layout.refLogKey(life, RefTxnId{1, v1}),
        .second_log_key = layout.refLogKey(life, RefTxnId{1, v2})};
}
}

/// (1) A >1000-key ref scan folds every pre-existing log exactly once: the cursor advances to the greatest
/// id and every referenced blob has in-degree exactly 1 (folded once, not skipped, not doubled).
TEST(CASRefGc, LargeRefScanFoldsEveryLogExactlyOnce)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    constexpr uint64_t N = 1200;   /// > 1000: forces multi-page LIST paging in the fold's global scan
    for (uint64_t i = 1; i <= N; ++i)
    {
        const ManifestRef mr = mref(i);
        writeManifestRaw(*backend, layout, ns, mr, {blobEntryFor("data", DB::UInt128(i))});
        seedCommittedAt(*backend, layout, ns, /*seq*/ i, "t" + std::to_string(i), mr, /*birth*/ i == 1);
    }
    writeRecoverableCkptForRawFixture(
        *backend, layout, ns,
        RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{1, N},
                .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt});

    Gc gc(store, kGc);
    ASSERT_NO_THROW(gc.runRegularRound());

    /// The durable cursor advanced to the greatest log id.
    EXPECT_EQ(foldCursorOf(*backend, layout, ns, 0), N)
        << "the fold must advance the per-table cursor to the greatest pre-existing log id";

    /// Every referenced blob folded EXACTLY once (in-degree 1). Spot-check a spread across the >1000 set.
    for (uint64_t i : {uint64_t{1}, uint64_t{2}, uint64_t{999}, uint64_t{1000}, uint64_t{1001}, N})
        EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(i)), 1)
            << "blob " << i << " must be folded exactly once (not skipped, not doubled)";
}

/// (2) A concurrent log appended AFTER the round's scan has passed its table is NOT skipped: the sealed
/// cursor stays below it, and the next round folds it.
TEST(CASRefGc, ConcurrentLogAfterScanIsFoldedNextRound)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    const ManifestRef r1 = mref(1);
    writeManifestRaw(*backend, layout, ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    const uint64_t v1 = publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, r1);

    Gc gc(store, kGc);
    gc.runRegularRound();   /// round 1 folds v1
    ASSERT_EQ(foldCursorOf(*backend, layout, ns, 0), v1);
    ASSERT_EQ(inDegreeOf(*backend, layout, DB::UInt128(1)), 1);

    /// A NEW log lands after the round sealed its cursor at v1 (a concurrent writer).
    const ManifestRef r2 = mref(2);
    writeManifestRaw(*backend, layout, ns, r2, {blobEntryFor("b", DB::UInt128(2))});
    const uint64_t v2 = publishCommittedTransition(*backend, layout, ns, "tbl2", std::nullopt, r2);
    ASSERT_GT(v2, v1);

    /// The sealed cursor is still v1 (< v2) -- the new log was never skipped past.
    EXPECT_EQ(foldCursorOf(*backend, layout, ns, 0), v1)
        << "a log that landed after the scan must remain below the durable cursor, never skipped";

    gc.runRegularRound();   /// round 2 folds v2
    EXPECT_EQ(foldCursorOf(*backend, layout, ns, 0), v2);
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(2)), 1)
        << "the next round must fold the concurrently-appended log";
}

/// (3) Fold barrier: a live precommit whose manifest body is absent clamps the table cursor below its
/// log (an anomaly is recorded), then folds once the body appears.
TEST(CASRefGc, FoldBarrierClampsBelowMissingBodyThenFoldsOnAppear)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    const ManifestRef pre = mref(7);
    /// No writeManifestRaw for `pre`: its body is intentionally absent (the live precommit's barrier).
    const uint64_t v = addPrecommitTransition(*backend, layout, ns, DB::UInt128(9), "part", std::nullopt, pre);

    Gc gc(store, kGc);
    RoundReport report;
    ASSERT_NO_THROW(report = gc.runRegularRound());
    EXPECT_TRUE(report.hasAnomaly(ns, /*shard*/0)) << "a missing live-precommit body must record an anomaly";
    EXPECT_LT(foldCursorOf(*backend, layout, ns, 0), v)
        << "the barrier must clamp the durable cursor BELOW the bodiless-precommit log";
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(1)), 0);

    /// The body appears (the build finished staging): the next fold passes the barrier.
    writeManifestRaw(*backend, layout, ns, pre, {blobEntryFor("p", DB::UInt128(1))});
    gc.runRegularRound();
    EXPECT_GE(foldCursorOf(*backend, layout, ns, 0), v) << "the barrier lifts once the body lands";
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(1)), 1);
}

/// (4) Edge cancellation: a manifest added then removed across a batch nets to zero in-degree and the
/// exclusively-owned blob is reclaimed.
TEST(CASRefGc, EdgeCancellationAddThenRemoveReclaimsBlob)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    const ManifestRef r = mref(1);
    writeBlobBody(*backend, layout, DB::UInt128(1));
    writeManifestRaw(*backend, layout, ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, r);   /// +1 for r's blob
    dropRefTransition(*backend, layout, ns, "tbl", r);                          /// -1: the add is cancelled

    Gc gc(store, kGc);
    ASSERT_TRUE(runToFixpoint(store, gc) < 64u) << "the add+remove batch must converge to a fixpoint";

    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(1)), 0)
        << "an added-then-removed manifest nets to zero in-degree";
    EXPECT_FALSE(blobPresent(*backend, layout, DB::UInt128(1)))
        << "the net-zero blob is reclaimed";
}

/// (5) A losing generation commit adopts nothing and deletes nothing: a round whose single round-commit
/// `gc/state` CAS is denied (deposed mid-round) must NOT advance the adopted (snap_generation, snap_attempt)
/// and must NOT delete the condemned-but-unadopted blob. Its fold seal is durable only under its OWN
/// never-adopted attempt (harmless debris).
TEST(CASRefGc, LosingGenerationCommitAdoptsNothingDeletesNothing)
{
    auto backend = std::make_shared<DeposeRoundCommitBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    const ManifestRef r = mref(1);
    writeBlobBody(*backend, layout, DB::UInt128(1));
    writeManifestRaw(*backend, layout, ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    gc.runRegularRound();   /// round 1: folds the +1 and adopts it cleanly
    store->renewWatermarkOnce();
    OperationForTest raw_op(*backend);
    const auto adopted = decodeGcState((*raw_op).read(layout.gcStateKey(), Retry::once())->bytes);
    ASSERT_GT(adopted.snap_generation, 0u);

    /// Drop the ref, then run the round whose commit is DENIED (losing leader).
    dropRefTransition(*backend, layout, ns, "tbl", r);
    backend->arm = true;
    EXPECT_ANY_THROW(gc.runRegularRound());
    backend->arm = false;

    /// The deposed round adopted NOTHING: the durable pointers are unchanged...
    const auto after = decodeGcState((*raw_op).read(layout.gcStateKey(), Retry::once())->bytes);
    EXPECT_EQ(after.snap_generation, adopted.snap_generation)
        << "a denied round-commit CAS must not advance the adopted generation";
    EXPECT_EQ(after.snap_attempt, adopted.snap_attempt);
    /// ...and it deleted NOTHING: the blob its unadopted fold condemned is still present.
    EXPECT_TRUE(blobPresent(*backend, layout, DB::UInt128(1)))
        << "a losing generation commit must never delete a blob against an unadopted fold";
}

/// (6) Ref-object cleanup trusts only a checkpoint-named recovery triple: an older `_log` and `_snap`
/// are deleted after the durable cursor reaches them, while that triple remains intact.
TEST(CASRefGc, RefObjectCleanupRetainsCheckpointNamedTriple)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch

    /// Two committed publishes -> logs {1,1} and {1,2}.
    const ManifestRef r1 = mref(1);
    const ManifestRef r2 = mref(2);
    writeManifestRaw(*backend, layout, ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    writeManifestRaw(*backend, layout, ns, r2, {blobEntryFor("b", DB::UInt128(2))});
    const uint64_t v1 = publishCommittedTransition(*backend, layout, ns, "t1", std::nullopt, r1);
    const uint64_t v2 = publishCommittedTransition(*backend, layout, ns, "t2", std::nullopt, r2);

    /// Two observed snapshots: an OLD one covering only v1, and the NEWEST covering v2. Both are real
    /// wire-format snapshot objects (the recovery codec reads them).
    RefTableSnapshot old_snap = minimalLiveSnapshot(ns.string(), RefTxnId{1, v1},
        {committedRow("t1", r1)});
    RefTableSnapshot new_snap = minimalLiveSnapshot(ns.string(), RefTxnId{1, v2},
        {committedRow("t1", r1), committedRow("t2", r2)});
    writeRefSnapshotRaw(*backend, layout, old_snap);
    writeRefSnapshotRaw(*backend, layout, new_snap);
    replaceRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, v2},
        .checkpoint_snapshot_id = RefTxnId{1, v2},
        .last_epoch_seal = std::nullopt,
    });

    const String log_v1_key = layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, v1});
    const String log_v2_key = layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, v2});
    const String old_snap_key = layout.refSnapshotKey(fixture::fixtureLife(ns), RefTxnId{1, v1});
    const String new_snap_key = layout.refSnapshotKey(fixture::fixtureLife(ns), RefTxnId{1, v2});
    OperationForTest raw_op(*backend);
    ASSERT_TRUE((*raw_op).head(log_v1_key, Retry::once()).has_value());
    ASSERT_TRUE((*raw_op).head(log_v2_key, Retry::once()).has_value());
    ASSERT_TRUE((*raw_op).head(old_snap_key, Retry::once()).has_value());

    Gc gc(store, kGc);
    runToFixpoint(store, gc);   /// folds v1,v2 (cursor -> v2) then cleans covered ref objects post-CAS

    /// The old log lies below both the durable cursor and the validated checkpoint base => DELETED.
    EXPECT_FALSE((*raw_op).head(log_v1_key, Retry::once()).has_value())
        << "a log below the checkpoint-named snapshot base and durable cursor must be deleted";
    /// The same-id ordinary log is part of recovery's triple and must survive.
    EXPECT_TRUE((*raw_op).head(log_v2_key, Retry::once()).has_value())
        << "the checkpoint-named non-seal log must survive with its snapshot";
    /// The older snapshot is deleted; the checkpoint-named snapshot is retained.
    EXPECT_FALSE((*raw_op).head(old_snap_key, Retry::once()).has_value()) << "an older snapshot must be deleted";
    EXPECT_TRUE((*raw_op).head(new_snap_key, Retry::once()).has_value()) << "the checkpoint-named snapshot must be retained";
}

/// `cleanupRefObjects`'s per-round cap. Five deletable logs share one
/// namespace with a tiny `gc_round_ref_cleanup_budget`; the per-key fail-close validation
/// (`deleteRefObject`'s catalog/lease revalidation before every exact delete) is untouched -- it is
/// NOT amortized, only the cohort size per round is capped. `planRefCleanup` recomputes the same
/// remaining candidates from durable state every round, so the excess needs no cursor of its own.
TEST(CASRefGc, RefObjectCleanupRespectsRoundBudgetAndConvergesAcrossRounds)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "test",
        .gc_round_ref_cleanup_budget = 1,
        .gc_fold_max_defer_rounds = 0});
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    fixture::admitLive(*backend, layout, ns);

    /// Six sequential replacements of the SAME ref -> six committed logs {1,1}..{1,6}.
    constexpr int kLogs = 6;
    std::optional<ManifestRef> prev;
    ManifestRef latest{};
    uint64_t last_seq = 0;
    for (int i = 1; i <= kLogs; ++i)
    {
        const ManifestRef r = mref(i);
        writeManifestRaw(*backend, layout, ns, r, {blobEntryFor("a" + std::to_string(i), DB::UInt128(static_cast<uint64_t>(i)))});
        last_seq = publishCommittedTransition(*backend, layout, ns, "t", prev, r);
        prev = r;
        latest = r;
    }

    /// A snapshot + checkpoint naming the LATEST row: every earlier log is below the checkpoint base.
    RefTableSnapshot snap = minimalLiveSnapshot(ns.string(), RefTxnId{1, last_seq}, {committedRow("t", latest)});
    writeRefSnapshotRaw(*backend, layout, snap);
    replaceRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, last_seq},
        .checkpoint_snapshot_id = RefTxnId{1, last_seq},
        .last_epoch_seal = std::nullopt,
    });

    std::vector<String> deletable_log_keys;
    for (int i = 1; i < kLogs; ++i)   /// {1,1}..{1,5}: strictly below the checkpoint base, hence deletable
        deletable_log_keys.push_back(layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, static_cast<uint64_t>(i)}));

    Gc gc(store, kGc);
    OperationForTest raw_op(*backend);
    auto countSurviving = [&]
    {
        size_t n = 0;
        for (const String & k : deletable_log_keys)
            if ((*raw_op).head(k, Retry::once()).has_value())
                ++n;
        return n;
    };
    ASSERT_EQ(countSurviving(), deletable_log_keys.size())
        << "nothing cleaned before the first round even runs";

    /// The SAME round that folds the whole tail also runs post-CAS cleanup, and with
    /// `gc_round_ref_cleanup_budget = 1` deletes exactly one of the five deletable candidates.
    runRegularRoundReclaiming(gc);
    EXPECT_EQ(countSurviving(), deletable_log_keys.size() - 1)
        << "a round with gc_round_ref_cleanup_budget=1 must delete exactly one ref object";

    /// Repeated budgeted rounds converge: the whole deletable tail eventually drains, none stranded.
    for (int i = 0; i < 10 && countSurviving() > 0; ++i)
        runRegularRoundReclaiming(gc);
    EXPECT_EQ(countSurviving(), 0u)
        << "the whole deletable tail must eventually drain under repeated budgeted rounds";
}

TEST(CASRefGc, RefObjectCleanupRetainsCheckpointPredecessorSealProof)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/cross-epoch-cleanup@cas@"};
    fixture::admitLive(*backend, layout, ns);
    const NamespaceLifeId life = fixture::fixtureLife(ns);
    const RefTxnId birth_id{1, 1};
    const RefTxnId seal_id{1, 2};
    const RefTxnId base_id{2, 1};

    const RefLogTxn birth{
        .ns = ns.string(),
        .txn_id = birth_id,
        .ops = {namespaceBirthOp()},
        .prev_epoch_seal = std::nullopt};
    RefOp seal_op;
    seal_op.kind = RefOpKind::EpochSeal;
    const RefLogTxn seal{
        .ns = ns.string(),
        .txn_id = seal_id,
        .ops = {std::move(seal_op)},
        .prev_epoch_seal = std::nullopt};
    const RefLogTxn base{
        .ns = ns.string(),
        .txn_id = base_id,
        .ops = {},
        .prev_epoch_seal = seal_id};
    fixture::writeRefLogRaw(*backend, layout, birth);
    fixture::writeRefLogRaw(*backend, layout, seal);
    fixture::writeRefLogRaw(*backend, layout, base);

    RefTableState state;
    applyRefLogTxn(state, birth);
    writeRefSnapshotRaw(*backend, layout, snapshotOf(state, ns.string()));
    applyRefLogTxn(state, seal);
    applyRefLogTxn(state, base);
    writeRefSnapshotRaw(*backend, layout, snapshotOf(state, ns.string()));
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = base_id,
        .checkpoint_snapshot_id = base_id,
        .last_epoch_seal = seal_id});

    Gc gc(store, kGc);
    runToFixpoint(store, gc);

    CasRequests requests(backend, Fence::open());
    CasOperation op = requests.admit();
    EXPECT_TRUE(op.head(layout.refLogKey(life, seal_id), Retry::once()).has_value())
        << "cleanup must retain the predecessor seal that proves the checkpoint base's epoch transition";
    const CasRefCatalog::Snapshot cut = CasRefCatalog::read(op, layout);
    const auto entry = std::find_if(cut.catalog.entries.begin(), cut.catalog.entries.end(),
        [&](const CatalogEntry & candidate) { return candidate.ns == ns; });
    ASSERT_NE(entry, cut.catalog.entries.end());
    const std::optional<CkptSample> checkpoint = readCkpt(op, layout, life);
    ASSERT_TRUE(checkpoint);
    EXPECT_NO_THROW((void)recoverRefTableDetailedFromAuthority(op, layout, *entry, checkpoint->ckpt));
}

TEST(CASRefGcCleanupAuthority, CatalogTokenMoveBeforeFirstChunkRefusesEveryRefObjectDelete)
{
    auto backend = std::make_shared<RefCleanupAuthorityRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RefCleanupFixture keys = seedTwoCoveredLogs(*backend, layout, RootNamespace{"00/aa@cas@"});

    Gc gc(store, kGc);
    /// Lands the move in the exact window between the round's own hot-scan catalog cut (what
    /// `authorityHolds` later compares `folded.catalog_cut` against) and everything after it -- "the
    /// catalog moved before the first chunk starts", the case spec §D's test (1) names.
    OperationForTest race_op(*backend);
    bool hook_fired = false;
    gc.setPostHotScanCatalogReadHookForTest([&]
    {
        hook_fired = true;
        RefCleanupAuthorityRaceBackend::moveRefCleanupAuthorityBeforeFirstChunk(
            RefCleanupAuthorityRaceBackend::Authority::Catalog, *race_op, layout);
    });
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    ASSERT_TRUE(hook_fired) << "the race hook never fired -- this test proves nothing about the race";

    OperationForTest head_op(*backend);
    EXPECT_TRUE((*head_op).head(keys.first_log_key, Retry::once()).has_value());
    EXPECT_TRUE((*head_op).head(keys.second_log_key, Retry::once()).has_value());
    EXPECT_EQ(backend->deleteCount(keys.first_log_key), 0u);
    EXPECT_EQ(backend->deleteCount(keys.second_log_key), 0u);
}

TEST(CASRefGcCleanupAuthority, CatalogTokenMoveBetweenChunksAllowsFirstAndRefusesSecondDelete)
{
    auto backend = std::make_shared<RefCleanupAuthorityRaceBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                                                .gc_bulk_delete_chunk_keys = 1, .gc_fold_max_defer_rounds = 0});
    const Layout & layout = store->layout();
    const RefCleanupFixture keys = seedTwoCoveredLogs(*backend, layout, RootNamespace{"00/aa@cas@"});
    backend->arm(
        RefCleanupAuthorityRaceBackend::Authority::Catalog,
        RefCleanupAuthorityRaceBackend::Timing::AfterFirstDelete, layout, keys.first_log_key);

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    OperationForTest raw_op(*backend);
    EXPECT_FALSE((*raw_op).head(keys.first_log_key, Retry::once()).has_value());
    EXPECT_TRUE((*raw_op).head(keys.second_log_key, Retry::once()).has_value());
    EXPECT_EQ(backend->deleteCount(keys.first_log_key), 1u);
    EXPECT_EQ(backend->deleteCount(keys.second_log_key), 0u);
}

TEST(CASRefGcCleanupAuthority, GcFenceMoveBeforeFirstChunkRefusesEveryRefObjectDelete)
{
    auto backend = std::make_shared<RefCleanupAuthorityRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RefCleanupFixture keys = seedTwoCoveredLogs(*backend, layout, RootNamespace{"00/aa@cas@"});

    Gc gc(store, kGc);
    /// Bumping `gc/state`'s lease directly from the hot-scan hook (as `Authority::Catalog` bumps the
    /// catalog above) would land BEFORE the round's own `round_commit` CAS (phase 13), which still
    /// holds the etag from lease adoption (phase 1) -- that CAS would then lose and the round would
    /// throw before `cleanupRefObjects` (phase 17) ever runs, so "nothing deleted" would hold
    /// vacuously. Instead, only ARM the seam here: the actual bump happens on `authorityHolds`'s own
    /// `gc/state` read (phase 17, long after `round_commit` landed) -- see the class doc comment above
    /// `moveRefCleanupAuthorityBeforeFirstChunk`.
    bool hook_fired = false;
    gc.setPostHotScanCatalogReadHookForTest([&]
    {
        hook_fired = true;
        backend->armGcFenceMoveOnAuthorityHoldsRevalidationForTest(layout);
    });
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    ASSERT_TRUE(hook_fired) << "the race hook never fired -- this test proves nothing about the race";

    OperationForTest raw_op(*backend);
    EXPECT_TRUE((*raw_op).head(keys.first_log_key, Retry::once()).has_value());
    EXPECT_TRUE((*raw_op).head(keys.second_log_key, Retry::once()).has_value());
    EXPECT_EQ(backend->deleteCount(keys.first_log_key), 0u);
    EXPECT_EQ(backend->deleteCount(keys.second_log_key), 0u);
}

TEST(CASRefGcCleanupAuthority, GcFenceMoveBetweenChunksAllowsFirstAndRefusesSecondDelete)
{
    auto backend = std::make_shared<RefCleanupAuthorityRaceBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                                                .gc_bulk_delete_chunk_keys = 1, .gc_fold_max_defer_rounds = 0});
    const Layout & layout = store->layout();
    const RefCleanupFixture keys = seedTwoCoveredLogs(*backend, layout, RootNamespace{"00/aa@cas@"});
    backend->arm(
        RefCleanupAuthorityRaceBackend::Authority::GcFence,
        RefCleanupAuthorityRaceBackend::Timing::AfterFirstDelete, layout, keys.first_log_key);

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    OperationForTest raw_op(*backend);
    EXPECT_FALSE((*raw_op).head(keys.first_log_key, Retry::once()).has_value());
    EXPECT_TRUE((*raw_op).head(keys.second_log_key, Retry::once()).has_value());
    EXPECT_EQ(backend->deleteCount(keys.first_log_key), 1u);
    EXPECT_EQ(backend->deleteCount(keys.second_log_key), 0u);
}

TEST(CASRefGcCleanupAuthority, LeaseMoveDuringAChunkLetsTheChunkCompleteAndNothingElse)
{
    auto backend = std::make_shared<RefCleanupAuthorityRaceBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                                                .gc_bulk_delete_chunk_keys = 1, .gc_fold_max_defer_rounds = 0});
    const Layout & layout = store->layout();
    const RefCleanupFixture keys = seedTwoCoveredLogs(*backend, layout, RootNamespace{"00/aa@cas@"});
    backend->arm(RefCleanupAuthorityRaceBackend::Authority::GcFence,
                 RefCleanupAuthorityRaceBackend::Timing::DuringChunk, layout, keys.first_log_key);

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    OperationForTest raw_op(*backend);
    EXPECT_FALSE((*raw_op).head(keys.first_log_key, Retry::once()).has_value()) << "the chunk in flight completes";
    EXPECT_TRUE((*raw_op).head(keys.second_log_key, Retry::once()).has_value()) << "the next chunk's revalidation refuses";
    EXPECT_EQ(backend->deleteCount(keys.first_log_key), 1u);
    EXPECT_EQ(backend->deleteCount(keys.second_log_key), 0u);
}

TEST(CASRefGcCleanupAuthority, RebirthDuringAChunkDeletesOnlyTheOldLifesKeys)
{
    auto backend = std::make_shared<RefCleanupAuthorityRaceBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                                                .gc_bulk_delete_chunk_keys = 1, .gc_fold_max_defer_rounds = 0});
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    const RefCleanupFixture keys = seedTwoCoveredLogs(*backend, layout, ns);
    backend->arm(RefCleanupAuthorityRaceBackend::Authority::CatalogRebirth,
                 RefCleanupAuthorityRaceBackend::Timing::DuringChunk, layout, keys.first_log_key, ns);

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    OperationForTest raw_op(*backend);
    EXPECT_FALSE((*raw_op).head(keys.first_log_key, Retry::once()).has_value());
    EXPECT_TRUE((*raw_op).head(keys.second_log_key, Retry::once()).has_value());
    /// Whatever the reborn life owns is untouched: its keys carry a different life id and were never
    /// in the cohort. Every key under the new life's stream prefix is still present -- `moveAuthority`'s
    /// `CatalogRebirth` branch seeds the reborn life's own `_log` and `_snap` right after the catalog
    /// rewrite lands (see its doc comment: any earlier and an "unknown incarnation" sweep elsewhere in
    /// the SAME round could claim them, since no catalog entry names that incarnation yet).
    const CasRefCatalog::Snapshot cut = CasRefCatalog::read(*raw_op, layout);
    const auto entry = std::find_if(cut.catalog.entries.begin(), cut.catalog.entries.end(),
                                    [&](const CatalogEntry & e) { return e.ns == ns; });
    ASSERT_NE(entry, cut.catalog.entries.end());
    const NamespaceLifeId reborn = NamespaceLifeId::fromCatalogEntry(entry->ns, entry->incarnation);
    ListPage page = (*raw_op).list(layout.namespaceStreamPrefix(reborn), "", 1000, Retry::once());
    ASSERT_FALSE(page.keys.empty()) << "the seeded new-life objects must be listed, or this test proves nothing";
    for (const ListedKey & listed : page.keys)
    {
        EXPECT_EQ(backend->deleteCount(listed.key), 0u) << listed.key;
        EXPECT_TRUE((*raw_op).head(listed.key, Retry::once()).has_value()) << listed.key;
    }

    /// The next round revalidates against the NEW catalog row: the old (dead) life is no longer named
    /// by any entry `cleanupRefObjects` walks, so its plan/cohort revalidation -- built fresh from the
    /// catalog entry each round -- has nothing of the old life's to touch. What actually happens to the old cohort's
    /// second key, once the round runs unsuppressed, is that the NAMESPACE JANITOR
    /// (`CasNamespaceJanitor.cpp:131`, `catalog_cut.life_index.resolve(*life_id)` failing for a
    /// physical life the catalog no longer names) reclaims it as leaked dead-life debris -- exactly
    /// spec §D's own words: "a moved catalog row means either a dropped life, whose keys the
    /// namespace janitor deletes anyway, or a reborn one". So the key does NOT survive; it survives
    /// past `cleanupRefObjects` specifically, then is reclaimed by a wholly separate, pre-existing
    /// mechanism this task never touches. Attribute the delete precisely rather than asserting
    /// "survives" and being right for an unrelated reason: capture the `ref_object_cleanup` phase's
    /// `suppressed` metric (to confirm the round actually ran, not merely suppressed everything, which
    /// an unusable checkpoint ANYWHERE would do -- see the checkpoint seeded above) and the
    /// `namespace_cleanup` phase's `janitor_deleted` metric, and independently confirm `cleanupRefObjects`
    /// itself deleted nothing this round via the GLOBAL `CASRefCleanupObjectsDeleted` counter (the
    /// `GcPhaseRecord::profile_events` delta is unavailable here: it needs a `CurrentThread` with an
    /// attached `ThreadStatus`, which a bare gtest thread does not have).
    std::optional<uint64_t> ref_cleanup_suppressed;
    std::optional<uint64_t> janitor_deleted;
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "ref_object_cleanup")
            if (const auto it = rec.metrics.find("suppressed"); it != rec.metrics.end())
                ref_cleanup_suppressed = it->second;
        if (rec.phase == "namespace_cleanup")
            if (const auto it = rec.metrics.find("janitor_deleted"); it != rec.metrics.end())
                janitor_deleted = it->second;
    });
    using ProfileEvents::global_counters;
    const auto ref_cleanup_deleted_before = global_counters[ProfileEvents::CASRefCleanupObjectsDeleted].load();
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    ASSERT_TRUE(ref_cleanup_suppressed.has_value()) << "the ref_object_cleanup phase row never fired";
    EXPECT_EQ(*ref_cleanup_suppressed, 0u)
        << "round two must actually run destructive work, not merely leave everything alone because it was suppressed";
    EXPECT_EQ(global_counters[ProfileEvents::CASRefCleanupObjectsDeleted].load(), ref_cleanup_deleted_before)
        << "cleanupRefObjects' own plan/cohort must delete NOTHING this round: the old life is not in it "
           "(no catalog entry names it) and the reborn life's own checkpoint-named log is not yet deletable";
    ASSERT_TRUE(janitor_deleted.has_value()) << "the namespace_cleanup phase row never fired";
    EXPECT_GE(*janitor_deleted, 1u)
        << "the old cohort's second key is expected to be reclaimed by the namespace janitor, not to survive";
    EXPECT_FALSE((*raw_op).head(keys.second_log_key, Retry::once()).has_value())
        << "the old cohort's second key is dead-life debris once its life no longer resolves in the "
           "catalog -- the namespace janitor reclaims it, exactly as spec §D says it would";
    /// The reborn life's own objects are untouched: round two's plan, cleanup and cohort are about the
    /// reborn life now, and none of what it seeded for itself is in that plan. The namespace janitor
    /// leaves them alone too, since they resolve fine against the CURRENT catalog entry.
    for (const ListedKey & listed : page.keys)
        EXPECT_TRUE((*raw_op).head(listed.key, Retry::once()).has_value()) << listed.key;
}

TEST(CASRefGc, RefObjectCleanupDeletesExactlyThePlannedSet)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch

    /// Two committed publishes -> logs {1,1} and {1,2}.
    const ManifestRef r1 = mref(1);
    const ManifestRef r2 = mref(2);
    writeManifestRaw(*backend, layout, ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    writeManifestRaw(*backend, layout, ns, r2, {blobEntryFor("b", DB::UInt128(2))});
    const uint64_t v1 = publishCommittedTransition(*backend, layout, ns, "t1", std::nullopt, r1);
    const uint64_t v2 = publishCommittedTransition(*backend, layout, ns, "t2", std::nullopt, r2);

    /// Two observed snapshots: an OLD one covering only v1, and the NEWEST covering v2. Both are real
    /// wire-format snapshot objects (the recovery codec reads them).
    RefTableSnapshot old_snap = minimalLiveSnapshot(ns.string(), RefTxnId{1, v1},
        {committedRow("t1", r1)});
    RefTableSnapshot new_snap = minimalLiveSnapshot(ns.string(), RefTxnId{1, v2},
        {committedRow("t1", r1), committedRow("t2", r2)});
    writeRefSnapshotRaw(*backend, layout, old_snap);
    writeRefSnapshotRaw(*backend, layout, new_snap);
    replaceRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, v2},
        .checkpoint_snapshot_id = RefTxnId{1, v2},
        .last_epoch_seal = std::nullopt,
    });

    /// The plan the pass computes: `listing` names every log and snapshot this round's scan would
    /// observe, `durable_cursor` is the fold cursor after folding both logs, `checkpoint_snapshot_id`
    /// is the checkpoint-named recovery snapshot, and this fixture never crosses an epoch, so there
    /// is no retained-seal proof.
    const NamespaceLifeId life = fixture::fixtureLife(ns);
    const RefTableListing listing{
        .logs = {RefTxnId{1, v1}, RefTxnId{1, v2}},
        .snapshots = {RefTxnId{1, v1}, RefTxnId{1, v2}}};
    const RefTxnId durable_cursor{1, v2};
    const RefTxnId checkpoint_snapshot_id{1, v2};
    const std::optional<RefTxnId> retained_log_proof = std::nullopt;

    OperationForTest op(*backend);
    const RefCleanupPlan plan = planRefCleanup(listing, durable_cursor, checkpoint_snapshot_id, retained_log_proof);
    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    for (const RefTxnId & id : plan.deletable_logs)
        EXPECT_FALSE((*op).head(layout.refLogKey(life, id), Retry::once()).has_value());
    for (const RefTxnId & id : plan.deletable_snapshots)
        EXPECT_FALSE((*op).head(layout.refSnapshotKey(life, id), Retry::once()).has_value());
    /// and every listed key NOT in the plan is present -- the chunked implementation deletes exactly
    /// the set the per-key implementation would have deleted, nothing more.
    const std::set<RefTxnId> deleted_logs(plan.deletable_logs.begin(), plan.deletable_logs.end());
    const std::set<RefTxnId> deleted_snapshots(plan.deletable_snapshots.begin(), plan.deletable_snapshots.end());
    for (const RefTxnId & id : listing.logs)
        if (!deleted_logs.contains(id))
            EXPECT_TRUE((*op).head(layout.refLogKey(life, id), Retry::once()).has_value())
                << "log " << renderRefTxnId(id) << " not in the plan must survive";
    for (const RefTxnId & id : listing.snapshots)
        if (!deleted_snapshots.contains(id))
            EXPECT_TRUE((*op).head(layout.refSnapshotKey(life, id), Retry::once()).has_value())
                << "snapshot " << renderRefTxnId(id) << " not in the plan must survive";
}

/// The same planned set as above, but the object storage rejects the cohort's one bulk
/// `removeManyWriteOnce` as NOT_IMPLEMENTED (a GCS-backed pool): `cleanupRefObjects`' call site falls
/// back to one admitted request per key (`removeChunkWriteOnceOrOneByOne`, CasGc.h), and the outcome --
/// which keys are gone, and the budget/profile-event accounting -- must be identical to the plain
/// bulk-request path above.
TEST(CASRefGc, RefObjectCleanupFallsBackToOnePerKeyWhenBatchDeleteIsUnsupported)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);

    const ManifestRef r1 = mref(1);
    const ManifestRef r2 = mref(2);
    writeManifestRaw(*backend, layout, ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    writeManifestRaw(*backend, layout, ns, r2, {blobEntryFor("b", DB::UInt128(2))});
    const uint64_t v1 = publishCommittedTransition(*backend, layout, ns, "t1", std::nullopt, r1);
    const uint64_t v2 = publishCommittedTransition(*backend, layout, ns, "t2", std::nullopt, r2);

    RefTableSnapshot old_snap = minimalLiveSnapshot(ns.string(), RefTxnId{1, v1},
        {committedRow("t1", r1)});
    RefTableSnapshot new_snap = minimalLiveSnapshot(ns.string(), RefTxnId{1, v2},
        {committedRow("t1", r1), committedRow("t2", r2)});
    writeRefSnapshotRaw(*backend, layout, old_snap);
    writeRefSnapshotRaw(*backend, layout, new_snap);
    replaceRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, v2},
        .checkpoint_snapshot_id = RefTxnId{1, v2},
        .last_epoch_seal = std::nullopt,
    });

    const NamespaceLifeId life = fixture::fixtureLife(ns);
    const RefTableListing listing{
        .logs = {RefTxnId{1, v1}, RefTxnId{1, v2}},
        .snapshots = {RefTxnId{1, v1}, RefTxnId{1, v2}}};
    const RefTxnId durable_cursor{1, v2};
    const RefTxnId checkpoint_snapshot_id{1, v2};
    const RefCleanupPlan plan = planRefCleanup(listing, durable_cursor, checkpoint_snapshot_id, std::nullopt);
    const uint64_t cohort_size = plan.deletable_logs.size() + plan.deletable_snapshots.size();
    ASSERT_GT(cohort_size, 0u) << "the fixture must actually have something to delete for this test to prove anything";

    /// One armed failure: the cohort's own bulk `removeManyWriteOnce` call fails as "batch delete not
    /// supported"; the fallback's per-key calls that follow are not armed and succeed.
    backend->failNextBulkRemoveWith(std::make_exception_ptr(
        DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "no batch delete")));
    const auto cleaned_before = ProfileEvents::global_counters[ProfileEvents::CASRefCleanupObjectsDeleted].load();

    OperationForTest op(*backend);
    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    for (const RefTxnId & id : plan.deletable_logs)
        EXPECT_FALSE((*op).head(layout.refLogKey(life, id), Retry::once()).has_value());
    for (const RefTxnId & id : plan.deletable_snapshots)
        EXPECT_FALSE((*op).head(layout.refSnapshotKey(life, id), Retry::once()).has_value());
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRefCleanupObjectsDeleted].load() - cleaned_before, cohort_size)
        << "the budget/profile-event accounting counts objects, unaffected by the fallback";
    /// 1 failed bulk attempt + one request per key in the cohort.
    EXPECT_EQ(backend->bulkRemoveCalls(), 1 + cohort_size);
}

/// Task 13 (spec §implementation-impact / §GC Budget): one fold+clean round increments every ref-intake
/// observability counter -- global LIST pages (Q), log-body GETs (K), manifest-body fold GETs (H), emitted
/// manifest edges, and cleaned old ref objects (D). Before/after deltas prove each site actually fires.
TEST(CASRefGc, RefIntakeIncrementsObservabilityCounters)
{
    using ProfileEvents::global_counters;
    const auto list_pages_before = global_counters[ProfileEvents::CASRefGlobalListPages].load();
    const auto log_gets_before   = global_counters[ProfileEvents::CASRefLogBodyGets].load();
    const auto mf_gets_before    = global_counters[ProfileEvents::CASRefManifestBodyFoldGets].load();
    const auto edges_before      = global_counters[ProfileEvents::CASRefEmittedEdges].load();
    const auto cleaned_before    = global_counters[ProfileEvents::CASRefCleanupObjectsDeleted].load();

    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    const ManifestRef r1 = mref(1);
    const ManifestRef r2 = mref(2);
    writeManifestRaw(*backend, layout, ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    writeManifestRaw(*backend, layout, ns, r2, {blobEntryFor("b", DB::UInt128(2))});
    const uint64_t v1 = publishCommittedTransition(*backend, layout, ns, "t1", std::nullopt, r1);
    const uint64_t v2 = publishCommittedTransition(*backend, layout, ns, "t2", std::nullopt, r2);
    /// A checkpoint-named snapshot base makes older listed objects eligible for cleanup once folded.
    writeRefSnapshotRaw(*backend, layout,
        minimalLiveSnapshot(ns.string(), RefTxnId{1, v2}, {committedRow("t1", r1), committedRow("t2", r2)}));
    replaceRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, v2},
        .checkpoint_snapshot_id = RefTxnId{1, v2},
        .last_epoch_seal = std::nullopt,
    });
    (void)v1;

    Gc gc(store, kGc);
    runToFixpoint(store, gc);

    EXPECT_GT(global_counters[ProfileEvents::CASRefGlobalListPages].load(), list_pages_before);
    EXPECT_GT(global_counters[ProfileEvents::CASRefLogBodyGets].load(), log_gets_before);
    EXPECT_GT(global_counters[ProfileEvents::CASRefManifestBodyFoldGets].load(), mf_gets_before);
    EXPECT_GT(global_counters[ProfileEvents::CASRefEmittedEdges].load(), edges_before);
    EXPECT_GT(global_counters[ProfileEvents::CASRefCleanupObjectsDeleted].load(), cleaned_before);
}

/// Task 13 e2e (in-process regression twin of the rustfs integration test): the whole snapshot+log
/// lifecycle over real wire-format objects and real GC rounds -- publish committed refs across two
/// tables, replace one (dropping a blob), publish a covering snapshot, drive GC to a fixpoint, and
/// assert the fold + ref-object cleanup + snapshot lifecycle plus the two read-only consumers:
/// `runFsck(*store).clean()` (the fsck CLI's verdict, oracle included) and `gc.previewDeletes().empty()`
/// (what `cas-gc-dryrun` reports). This is the deterministic permanent twin the unit sweep keeps running.
TEST(CASRefGc, RefSnaplogLifecycleE2E)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns_a{"00/aa@cas@"};
    fixture::admitLive(*backend, store->layout(), ns_a);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch
    const RootNamespace ns_b{"00/bb@cas@"};

    /// Two tables with committed refs naming present manifests + blobs (insert-like). ns_a's ref is then
    /// re-published to a second manifest, dropping the first manifest's blob (a replace: -1 old, +1 new).
    const ManifestRef a1 = mref(1);
    const ManifestRef a2 = mref(2);
    const ManifestRef b1 = mref(3);
    writeBlobBody(*backend, layout, DB::UInt128(1));
    writeBlobBody(*backend, layout, DB::UInt128(2));
    writeBlobBody(*backend, layout, DB::UInt128(3));
    writeManifestRaw(*backend, layout, ns_a, a1, {blobEntryFor("a", DB::UInt128(1))});
    writeManifestRaw(*backend, layout, ns_a, a2, {blobEntryFor("a", DB::UInt128(2))});
    writeManifestRaw(*backend, layout, ns_b, b1, {blobEntryFor("b", DB::UInt128(3))});
    const uint64_t va1 = publishCommittedTransition(*backend, layout, ns_a, "t", std::nullopt, a1);
    const uint64_t va2 = publishCommittedTransition(*backend, layout, ns_a, "t", a1, a2);   /// replace a1 -> a2
    publishCommittedTransition(*backend, layout, ns_b, "t", std::nullopt, b1);
    /// The semantic transition helper has already published the exact CTE for each life.

    /// The writer's compaction: a snapshot of ns_a covering its greatest log (va2), the same
    /// deterministic bytes the oracle recomputes.
    CasRequests requests(backend, Fence::open());
    CasOperation op = requests.admit();
    const CasRefCatalog::Snapshot catalog_cut = CasRefCatalog::read(op, layout);
    const RefTableState sa = recoverRefTableDetailedAtCatalogCutForTest(*backend, layout, catalog_cut, ns_a).state;
    writeRefSnapshotRaw(*backend, layout, snapshotOf(sa, ns_a.string()));
    const NamespaceLifeId life_a = store->namespaceLife(ns_a);
    const CkptSample before_snapshot_publish = *readCkpt(op, layout, life_a);
    RefCkpt after_snapshot_publish = before_snapshot_publish.ckpt;
    after_snapshot_publish.checkpoint_snapshot_id = RefTxnId{1, va2};
    ASSERT_TRUE(std::holds_alternative<Committed>(op.replace(
        layout.refCkptKey(life_a), encodeRefCkpt(after_snapshot_publish),
        before_snapshot_publish.etag, Retry::standard())));

    Gc gc(store, kGc);
    runToFixpoint(store, gc);

    /// Snapshot lifecycle: the covering snapshot is retained; the covered logs (folded + snapshot-covered)
    /// are cleaned; the replaced manifest's blob is reclaimed while the live blobs survive.
    EXPECT_TRUE(op.head(layout.refSnapshotKey(fixture::fixtureLife(ns_a), RefTxnId{1, va2}), Retry::once()).has_value())
        << "covering snapshot retained";
    EXPECT_FALSE(op.head(layout.refLogKey(fixture::fixtureLife(ns_a), RefTxnId{1, va1}), Retry::once()).has_value()) << "covered log cleaned";
    EXPECT_FALSE(blobPresent(*backend, layout, DB::UInt128(1))) << "replaced blob reclaimed";
    EXPECT_TRUE(blobPresent(*backend, layout, DB::UInt128(2))) << "live blob survives";
    EXPECT_TRUE(blobPresent(*backend, layout, DB::UInt128(3))) << "other table's blob survives";

    /// Read-only consumers agree: fsck recovers through the exact checkpoint base and reports no dangle,
    /// while cas-gc-dryrun has no pending content deletes. Covered LIST debris is not diagnostic authority.
    const FsckReport rep = runFsck(*store, /*detail*/true);
    EXPECT_TRUE(rep.clean());
    EXPECT_EQ(rep.dangling, 0u);
    EXPECT_TRUE(gc.previewDeletes().empty()) << "cas-gc-dryrun equivalent: no pending content deletes";
}

/// (8) A malformed/adversarial ref key aborts ref folding for the round: no partial delta, no cursor
/// advance. The malformed key is a real object under `cas/ns/stream/` whose `RefTxnId` render is invalid.
TEST(CASRefGc, MalformedRefKeyAbortsRefFoldingNoPartialDelta)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    const ManifestRef r = mref(1);
    writeManifestRaw(*backend, layout, ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, r);
    /// The semantic transition helper has already published the exact CTE.

    /// Plant a malformed ref key under the ref prefix (a `_log` with a non-canonical id render).
    const NamespaceLifeId life = store->namespaceLife(ns);
    {
        OperationForTest seed_op(*backend);
        (*seed_op).create(layout.namespaceStreamPrefix(life) + "_log/not-a-valid-txn-id", "garbage", Retry::once());
    }

    Gc gc(store, kGc);
    /// The fold's `groupRefKeys` rejects the unrecognized key and ABORTS ref folding for the round (spec
    /// §Step 2: a malformed key cannot produce a partial ref delta or authorize destructive work). The
    /// round CATCHES this internally and survives -- it must not propagate, and must not fold anything.
    ASSERT_NO_THROW(gc.runRegularRound());

    /// No partial delta, no cursor advance: the valid log's blob was NOT folded.
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(1)), 0)
        << "a malformed ref key must abort the round before any partial ref delta lands";
    EXPECT_EQ(foldCursorOf(*backend, layout, ns, 0), 0u)
        << "the durable cursor must not advance on an aborted round";
}

/// (8b) A non-canonical physical life segment is the OTHER way a ref key can be malformed, and it must
/// land on exactly the path (8) pins -- abort ref folding, record the anomaly, COMPLETE the round.
///
/// It gets its own test because the failure mode is worse than a lost round. The parser REFUSES this
/// shape by name rather than returning `std::nullopt`, so it is the one malformed key that can throw
/// from the round's global `cas/ns/stream/` enumeration, which runs in `defer_decision` -- before the fold,
/// and outside the fold's catch. Escaping there does not merely fail one round: GC is the only thing
/// that could ever delete the key, so a round that dies on it dies on it again every time, forever.
/// The enumeration must therefore absorb the refusal per key and leave the key unindexed in
/// `scan.keys`, exactly as it already does for every other malformed shape, and let `groupRefKeys`
/// raise it once where the round is ready to catch it.
TEST(CASRefGc, NonCanonicalLifeKeyAbortsRefFoldingWithoutWedgingTheRound)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    const ManifestRef r = mref(1);
    writeManifestRaw(*backend, layout, ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, r);

    /// A ref log whose supposed life segment contains logical namespace text rather than one canonical
    /// opaque id. Only a foreign or corrupt writer can put this key here, and the pool must survive it.
    const String noncanonical_life =
        layout.casRefsPrefix() + ns.string() + "/_log/" + renderRefTxnId(RefTxnId{1, 1}) + ".zst";
    OperationForTest raw_op(*backend);
    ASSERT_TRUE(std::holds_alternative<Committed>((*raw_op).create(noncanonical_life, "garbage", Retry::once())));

    Gc gc(store, kGc);
    RoundReport rep;
    ASSERT_NO_THROW(rep = gc.runRegularRound())
        << "the round must COMPLETE: a key GC alone could remove must never abort the round that would";
    EXPECT_TRUE(rep.hasAnomaly(RootNamespace{}, /*shard*/ 0))
        << "the refusal must surface as the fold's abort anomaly, not vanish";
    EXPECT_EQ(rep.deleted, 0u);
    EXPECT_EQ(rep.redeleted, 0u);

    /// Same fail-close as (8): no partial delta, no cursor advance.
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(1)), 0)
        << "an aborted ref fold must land no partial ref delta";
    EXPECT_EQ(foldCursorOf(*backend, layout, ns, 0), 0u)
        << "the durable cursor must not advance on an aborted round";

    /// The wedge is only visible over time: the key is still there (nothing deletes it), so a second
    /// round meets it again. It must survive that one too.
    ASSERT_TRUE((*raw_op).head(noncanonical_life, Retry::once()).has_value()) << "precondition: nothing removed the key";
    ASSERT_NO_THROW(gc.runRegularRound()) << "a round that dies on this key would die on it forever";
}

/// Coverage gap (Task 13a): a ref log at a CANONICAL key but with an undecodable BODY -- distinct from a
/// malformed *key* (which aborts earlier at the group step, above). This exercises the
/// GET-then-decode-throw path.
///
/// Its blast radius is the NAMESPACE, not the round (spec §5: the whole-round abort survives only for a
/// key that cannot be attributed to any namespace). The body sits at the position the arithmetic walk
/// reads next, so the walk stops there: everything below it stays folded (a transaction applies
/// atomically -- there is no partial delta either way), the cursor never moves past it, and the recorded
/// anomaly suppresses every destructive step of the round, so nothing the unfolded tail might still
/// reference can be reclaimed.
TEST(CASRefGc, InvalidRefLogBodyHoldsNamespaceNoPartialDelta)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch

    const ManifestRef r = mref(1);
    writeBlobBody(*backend, layout, DB::UInt128(1));
    writeManifestRaw(*backend, layout, ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    ASSERT_EQ(inDegreeOf(*backend, layout, DB::UInt128(1)), 1) << "published and folded";

    /// Now DROP the ref, so the blob is genuinely unreferenced once that record folds, and only then
    /// plant the invalid body at the walk's very next position. This ordering is what makes the
    /// suppression assertion below mean something: asserting that a LIVE blob survives a held round
    /// proves nothing, since a live blob is never reclaimable in the first place.
    const uint64_t dropped = dropRefTransition(*backend, layout, ns, "tbl", r);

    /// A canonical `_log` key (groupRefKeys accepts it) whose body cannot be decoded: the fold GETs it
    /// and `decodeRefLogTxn` throws.
    const String garbage_key = layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, dropped + 1});
    {
        OperationForTest seed_op(*backend);
        (*seed_op).create(garbage_key, "garbage-not-a-valid-reflog-body", Retry::once());
    }
    /// The corruption claims the next committed position. Advance only the durable frontier, not the
    /// log body, so recovery must exact-GET and hold this malformed object instead of ignoring F+1.
    advanceRecoverableCkptForRawFixture(*backend, layout, ns, RefTxnId{1, dropped + 1});

    /// Eight rounds under the hold. Each one catches the hold internally and survives.
    for (int i = 0; i < 8; ++i)
    {
        ASSERT_NO_THROW(runRegularRoundReclaiming(gc));
        store->renewWatermarkOnce();
    }

    EXPECT_EQ(foldCursorOf(*backend, layout, ns, 0), dropped)
        << "the durable cursor must stop BELOW the invalid record, and never advance past it";
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(1)), 0)
        << "the complete transaction below the invalid body folded -- the drop applied, so the blob is "
           "unreferenced and would be reclaimed by any unsuppressed round";
    EXPECT_TRUE(blobPresent(*backend, layout, DB::UInt128(1)))
        << "the held namespace's anomaly suppresses graduation and pending deletes: an unreferenced "
           "blob is NOT reclaimed while any namespace is held, because the unfolded tail behind the "
           "hold may still name it";

    /// DELETING THE EVIDENCE DOES NOT RELEASE THE HOLD. The hold is durable and clears by exactly one
    /// event -- the fold resolving its offending position -- so an object that stops answering does not
    /// turn the gap into a frontier. It is the same observation a lying store produces, and it is
    /// precisely what made the hold necessary; if an absent could clear it, the whole mechanism would
    /// be defeated by the corruption it exists to survive. (Before durable holds this delete DID
    /// release the namespace, which is the hole Task 8 closed.)
    OperationForTest evidence_op(*backend);
    const auto h = (*evidence_op).head(garbage_key, Retry::once());
    ASSERT_TRUE(h.has_value());
    ASSERT_EQ((*evidence_op).remove(garbage_key, h->etag, Retry::once()), Removal::Removed);

    for (int i = 0; i < 4; ++i)
    {
        ASSERT_NO_THROW(runRegularRoundReclaiming(gc));
        store->renewWatermarkOnce();
    }
    EXPECT_TRUE(blobPresent(*backend, layout, DB::UInt128(1)))
        << "the hold still stands: nothing resolved the offending position, an absent proved nothing";

    /// REPAIR is the release: a DECODABLE record at the offending position. The fold reads it, folds
    /// through it, seals a cursor above it -- and only then does the namespace stop being held and
    /// destruction resumes. The CTE already claims this position, so this must replace the repaired
    /// body at its exact id rather than use the semantic wrapper, which would attempt a non-monotone
    /// checkpoint advance.
    const ManifestRef r2 = mref(2);
    writeBlobBody(*backend, layout, DB::UInt128(2));
    writeManifestRaw(*backend, layout, ns, r2, {blobEntryFor("b", DB::UInt128(2))});
    writeTxnAt(*backend, layout, ns, RefTxnId{1, dropped + 1}, publishCommittedOps("tbl2", r2));

    ASSERT_TRUE(runToFixpoint(store, gc) < 64u) << "the released namespace must converge";
    EXPECT_EQ(foldCursorOf(*backend, layout, ns, 0), dropped + 1) << "the walk folded through the hold";
    EXPECT_FALSE(blobPresent(*backend, layout, DB::UInt128(1)))
        << "once the hold clears, the unreferenced blob is reclaimed -- so the survival above was the "
           "suppression doing its job, not the blob being unreclaimable";
    EXPECT_TRUE(blobPresent(*backend, layout, DB::UInt128(2))) << "the repair's own blob is referenced";
}

/// Coverage gap (Task 13a): the per-table baseline guard (spec §Offline Recovery) has no positive-trip
/// test at HEAD -- the adapted successor of the retired CASGCBaselineGuard.FreshStateOverTrimmedJournals
/// contract. A table whose logs at/below its newest snapshot are gone and that has no sealed fold cursor
/// is the "a prior fold advanced+cleaned covered logs, then gc/state was lost" signature: folding it from
/// {0,0} would emit no edges and mass-condemn its still-referenced blob. GC must refuse the round before
/// any delete. The existing CASGCBaselineGuard tests cover only the genuinely-fresh pass case and the
/// adopted-seal-missing guard, not this branch.
TEST(CASRefGc, BaselineGuardRefusesWhenSnapshotSurvivesWithoutLogsOrCursor)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();

    /// Table A is healthy (a committed ref with its manifest+blob, no snapshot), giving GC a normal table
    /// to fold in the same round.
    const RootNamespace ns_a{"00/aa@cas@"};
    const ManifestRef ra = mref(1);
    writeBlobBody(*backend, layout, DB::UInt128(1));
    writeManifestRaw(*backend, layout, ns_a, ra, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, layout, ns_a, "ta", std::nullopt, ra);

    /// Table B is poisoned: a durable snapshot survives, but its logs at/below it are GONE and B has no
    /// sealed cursor (first round -> no adopted parent cursors). This is the exact baseline-guard input.
    const RootNamespace ns_b{"00/bb@cas@"};
    /// Stage B (Task 4-C): `writeRefSnapshotRaw` deliberately does NOT self-admit (several fixtures
    /// build a table with no catalog entry on purpose), so without this `ns_b` would never enter the
    /// catalog at all and would be invisible to the round -- the baseline guard below could then never
    /// fire, since it never runs on a namespace outside the universe.
    fixture::admitLive(*backend, layout, ns_b);
    const ManifestRef rb = mref(2);
    writeBlobBody(*backend, layout, DB::UInt128(2));
    writeManifestRaw(*backend, layout, ns_b, rb, {blobEntryFor("b", DB::UInt128(2))});
    writeRefSnapshotRaw(*backend, layout, minimalLiveSnapshot(ns_b.string(), RefTxnId{1, 5},
        {committedRow("tb", rb)}));

    /// The baseline guard must fail closed BEFORE any destructive step (first round: no prior fold seal,
    /// so the failure can only come from the baseline guard, not the seal-divergence guard).
    Gc gc(store, kGc);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { gc.runRegularRound(); });
    EXPECT_TRUE(blobPresent(*backend, layout, DB::UInt128(1))) << "table A's blob survives the refusal";
    EXPECT_TRUE(blobPresent(*backend, layout, DB::UInt128(2)))
        << "table B's blob must NOT be condemned -- the guard fires before any delete";
}

/// A catalog-admitted life without a parent cursor is a valid fresh fold target when it has no
/// snapshot or logs. The fold must seed its successor seal from every plan row, not only the
/// parent-cursor subset used by the baseline guard.
TEST(CASRefGc, CatalogAdmittedFreshLifeWithoutParentSeedsSuccessorSeal)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    fixture::admitLive(*backend, layout, ns);

    CasRequests requests(backend, Fence::open());
    CasOperation op = requests.admit();
    const CasRefCatalog::Snapshot catalog_cut = CasRefCatalog::read(op, layout);
    ASSERT_EQ(catalog_cut.catalog.entries.size(), 1u);
    const UInt128 life_id = catalog_cut.catalog.entries.front().incarnation;

    Gc gc(store, kGc);
    ASSERT_NO_THROW(gc.runRegularRound());

    const GcState state = decodeGcState(op.read(layout.gcStateKey(), Retry::once())->bytes);
    const CasFoldSeal seal = decodeFoldSeal(
        op.read(layout.foldSealKey(state.snap_generation, state.snap_attempt), Retry::once())->bytes);
    EXPECT_TRUE(seal.ref_lives.contains(life_id));
}
