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
        if (no_work && !anyCondemnedInSeal(s->backend(), s->layout()))
            break;
    }
    return rounds;
}

bool blobPresent(Backend & b, const Layout & layout, const UInt128 & hash)
{
    return b.head(layout.blobKey(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hash)})).exists;
}

/// Denies ONCE the single round-commit `gc/state` CAS that advances `snap_generation` (the losing
/// leader deposed mid-round). The denied round leaves only never-adopted attempt-scoped debris.
class DeposeRoundCommitBackend : public InMemoryBackend
{
public:
    CasResult casPut(const String & key, const String & bytes, const std::optional<Token> & expected,
                     const ObjectMeta & meta) override
    {
        if (arm && key == "p/gc/state")
        {
            const auto stored = get(key);
            const uint64_t stored_gen = stored ? decodeGcState(stored->bytes).snap_generation : 0;
            if (decodeGcState(bytes).snap_generation > stored_gen)
            {
                arm = false;
                throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
                    "test-injected: round-commit gc/state CAS denied (losing leader deposed mid-round)");
            }
        }
        return InMemoryBackend::casPut(key, bytes, expected, meta);
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
    };

    enum class Timing : uint8_t
    {
        BeforeFirstDelete,
        AfterFirstDelete,
    };

    void arm(
        Authority authority_, Timing timing_, const Layout & layout,
        const String & first_cleanup_key_)
    {
        authority = authority_;
        timing = timing_;
        catalog_key = layout.refCatalogKey();
        gc_state_key = layout.gcStateKey();
        first_cleanup_key = first_cleanup_key_;
        armed = true;
    }

    HeadResult head(const String & key) override
    {
        HeadResult result = CountingBackend::head(key);
        if (armed && timing == Timing::BeforeFirstDelete && key == first_cleanup_key)
            moveAuthority();
        return result;
    }

    DeleteOutcome deleteExact(const String & key, const Token & token) override
    {
        DeleteOutcome result = CountingBackend::deleteExact(key, token);
        if (armed && timing == Timing::AfterFirstDelete && key == first_cleanup_key)
            moveAuthority();
        return result;
    }

private:
    void moveAuthority()
    {
        armed = false;
        const String & key = authority == Authority::Catalog ? catalog_key : gc_state_key;
        const auto got = CountingBackend::get(key);
        if (!got)
            throw std::runtime_error("test-injected cleanup authority object is absent");

        String bytes = got->bytes;
        if (authority == Authority::GcFence)
        {
            GcState moved = decodeGcState(bytes);
            ++moved.lease.seq;
            bytes = encodeGcState(moved);
        }
        if (CountingBackend::casPut(key, bytes, got->token).outcome != CasOutcome::Committed)
            throw std::runtime_error("test-injected cleanup authority move lost its CAS");
    }

    Authority authority = Authority::Catalog;
    Timing timing = Timing::BeforeFirstDelete;
    String catalog_key;
    String gc_state_key;
    String first_cleanup_key;
    bool armed = false;
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
    const auto adopted = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    ASSERT_GT(adopted.snap_generation, 0u);

    /// Drop the ref, then run the round whose commit is DENIED (losing leader).
    dropRefTransition(*backend, layout, ns, "tbl", r);
    backend->arm = true;
    EXPECT_ANY_THROW(gc.runRegularRound());
    backend->arm = false;

    /// The deposed round adopted NOTHING: the durable pointers are unchanged...
    const auto after = decodeGcState(backend->get(layout.gcStateKey())->bytes);
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
    ASSERT_TRUE(backend->head(log_v1_key).exists);
    ASSERT_TRUE(backend->head(log_v2_key).exists);
    ASSERT_TRUE(backend->head(old_snap_key).exists);

    Gc gc(store, kGc);
    runToFixpoint(store, gc);   /// folds v1,v2 (cursor -> v2) then cleans covered ref objects post-CAS

    /// The old log lies below both the durable cursor and the validated checkpoint base => DELETED.
    EXPECT_FALSE(backend->head(log_v1_key).exists)
        << "a log below the checkpoint-named snapshot base and durable cursor must be deleted";
    /// The same-id ordinary log is part of recovery's triple and must survive.
    EXPECT_TRUE(backend->head(log_v2_key).exists)
        << "the checkpoint-named non-seal log must survive with its snapshot";
    /// The older snapshot is deleted; the checkpoint-named snapshot is retained.
    EXPECT_FALSE(backend->head(old_snap_key).exists) << "an older snapshot must be deleted";
    EXPECT_TRUE(backend->head(new_snap_key).exists) << "the checkpoint-named snapshot must be retained";
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
    auto countSurviving = [&]
    {
        size_t n = 0;
        for (const String & k : deletable_log_keys)
            if (backend->head(k).exists)
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

    EXPECT_TRUE(backend->head(layout.refLogKey(life, seal_id)).exists)
        << "cleanup must retain the predecessor seal that proves the checkpoint base's epoch transition";
    const CasRefCatalog::Snapshot cut = CasRefCatalog::read(*backend, layout);
    const auto entry = std::find_if(cut.catalog.entries.begin(), cut.catalog.entries.end(),
        [&](const CatalogEntry & candidate) { return candidate.ns == ns; });
    ASSERT_NE(entry, cut.catalog.entries.end());
    const std::optional<CkptSample> checkpoint = readCkpt(*backend, layout, life);
    ASSERT_TRUE(checkpoint);
    EXPECT_NO_THROW((void)recoverRefTableDetailedFromAuthority(*backend, layout, *entry, checkpoint->ckpt));
}

TEST(CASRefGcCleanupAuthority, CatalogTokenMoveBeforeFirstDeleteRefusesEveryRefObjectDelete)
{
    auto backend = std::make_shared<RefCleanupAuthorityRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RefCleanupFixture keys = seedTwoCoveredLogs(*backend, layout, RootNamespace{"00/aa@cas@"});
    backend->arm(
        RefCleanupAuthorityRaceBackend::Authority::Catalog,
        RefCleanupAuthorityRaceBackend::Timing::BeforeFirstDelete, layout, keys.first_log_key);

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    EXPECT_TRUE(backend->head(keys.first_log_key).exists);
    EXPECT_TRUE(backend->head(keys.second_log_key).exists);
    EXPECT_EQ(backend->deleteCount(keys.first_log_key), 0u);
    EXPECT_EQ(backend->deleteCount(keys.second_log_key), 0u);
}

TEST(CASRefGcCleanupAuthority, CatalogTokenMoveBetweenKeysAllowsFirstAndRefusesSecondDelete)
{
    auto backend = std::make_shared<RefCleanupAuthorityRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RefCleanupFixture keys = seedTwoCoveredLogs(*backend, layout, RootNamespace{"00/aa@cas@"});
    backend->arm(
        RefCleanupAuthorityRaceBackend::Authority::Catalog,
        RefCleanupAuthorityRaceBackend::Timing::AfterFirstDelete, layout, keys.first_log_key);

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    EXPECT_FALSE(backend->head(keys.first_log_key).exists);
    EXPECT_TRUE(backend->head(keys.second_log_key).exists);
    EXPECT_EQ(backend->deleteCount(keys.first_log_key), 1u);
    EXPECT_EQ(backend->deleteCount(keys.second_log_key), 0u);
}

TEST(CASRefGcCleanupAuthority, GcFenceMoveBeforeFirstDeleteRefusesEveryRefObjectDelete)
{
    auto backend = std::make_shared<RefCleanupAuthorityRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RefCleanupFixture keys = seedTwoCoveredLogs(*backend, layout, RootNamespace{"00/aa@cas@"});
    backend->arm(
        RefCleanupAuthorityRaceBackend::Authority::GcFence,
        RefCleanupAuthorityRaceBackend::Timing::BeforeFirstDelete, layout, keys.first_log_key);

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    EXPECT_TRUE(backend->head(keys.first_log_key).exists);
    EXPECT_TRUE(backend->head(keys.second_log_key).exists);
    EXPECT_EQ(backend->deleteCount(keys.first_log_key), 0u);
    EXPECT_EQ(backend->deleteCount(keys.second_log_key), 0u);
}

TEST(CASRefGcCleanupAuthority, GcFenceMoveBetweenKeysAllowsFirstAndRefusesSecondDelete)
{
    auto backend = std::make_shared<RefCleanupAuthorityRaceBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    const Layout & layout = store->layout();
    const RefCleanupFixture keys = seedTwoCoveredLogs(*backend, layout, RootNamespace{"00/aa@cas@"});
    backend->arm(
        RefCleanupAuthorityRaceBackend::Authority::GcFence,
        RefCleanupAuthorityRaceBackend::Timing::AfterFirstDelete, layout, keys.first_log_key);

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    EXPECT_FALSE(backend->head(keys.first_log_key).exists);
    EXPECT_TRUE(backend->head(keys.second_log_key).exists);
    EXPECT_EQ(backend->deleteCount(keys.first_log_key), 1u);
    EXPECT_EQ(backend->deleteCount(keys.second_log_key), 0u);
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
    const CasRefCatalog::Snapshot catalog_cut = CasRefCatalog::read(*backend, layout);
    const RefTableState sa = recoverRefTableDetailedAtCatalogCutForTest(*backend, layout, catalog_cut, ns_a).state;
    writeRefSnapshotRaw(*backend, layout, snapshotOf(sa, ns_a.string()));
    const NamespaceLifeId life_a = store->namespaceLife(ns_a);
    const CkptSample before_snapshot_publish = *readCkpt(*backend, layout, life_a);
    RefCkpt after_snapshot_publish = before_snapshot_publish.ckpt;
    after_snapshot_publish.checkpoint_snapshot_id = RefTxnId{1, va2};
    ASSERT_EQ(backend->casPut(
        layout.refCkptKey(life_a), encodeRefCkpt(after_snapshot_publish), before_snapshot_publish.token).outcome,
        CasOutcome::Committed);

    Gc gc(store, kGc);
    runToFixpoint(store, gc);

    /// Snapshot lifecycle: the covering snapshot is retained; the covered logs (folded + snapshot-covered)
    /// are cleaned; the replaced manifest's blob is reclaimed while the live blobs survive.
    EXPECT_TRUE(backend->head(layout.refSnapshotKey(fixture::fixtureLife(ns_a), RefTxnId{1, va2})).exists)
        << "covering snapshot retained";
    EXPECT_FALSE(backend->head(layout.refLogKey(fixture::fixtureLife(ns_a), RefTxnId{1, va1})).exists) << "covered log cleaned";
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
    backend->putIfAbsent(layout.namespaceStreamPrefix(life) + "_log/not-a-valid-txn-id", "garbage");

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
    ASSERT_EQ(backend->putIfAbsent(noncanonical_life, "garbage").outcome, PutOutcome::Done);

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
    ASSERT_TRUE(backend->head(noncanonical_life).exists) << "precondition: nothing removed the key";
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
    backend->putIfAbsent(garbage_key, "garbage-not-a-valid-reflog-body");
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
    const HeadResult h = backend->head(garbage_key);
    ASSERT_TRUE(h.exists);
    ASSERT_EQ(backend->deleteExact(garbage_key, h.token).kind, DeleteOutcome::Kind::Deleted);

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

    const CasRefCatalog::Snapshot catalog_cut = CasRefCatalog::read(*backend, layout);
    ASSERT_EQ(catalog_cut.catalog.entries.size(), 1u);
    const UInt128 life_id = catalog_cut.catalog.entries.front().incarnation;

    Gc gc(store, kGc);
    ASSERT_NO_THROW(gc.runRegularRound());

    const GcState state = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    const CasFoldSeal seal = decodeFoldSeal(
        backend->get(layout.foldSealKey(state.snap_generation, state.snap_attempt))->bytes);
    EXPECT_TRUE(seal.ref_lives.contains(life_id));
}
