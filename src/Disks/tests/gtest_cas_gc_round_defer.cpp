#include <gtest/gtest.h>

#include <set>
#include <limits>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcMaintenanceState.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasNamespaceJanitor.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Common/ProfileEvents.h>
#include "cas_test_helpers.h"

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace ProfileEvents
{
extern const Event CASGCRefWalkPlansBuilt;
}

namespace
{
const UInt128 kGc = UInt128(0xAB);
}

TEST(CASGCRoundDefer, PredicateTruthTable)
{
    /// threshold=1 (default): defer ONLY when zero shards changed AND no graduation due AND within bound.
    EXPECT_TRUE (shouldDeferRound(/*changed*/0, /*grad_due*/false, /*since*/0, /*threshold*/1, /*max*/8));
    EXPECT_FALSE(shouldDeferRound(1, false, 0, 1, 8));   // a shard changed => fold
    EXPECT_FALSE(shouldDeferRound(0, true,  0, 1, 8));   // graduation due => force fold
    EXPECT_FALSE(shouldDeferRound(0, false, 8, 1, 8));   // defer bound reached => force fold

    /// threshold=3 (batching): defer while accumulated changed shards < threshold, no grad, within bound.
    EXPECT_TRUE (shouldDeferRound(2, false, 0, 3, 8));
    EXPECT_FALSE(shouldDeferRound(3, false, 0, 3, 8));   // reached threshold => fold
    EXPECT_FALSE(shouldDeferRound(2, true,  0, 3, 8));   // graduation due => force fold regardless of size
    EXPECT_FALSE(shouldDeferRound(2, false, 8, 3, 8));   // bound reached => force fold
}

/// `graduationDue` reads ZERO-I/O from the adopted seal's `condemned_summary`. An
/// entry whose oldest non-pending condemn round crosses current_round forces it true; a delete_pending
/// entry forces it true regardless of the round; otherwise false.
TEST(CASGCRoundDefer, GraduationDueDetectsDuePendingAndRoundCrossing)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();

    /// Adopt a seal whose shard-0 summary holds one condemned-but-not-yet-graduated entry (round 2).
    injectCondemnedSummarySeal(*backend, layout, /*generation*/1, /*attempt*/1, /*gc_shards*/1,
        {{0, CondemnedSummary{.condemned_total = 1, .pending_total = 0,
                              .oldest_nonpending_condemn_round = 2}}});

    Gc gc(store, kGc);
    OperationForTest raw_op(*backend);
    const GcState state = decodeGcState((*raw_op).read(layout.gcStateKey(), Retry::once())->bytes);

    EXPECT_FALSE(gc.graduationDueForTest(state, /*current_round=*/2))
        << "oldest non-pending condemn round (2) is not < current_round (2); not yet due to graduate";
    EXPECT_TRUE(gc.graduationDueForTest(state, /*current_round=*/3))
        << "oldest non-pending condemn round (2) < current_round (3) => due to graduate";

    /// Re-adopt a seal whose summary entry is delete_pending: due regardless of the round.
    injectCondemnedSummarySeal(*backend, layout, /*generation*/1, /*attempt*/1, /*gc_shards*/1,
        {{0, CondemnedSummary{.condemned_total = 1, .pending_total = 1,
                              .oldest_nonpending_condemn_round = std::numeric_limits<uint64_t>::max()}}});
    const GcState state_pending = decodeGcState((*raw_op).read(layout.gcStateKey(), Retry::once())->bytes);

    EXPECT_TRUE(gc.graduationDueForTest(state_pending, /*current_round=*/0))
        << "a delete_pending entry must force graduationDue true regardless of current_round";
}

/// graduationDue fail-closed: when the adopted seal OBJECT is deleted out from under gc/state, the signal
/// must be TRUE (forces the fold so the round's own fail-closed path surfaces the corrupt bookkeeping),
/// never a silent defer.
TEST(CASGCRoundDefer, GraduationDueFailsClosedWhenSealMissing)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();

    injectCondemnedSummarySeal(*backend, layout, /*generation*/1, /*attempt*/1, /*gc_shards*/1,
        {{0, CondemnedSummary{}}});
    OperationForTest raw_op(*backend);
    const GcState state = decodeGcState((*raw_op).read(layout.gcStateKey(), Retry::once())->bytes);

    /// Delete the adopted seal object (corrupt destructive bookkeeping).
    const String seal_key = layout.foldSealKey(state.snap_generation, state.snap_attempt);
    const auto h = (*raw_op).head(seal_key, Retry::once());
    ASSERT_TRUE(h.has_value());
    ASSERT_EQ((*raw_op).remove(seal_key, h->etag, Retry::once()), Removal::Removed);

    Gc gc(store, kGc);
    EXPECT_TRUE(gc.graduationDueForTest(state, /*current_round=*/5))
        << "a missing adopted seal must fail-closed to a forced fold";
}

/// graduationDue is FALSE on a TOTAL all-zero summary: nothing condemned in any shard => nothing due.
TEST(CASGCRoundDefer, GraduationDueFalseOnAllZeroSummary)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_shards = 2});
    const Layout & layout = store->layout();

    injectCondemnedSummarySeal(*backend, layout, /*generation*/1, /*attempt*/1, /*gc_shards*/2,
        {{0, CondemnedSummary{}}, {1, CondemnedSummary{}}});
    OperationForTest raw_op(*backend);
    const GcState state = decodeGcState((*raw_op).read(layout.gcStateKey(), Retry::once())->bytes);

    Gc gc(store, kGc);
    EXPECT_FALSE(gc.graduationDueForTest(state, /*current_round=*/9))
        << "an all-zero total summary means nothing is due to graduate";

    /// Fail-closed if the summary is NOT total over gc_shards (shard 1 missing).
    injectCondemnedSummarySeal(*backend, layout, /*generation*/1, /*attempt*/1, /*gc_shards*/2,
        {{0, CondemnedSummary{}}});
    const GcState partial = decodeGcState((*raw_op).read(layout.gcStateKey(), Retry::once())->bytes);
    EXPECT_TRUE(gc.graduationDueForTest(partial, /*current_round=*/9))
        << "a summary not total over gc_shards is corrupt => fail-closed force-fold";
}

/// `listRefPrefix`'s `changed_shards`: with the fold seal covering shard s at its current token, a quiescent pool reports
/// 0; after one publish to a ref in shard s, it reports 1.
TEST(CASGCRoundDefer, ChangedShardCountIsZeroWhenQuiescent)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r1{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 0xAA};

    writeBlobBody(*backend, layout, UInt128(1));
    writeManifestRaw(*backend, layout, ns, r1, {blobEntryFor("a", UInt128(1))});
    publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, r1);

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);   /// fold; the round's own trim then rewrites the
                                                         /// shard (compacting the just-folded event), so
                                                         /// its sealed token is the PRE-trim snapshot.
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);   /// a second, work-free round: nothing left to
                                                         /// trim, so THIS round's fold seal finally
                                                         /// captures the shard's actual current token.

    OperationForTest raw_op(*backend);
    const GcState quiescent_state = decodeGcState((*raw_op).read(layout.gcStateKey(), Retry::once())->bytes);
    EXPECT_EQ(gc.listRefPrefixForTest(quiescent_state).changed_shards, 0u)
        << "a quiescent shard (listed token == sealed token) must not count as changed";

    /// Publish a second ref into the SAME shard: its LISTED token now differs from what
    /// `quiescent_state`'s adopted fold seal recorded.
    const ManifestRef r2{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = 0xBB};
    writeBlobBody(*backend, layout, UInt128(2));
    writeManifestRaw(*backend, layout, ns, r2, {blobEntryFor("b", UInt128(2))});
    publishCommittedTransition(*backend, layout, ns, "tbl2", std::nullopt, r2);

    EXPECT_EQ(gc.listRefPrefixForTest(quiescent_state).changed_shards, 1u)
        << "one shard whose token advanced since the sealed generation must count as changed";
}

/// Mutation caught: widening the hot LIST from `cas/ns/stream/` to `cas/ns/` would offer `_ckpt` and
/// `_files` state objects to the fold. The backend-observed result set must contain both immutable
/// stream kinds and neither state kind.
TEST(CASGCRoundDefer, HotEnumerationOffersLogsAndSnapshotsButNeverCheckpointOrFiles)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(RootNamespace{"name-must-not-appear"}, UInt128{0x123});
    const RefTxnId id{1, 1};
    const String log_key = layout.refLogKey(life, id);
    const String snap_key = layout.refSnapshotKey(life, id);
    const String ckpt_key = layout.refCkptKey(life);
    const String file_key = layout.namespaceFileKey(life, "f");
    {
        OperationForTest seed_op(*backend);
        ASSERT_TRUE(std::holds_alternative<Committed>((*seed_op).create(log_key, "log", Retry::once())));
        ASSERT_TRUE(std::holds_alternative<Committed>((*seed_op).create(snap_key, "snap", Retry::once())));
        ASSERT_TRUE(std::holds_alternative<Committed>((*seed_op).create(ckpt_key, "ckpt", Retry::once())));
        ASSERT_TRUE(std::holds_alternative<Committed>((*seed_op).create(file_key, "file", Retry::once())));
    }
    backend->resetCounts();

    Gc gc(store, kGc);
    const RefScanSummary scan = gc.listRefPrefixForTest(GcState{});
    const std::set<String> offered(scan.keys.begin(), scan.keys.end());
    EXPECT_EQ(offered, (std::set<String>{log_key, snap_key}));
    EXPECT_EQ(backend->listCount(layout.namespaceStreamRootPrefix()), 1u);
    EXPECT_EQ(backend->listCount(layout.namespaceRootPrefix()), 0u);
    EXPECT_EQ(backend->listCount(layout.namespaceStateRootPrefix()), 0u);
}

/// The authoritative cut follows the completed hot LIST. A listed life absent from that later cut is
/// inert dead-life debris: it is not admitted and does not defer the round or read the body.
TEST(CASGCRoundDefer, ListedLifeAbsentFromThePostListCatalogCutIsInertDebris)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const NamespaceLifeId unknown = NamespaceLifeId::fromCatalogEntry(RootNamespace{"cannot-authorize"}, UInt128{0x456});
    const String log_key = layout.refLogKey(unknown, RefTxnId{1, 1});
    {
        OperationForTest seed_op(*backend);
        ASSERT_TRUE(std::holds_alternative<Committed>((*seed_op).create(log_key, "not-read-on-defer", Retry::once())));
    }
    backend->resetCounts();

    Gc gc(store, kGc);
    const RoundReport report = gc.runRegularRound({}, /*allow_steal=*/true, UniversePolicy::Authoritative);
    EXPECT_FALSE(report.deferred);
    EXPECT_EQ(backend->getCount(log_key), 0u)
        << "inert means the body is never read: the life is absent from the authoritative cut, so no "
           "admission and no fold intake can touch it";
    /// The debris IS reclaimed in this round, and that is the janitor's designed job, not the fold's:
    /// a life id absent from the catalog cut is a dead life, and the namespace janitor deletes its
    /// objects by exact token behind the same fence. A round over a proved-empty catalog completes its
    /// frontier, so nothing suppresses that reclaim any more -- the object is dropped without ever
    /// being read or admitted, which is exactly what "inert debris" means here.
    EXPECT_EQ(backend->deleteCount(log_key), 1u);
}

/// The post-LIST cut classifies every immutable stream kind, not only logs. A snapshot belonging to a
/// life absent from that later cut is inert debris and its body is not read.
TEST(CASGCRoundDefer, SnapshotLifeAbsentFromThePostListCatalogCutIsInertDebris)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const NamespaceLifeId unknown = NamespaceLifeId::fromCatalogEntry(RootNamespace{"cannot-authorize"}, UInt128{0x457});
    const String snapshot_key = layout.refSnapshotKey(unknown, RefTxnId{1, 1});
    {
        OperationForTest seed_op(*backend);
        ASSERT_TRUE(std::holds_alternative<Committed>((*seed_op).create(snapshot_key, "not-read-on-defer", Retry::once())));
    }
    backend->resetCounts();

    Gc gc(store, kGc);
    const RoundReport report = gc.runRegularRound({}, /*allow_steal=*/true, UniversePolicy::Authoritative);
    EXPECT_FALSE(report.deferred);
    EXPECT_EQ(backend->getCount(snapshot_key), 0u)
        << "inert means the body is never read, whatever immutable stream kind it is";
    /// As for the log above: the dead life's snapshot is reclaimed by the janitor by exact token,
    /// never read and never admitted.
    EXPECT_EQ(backend->deleteCount(snapshot_key), 1u);
}

/// ---- Task 4: the DEFER short-circuit wired into runRegularRound ----

/// Idle round re-adopts: after a settled round, a subsequent round with zero changed shards and no
/// graduation due sets report.deferred=true and performs dramatically less generation-run I/O than a
/// real fold round (no `blob_target` run object touched at all -- the fold never runs). Snap
/// generation/attempt are untouched (the snapshot is not rebuilt).
///
/// SETTLING NOTE: immutable `_log` objects are never trimmed in place (unlike the legacy mutable shard
/// journal, whose fold-then-trim token rewrite forced a second settling round), so the pool quiesces the
/// round AFTER the folding round -- the very next round defers.
TEST(CASGCRoundDefer, IdleRoundDefersAndReadsNoGeneration)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 0xAA};
    writeBlobBody(*backend, store->layout(), UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    backend->resetCounts();
    const RoundReport fold_rep = gc.runRegularRound();   /// round 1: folds the +1 (no trim-lag, quiesces at once)
    ASSERT_FALSE(fold_rep.deferred);
    const uint64_t fold_round_gets = backend->getTotal();
    EXPECT_GT(fold_round_gets, 0u) << "sanity: a real fold round performs some GETs";

    OperationForTest raw_op(*backend);
    const auto st_before = decodeGcState((*raw_op).read(store->layout().gcStateKey(), Retry::once())->bytes);

    backend->resetCounts();
    const RoundReport rep = gc.runRegularRound();   /// round 2: genuinely quiesced now => must defer
    const uint64_t defer_round_gets = backend->getTotal();

    EXPECT_TRUE(rep.deferred) << "a settled idle round must re-adopt the sealed generation, not fold";
    /// A deferred round mints no new round (CasGc.cpp:runRegularRound's defer branch), so the honest
    /// `report.round` is the round that was ALREADY adopted before this round started -- the same round
    /// the preceding fold round committed. Guards against the bug where the defer path returned WITHOUT
    /// ever assigning `report.round`, leaving it at its zero-initialized default and making every
    /// deferred round print `CA GC round 0` regardless of how far GC had actually progressed.
    EXPECT_NE(rep.round, 0u) << "a deferred round must report a truthful, nonzero round number";
    EXPECT_EQ(rep.round, fold_rep.round)
        << "a deferred round re-adopts the already-committed round, not a fabricated new one";

    const auto st_after = decodeGcState((*raw_op).read(store->layout().gcStateKey(), Retry::once())->bytes);
    EXPECT_EQ(st_after.snap_generation, st_before.snap_generation)
        << "a deferred round must not mint a new generation (snapshot rebuild elided)";
    EXPECT_EQ(st_after.snap_attempt, st_before.snap_attempt);

    /// SECONDARY (not over-fit to "exactly 0 gets" -- the decision itself pays a bounded retired-list +
    /// discovery-LIST cost that may share the same get counter): the deferred round touches NO
    /// blob_target run object at all (fold never runs, so foldDeltasIntoGeneration never executes), and
    /// its total get volume sits far below a genuine fold round's.
    EXPECT_EQ(backend->ioCountForKeysContaining("/blob_target/"), 0u)
        << "a deferred round must never GET/getStream/PUT any blob_target run object";
    EXPECT_LT(defer_round_gets, fold_round_gets)
        << "a deferred round's read volume must sit far below a real fold round's";
}

/// Every ordinary round constructs one complete catalog-authoritative walk plan after the hot LIST,
/// before deciding DEFER. A fold consumes that exact frozen plan; it must not build another one.
TEST(CASGCRoundDefer, FoldAndDeferEachBuildExactlyOneCompletePostListWalkPlan)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/one-walk-plan@cas@"};
    const ManifestRef ref{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 1};
    writeBlobBody(*backend, layout, UInt128{1});
    writeManifestRaw(*backend, layout, ns, ref, {blobEntryFor("a", UInt128{1})});
    publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, ref);

    Gc gc(store, kGc);
    std::vector<GcPhaseRecord> phases;
    gc.setPhaseSink([&](const GcPhaseRecord & phase) { phases.push_back(phase); });

    backend->resetCounts();
    const uint64_t fold_builds_before
        = ProfileEvents::global_counters[ProfileEvents::CASGCRefWalkPlansBuilt].load();
    ASSERT_FALSE(gc.runRegularRound().deferred);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASGCRefWalkPlansBuilt].load() - fold_builds_before, 1u);
    EXPECT_EQ(backend->listCount(layout.namespaceStreamRootPrefix()), 1u)
        << "the hot walk must enumerate the stream tree exactly once";
    EXPECT_EQ(backend->listCount(layout.namespaceRootPrefix()), 1u)
        << "the bounded janitor page is a distinct ownership-tree enumeration";
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), 3u)
        << "generation zero has no drain read: one cut builds the hot walk plan, one follows the janitor "
           "page, and `planManifestCursorPage` takes its own";
    const auto fold_decision = std::find_if(phases.begin(), phases.end(), [](const GcPhaseRecord & phase)
    {
        return phase.phase == "defer_decision";
    });
    ASSERT_NE(fold_decision, phases.end());
    EXPECT_EQ(fold_decision->metrics.at("walk_plan_builds"), 1u);
    EXPECT_EQ(fold_decision->metrics.at("walk_plan_rows"), 1u);
    const auto fold_cleanup = std::find_if(phases.begin(), phases.end(), [](const GcPhaseRecord & phase)
    {
        return phase.phase == "namespace_cleanup";
    });
    ASSERT_NE(fold_cleanup, phases.end());
    EXPECT_EQ(fold_cleanup->metrics.at("janitor_pages"), 1u);

    phases.clear();
    backend->resetCounts();
    const uint64_t defer_builds_before
        = ProfileEvents::global_counters[ProfileEvents::CASGCRefWalkPlansBuilt].load();
    ASSERT_TRUE(gc.runRegularRound().deferred);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASGCRefWalkPlansBuilt].load() - defer_builds_before, 1u);
    EXPECT_EQ(backend->listCount(layout.namespaceStreamRootPrefix()), 1u)
        << "a deferred round still builds exactly one complete hot walk plan";
    EXPECT_EQ(backend->listCount(layout.namespaceRootPrefix()), 1u)
        << "the janitor remains one separately paced ownership-tree page";
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), 3u)
        << "one adopted-parent drain cut, one post-hot-LIST cut, and one post-janitor-page cut";
    const auto defer_decision = std::find_if(phases.begin(), phases.end(), [](const GcPhaseRecord & phase)
    {
        return phase.phase == "defer_decision";
    });
    ASSERT_NE(defer_decision, phases.end());
    EXPECT_EQ(defer_decision->metrics.at("walk_plan_builds"), 1u);
    EXPECT_EQ(defer_decision->metrics.at("walk_plan_rows"), 1u);
    EXPECT_EQ(defer_decision->metrics.at("walk_plan_dropped_parent_rows"), 0u);
    EXPECT_EQ(defer_decision->metrics.at("walk_plan_dropped_listed_lives"), 0u);
    EXPECT_EQ(defer_decision->metrics.at("walk_plan_dropped_tails"), 0u);
    const auto defer_cleanup = std::find_if(phases.begin(), phases.end(), [](const GcPhaseRecord & phase)
    {
        return phase.phase == "namespace_cleanup";
    });
    ASSERT_NE(defer_cleanup, phases.end());
    EXPECT_EQ(defer_cleanup->metrics.at("janitor_pages"), 1u);
}

/// A maintenance cursor can be left between pages while the correctness state is already quiescent.
/// The next acquired round may DEFER its fold, but it has no authoritative destructive verdict. It
/// must therefore inspect exactly one janitor page without deleting OR advancing past it; the bounded
/// forced fold then retries the same page under its computed global gate and reclaims the debris.
TEST(CASGCRoundDefer, DeferredRoundRetriesPartialJanitorPageAtForcedFoldWithoutPublishingSuccessor)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/1);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout & layout = store->layout();
    const NamespaceLifeId dead_a
        = NamespaceLifeId::fromCatalogEntry(RootNamespace{"dead/a"}, UInt128{0xDA});
    const NamespaceLifeId dead_b
        = NamespaceLifeId::fromCatalogEntry(RootNamespace{"dead/b"}, UInt128{0xDB});
    const String key_a = layout.refCkptKey(dead_a);
    const String key_b = layout.refCkptKey(dead_b);
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key_a, "dead-a", Retry::once())));
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key_b, "dead-b", Retry::once())));

    /// Establish real opaque backend progress rather than fabricating a cursor value. One key remains
    /// after this page and the durable cursor must be non-empty.
    const NamespaceJanitorResult first_page
        = NamespaceJanitor(requests, layout, 1).runOnePage(false, [] { return true; });
    ASSERT_EQ(first_page.pages, 1u);
    ASSERT_EQ(first_page.deleted, 1u);
    const GcMaintenanceReadResult partial = readGcMaintenanceState(op, layout);
    ASSERT_EQ(partial.status, GcMaintenanceReadStatus::Valid);
    ASSERT_TRUE(partial.state);
    ASSERT_FALSE(partial.state->janitor_cursor.empty());
    ASSERT_EQ(static_cast<uint64_t>(op.head(key_a, Retry::once()).has_value()) + static_cast<uint64_t>(op.head(key_b, Retry::once()).has_value()), 1u);

    /// Give the forced fold a nonempty, fully proved authoritative universe. The R11 floor correctly
    /// refuses to open the destructive gate for an empty 0-of-0 universe even in the test-only policy.
    const RootNamespace live_namespace{"live/frontier@cas@"};
    fixture::admitLive(*backend, layout, live_namespace);
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(
        layout.refCkptKey(fixture::fixtureLife(live_namespace)),
        encodeRefCkpt(RefCkpt{
            .life_epoch = std::optional<uint64_t>{1},
            .checkpoint_snapshot_id = std::nullopt,
            .last_epoch_seal = std::nullopt}),
        Retry::once())));

    backend->resetCounts();
    std::vector<GcPhaseRecord> phases;
    Gc gc(store, kGc);
    gc.setPhaseSink([&](const GcPhaseRecord & phase) { phases.push_back(phase); });
    const uint64_t plans_before
        = ProfileEvents::global_counters[ProfileEvents::CASGCRefWalkPlansBuilt].load();

    const RoundReport report = gc.runRegularRound();

    ASSERT_TRUE(report.acquired_lease);
    ASSERT_TRUE(report.deferred);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASGCRefWalkPlansBuilt].load() - plans_before, 1u)
        << "DEFER still constructs its one immutable hot walk plan, never a second janitor-derived plan";
    EXPECT_EQ(backend->listCount(layout.namespaceStreamRootPrefix()), 1u);
    EXPECT_EQ(backend->listCount(layout.namespaceRootPrefix()), 1u)
        << "the deferred round must inspect exactly one separately paced janitor page";
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), 2u)
        << "generation zero pays one hot walk-plan cut and one post-janitor-page cut";
    const auto cleanup = std::find_if(phases.begin(), phases.end(), [](const GcPhaseRecord & phase)
    {
        return phase.phase == "namespace_cleanup";
    });
    ASSERT_NE(cleanup, phases.end());
    EXPECT_EQ(cleanup->metrics.at("janitor_pages"), 1u);
    EXPECT_GE(cleanup->metrics.at("janitor_keys"), 1u);
    EXPECT_EQ(cleanup->metrics.at("janitor_deleted"), 0u);

    const GcMaintenanceReadResult deferred_progress = readGcMaintenanceState(op, layout);
    ASSERT_EQ(deferred_progress.status, GcMaintenanceReadStatus::Valid);
    ASSERT_TRUE(deferred_progress.state);
    EXPECT_EQ(deferred_progress.state->janitor_cursor, partial.state->janitor_cursor)
        << "a suppressed DEFER page is undecided and must remain selected for the authoritative fold";
    EXPECT_EQ(static_cast<uint64_t>(op.head(key_a, Retry::once()).has_value()) + static_cast<uint64_t>(op.head(key_b, Retry::once()).has_value()), 1u);

    const auto gc_state = op.read(layout.gcStateKey(), Retry::once());
    ASSERT_TRUE(gc_state);
    const GcState state = decodeGcState(gc_state->bytes);
    EXPECT_EQ(state.snap_generation, 0u);
    EXPECT_EQ(state.snap_attempt, 0u);
    EXPECT_FALSE(op.head(layout.foldSealKey(1, 1), Retry::once()).has_value())
        << "maintenance on DEFER must not publish a fold successor";

    backend->resetCounts();
    phases.clear();
    const RoundReport folded = gc.runRegularRound({}, true, UniversePolicy::Authoritative);
    ASSERT_TRUE(folded.acquired_lease);
    ASSERT_FALSE(folded.deferred)
        << "gc_fold_max_defer_rounds=1 forces the round immediately following one DEFER to fold";
    EXPECT_EQ(backend->listCount(layout.namespaceRootPrefix()), 1u)
        << "the authoritative fold must run the janitor exactly once, not once per call site";
    const auto folded_cleanup = std::find_if(phases.begin(), phases.end(), [](const GcPhaseRecord & phase)
    {
        return phase.phase == "namespace_cleanup";
    });
    ASSERT_NE(folded_cleanup, phases.end());
    EXPECT_EQ(folded_cleanup->metrics.at("janitor_pages"), 1u);
    EXPECT_GE(folded_cleanup->metrics.at("janitor_keys"), 1u);
    EXPECT_EQ(folded_cleanup->metrics.at("janitor_deleted"), 1u);
    EXPECT_EQ(static_cast<uint64_t>(op.head(key_a, Retry::once()).has_value()) + static_cast<uint64_t>(op.head(key_b, Retry::once()).has_value()), 0u)
        << "the fold must retry and delete the exact page that DEFER left undecided";
    const GcMaintenanceReadResult completed = readGcMaintenanceState(op, layout);
    ASSERT_EQ(completed.status, GcMaintenanceReadStatus::Valid);
    ASSERT_TRUE(completed.state);
    EXPECT_TRUE(completed.state->janitor_cursor.empty());
}

/// The same idle-defer property under a sharded blob-target GC (gc_shards=2): graduationDue's loop
/// over state.retired_refs and `listRefPrefix`'s discovery must both settle to "nothing due" once
/// quiesced, regardless of how many gc-shards partition the retired bookkeeping.
TEST(CASGCRoundDefer, IdleRoundDefersUnderShardedGc)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .gc_shards = 2});
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 0xAA};
    writeBlobBody(*backend, store->layout(), UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    ASSERT_FALSE(gc.runRegularRound().deferred);   /// round 1: folds the publish

    /// Immutable `_log` objects are never trimmed in place, so there is no fold-then-trim token-rewrite
    /// lag: the pool quiesces after the folding round, and the very next round defers.
    const RoundReport rep = gc.runRegularRound();   /// round 2: quiesced
    EXPECT_TRUE(rep.deferred) << "idle pool under gc_shards=2 must defer once settled";
}

/// The +1 guard (mirror of the 2026-06-27 leak): a blob condemned + published delete_pending, then
/// re-referenced WHILE it is pending, must NOT be over-deleted -- the due graduation forces a fold
/// (never a defer) that sees the +1 and spares the blob.
TEST(CASGCRoundDefer, DueGraduationForcesFoldAndSparesReReferencedBlob)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const UInt128 blob(1);
    const ManifestRef r1{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 0xAA};
    writeBlobBody(*backend, store->layout(), blob);
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r1);
    Gc gc(store, kGc);

    runRegularRoundReclaiming(gc);                 /// folds the +1; blob referenced
    store->renewWatermarkOnce();
    dropRefTransition(*backend, store->layout(), ns, "tbl", r1);   /// the -1 condemns it

    runRegularRoundReclaiming(gc);                 /// the condemning round
    store->renewWatermarkOnce();

    /// Drive rounds until the entry graduates (published delete_pending) -- mirrors
    /// CASGCAckFloor.CondemnThenDeleteNextRoundAfterAcks. It is still PRESENT at that pass, and the
    /// ack floor is by construction already past its condemn_round (that is what graduated it).
    bool saw_pending = false;
    for (int i = 0; i < 6 && !saw_pending; ++i)
    {
        runRegularRoundReclaiming(gc);
        store->renewWatermarkOnce();
        for (const RetiredEntry & e : currentRetiredSet(*backend, store->layout(), /*shard*/0))
            if (e.ref == DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(blob)} && e.delete_pending)
                saw_pending = true;
    }
    ASSERT_TRUE(saw_pending) << "entry never reached delete_pending";
    ASSERT_FALSE(blobAbsent(*backend, store->layout(), blob)) << "pending: still present this pass";

    /// While B sits delete_pending, a NEW manifest re-references it -- a genuine +1 racing the
    /// already-published pending delete.
    const ManifestRef r2{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = 0xBB};
    writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("b", blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl2", std::nullopt, r2);

    /// The next pass would otherwise execute B's pending exact-token delete; graduationDue must force
    /// a FOLD (never a DEFER) so the +1 is folded in and the blob is spared, not deleted.
    const RoundReport rep = runRegularRoundReclaiming(gc);
    EXPECT_FALSE(rep.deferred) << "a due graduation must force a fold, never defer";
    EXPECT_FALSE(blobAbsent(*backend, store->layout(), blob)) << "the re-referenced blob must survive";

    const FsckReport fsck = runFsck(*store, /*detail*/true);
    EXPECT_EQ(fsck.dangling, 0u);
}

/// Companion to the test above: it proves `graduationDue` is the SOLE fold trigger at the assertion
/// round. `DueGraduationForcesFoldAndSparesReReferencedBlob` opens its store at the DEFAULT
/// `gc_fold_threshold` (1), so at its assertion round the +1 re-reference ALSO makes
/// `changed_shards (>= 1) >= fold_threshold (1)` true -- that branch of `shouldDeferRound` would force
/// the very same fold even if `graduationDue` were deleted or hard-wired false. Here `gc_fold_threshold`
/// and `gc_fold_max_defer_rounds` are both set to 1000, so neither the changed-shards branch (one
/// changed shard is nowhere near 1000) nor the liveness-bound branch (this is round 1) can fire --
/// `graduationDue` is the ONLY thing in `shouldDeferRound` that can force this round's fold, making
/// `EXPECT_FALSE(rep.deferred)` below load-bearing for `graduationDue` specifically.
TEST(CASGCRoundDefer, DueGraduationIsSoleFoldTriggerAtHighThreshold)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .gc_fold_threshold = 1000, .gc_fold_max_defer_rounds = 1000});
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    const UInt128 blob(1);

    Gc gc(store, kGc);
    /// Warm-up round on the still-empty pool: `gc/state` does not exist yet, so lease acquisition takes
    /// the create-fresh path and succeeds immediately (`gc_id` becomes the owner in storage). This
    /// matters because the `injectCondemnedSummarySeal` seeding below writes `gc/state` directly, and a fresh `Gc`
    /// object's FIRST-EVER `acquireOrRenewLease` call against a PRE-EXISTING lease it has never observed
    /// refuses to steal it (two-observation safety against stealing from a live incumbent) -- it would
    /// return `acquired_lease=false` and the round would bail out BEFORE the fold-decision code, making
    /// `EXPECT_FALSE(rep.deferred)` below vacuously true regardless of `graduationDue`. Running this
    /// warm-up round FIRST makes `gc_id` the observed incumbent, so the assertion round's lease RENEWAL
    /// (not a steal) succeeds unconditionally and the round actually reaches the decision it's testing.
    gc.runRegularRound();

    writeBlobBody(*backend, layout, blob);

    /// Seed the adopted fold seal's condemned_summary with B already `delete_pending` (pending_total = 1).
    /// `graduationDue`
    /// reads this summary ZERO-I/O from the adopted seal — a `delete_pending` entry forces
    /// it true regardless of the round. At `gc_fold_threshold = 1000` a real condemn -> graduate pipeline of
    /// `runRegularRound` calls is not usable to set this up: every round before graduation would ITSELF
    /// defer (nothing due yet, and changed_shards never nears 1000), so the due-pending summary is injected
    /// directly instead of driven through real rounds.
    injectCondemnedSummarySeal(*backend, layout, /*generation*/1, /*attempt*/1, /*gc_shards*/1,
        {{0, CondemnedSummary{.condemned_total = 1, .pending_total = 1,
                              .oldest_nonpending_condemn_round = std::numeric_limits<uint64_t>::max()}}});

    /// The +1: a fresh manifest re-references B while it sits `delete_pending` -- one changed shard,
    /// far below the threshold of 1000.
    const ManifestRef r{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 0xBB};
    writeManifestRaw(*backend, layout, ns, r, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, r);

    const RoundReport rep = gc.runRegularRound();

    /// DISCRIMINATING (load-bearing): with graduationDue intact, the due delete_pending entry forces
    /// the fold. If graduationDue were broken/hard-wired false, changed_shards (1) < threshold (1000)
    /// and the defer bound (1000) is nowhere near reached, so `shouldDeferRound` would return true and
    /// this round would DEFER instead.
    EXPECT_FALSE(rep.deferred) << "a due graduation must be the SOLE fold trigger at a high fold threshold";
    EXPECT_FALSE(blobAbsent(*backend, layout, blob)) << "the re-referenced blob must survive the forced fold";

    const FsckReport fsck = runFsck(*store, /*detail*/true);
    EXPECT_EQ(fsck.dangling, 0u);
}

/// Bounded deferral: with a large fold_threshold and a small standing delta (one shard changed,
/// forever, since deferring never resolves it), at most gc_fold_max_defer_rounds consecutive rounds
/// defer, then one round forces a fold (the liveness bound).
TEST(CASGCRoundDefer, BoundedDeferralForcesFoldWithinWindow)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .gc_fold_threshold = 100, .gc_fold_max_defer_rounds = 3});
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 0xAA};
    writeBlobBody(*backend, store->layout(), UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    for (int i = 0; i < 3; ++i)
    {
        const RoundReport rep = gc.runRegularRound();
        EXPECT_TRUE(rep.deferred) << "round " << (i + 1) << " is within the defer bound";
    }
    const RoundReport rep4 = gc.runRegularRound();
    EXPECT_FALSE(rep4.deferred) << "the 4th round hits the defer bound and must force-fold";
}
