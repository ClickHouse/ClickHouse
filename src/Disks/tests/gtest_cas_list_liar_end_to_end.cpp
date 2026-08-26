#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include "cas_test_helpers.h"

#include <map>
#include <string>
#include <vector>

/// THE 2026-07-25 RELEASE BLOCKER, AS A PERMANENT REGRESSION.
///
/// The defect the object store actually exhibited (`reports/2026-07-26-list-incompleteness-proof/`):
/// objects that were durable, acked, and readable by exact key were OMITTED from enumeration, while a
/// LATER key under the same prefix was listed. Nothing was lost and nothing was corrupt -- the store
/// simply under-reported what it held.
///
/// Every CAS reader that treated a listing as a CENSUS then drew a false conclusion from it, and the
/// two that mattered drew ruinous ones. The GC fold walked the ids the listing returned, so it skipped
/// the omitted records' owner edges AND sealed a cursor above them -- and nothing ever re-reads below a
/// sealed cursor, so those edges were lost permanently: blobs that were still referenced looked
/// unreferenced forever after. Recovery replayed the listing, so a table came back missing an ACKED
/// transaction while looking perfectly healthy.
///
/// The answer is that a listing is a HINT and arithmetic is the census. Ids are dense `1..T`
/// within `(namespace, writer_epoch)` (INV-1), so the next record's id is COMPUTABLE and every record
/// is read by EXACT KEY. A hidden-but-durable contiguous id is then a NON-EVENT -- the walk finds it
/// anyway -- while a genuinely absent expected id is a durable HOLD, never a silent skip.
///
/// This file is that claim stated end to end, against a store that lies exactly the way the real one
/// did. `setListOmissions` names the omitted keys; `get`/`head`/`putIfAbsent`/`casPut`/`deleteExact`
/// keep serving them honestly. Each test below asserts the lie changed NOTHING -- not the folded
/// edges, not the cursor, not the recovered table, not fsck's verdict -- and the arms that are about
/// reclamation additionally assert that reclamation still happens, so "nothing was deleted" can never
/// pass for "the lie was harmless".
///
/// The unit-level statements about the walk itself live in `gtest_cas_gc_arithmetic_intake.cpp`, and
/// the destructive gate's own inventory lives in `gtest_cas_gc_frontier_gate.cpp`. This file is the
/// INTEGRATION of the two: real rounds, real recovery, real fsck, one lying store.
///
/// The suite name is prefixed `Cas` so the `Cas*` unit-test gate filter covers it.

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

const UInt128 kGc = hexToU128("00000000000000000000000000000001");

/// The lying store: LIST omits the named keys, everything else serves them honestly. Composed over
/// `CountingBackend` so the arms whose subject is reclamation can assert on DELETES rather than only on
/// what survived.
using LiarBackend = HintHoleBackendOn<CountingBackend>;

String blobKeyOf(const Layout & layout, const DB::UInt128 & hash)
{
    return layout.blobKey(legacyMetaTestRef(hash));
}

/// The sealed fold cursor for `ns` as a full `RefTxnId`. Every fixture here writes ids inside writer
/// epoch 1, which is the assumption `foldCursorOf` (returning the sequence alone) already makes.
RefTxnId sealedCursorOf(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    return RefTxnId{1, foldCursorOf(backend, layout, ns, /*shard*/ 0)};
}

/// Drop the committed ref `ref_name` (currently naming `old_ref`) as ONE transaction at EXACTLY `id`.
/// The `dropRefTransition` helper allocates its id by LISTING, which a fixture that hides keys must
/// never do -- it would allocate over a hidden record. Every id in this file is therefore chosen.
void dropAt(Backend & backend, const Layout & layout, const RootNamespace & ns, const RefTxnId & id,
            const String & ref_name, const ManifestRef & old_ref)
{
    writeTxnAt(backend, layout, ns, id,
               {ownerTransitionOp(RefOwnerBinding{RefOwnerKind::Committed, ref_name, old_ref}, std::nullopt)});
}

/// The manifest `publishAt` mints for a given (id, build_sequence) -- needed to drop that ref later.
ManifestRef publishedManifest(const RefTxnId & id, uint64_t build_sequence)
{
    return ManifestRef{.writer_epoch = id.writer_epoch, .build_sequence = build_sequence, .manifest_ordinal = 1};
}

/// One round plus everything a verdict in this file is allowed to rest on: the report (which carries
/// the anomaly list), the two intake phase rows (which carry the hold count and the one remaining
/// whole-round ref abort), and the gate's own verdict off `fold_reduce` (read, not recomputed, so a test
/// cannot agree with a wrong formula just as readily as with the right one).
struct RoundEvidence
{
    RoundReport report;
    std::map<String, UInt64> intake;   /// `fold_ref_intake`
    std::map<String, UInt64> group;    /// `fold_ref_group`
    bool saw_fold = false;
    bool frontier_complete = false;
    bool suppress_destructive = false;
};

RoundEvidence runRoundCapturing(Gc & gc, UniversePolicy policy)
{
    RoundEvidence evidence;
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            evidence.intake = rec.metrics;
        else if (rec.phase == "fold_ref_group")
            evidence.group = rec.metrics;
        else if (rec.phase == "fold_reduce")
        {
            evidence.saw_fold = true;
            if (const auto it = rec.metrics.find("frontier_complete"); it != rec.metrics.end())
                evidence.frontier_complete = it->second != 0;
            if (const auto it = rec.metrics.find("suppress_destructive"); it != rec.metrics.end())
                evidence.suppress_destructive = it->second != 0;
        }
    });
    evidence.report = gc.runRegularRound({}, /*allow_steal*/ true, policy);
    gc.setPhaseSink({});
    return evidence;
}

/// "ZERO ANOMALIES", spelled out once so every test means the same thing by it: the round recorded no
/// anomaly, sealed no hold, and did not abort ref folding. A lie the walk absorbs must be invisible in
/// all three -- a hold in particular would be a WRONG (if safe) answer, since it would suppress the
/// round's destructive half over records that were durable all along.
void expectNoAnomalies(const RoundEvidence & evidence, const char * where)
{
    EXPECT_TRUE(evidence.report.anomalies.empty())
        << where << ": the round recorded " << evidence.report.anomalies.size()
        << " anomaly/anomalies; a hidden-but-durable contiguous id is a NON-EVENT";
    ASSERT_FALSE(evidence.intake.empty()) << where << ": no `fold_ref_intake` row was emitted";
    EXPECT_EQ(evidence.intake.at("tables_held"), 0u)
        << where << ": a namespace was HELD -- the walk mistook an omitted-but-durable record for a gap";
    EXPECT_EQ(evidence.intake.at("ref_folding_aborted"), 0u) << where;
    ASSERT_FALSE(evidence.group.empty()) << where << ": no `fold_ref_group` row was emitted";
    EXPECT_EQ(evidence.group.at("ref_folding_aborted"), 0u) << where;
}

/// The pool's view of a table, rendered so a failing comparison prints something a human can read.
std::map<String, String> refsOf(const PoolPtr & store, const RootNamespace & ns)
{
    std::map<String, String> out;
    for (const auto & [ref_name, resolved] : store->listRefs(ns))
        out[ref_name] = std::to_string(resolved.manifest_id.ref.writer_epoch) + "/"
            + std::to_string(resolved.manifest_id.ref.build_sequence) + "/"
            + std::to_string(resolved.manifest_id.ref.manifest_ordinal);
    return out;
}

/// THE STREAM UNDER TEST, written identically into any backend: five ordinary publishes at
/// `{1,1}..{1,5}`, each pinning its own blob, plus the `_ckpt` a recovering reader starts from. Shared
/// so the oracle arms can seed a lying store and an honest one from the SAME code and compare outcomes
/// rather than compare against a hand-written expectation.
void seedFiveRecordStream(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    seedPoolMetaForRestart(backend, layout.poolPrefix());
    for (uint64_t i = 1; i <= 5; ++i)
        publishAt(backend, layout, ns, RefTxnId{1, i}, "ref_" + std::to_string(i), i,
                  DB::UInt128(i), /*birth=*/i == 1);
    writeRecoverableCkptForRawFixture(backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 5},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });
}

/// The exact defect shape: ids 3 and 4 invisible while the LATER id 5 is visible.
std::vector<String> hiddenMiddleOf(const Layout & layout, const RootNamespace & ns)
{
    return {layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 3}),
            layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 4})};
}

PoolConfig recoveryPoolConfig()
{
    PoolConfig config;
    config.pool_prefix = "p";
    config.server_root_id = "test";
    config.server_id = DB::UInt128(1);
    /// No background publication: a threshold-triggered snapshot would move the base under the
    /// comparison these tests make about what recovery reconstructed.
    config.snapshot_log_count_threshold = 1ULL << 40;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    return config;
}

PoolPtr openRecoveryPool(const std::shared_ptr<LiarBackend> & backend)
{
    seedPoolMetaForRestart(*backend, "p");
    return Pool::open(backend, recoveryPoolConfig());
}

}

/// ===================== THE BLOCKER, FULL PIPELINE =====================
///
/// Five durable records; the store lists 1, 2 and 5 and pretends 3 and 4 do not exist. Arithmetic
/// intake never asks the listing what to read next, so all five fold, every blob keeps its owner edge,
/// and the cursor lands on the true tail.
///
/// Under listing-driven intake this fails on the BLOBS, not on the cursor: the cursor still reaches
/// `{1,5}` (the last listed id) while records 3 and 4 were never folded -- and since nothing re-reads
/// below a sealed cursor, their edges are gone for good. That is the production damage, exactly.
TEST(CASListLiarEndToEnd, TheHiddenMiddleOfTheStreamFoldsThroughUnnoticed)
{
    auto backend = std::make_shared<LiarBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/blocker@cas@"};

    seedFiveRecordStream(*backend, layout, ns);
    backend->setListOmissions(hiddenMiddleOf(layout, ns));

    Gc gc(store, kGc);
    const RoundEvidence evidence = runRoundCapturing(gc, UniversePolicy::kDefault);
    ASSERT_TRUE(evidence.report.acquired_lease);
    ASSERT_GT(backend->holesServed(), 0u)
        << "the omission was never actually served -- the test would pass vacuously";

    EXPECT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 5}))
        << "the walk must reach the true tail of the stream";
    for (uint64_t i = 1; i <= 5; ++i)
        EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(i)), 1)
            << "blob " << i << " lost its owner edge: its record was skipped because the store hid it";

    expectNoAnomalies(evidence, "hidden middle");
    EXPECT_EQ(evidence.intake.at("logs_applied"), 5u) << "all five records are APPLIED, not three";
    EXPECT_EQ(evidence.intake.at("logs_accounted"), evidence.intake.at("logs_applied"))
        << "probe B1: the arithmetic cut the cursors claim must equal what the walk applied";
}

/// RECOVERY, UNDER THE SAME LIE, AGAINST AN HONEST ORACLE. The comparison is against a second pool
/// seeded by the SAME code over a store that does not lie -- not against a hand-written expectation,
/// which could encode the same mistake the code makes.
TEST(CASListLiarEndToEnd, RecoveryUnderTheSameLieReconstructsExactlyTheTruth)
{
    const Layout layout("p");
    const RootNamespace ns{"00/recover@cas@"};

    auto honest_backend = std::make_shared<LiarBackend>();
    seedFiveRecordStream(*honest_backend, layout, ns);
    auto honest = openRecoveryPool(honest_backend);
    const std::map<String, String> truth = refsOf(honest, ns);

    auto lying_backend = std::make_shared<LiarBackend>();
    seedFiveRecordStream(*lying_backend, layout, ns);
    lying_backend->setListOmissions(hiddenMiddleOf(layout, ns));
    auto lying = openRecoveryPool(lying_backend);
    const std::map<String, String> recovered = refsOf(lying, ns);

    ASSERT_GT(lying_backend->holesServed(), 0u)
        << "the omission was never actually served -- the test would pass vacuously";
    EXPECT_EQ(truth.size(), 5u) << "the oracle itself must see all five published refs";
    EXPECT_EQ(recovered, truth)
        << "a table recovered under an omitting listing must be byte-identical to the truth; the "
           "blocker's recovery came back missing an ACKED transaction and looked healthy";
}

/// ===================== THE DATA-LOSS ARM =====================
///
/// A blob with two owners. The `+1` that publishes the SECOND owner rides the hidden id; the `-1` that
/// releases the first is visible and lands above it. The arithmetic fold reads both, so the blob's
/// in-degree is 1 and it is never condemned.
///
/// Listing-driven intake folds the visible `-1`, never folds the hidden `+1`, and seals the cursor
/// above it: the blob's in-degree reads zero while a live ref still names it, and the round deletes
/// data that is referenced. That is the data loss, and it is why this arm asserts the blob was never
/// even offered for deletion rather than merely that it is still present.
TEST(CASListLiarEndToEnd, AHiddenPlusOneKeepsItsBlobWhenAVisibleMinusOneLandsLater)
{
    auto backend = std::make_shared<LiarBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/dataloss@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch
    const DB::UInt128 shared(0x5ade);

    /// `ref_a` and `ref_b` both pin `shared`; `ref_b`'s publish is the record the store will hide.
    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_a", 1, shared, /*birth=*/true);
    publishAt(*backend, layout, ns, RefTxnId{1, 2}, "ref_b", 2, shared);
    dropAt(*backend, layout, ns, RefTxnId{1, 3}, "ref_a", publishedManifest(RefTxnId{1, 1}, 1));
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 3},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    backend->setListOmissions({layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 2})});

    Gc gc(store, kGc);
    const RoundEvidence first = runRoundCapturing(gc, UniversePolicy::Authoritative);
    ASSERT_TRUE(first.report.acquired_lease);
    ASSERT_GT(backend->holesServed(), 0u)
        << "the omission was never actually served -- the test would pass vacuously";
    expectNoAnomalies(first, "hidden +1");

    EXPECT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 3}));
    EXPECT_EQ(inDegreeOf(*backend, layout, shared), 1)
        << "the hidden publish's `+1` must be folded: `ref_b` still owns this blob";

    /// Rounds that are ALLOWED to reclaim, and would, if the in-degree were wrong.
    for (int i = 0; i < 5; ++i)
    {
        runRegularRoundReclaiming(gc);
        store->renewWatermarkOnce();
    }
    EXPECT_TRUE(backend->head(blobKeyOf(layout, shared)).exists)
        << "a blob a live ref still names was DELETED -- the hidden `+1` was never folded";
    EXPECT_EQ(backend->deleteCount(blobKeyOf(layout, shared)), 0u)
        << "not merely still present: the delete was never even attempted";
}

/// ===================== THE LEAK ARM =====================
///
/// The mirror image, and the reason the arm above is not the whole story. Here the hidden record
/// carries the `-1` that releases the blob's last owner, and a visible record lands above it. The
/// arithmetic fold reads the `-1`, so the in-degree reaches zero and the blob is actually reclaimed.
///
/// Listing-driven intake skips the `-1` and seals the cursor above it, so the blob keeps a phantom
/// owner forever: not data loss, but an object no incremental round can ever reclaim. Asserting the
/// blob DOES go away is also what stops the data-loss arm above from being satisfiable by a fold that
/// simply never deletes anything.
TEST(CASListLiarEndToEnd, AHiddenMinusOneIsStillFoldedSoTheBlobIsActuallyReclaimed)
{
    auto backend = std::make_shared<LiarBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/leak@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch
    const DB::UInt128 released(0xdea1);
    const DB::UInt128 unrelated(0xb00c);

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_a", 1, released, /*birth=*/true);
    dropAt(*backend, layout, ns, RefTxnId{1, 2}, "ref_a", publishedManifest(RefTxnId{1, 1}, 1));
    /// A VISIBLE record above the hidden one. Without it the hidden id would be the stream's tail, and
    /// a listing-driven walk would merely stop below it -- deferring the `-1` rather than sealing past
    /// it, which is not the permanent damage this arm is about.
    publishAt(*backend, layout, ns, RefTxnId{1, 3}, "ref_c", 3, unrelated);
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 3},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    backend->setListOmissions({layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 2})});

    Gc gc(store, kGc);
    const RoundEvidence condemning = runRoundCapturing(gc, UniversePolicy::Authoritative);
    ASSERT_TRUE(condemning.report.acquired_lease);
    ASSERT_GT(backend->holesServed(), 0u)
        << "the omission was never actually served -- the test would pass vacuously";
    expectNoAnomalies(condemning, "hidden -1");

    EXPECT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 3}));
    EXPECT_EQ(inDegreeOf(*backend, layout, released), 0)
        << "the hidden `-1` must be folded: nothing owns this blob any more";
    EXPECT_TRUE(backend->head(blobKeyOf(layout, released)).exists)
        << "round pacing: the round that CONDEMNS never also deletes";

    store->renewWatermarkOnce();
    EXPECT_TRUE(runRoundsUntilAbsent(store, gc, *backend, layout, released))
        << "the blob was never reclaimed -- the hidden `-1` left it pinned by a phantom owner";
    EXPECT_TRUE(backend->head(blobKeyOf(layout, unrelated)).exists)
        << "and the still-owned blob is untouched";
}

/// ===================== THE CROSS-NAMESPACE SHOT =====================
///
/// The shape that is not about walking a single namespace: it is about a namespace the store hides in
/// its ENTIRETY, never just one record inside it.
///
/// Two namespaces share a blob. `visible` publishes it and then drops it, so the round observes `+1`
/// then `-1` and reads the blob's in-degree as zero. `hidden` also owns it -- durably, acked, readable
/// by exact key -- but the store omits its ENTIRE ref stream, so no listing mentions it. `hidden`'s own
/// publish still leaves it a real `_ckpt`, and a `_ckpt` is read by exact key, so the arithmetic walk's
/// first probe finds and folds `hidden`'s `+1` regardless of what the listing omits: the blob survives
/// on its own complete, folded frontier.
///
/// This is NOT a duplicate of `gtest_cas_gc_frontier_gate.cpp`'s twin: that file's backend hides only a
/// hint prefix, while this one is the end-to-end LIST-liar backend from this file's own header -- the
/// distinct thing this test proves is that arithmetic intake reads a record the backend actively hides
/// from every enumeration, in the full pipeline (real pool, real recovery-shaped checkpoints), not that
/// the gate's universe/count terms hold. `gtest_cas_gc_frontier_gate.cpp` owns those terms: its
/// (3a)/(3b)/(3c) suppressor arms are what pin `universe_authoritative`, the empty-universe floor, and
/// the probe budget -- terms this fixture cannot exercise, because grounding both namespaces here makes
/// `frontier_namespaces > 0` and `universe_authoritative` true unconditionally.

namespace
{
/// Build the shared-blob scenario and return the manifest `visible` will drop. Both namespaces are
/// grounded with a real `_ckpt` reflecting what was actually published (`writeRecoverableCkptForRawFixture`,
/// the idiom every other test in this file uses): otherwise neither namespace has a usable checkpoint at
/// all, the round suppresses on that anomaly alone, and the scenario proves nothing about `hidden`
/// specifically.
ManifestRef buildKillShot(const std::shared_ptr<LiarBackend> & backend, const Layout & layout,
                          const RootNamespace & hidden, const RootNamespace & visible,
                          const DB::UInt128 & blob)
{
    publishAt(*backend, layout, hidden, RefTxnId{1, 1}, "kept_ref", 1, blob, /*birth=*/true);
    writeRecoverableCkptForRawFixture(*backend, layout, hidden, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    publishAt(*backend, layout, visible, RefTxnId{1, 1}, "dropped_ref", 2, blob, /*birth=*/true);
    const ManifestRef dropped = publishedManifest(RefTxnId{1, 1}, 2);
    dropAt(*backend, layout, visible, RefTxnId{1, 2}, "dropped_ref", dropped);
    writeRecoverableCkptForRawFixture(*backend, layout, visible, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    /// The whole of `hidden`'s ref stream goes invisible -- the namespace itself is what the listing
    /// stops mentioning, not a record inside it. Its `_ckpt` stays readable by exact key, which is what
    /// lets the arithmetic walk find and fold its birth despite the omission.
    backend->setListOmissions({layout.refLogKey(fixture::fixtureLife(hidden), RefTxnId{1, 1}),
                               layout.refCkptKey(fixture::fixtureLife(hidden))});
    return dropped;
}
}

/// Rounds on the PRODUCTION path -- no policy argument anywhere -- because that is the posture the
/// claim is about: the arithmetic walk's exact-key probe reaches `hidden`'s birth despite the store
/// hiding its whole stream from every listing, so the frontier it proves is complete and the blob
/// survives on its own folded in-degree, not on a caller declining to supply a universe.
TEST(CASListLiarEndToEnd, AHiddenNamespacesBirthIsFoundByExactKeyAndSavesTheBlobOnACompleteFrontier)
{
    auto backend = std::make_shared<LiarBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace hidden{"00/hidden@cas@"};
    const RootNamespace visible{"00/visible@cas@"};
    const DB::UInt128 blob(0x5ade);

    buildKillShot(backend, layout, hidden, visible, blob);

    Gc gc(store, kGc);
    backend->resetCounts();
    RoundEvidence evidence;
    for (int i = 0; i < 5; ++i)
    {
        const RoundEvidence round = runRoundCapturing(gc, UniversePolicy::kDefault);
        if (round.saw_fold)
            evidence = round;
        store->renewWatermarkOnce();
    }

    ASSERT_TRUE(evidence.saw_fold) << "no round folded, so none published a gate verdict";
    ASSERT_GT(backend->holesServed(), 0u)
        << "the omission was never actually served -- the test would pass vacuously";
    EXPECT_TRUE(backend->head(blobKeyOf(layout, blob)).exists)
        << "the blob a hidden namespace still owns must survive";
    EXPECT_EQ(backend->deleteCount(blobKeyOf(layout, blob)), 0u)
        << "not merely still present: the blob must never even be offered for deletion";
    EXPECT_TRUE(evidence.frontier_complete)
        << "`hidden`'s own `_ckpt` is read by exact key, so its frontier is provable despite the "
           "listing omission -- if this is false the blob above survived on suppression instead of on "
           "its own in-degree, which proves nothing about the edge";
    EXPECT_FALSE(evidence.suppress_destructive);
}

/// The arm above asserts "nothing was deleted", which on its own does not distinguish the gate correctly
/// refusing from the round simply never deleting anything. Positive control:
/// `hidden` drops its OWN reference too (still by exact key, still hidden from every listing), so its
/// frontier is REALLY proven by the arithmetic-intake exact-key probe -- never declared so by fiat --
/// and the blob is REALLY unreferenced by both namespaces. The round drains it.
TEST(CASListLiarEndToEnd, TheSameBlobDrainsOnceHiddenGenuinelyProvesItsOwnFrontier)
{
    auto backend = std::make_shared<LiarBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace hidden{"00/hidden@cas@"};
    const RootNamespace visible{"00/visible@cas@"};
    const DB::UInt128 blob(0x5ade);

    publishAt(*backend, layout, hidden, RefTxnId{1, 1}, "kept_ref", 1, blob, /*birth=*/true);
    const ManifestRef kept = publishedManifest(RefTxnId{1, 1}, 1);
    writeRecoverableCkptForRawFixture(*backend, layout, hidden, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    Gc gc(store, kGc);

    /// `hidden`'s birth is folded (and its cursor SEALED) while everything is still listed. Its
    /// checkpoint proves that exact initial frontier; the real fold then makes the arithmetic
    /// (cursor-relative) genesis available for what follows.
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    store->renewWatermarkOnce();

    /// NOW `hidden` drops its own reference, and ONLY THEN does its whole prefix vanish from LIST. With
    /// a sealed cursor already in hand, the walk's genesis for `hidden` is arithmetic (`cursor + 1`), so
    /// this drop is found and folded by exact key alone -- the arithmetic-intake mechanism this whole
    /// file is about, exercised honestly rather than declared past by fiat.
    dropAt(*backend, layout, hidden, RefTxnId{1, 2}, "kept_ref", kept);
    advanceRecoverableCkptForRawFixture(*backend, layout, hidden, RefTxnId{1, 2});
    backend->hidePrefix(layout.namespaceStreamPrefix(fixture::fixtureLife(hidden)));

    publishAt(*backend, layout, visible, RefTxnId{1, 1}, "dropped_ref", 2, blob, /*birth=*/true);
    const ManifestRef dropped = publishedManifest(RefTxnId{1, 1}, 2);
    dropAt(*backend, layout, visible, RefTxnId{1, 2}, "dropped_ref", dropped);
    writeRecoverableCkptForRawFixture(*backend, layout, visible, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    for (int i = 0; i < 5; ++i)
    {
        runRegularRoundReclaiming(gc);
        store->renewWatermarkOnce();
    }

    ASSERT_GT(backend->holesServed(), 0u)
        << "the omission was never actually served -- the test would pass vacuously";
    EXPECT_FALSE(backend->head(blobKeyOf(layout, blob)).exists)
        << "both namespaces genuinely proved their frontier and the blob is genuinely unreferenced -- "
           "the round must still be able to reclaim it";
}

/// ===================== FSCK =====================
///
/// fsck runs two checkpoint-grounded passes over a namespace's ref stream.
///
///   * `checkRefStream` walks arithmetically by exact key. An omitted-but-durable record is a
///     non-event to it, exactly as it is to the GC fold. That is the pass the arms below pin.
///   * the reachability pass uses the same catalog row and exact `_ckpt` to recover the ref table
///     without stream enumeration.
///
/// Both arms are written against an HONEST TWIN seeded by the same code, not against hand-written
/// expectations: the claim is "identical to the truth", and a pass that quietly examined fewer records
/// would satisfy a hand-written "clean" just as well.

TEST(CASListLiarEndToEnd, FsckArithmeticStreamAuditIsUnmovedByAHiddenMiddle)
{
    const Layout layout("p");
    const RootNamespace ns{"00/fsck@cas@"};

    auto honest_backend = std::make_shared<LiarBackend>();
    seedFiveRecordStream(*honest_backend, layout, ns);
    auto honest = openRecoveryPool(honest_backend);
    const FsckReport truth = runFsck(*honest, /*detail=*/true);

    auto lying_backend = std::make_shared<LiarBackend>();
    seedFiveRecordStream(*lying_backend, layout, ns);
    lying_backend->setListOmissions(hiddenMiddleOf(layout, ns));
    auto lying = openRecoveryPool(lying_backend);
    const FsckReport under_lie = runFsck(*lying, /*detail=*/true);

    ASSERT_GT(lying_backend->holesServed(), 0u)
        << "the omission was never actually served -- the test would pass vacuously";

    EXPECT_TRUE(truth.clean()) << "the oracle itself must be clean, or the comparison means nothing";
    EXPECT_GT(truth.ref_records_walked, 0u);
    EXPECT_GT(truth.reachable, 0u)
        << "the honest oracle must recover at least one live object, or reachability equality is vacuous";

    /// The arithmetic pass: a hidden record is a non-event, and no finding is manufactured out of it.
    EXPECT_TRUE(under_lie.clean())
        << "fsck must not manufacture a finding out of an omitted-but-durable record";
    EXPECT_EQ(under_lie.chain_broken, 0u)
        << "a record the listing hid is NOT a broken chain: the walk reads it by exact key";
    EXPECT_EQ(under_lie.dangling, 0u);
    EXPECT_EQ(under_lie.ref_records_walked, truth.ref_records_walked)
        << "the arithmetic walk must read the SAME number of records under the lie";

    EXPECT_EQ(under_lie.unchecked, 0u)
        << "an omitted durable record must not turn a healthy checkpoint-bounded namespace unchecked";
    EXPECT_EQ(under_lie.reachable, truth.reachable)
        << "the reachability recovery must observe the same exact committed frontier under the lie";
}

/// A hidden tail record is the silent variant of the historical residual: a LIST-driven replay could
/// return a plausible but short table. Checkpoint-bounded recovery must produce the honest table even
/// though the list omission is served.
TEST(CASListLiarEndToEnd, FsckReachabilityRecoveryMatchesTruthUnderAHiddenTailTransaction)
{
    const Layout layout("p");
    const RootNamespace ns{"00/fsck_tail@cas@"};
    /// Stage B (Task 4-C): no pin needed -- `seedFiveRecordStream` below calls `publishAt` (draining
    /// into `writeRefLogTxnRaw`), which admits `ns` into each of the two independent backends' own
    /// catalogs itself.

    auto honest_backend = std::make_shared<LiarBackend>();
    seedFiveRecordStream(*honest_backend, layout, ns);
    auto honest = openRecoveryPool(honest_backend);
    const FsckReport truth = runFsck(*honest, /*detail=*/true);

    auto lying_backend = std::make_shared<LiarBackend>();
    seedFiveRecordStream(*lying_backend, layout, ns);
    lying_backend->setListOmissions({layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 5})});
    auto lying = openRecoveryPool(lying_backend);
    const FsckReport under_lie = runFsck(*lying, /*detail=*/true);

    ASSERT_GT(lying_backend->holesServed(), 0u)
        << "the omission was never actually served -- the test would pass vacuously";
    EXPECT_GT(truth.reachable, 0u)
        << "the honest oracle must recover at least one live object, or reachability equality is vacuous";

    /// The arithmetic pass is unmoved here too: it probes `{1,5}` by exact key and finds it.
    EXPECT_EQ(under_lie.chain_broken, 0u);
    EXPECT_EQ(under_lie.ref_records_walked, truth.ref_records_walked)
        << "the arithmetic walk reads the hidden tail by exact key, so it counts the same records";

    EXPECT_EQ(under_lie.unchecked, 0u)
        << "a LIST omission must not make a checkpoint-bounded namespace unchecked";
    EXPECT_EQ(under_lie.reachable, truth.reachable)
        << "the exact committed frontier must include the hidden tail transaction";
}
