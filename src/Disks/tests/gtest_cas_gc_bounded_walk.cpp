#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include "cas_test_helpers.h"

/// THE BOUNDED FOLD WALK.
///
/// Arithmetic ref intake reads the next record by exact key -- `cursor + 1` -- and stops when that read
/// comes back absent. That is exact and immune to a lying listing, and it is also, on its own, a walk
/// with no last record: a namespace whose writer keeps appending never produces the absent read, so a
/// round's duration stopped being `backlog / walker_rate` and became `backlog / (walker - writer)`. It
/// diverges the moment a writer keeps up. Measured on a hot pool: ZERO completed GC rounds in 42
/// minutes, and with them nothing that paces on rounds -- fold seal, cursors, the sampled store-quality
/// detector, ref-object cleanup -- ever ran again.
///
/// The bound is `_ckpt.committed_through`, snapshotted once per namespace before the walk and never
/// re-read within the round, so the work is finite and fixed before the round began however fast the
/// writer appends. It is the AUTHORITY ceiling too -- a record above it is durable but is not logical
/// history yet -- so ONE comparison both terminates the round and refuses to fold uncommitted work.
///
/// IT DOES NOT BOUND WHAT THE ROUND READS, and that is not an oversight. The read at `cursor + 1`
/// produces the frontier proof, an unproven namespace suppresses all
/// destructive work, and suppression stops the ref-object cleanup that would have drained the listing --
/// so a namespace that stops being read can never become provable again by any route. "Skip the quiet
/// namespace entirely" therefore is not a cheaper version of this design, it is a GC that permanently
/// reclaims nothing; the saving is one `GET` and that `GET` is the proof.
///
/// So the properties these tests pin are:
///   * a round folds through its round-start committed frontier and no further, whatever lands meanwhile;
///   * a namespace whose tail did not move folds NOTHING and a matching CTE proves its carried cursor;
///   * a round where no tail moved is skipped outright by the existing defer machinery;
///   * a raw record ABOVE the committed frontier neither extends the fold nor suppresses destruction;
///   * a manifest edge fold costs one GET and never a HEAD;
///   * a namespace that folded nothing still keeps its sealed coverage row.

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

const UInt128 kGc = hexToU128("00000000000000000000000000000001");

/// Composed over `CountingBackend` because these tests assert REQUEST COUNTS: "folds nothing and reads
/// once" is the claim, and only a counting backend can check it.
using CountingHintHoleBackend = DB::Cas::tests::HintHoleBackendOn<DB::Cas::tests::CountingBackend>;

/// The `RefCoverage` the newest fold seal recorded for `ns`'s opaque catalog life. Scans downward from
/// the adopted generation for the most recent fold seal, mirroring `foldCursorOf`'s reasoning (a
/// completed round's `gc/state` points at the recheck generation, which writes a completion seal rather
/// than a fold seal).
std::optional<RefCoverage> coverageOf(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    const uint64_t gen = currentGenerationOf(backend, layout);
    const uint64_t attempt = currentAttemptOf(backend, layout);
    const UInt128 life_id = catalogLifeIdForTest(backend, layout, ns);
    for (uint64_t g = gen; ; --g)
    {
        if (const auto got = backend.get(layout.foldSealKey(g, attempt)))
        {
            const CasFoldSeal seal = decodeFoldSeal(got->bytes);
            const auto it = seal.ref_lives.find(life_id);
            if (it == seal.ref_lives.end())
                return std::nullopt;
            return it->second.coverage;
        }
        if (g == 0)
            return std::nullopt;
    }
}

RefTxnId cursorOf(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    const auto cov = coverageOf(backend, layout, ns);
    return cov ? cov->last_folded_ref_id : RefTxnId{};
}

/// A phase metric, or 0 when the row does not carry it. Reading it this way rather than through
/// `std::map::at` is deliberate: against the unbounded walk these columns do not exist yet, and a
/// missing column should fail the assertion that names it, not abort the test with an exception.
UInt64 metric(const std::map<String, UInt64> & row, const String & name)
{
    const auto it = row.find(name);
    return it == row.end() ? 0 : it->second;
}

/// Every key the backend was asked to delete, so a failing zero-delete assertion names the site that
/// leaked instead of only reporting a count.
String deletedKeysMessage(const CountingBackend & backend)
{
    String out;
    for (const String & key : backend.deletedKeys())
        out += "\n    " + key;
    return out.empty() ? String{" (none)"} : out;
}

/// Every `_log/` GET this round issued against `ns` over ids `first..last` -- the "read once, fold
/// nothing" claim, made against the store rather than against a counter the fold keeps about itself.
uint64_t refLogGetsFor(const CountingBackend & backend, const Layout & layout, const RootNamespace & ns,
                       uint64_t first, uint64_t last, uint64_t epoch = 1)
{
    uint64_t total = 0;
    for (uint64_t i = first; i <= last; ++i)
        total += backend.getCount(layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{epoch, i}));
    return total;
}

/// One round driven directly on `Gc`, capturing the `fold_ref_intake` phase row. Driving `Gc` rather
/// than `CasGcScheduler` is what lets a caller choose the universe policy, which the suppression test
/// needs. An EMPTY row means the round deferred and folded nothing at all.
std::map<String, UInt64> runRoundCapturingIntake(Gc & gc, UniversePolicy policy = UniversePolicy::kDefault)
{
    std::map<String, UInt64> intake;
    gc.setPhaseSink([&](const GcPhaseRecord & rec)
    {
        if (rec.phase == "fold_ref_intake")
            intake = rec.metrics;
    });
    const RoundReport report = gc.runRegularRound({}, /*allow_steal*/true, policy);
    gc.setPhaseSink({});
    EXPECT_TRUE(report.acquired_lease) << "the round must have run at all";
    return intake;
}

/// A store whose writer keeps pace with the walker EXACTLY: every time the fold reads the newest record
/// by exact key, one more record lands above it.
///
/// This is a mid-round appender expressed as a synchronous hook rather than as a thread, and the
/// determinism is the point. The property under test is "the round stops at the tail it froze, however
/// much arrives afterwards", and a thread can only make appends arrive at times the scheduler chooses --
/// including, on an unlucky run, entirely after the walk has gone past. The hook reproduces the WORST
/// case (writer rate == walker rate, the rate at which the unbounded walk provably never terminates) on
/// every run, and `max_appends` bounds it so that the UNPATCHED walk still finishes and can be measured
/// rather than hanging the suite.
class ChasingWriterBackend : public CountingBackend
{
public:
    using CountingBackend::get;

    /// Start appending above `published_through` (writer epoch 1) whenever the tail is read, up to
    /// `max_appends` further records.
    void arm(const Layout * layout_, const RootNamespace & ns_, uint64_t published_through, uint64_t max_appends)
    {
        layout = layout_;
        ns = ns_;
        published = published_through;
        limit = published_through + max_appends;
    }

    /// Stop appending; the tail stands still from here on.
    void disarm() { layout = nullptr; }

    uint64_t publishedThrough() const { return published; }

    std::optional<DB::Cas::GetResult> get(const String & key, DB::Cas::Range range) override
    {
        auto result = CountingBackend::get(key, range);
        if (!layout || appending || published >= limit)
            return result;
        if (key != layout->refLogKey(fixture::fixtureLife(ns), RefTxnId{1, published}))
            return result;

        /// The walk just consumed the tail; the writer answers with the next record. Guarded against
        /// re-entry because publishing issues backend calls of its own.
        appending = true;
        const uint64_t next = published + 1;
        publishAt(*this, *layout, ns, RefTxnId{1, next}, "ref_" + std::to_string(next), next, DB::UInt128(next));
        published = next;
        appending = false;
        return result;
    }

private:
    const Layout * layout = nullptr;
    RootNamespace ns{};
    uint64_t published = 0;
    uint64_t limit = 0;
    bool appending = false;
};

/// Publish ids `first .. last` of `ns` in writer epoch 1, each pinning its own blob.
void publishRange(Backend & backend, const Layout & layout, const RootNamespace & ns, uint64_t first, uint64_t last)
{
    for (uint64_t i = first; i <= last; ++i)
        publishAt(backend, layout, ns, RefTxnId{1, i}, "ref_" + std::to_string(i), i, DB::UInt128(i),
                  /*birth=*/i == 1);
}

}

/// ===================== (a) THE ROUND FOLDS THROUGH THE TAIL IT FROZE =====================
///
/// `planted` records are durable when the round starts. While it walks, the writer keeps pace exactly --
/// every record the fold reads is answered with another one above it. The round must fold through the
/// tail it saw at round start and stop, leaving the stragglers to the round that lists them.
///
/// Against the unbounded walk this fails on the cursor: it chases the appends and seals a cursor far
/// above the round-start tail. On a real pool nothing bounds that chase at all; the appender here stops
/// after `appended_mid_round` so the unpatched behaviour is measurable rather than a hang.
TEST(CASGCBoundedWalk, ARoundFoldsThroughItsRoundStartTailAndLeavesTheStragglers)
{
    const uint64_t planted = 6;
    const uint64_t appended_mid_round = 40;

    auto backend = std::make_shared<ChasingWriterBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/hot@cas@"};

    publishRange(*backend, layout, ns, 1, planted);
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, planted},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });
    backend->arm(&layout, ns, planted, appended_mid_round);

    Gc gc(store, kGc);
    const std::map<String, UInt64> intake = runRoundCapturingIntake(gc);

    ASSERT_GT(backend->publishedThrough(), planted)
        << "the mid-round appender never fired, so this test proves nothing about a moving tail";

    const auto cov = coverageOf(*backend, layout, ns);
    ASSERT_TRUE(cov.has_value()) << "the round must seal a coverage row for the namespace it walked";
    EXPECT_EQ(cov->last_folded_ref_id, (RefTxnId{1, planted}))
        << "the walk must fold through the round-start tail and no further -- it chased the writer";
    EXPECT_FALSE(cov->hold.has_value()) << "reaching the committed frontier is not a hold";
    EXPECT_NE(cov->classification, 4) << "reaching the committed frontier is not a clamp";
    EXPECT_EQ(metric(intake, "tails_advanced"), 1u);
    EXPECT_EQ(metric(intake, "logs_applied"), planted) << "exactly the round-start backlog was folded";

    /// The stragglers are not lost: the next round's listing has a higher tail and folds through it.
    backend->disarm();
    const uint64_t total = backend->publishedThrough();
    ASSERT_GT(total, planted);
    advanceRecoverableCkptForRawFixture(*backend, layout, ns, RefTxnId{1, total});
    const std::map<String, UInt64> second = runRoundCapturingIntake(gc);
    EXPECT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{1, total}))
        << "the records that landed mid-round are folded by the round that lists them";
    EXPECT_EQ(metric(second, "tails_advanced"), 1u);
}

/// ===================== (b) A CTE-AUTHORIZED UNCHANGED NAMESPACE FOLDS NOTHING =====================
///
/// Two namespaces; only one gets a new record. The unchanged one must fold NOTHING. Its CTE already
/// authorizes the sealed cursor as a frontier, so an exact probe at `cursor + 1` would be redundant.
TEST(CASGCBoundedWalk, ACTEAuthorizedUnchangedNamespaceFoldsNothingWithoutAProbe)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace moved{"00/moved@cas@"};
    const RootNamespace still{"00/still@cas@"};

    publishRange(*backend, layout, moved, 1, 2);
    publishRange(*backend, layout, still, 1, 2);
    writeRecoverableCkptForRawFixture(*backend, layout, moved, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });
    writeRecoverableCkptForRawFixture(*backend, layout, still, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    Gc gc(store, kGc);
    runRoundCapturingIntake(gc);
    ASSERT_EQ(cursorOf(*backend, layout, still), (RefTxnId{1, 2})) << "the seeding round must fold both";
    ASSERT_EQ(cursorOf(*backend, layout, moved), (RefTxnId{1, 2}));

    /// Only `moved` advances.
    publishAt(*backend, layout, moved, RefTxnId{1, 3}, "ref_3", 3, DB::UInt128(0x33));
    advanceRecoverableCkptForRawFixture(*backend, layout, moved, RefTxnId{1, 3});

    backend->resetCounts();
    const std::map<String, UInt64> intake = runRoundCapturingIntake(gc);

    EXPECT_EQ(refLogGetsFor(*backend, layout, still, 1, 8), 0u)
        << "the CTE already proves the unchanged namespace's sealed frontier";
    EXPECT_EQ(backend->getCount(layout.refLogKey(fixture::fixtureLife(still), RefTxnId{1, 3})), 0u)
        << "a valid CTE needs no successor probe";
    EXPECT_EQ(metric(intake, "tails_unchanged"), 1u);
    EXPECT_EQ(metric(intake, "tails_advanced"), 1u);
    EXPECT_EQ(metric(intake, "logs_applied"), 1u) << "only the one new record was folded, pool-wide";
    EXPECT_EQ(cursorOf(*backend, layout, moved), (RefTxnId{1, 3})) << "the advanced namespace still folds";
}

/// ===================== (c) A ROUND WHERE NO TAIL MOVED IS SKIPPED OUTRIGHT =====================
///
/// The round-level skip is the EXISTING defer machinery, whose signal is already exactly this
/// comparison: `RefScanSummary::changed_shards` counts the namespaces whose greatest listed log sits
/// above their sealed cursor. So a round in which no tail moved folds nothing at all -- no intake phase,
/// no per-namespace walk, not even the probes -- and an append un-defers it.
TEST(CASGCBoundedWalk, ARoundWhereNoTailMovedIsDeferredAndAnAppendUnDefersIt)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend);   /// the DEFAULT defer window: this test is about the skip
    const Layout & layout = store->layout();
    const RootNamespace a{"00/aa@cas@"};
    const RootNamespace b{"00/bb@cas@"};

    publishRange(*backend, layout, a, 1, 2);
    publishRange(*backend, layout, b, 1, 2);
    writeRecoverableCkptForRawFixture(*backend, layout, a, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });
    writeRecoverableCkptForRawFixture(*backend, layout, b, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    Gc gc(store, kGc);
    ASSERT_FALSE(runRoundCapturingIntake(gc).empty()) << "the seeding round must actually fold";
    ASSERT_EQ(cursorOf(*backend, layout, a), (RefTxnId{1, 2}));
    ASSERT_EQ(cursorOf(*backend, layout, b), (RefTxnId{1, 2}));

    backend->resetCounts();
    EXPECT_TRUE(runRoundCapturingIntake(gc).empty())
        << "no tail moved, so the round has no fold to run";
    EXPECT_EQ(refLogGetsFor(*backend, layout, a, 1, 8), 0u) << "a deferred round reads no ref log at all";
    EXPECT_EQ(refLogGetsFor(*backend, layout, b, 1, 8), 0u);

    /// One append, in `a` only.
    publishAt(*backend, layout, a, RefTxnId{1, 3}, "ref_3", 3, DB::UInt128(0xaa3));
    advanceRecoverableCkptForRawFixture(*backend, layout, a, RefTxnId{1, 3});

    backend->resetCounts();
    const std::map<String, UInt64> woken = runRoundCapturingIntake(gc);
    ASSERT_FALSE(woken.empty()) << "an append must un-defer the round";
    EXPECT_EQ(metric(woken, "tails_advanced"), 1u) << "the appended-to namespace is walked again";
    EXPECT_EQ(metric(woken, "tails_unchanged"), 1u) << "and only that one";
    EXPECT_EQ(cursorOf(*backend, layout, a), (RefTxnId{1, 3}));
    EXPECT_EQ(refLogGetsFor(*backend, layout, b, 1, 8), 0u)
        << "the still-quiet namespace's CTE proves its carried frontier without a probe";
}

/// ============ (d) A RAW RECORD ABOVE THE COMMITTED FRONTIER PROVES AND SUPPRESSES NOTHING ============
///
/// This is the safety argument. It is stated under an explicit `Authoritative` policy so that the claim
/// is about the frontier terms and not about which policy the caller happened to pass.
///
/// The CTE fixes the namespace's committed frontier at `{1,3}`. A raw record ABOVE that frontier was
/// never committed, so it is neither a reason to extend the fold nor a reason to suppress destruction:
/// LIST may observe it, but it cannot manufacture a later authoritative frontier. The dropped blob is
/// therefore reclaimable after the normal condemn/graduation pipeline.
TEST(CASGCBoundedWalk, ARawRecordBeyondTheCommittedFrontierCannotSuppressDestruction)
{
    auto backend = std::make_shared<ChasingWriterBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/hot@cas@"};
    const DB::UInt128 blob(0xd00d);

    /// Publish a blob and drop it, both within the round-start listing: its folded in-degree returns to
    /// zero, so a round with a complete frontier would condemn it.
    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "kept", 1, DB::UInt128(0x1), /*birth=*/true);
    const ManifestRef doomed{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = 1};
    writeBlobBody(*backend, layout, blob);
    writeManifestRaw(*backend, layout, ns, doomed, {blobEntryFor("data.bin", blob)});
    writeTxnAt(*backend, layout, ns, RefTxnId{1, 2}, publishCommittedOps("doomed", doomed));
    dropRefTransition(*backend, layout, ns, "doomed", doomed);

    /// One record lands mid-round, above the committed frontier.
    backend->arm(&layout, ns, /*published_through*/ 3, /*max_appends*/ 1);

    /// From HERE the deletes are the ROUND's. Opening the pool runs a capability probe that writes and
    /// deletes its own `_probe/` keys, and counting those against the round would make this assertion
    /// fail on debris that has nothing to do with the destructive gate.
    backend->resetCounts();

    Gc gc(store, kGc);
    const std::map<String, UInt64> intake = runRoundCapturingIntake(gc, UniversePolicy::Authoritative);

    ASSERT_EQ(backend->publishedThrough(), 4u) << "the mid-round appender never fired";
    ASSERT_EQ(metric(intake, "tails_advanced"), 1u) << "the namespace must have been walked";
    EXPECT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{1, 3})) << "and folded exactly its committed frontier";
    EXPECT_EQ(metric(intake, "frontier_proven"), 1u)
        << "the CTE, not the raw record beyond it, fixes the namespace frontier";
    EXPECT_EQ(metric(intake, "frontier_namespaces"), 1u) << "it is still in the round's universe";
    EXPECT_EQ(backend->deleteTotal(), 1u)
        << "the committed frontier permits the round's immediate manifest cleanup. Deleted:"
        << deletedKeysMessage(*backend);
    EXPECT_TRUE(backend->head(layout.blobKey(legacyMetaTestRef(blob))).exists);

    /// The raw F+1 record remains outside the CTE; it cannot defer the normal destructive pipeline.
    backend->disarm();
    EXPECT_TRUE(runRoundsUntilAbsent(store, gc, *backend, layout, blob, /*max_rounds*/ 8))
        << "the committed frontier must permit reclamation despite the raw record above it";
}

/// A store that hides a namespace's records from every LIST does not lose them: the namespace goes
/// QUIET, and a quiet namespace is exactly the shape the exact-key probe at `cursor + 1` exists for. It
/// has no listed tail at all, and no bound is taken from a listing anyway -- bounding a namespace by a
/// tail the liar refuses to admit to would hand it the omission it was hoping for.
TEST(CASGCBoundedWalk, AListHiddenTailIsCaughtAndFoldedByTheQuietProbePath)
{
    auto backend = std::make_shared<CountingHintHoleBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/liar@cas@"};

    publishRange(*backend, layout, ns, 1, 2);
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });
    Gc gc(store, kGc);
    runRoundCapturingIntake(gc);
    ASSERT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{1, 2}));

    /// A third record lands and the store stops listing the namespace at the same moment: its listed
    /// tail is now nothing at all, while `{1, 3}` is durable and readable by exact key.
    publishAt(*backend, layout, ns, RefTxnId{1, 3}, "ref_3", 3, DB::UInt128(0x1a3));
    advanceRecoverableCkptForRawFixture(*backend, layout, ns, RefTxnId{1, 3});
    backend->hidePrefix(layout.namespaceStreamPrefix(fixture::fixtureLife(ns)));

    const std::map<String, UInt64> intake = runRoundCapturingIntake(gc);
    EXPECT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{1, 3}))
        << "the exact-key probe sees what LIST omits, so the hidden record is folded, not lost";
    EXPECT_EQ(metric(intake, "unhinted_quiet_walked"), 1u) << "it is the quiet path that caught it";
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(0x1a3)), 1)
        << "the hidden record's owner edge must be folded, or its blob looks unreferenced";
}

/// ===================== (e) ONE ROUND TRIP PER MANIFEST EDGE =====================
///
/// The edge fold used to pay HEAD-then-GET: two serial round trips per manifest edge, on every folded
/// log, where the GET alone already answers "is it there".
TEST(CASGCBoundedWalk, ManifestEdgeFoldsPayAGetAndNeverAHead)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    const uint64_t records = 3;
    publishRange(*backend, layout, ns, 1, records);
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, records},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    backend->resetCounts();
    Gc gc(store, kGc);
    runRoundCapturingIntake(gc);
    ASSERT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{1, records})) << "the round must have folded them";

    uint64_t manifest_heads = 0;
    uint64_t manifest_gets = 0;
    for (uint64_t i = 1; i <= records; ++i)
    {
        const ManifestId id{ns, ManifestRef{.writer_epoch = 1, .build_sequence = i, .manifest_ordinal = 1}};
        manifest_heads += backend->headCount(layout.manifestKey(id));
        manifest_gets += backend->getCount(layout.manifestKey(id));
    }
    EXPECT_EQ(manifest_heads, 0u) << "the fold must not HEAD a manifest body it is about to GET";
    EXPECT_GT(manifest_gets, 0u) << "the bodies were read, so the counters really are watching these keys";
}

/// An absent manifest body still takes the record-and-continue path -- the GET's own absence is the
/// signal the HEAD used to carry -- and still raises the fold barrier without ever HEADing the key.
TEST(CASGCBoundedWalk, AnAbsentManifestBodyStillHoldsWithoutAHead)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    publishRange(*backend, layout, ns, 1, 3);
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 3},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });
    const ManifestId gone{ns, ManifestRef{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = 1}};
    deleteManifestBody(*backend, layout, gone);

    backend->resetCounts();
    Gc gc(store, kGc);
    runRoundCapturingIntake(gc);

    const auto cov = coverageOf(*backend, layout, ns);
    ASSERT_TRUE(cov.has_value());
    EXPECT_EQ(cov->last_folded_ref_id, (RefTxnId{1, 1}))
        << "the cursor stays BELOW the log whose manifest body is missing";
    ASSERT_TRUE(cov->hold.has_value()) << "an absent committed manifest body raises the fold barrier";
    EXPECT_EQ(cov->hold->reason, HoldReason::ManifestBodyMissing);
    EXPECT_EQ(cov->hold->offending_position, (RefTxnId{1, 2}));
    EXPECT_EQ(cov->classification, 4);
    EXPECT_EQ(backend->headCount(layout.manifestKey(gone)), 0u)
        << "absence is decided by the GET, so the missing body costs no HEAD either";
}

/// ===================== (f) A NAMESPACE THAT FOLDED NOTHING KEEPS ITS COVERAGE ROW ===================
///
/// The fold seal writes a row only for the namespaces the intake loop visits, so any future shortcut
/// that stops visiting a namespace with nothing to fold would DROP its cursor -- and a dropped cursor is
/// not a lost optimisation, it is a re-fold from `{0, 0}`: every owner edge counted a second time, every
/// blob's in-degree inflated, and the eventual correction mass-condemning live data. This pins the row
/// against exactly that.
TEST(CASGCBoundedWalk, ANamespaceThatFoldedNothingKeepsItsSealedCursor)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace quiet{"00/quiet@cas@"};
    const RootNamespace moved{"00/moved@cas@"};

    publishRange(*backend, layout, quiet, 1, 3);
    publishRange(*backend, layout, moved, 1, 1);
    writeRecoverableCkptForRawFixture(*backend, layout, quiet, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 3},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });
    writeRecoverableCkptForRawFixture(*backend, layout, moved, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    Gc gc(store, kGc);
    runRoundCapturingIntake(gc);
    const auto before = coverageOf(*backend, layout, quiet);
    ASSERT_TRUE(before.has_value());
    ASSERT_EQ(before->last_folded_ref_id, (RefTxnId{1, 3}));
    ASSERT_FALSE(before->hold.has_value());

    /// A second round in which `quiet` folds nothing and `moved` does, so the round really does write a
    /// new seal that could have dropped the row.
    publishAt(*backend, layout, moved, RefTxnId{1, 2}, "ref_2", 2, DB::UInt128(0x22));
    advanceRecoverableCkptForRawFixture(*backend, layout, moved, RefTxnId{1, 2});
    const std::map<String, UInt64> intake = runRoundCapturingIntake(gc);
    ASSERT_EQ(metric(intake, "tails_unchanged"), 1u) << "the fixture must actually exercise the quiet case";

    const auto after = coverageOf(*backend, layout, quiet);
    ASSERT_TRUE(after.has_value())
        << "the coverage row was DROPPED -- the next round would re-fold this namespace from {0,0}";
    /// The CURSOR and the HOLD are what the next round trusts, and both ride unchanged.
    /// `classification` legitimately moves from 2 ("this round folded records") to 1 ("unchanged"),
    /// because that is what the round did — it is the one field that may differ, so it is the one field
    /// asserted loosely.
    EXPECT_EQ(after->last_folded_ref_id, before->last_folded_ref_id)
        << "a namespace that folded nothing must keep the cursor it had";
    EXPECT_EQ(after->hold, before->hold);
    EXPECT_NE(after->classification, 4) << "folding nothing is not a clamp";
    EXPECT_EQ(metric(intake, "frontier_namespaces"), 2u)
        << "it stays in the round's universe, so its proof is still owed";
}
