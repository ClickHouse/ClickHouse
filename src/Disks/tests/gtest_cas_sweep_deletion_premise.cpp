#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include "cas_sweep_test_support.h"
#include "cas_test_helpers.h"

#include <vector>

using namespace DB::Cas;
using namespace DB::Cas::tests;

/// Spec §6, the sweep deletion premise. A manifest of an epoch-`E` build is deletable only when the
/// namespace cursor has consumed epoch `E`'s seal AND no unconsumed tail record above the cursor names
/// it as a removal target; on ANY uncertainty the sweep RETAINS and says why.
///
/// WHY THE CURSOR AND NOT A LISTING. The sweep's pre-existing protection view is assembled from an
/// enumeration of the namespace's ref objects, and arithmetic ref intake demoted exactly that
/// enumeration to a hint: a store may omit a durable key from a `LIST`. A hidden `+1` above the cursor
/// therefore makes an owned manifest look unowned, and deleting it is data loss; a hidden `-1` makes a
/// removal target look unprotected, and deleting it clamps the fold forever on the missing body. The
/// premise closes the first by arithmetic (grants do not cross epochs, and an epoch is left only over
/// its consumed seal) and the second by refusing whenever the tail is not decidable.
namespace
{

/// The build's epoch. The namespace's seeded ref log lives at writer epoch 1 (`appendRefLogSeed`), so
/// naming the build's epoch 1 as well keeps the fixture coherent: a cursor at `{2, _}` is then a cursor
/// that genuinely crossed epoch 1's closing seal, not an invented number above an unrelated stream.
constexpr uint64_t kBuildEpoch = 1;
const String kServerRoot = "00";

ManifestRef ref(uint64_t seq, uint64_t ordinal)
{
    return ManifestRef{.writer_epoch = kBuildEpoch, .build_sequence = seq,
                       .manifest_ordinal = static_cast<uint32_t>(ordinal)};
}

BuildPrefix buildPrefix(uint64_t seq)
{
    return BuildPrefix{.writer_epoch = kBuildEpoch, .build_sequence = seq};
}

/// A pool with ONE eligible-but-unowned manifest body under build sequence 5: the shape the sweep is
/// meant to reclaim, so that every test below differs only in the durable fold state.
struct OrphanFixture
{
    std::shared_ptr<InMemoryBackend> backend = std::make_shared<InMemoryBackend>();
    PoolPtr store;
    RootNamespace ns{"00/aa@cas@"};
    ManifestRef orphan = ref(5, 0xAB);

    OrphanFixture()
    {
        store = openPoolForTest(backend);
        /// This fixture has no ref transaction, but it is a normal empty catalog life rather than the
        /// deliberate missing-checkpoint corruption shape. State that empty recovery frontier before
        /// exercising the independent sweep-deletion premise.
        casAdmitRecoverableEntry(*backend, store->layout(), ns);
        writeManifestRaw(*backend, store->layout(), ns, orphan, {blobEntryFor("a", DB::UInt128(1))});
        /// min_active 6 > build_sequence 5: the durable watermark fact makes the prefix ELIGIBLE, which
        /// is the half the premise sits on top of.
        setWatermarkMinActive(*backend, store->layout(), kServerRoot, kBuildEpoch, /*min_active*/6);
    }

    String orphanKey() const { return store->layout().manifestKey(ManifestId{ns, orphan}); }
    bool orphanExists() const { return backend->head(orphanKey()).exists; }
};

}

/// Rule (1), the load-bearing case. The cursor is still INSIDE the build's own epoch, so epoch 1's
/// closing seal is not proven consumed and an unfolded `+1` naming this build may still exist above the
/// cursor. The body survives, and the sweep says so through its `warnings` out-param.
TEST(CASSweepDeletionPremise, AnUnconsumedEpochSealRetainsTheBuildsManifests)
{
    OrphanFixture f;
    seedFoldCursorForTest(*f.backend, f.store->layout(), f.ns, RefTxnId{kBuildEpoch, 3});

    std::vector<String> warnings;
    const uint64_t deleted = sweepNamespace(*f.store, f.ns, buildPrefix(5), &warnings);

    EXPECT_EQ(deleted, 0u);
    EXPECT_TRUE(f.orphanExists())
        << "the cursor sits at {1,3}, inside the build's own epoch -- epoch 1's closing seal is not "
           "consumed, so a grant naming this build may still be unfolded above the cursor";
    ASSERT_EQ(warnings.size(), 1u) << "a retained manifest is a visible decision, not a silent one";
    EXPECT_NE(warnings[0].find(f.orphanKey()), String::npos);
    EXPECT_NE(warnings[0].find("seal"), String::npos);
}

/// Rule (1) satisfied and the tail clean: the ordinary reclaim still happens, by exact token.
TEST(CASSweepDeletionPremise, AConsumedEpochSealWithACleanTailDeletes)
{
    OrphanFixture f;
    /// A cursor at `{2, 1}` is in an epoch strictly above the build's. An epoch is left ONLY over its
    /// consumed `EpochSeal`, so this cursor is durable proof that every epoch-1 record is folded.
    seedFoldCursorForTest(*f.backend, f.store->layout(), f.ns, RefTxnId{kBuildEpoch + 1, 1});

    std::vector<String> warnings;
    const uint64_t deleted = sweepNamespace(*f.store, f.ns, buildPrefix(5), &warnings);

    EXPECT_EQ(deleted, 1u);
    EXPECT_FALSE(f.orphanExists());
    EXPECT_TRUE(warnings.empty()) << "nothing was retained, so nothing is warned about";
}

/// Uncertainty rule, hold arm. The cursor HAS consumed epoch 1's seal, so rule (1) alone would let the
/// body go -- but the namespace is held, which means the fold could not account for everything at or
/// above the held position. A held namespace retains everything under it.
TEST(CASSweepDeletionPremise, AHeldNamespaceRetainsEvenAboveAConsumedSeal)
{
    OrphanFixture f;
    const RefHold hold{.reason = HoldReason::GapBelowWitness,
                       .offending_position = RefTxnId{kBuildEpoch + 1, 4},
                       .retry_count = 2, .next_retry_round = 9};
    seedFoldCursorForTest(*f.backend, f.store->layout(), f.ns, RefTxnId{kBuildEpoch + 1, 3}, hold);

    std::vector<String> warnings;
    const uint64_t deleted = sweepNamespace(*f.store, f.ns, buildPrefix(5), &warnings);

    EXPECT_EQ(deleted, 0u);
    EXPECT_TRUE(f.orphanExists());
    ASSERT_EQ(warnings.size(), 1u);
    EXPECT_NE(warnings[0].find("held"), String::npos);
    EXPECT_NE(warnings[0].find(String{holdReasonToWord(HoldReason::GapBelowWitness)}), String::npos)
        << "the retain reason names WHAT stopped the namespace, not just that something did";
}

/// Uncertainty rule, unreached-frontier arm in its most complete form: the adopted seal carries no row
/// for this namespace at all, so no round has ever sealed a cursor for it and nothing about its ref
/// stream is proven. This is also the state of a pool whose GC has never run.
TEST(CASSweepDeletionPremise, ANamespaceWithNoSealedCursorRetains)
{
    OrphanFixture f;
    /// A seal exists and is adopted, but it covers a DIFFERENT namespace.
    seedFoldCursorForTest(*f.backend, f.store->layout(), RootNamespace{"00/zz@cas@"},
                          RefTxnId{kBuildEpoch + 1, 1});

    std::vector<String> warnings;
    const uint64_t deleted = sweepNamespace(*f.store, f.ns, buildPrefix(5), &warnings);

    EXPECT_EQ(deleted, 0u);
    EXPECT_TRUE(f.orphanExists());
    ASSERT_EQ(warnings.size(), 1u);
    EXPECT_NE(warnings[0].find("coverage"), String::npos);
}

/// Rule (2). Removals cross epochs, so a record in a LATER epoch can name an earlier epoch's build as a
/// removal target; deleting the body before that `-1` folds clamps the fold forever on the missing body.
/// The predicate is exercised directly here because the sweep's own protection view already spares a
/// listed tail removal before the premise is ever consulted -- the point of the rule is that the SAME
/// answer is reached by the predicate both paths share, so neither path can lose it.
TEST(CASSweepDeletionPremise, AnUnconsumedTailRemovalRetainsItsTarget)
{
    OrphanFixture f;
    const String key = f.orphanKey();

    NamespaceFoldView view;
    RefCoverage cov;
    cov.classification = 2;
    cov.last_folded_ref_id = RefTxnId{kBuildEpoch + 1, 1};   /// rule (1) satisfied
    view.coverage = cov;
    view.tail_removal_targets.insert(key);

    String reason;
    EXPECT_FALSE(manifestDeletionPremise(view, ManifestKey{key, buildPrefix(5)}, &reason));
    EXPECT_NE(reason.find("removal"), String::npos);

    /// The same view without the removal target admits the deletion, so the retention above is the
    /// removal target's doing and nothing else's.
    view.tail_removal_targets.clear();
    reason.clear();
    EXPECT_TRUE(manifestDeletionPremise(view, ManifestKey{key, buildPrefix(5)}, &reason));
    EXPECT_TRUE(reason.empty());
}

/// Both sweep paths call the ONE predicate: the cursor-paced page must refuse the same body the
/// per-namespace sweep refuses, for the same reason.
TEST(CASSweepDeletionPremise, TheCursorPagePathHonoursTheSamePremise)
{
    OrphanFixture f;
    seedFoldCursorForTest(*f.backend, f.store->layout(), f.ns, RefTxnId{kBuildEpoch, 3});

    const ManifestSweepResult held = sweepManifestCursorPageForTest(*f.store, "", /*list_budget*/100, /*delete_budget*/10);
    EXPECT_EQ(held.deleted, 0u);
    EXPECT_GE(held.skipped, 1u);
    EXPECT_TRUE(f.orphanExists());

    /// Consume the seal and the very same page deletes it.
    seedFoldCursorForTest(*f.backend, f.store->layout(), f.ns, RefTxnId{kBuildEpoch + 1, 1});
    const ManifestSweepResult freed = sweepManifestCursorPageForTest(*f.store, "", /*list_budget*/100, /*delete_budget*/10);
    EXPECT_EQ(freed.deleted, 1u);
    EXPECT_FALSE(f.orphanExists());
}

/// WHAT THE PREMISE COSTS, pinned so it is a stated behaviour rather than something a later reader
/// discovers. The pure pre-precommit orphan -- a manifest body staged by a writer that crashed before
/// appending any ref record for it -- lives under a namespace whose ref stream may not exist at all.
/// Such a namespace never enters the fold's universe, so no round ever seals a cursor for it, so no
/// epoch's closing seal is ever consumed for it, so the premise retains its debris INDEFINITELY. That
/// is the safe direction and it is deliberate, but it is not "delay": reclaiming this class needs the
/// sweep's own rework (registers R2/R3, Stage B) -- the writer duty queue that knows what it staged, and
/// the nomination path. The premise ships as the safety floor, not as the reclaim policy.
TEST(CASSweepDeletionPremise, DebrisUnderANamespaceTheFoldNeverWalksIsRetainedIndefinitely)
{
    OrphanFixture f;
    /// No ref stream, no coverage row -- repeated passes change nothing.
    std::vector<String> warnings;
    for (int pass = 0; pass < 3; ++pass)
        EXPECT_EQ(sweepNamespace(*f.store, f.ns, buildPrefix(5), &warnings), 0u) << "pass " << pass;

    EXPECT_TRUE(f.orphanExists());
    EXPECT_EQ(warnings.size(), 3u) << "every pass reports the retention rather than going quiet";
}

/// RETENTION IS VISIBLE ON THE PATH THAT ACTUALLY SWEEPS. `planManifestCursorPage` has no `warnings`
/// out-param -- the background sweep answers to nobody but its phase row -- so the premise's refusals
/// have to leave the process as COUNTERS or not at all. In Stage A that is nearly the whole story of
/// the sweep, because rule (1) is satisfiable only for a closed-and-folded epoch.
///
/// Non-vacuous by construction: two namespaces on ONE page are retained for DIFFERENT reasons, so a
/// counter wired to the wrong class, or one bucket catching everything, changes the answer. A
/// single-reason page would pass against a single mislabelled counter.
TEST(CASSweepDeletionPremise, DistinctRetainReasonsLandInDistinctCounters)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();

    /// Namespace A: a cursor still INSIDE the build's epoch -> rule (1), `unconsumed_seal`.
    const RootNamespace ns_a{"00/aa@cas@"};
    const ManifestRef ref_a = ref(5, 0xA1);
    casAdmitRecoverableEntry(*backend, layout, ns_a);
    writeManifestRaw(*backend, layout, ns_a, ref_a, {blobEntryFor("a", DB::UInt128(1))});
    seedFoldCursorForTest(*backend, layout, ns_a, RefTxnId{kBuildEpoch, 3});

    /// Namespace B: a HELD row whose cursor is well above the build's epoch, so rule (1) is satisfied
    /// and the hold is demonstrably what retained it.
    const RootNamespace ns_b{"00/bb@cas@"};
    const ManifestRef ref_b = ref(5, 0xB1);
    casAdmitRecoverableEntry(*backend, layout, ns_b);
    writeManifestRaw(*backend, layout, ns_b, ref_b, {blobEntryFor("b", DB::UInt128(2))});
    const RefHold hold{.reason = HoldReason::BodyUndecodable,
                       .offending_position = RefTxnId{kBuildEpoch + 1, 9},
                       .retry_count = 1, .next_retry_round = 4};
    seedFoldCursorForTest(*backend, layout, ns_b, RefTxnId{kBuildEpoch + 1, 8}, hold);

    setWatermarkMinActive(*backend, layout, kServerRoot, kBuildEpoch, /*min_active*/6);

    const ManifestSweepResult result = sweepManifestCursorPageForTest(*store, "", /*list_budget*/100, /*delete_budget*/10);

    EXPECT_EQ(result.deleted, 0u);
    EXPECT_EQ(result.retained_unconsumed_seal, 1u) << "namespace A's cursor is inside its build's epoch";
    EXPECT_EQ(result.retained_hold, 1u) << "namespace B is held";
    EXPECT_EQ(result.retained_no_coverage, 0u);
    EXPECT_EQ(result.retained_tail_removal, 0u);
    EXPECT_GE(result.skipped, 2u) << "both retentions are also ordinary skips";

    /// The rollup an operator reads. The two classes tie at one each and the tie resolves by enum
    /// order, which is what keeps an unchanged pool reporting an unchanged verdict pass after pass.
    const auto top = result.topRetainReason();
    EXPECT_EQ(top.second, 1u);
    EXPECT_EQ(top.first, SweepRetainClass::Hold);
    EXPECT_EQ(String{sweepRetainClassName(SweepRetainClass::UnconsumedSeal)}, "unconsumed_seal");

    /// A page with no candidates reports nothing: the counters carry the premise's own refusals, not
    /// ordinary skips.
    auto empty_backend = std::make_shared<InMemoryBackend>();
    auto empty_store = openPoolForTest(empty_backend);
    const ManifestSweepResult nothing =
        sweepManifestCursorPageForTest(*empty_store, "", /*list_budget*/100, /*delete_budget*/10);
    EXPECT_EQ(nothing.topRetainReason().first, SweepRetainClass::None);
    EXPECT_EQ(nothing.topRetainReason().second, 0u);
}

/// STAGE B SEAM (registers R2/R3). The premise is the per-manifest SAFETY floor and nothing else: it
/// says when a body may go, never who nominates it or when. The sweep's own rework -- the writer duty
/// queue that reclaims its own live epoch's debris, and the nomination path -- attaches here, and must
/// satisfy this predicate rather than replace it.

/// Uncertainty rule, budget arm. A candidate the page never DECIDED on -- the delete budget ran out
/// before it -- is retained, and the cursor must not step over it: the sweep's cursor is a
/// cleanup-progress hint whose skipped range is not revisited until a full wrap, so advancing past an
/// undecided candidate converts "retained this round" into "unexamined for a whole cycle".
TEST(CASSweepDeletionPremise, AnExhaustedDeleteBudgetRetainsAndDoesNotStepOverTheRest)
{
    OrphanFixture f;
    const ManifestRef second = ref(5, 0xAC);
    const ManifestRef third = ref(5, 0xAD);
    writeManifestRaw(*f.backend, f.store->layout(), f.ns, second, {blobEntryFor("b", DB::UInt128(2))});
    writeManifestRaw(*f.backend, f.store->layout(), f.ns, third, {blobEntryFor("c", DB::UInt128(3))});
    seedFoldCursorForTest(*f.backend, f.store->layout(), f.ns, RefTxnId{kBuildEpoch + 1, 1});

    const ManifestSweepResult first = sweepManifestCursorPageForTest(*f.store, "", /*list_budget*/100, /*delete_budget*/1);
    EXPECT_EQ(first.deleted, 1u);
    EXPECT_FALSE(first.wrapped)
        << "the page stopped on an exhausted budget with candidates left, so it did not reach the end";
    ASSERT_FALSE(first.next_cursor.empty());

    /// Resume: the two survivors are still ahead of the cursor, so a budgeted continuation reaches them.
    const ManifestSweepResult second_page =
        sweepManifestCursorPageForTest(*f.store, first.next_cursor, /*list_budget*/100, /*delete_budget*/10);
    EXPECT_EQ(second_page.deleted, 2u)
        << "the cursor must not have stepped over the candidates the exhausted budget left undecided";

    size_t surviving = 0;
    for (const ManifestRef & r : {f.orphan, second, third})
        if (f.backend->head(f.store->layout().manifestKey(ManifestId{f.ns, r})).exists)
            ++surviving;
    EXPECT_EQ(surviving, 0u);
}

/// MANDATORY liveness proof: a namespace whose committed-tail recovery walk
/// can never finish within one round's `sweep_recovery_op_budget` must not wedge the cursor page for
/// every subsequent round. Six eligible candidates share ONE namespace whose tail is ~200 unrelated
/// committed transactions above the fold cursor -- far more than the tiny per-round recovery-op budget
/// can traverse -- so `activeManifestKeys` reports `recovery_incomplete` on every attempt, every one of
/// this namespace's candidates is retained (never nominated, never deleted), yet the page still DECIDES
/// them (a retained candidate is a decision) and the cursor advances across pages until the whole
/// keyspace is covered.
TEST(CASSweepDeletionPremise, RecoveryWorkBudgetRetainsAndConvergesWithoutWedgingTheCursor)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    /// `casAdmitEntry` (bare, no `_ckpt`) rather than `casAdmitRecoverableEntry` (which pre-seeds an
    /// EMPTY `_ckpt`, `committed_through = nullopt`): the tail below is built entirely from real
    /// `publishCommittedTransition` calls, whose first call needs `readCkpt` to see NOTHING yet so it
    /// takes the fresh-`_ckpt` `putIfAbsent` path instead of `advanceRecoverableCkptForRawFixture`'s
    /// monotonic-advance-from-existing-value path (which throws on a null `committed_through`).
    casAdmitEntry(*backend, layout, ns);
    setWatermarkMinActive(*backend, layout, kServerRoot, kBuildEpoch, /*min_active*/1000);

    /// Six orphan candidates, all eligible (build_sequence << min_active), none owned by any ref.
    constexpr int kCandidates = 6;
    for (int i = 1; i <= kCandidates; ++i)
        writeManifestRaw(*backend, layout, ns, ref(i, 1),
                         {blobEntryFor("c" + std::to_string(i), DB::UInt128(static_cast<uint64_t>(i)))});

    /// A committed tail of ~200 UNRELATED transactions above the fold cursor. None of these need a
    /// manifest body of their own -- the recovery walk only GETs and decodes the ref-log transactions,
    /// never the bodies they name.
    constexpr int kTailSize = 200;
    for (int i = 0; i < kTailSize; ++i)
        publishCommittedTransition(*backend, layout, ns, "tail" + std::to_string(i), std::nullopt, ref(2000 + i, 1));
    seedFoldCursorForTest(*backend, layout, ns, RefTxnId{kBuildEpoch, 1});

    uint64_t total_retained_work_budget = 0;
    uint64_t total_skipped = 0;
    uint64_t total_deleted = 0;
    String cursor;
    bool wrapped = false;
    int pages = 0;
    for (; pages < 10 && !wrapped; ++pages)
    {
        /// A FRESH budget every page, exactly like production's one-instance-per-round contract --
        /// the same namespace's recovery walk re-attempts and re-exhausts every time, by design (a
        /// pathological namespace does not get to starve every OTHER page of budget forever).
        GcRoundWorkBudget budget;
        budget.max_sweep_recovery_ops = 5;   /// far below the ~200-record tail
        const ManifestSweepResult result = sweepManifestCursorPageForTest(*store, cursor, /*list_budget*/3, /*delete_budget*/10, &budget);

        /// LIVENESS: every page decides at least one candidate (a retained one counts) or wraps.
        EXPECT_TRUE(result.skipped > 0 || result.deleted > 0 || result.wrapped)
            << "page " << pages << " decided nothing and did not wrap -- a wedge";
        /// Every namespace this page touches hits the recovery-op-exhausted cause AT LEAST once --
        /// only the FIRST candidate of an errored namespace on a page carries the specific retain-class
        /// counter (the SAME pre-existing convention `retained_no_coverage`/`retained_hold` already
        /// use); every other candidate of that namespace still lands in the generic `skipped` tally.
        EXPECT_GE(result.retained_work_budget, 1u)
            << "page " << pages << " never attributed a candidate to the recovery-budget cause";

        total_retained_work_budget += result.retained_work_budget;
        total_skipped += result.skipped;
        total_deleted += result.deleted;
        wrapped = result.wrapped;
        ASSERT_NE(cursor, result.next_cursor) << "page " << pages << " made no cursor progress";
        cursor = result.next_cursor;
    }

    EXPECT_TRUE(wrapped) << "the whole small keyspace must be fully covered well within 10 pages";
    EXPECT_EQ(total_deleted, 0u) << "the pathological namespace's candidates are never safe to nominate";
    EXPECT_EQ(total_skipped, static_cast<uint64_t>(kCandidates))
        << "every one of the six candidates was decided (skipped), none silently dropped from the page";
    EXPECT_GE(total_retained_work_budget, 1u);
    for (int i = 1; i <= kCandidates; ++i)
        EXPECT_TRUE(backend->head(layout.manifestKey(ManifestId{ns, ref(i, 1)})).exists)
            << "candidate " << i << " must survive: it was never proven safe to delete";
}

/// The per-page NAMESPACE cap. Two otherwise-independently-deletable
/// namespaces share one page; with `max_sweep_namespaces = 1`, only the first namespace this page
/// touches gets a protection view built at all -- the second is retained under the work-budget cause,
/// never given a partial or best-effort view.
TEST(CASSweepDeletionPremise, NamespaceWorkBudgetCapsDistinctViewsPerPage)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();

    const RootNamespace ns_a{"00/aa@cas@"};
    const RootNamespace ns_b{"00/bb@cas@"};
    const ManifestRef ref_a = ref(5, 0xA1);
    const ManifestRef ref_b = ref(5, 0xB1);
    casAdmitRecoverableEntry(*backend, layout, ns_a);
    casAdmitRecoverableEntry(*backend, layout, ns_b);
    writeManifestRaw(*backend, layout, ns_a, ref_a, {blobEntryFor("a", DB::UInt128(1))});
    writeManifestRaw(*backend, layout, ns_b, ref_b, {blobEntryFor("b", DB::UInt128(2))});
    /// Both namespaces satisfy rule (1) (cursor past the build's own epoch) and have no committed tail
    /// at all (`_ckpt.committed_through` unset), so absent the namespace cap BOTH would delete.
    seedFoldCursorForTest(*backend, layout, ns_a, RefTxnId{kBuildEpoch + 1, 1});
    seedFoldCursorForTest(*backend, layout, ns_b, RefTxnId{kBuildEpoch + 1, 1});
    setWatermarkMinActive(*backend, layout, kServerRoot, kBuildEpoch, /*min_active*/6);

    GcRoundWorkBudget budget;
    budget.max_sweep_namespaces = 1;

    const ManifestSweepResult result = sweepManifestCursorPageForTest(*store, "", /*list_budget*/100, /*delete_budget*/10, &budget);

    EXPECT_EQ(result.deleted, 1u) << "exactly one namespace's view could be built this page";
    EXPECT_EQ(result.retained_work_budget, 1u)
        << "the other namespace's candidate is retained, never decided from a missing view";
    EXPECT_EQ(budget.sweep_namespaces_used, 1u);

    size_t surviving = 0;
    for (const auto & p : std::vector<std::pair<RootNamespace, ManifestRef>>{{ns_a, ref_a}, {ns_b, ref_b}})
        if (backend->head(layout.manifestKey(ManifestId{p.first, p.second})).exists)
            ++surviving;
    EXPECT_EQ(surviving, 1u) << "exactly one candidate remains -- the one whose namespace had no budget left";
}
