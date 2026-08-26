#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include "cas_test_helpers.h"

#include <mutex>
#include <set>

/// ARITHMETIC REF INTAKE (spec 2026-07-27 "ref chain complete cut" §5).
///
/// The GC fold used to walk the ids the round's LIST returned. That made the LIST a source of TRUTH
/// about which records exist, and an object store that omits a durable key from a listing -- observed
/// in production as the `0x1430c`/`0x1430d` shape -- silently skipped those records' owner edges and
/// then sealed a cursor ABOVE them, so their blobs looked unreferenced forever after.
///
/// Under INV-1 (per-namespace contiguous ids) the ids within one `(namespace, writer_epoch)` are dense
/// `1..T`, so the next record's id is COMPUTABLE: `cursor + 1`. The fold therefore steps by arithmetic
/// and reads each expected id by EXACT key (the per-record GET was always owed -- the round read every
/// record's body anyway). The listing is demoted to a HINT with two jobs: it says which namespaces
/// exist, and it supplies the witnesses that make an absent expected-next decidable:
///
///   * absent at `expected`, no listed id above it  => the namespace's frontier this round (normal end)
///   * absent at `expected`, a listed id above it   => IMPOSSIBLE under contiguity: the store is lying
///                                                     or a durable record was lost. Hold the namespace
///                                                     (classification 4), cursor unmoved.
///
/// Epochs are crossed ONLY by consuming the `EpochSeal` that closes an epoch (INV-2). The seal folds as
/// an applied no-op (probe B2: `produced=false`), and the next epoch's start is `{E', 1}` -- reached
/// through the `prev_epoch_seal` back-chain, never guessed from the hint, so an epoch the hint omits
/// entirely is still walked.
///
/// These tests drive REAL rounds over an in-memory pool whose LIST omits keys that are genuinely
/// present (`HintHoleBackend`) -- the production shape, reproduced.

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

const UInt128 kGc = hexToU128("00000000000000000000000000000001");

/// The lying store is `HintHoleBackend` from `cas_test_helpers.h`: LIST permanently omits keys that
/// stay readable by exact key, so these tests exercise the intake's arithmetic walk rather than any
/// one `list` call.

/// The `RefCoverage` the newest fold seal recorded for `ns`'s opaque catalog life, or nullopt when the
/// seal has no entry for it. Scans downward from the adopted generation for the most recent fold seal,
/// mirroring `foldCursorOf`'s reasoning (a completed round's `gc/state` points at the recheck generation).
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

uint8_t classificationOf(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    const auto cov = coverageOf(backend, layout, ns);
    return cov ? cov->classification : 0;
}

/// The `fold_ref_intake` phase metrics of the round `sched` runs -- the only place probe B1's two
/// numbers are observable.
std::map<String, UInt64> runRoundAndReadIntakeMetrics(const PoolPtr & store)
{
    std::vector<GcRoundLogRecord> rows;
    CasGcScheduler sched(store, std::chrono::seconds(1), "test::gc", "ca",
                         [&](const GcRoundLogRecord & r) { rows.push_back(r); });
    EXPECT_TRUE(sched.runOneRoundNow(GcRoundLogRecord::Trigger::Manual).acquired_lease);
    for (const GcRoundLogRecord & r : rows)
        if (r.event_type == GcRoundLogRecord::EventType::Phase && r.phase == "fold_ref_intake")
            return r.phase_metrics;
    return {};
}

}

/// ===================== THE BLOCKER, AS A UNIT TEST =====================
///
/// Five records, all durable and all readable by exact key; the hint omits the two in the MIDDLE.
/// Arithmetic intake never consults the hint for what to read next, so the omission is a non-event:
/// every record folds, every blob keeps its owner edge, and the cursor lands on the true tail.
///
/// Under listing-driven intake this test fails on the blobs, not on the cursor: the cursor still
/// reached `{1, 5}` (the last LISTED id) while the two hidden records' edges were never folded -- the
/// exact damage shape the production blocker caused, since a cursor sealed above an unfolded record can
/// never be re-read.
TEST(CASGCArithmeticIntake, HintOmittingMiddleRecordsFoldsThroughUnnoticed)
{
    auto backend = std::make_shared<HintHoleBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch

    for (uint64_t i = 1; i <= 5; ++i)
        publishAt(*backend, layout, ns, RefTxnId{1, i}, "ref_" + std::to_string(i), i,
                  DB::UInt128(i), /*birth=*/i == 1);
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 5},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    backend->hide(layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 3}));
    backend->hide(layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 4}));

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    ASSERT_GT(backend->holesServed(), 0u) << "the hint hole was never actually served";

    EXPECT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{1, 5}));
    EXPECT_EQ(classificationOf(*backend, layout, ns), 2) << "a folded namespace is `changed`";
    for (uint64_t i = 1; i <= 5; ++i)
        EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(i)), 1)
            << "blob " << i << " lost its owner edge: its record was skipped because the hint omitted it";
}

/// A clean, hole-free namespace still ends its walk exactly where it should: at the first absent id,
/// with no witness above it and therefore no hold.
TEST(CASGCArithmeticIntake, WalkEndsAtFrontierWithoutHold)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// Fold every round: the default defer window would skip the second round entirely and leave the
    /// first round's seal in place, so the assertion below would read a stale coverage record.
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    for (uint64_t i = 1; i <= 3; ++i)
        publishAt(*backend, layout, ns, RefTxnId{1, i}, "ref_" + std::to_string(i), i,
                  DB::UInt128(i), /*birth=*/i == 1);
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 3},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    EXPECT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{1, 3}));
    EXPECT_EQ(classificationOf(*backend, layout, ns), 2);

    /// A second round over an unchanged namespace pays exactly one exact GET, finds the same frontier,
    /// and neither advances nor holds.
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    EXPECT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{1, 3}));
    EXPECT_EQ(classificationOf(*backend, layout, ns), 1) << "an unchanged namespace is `carried`";
}

/// ===================== EPOCHS ARE CROSSED ONLY BY CONSUMING A SEAL =====================
///
/// `{1,1} {1,2} seal{1,3} | {2,1} {2,2}`: the seal is applied as a table no-op, counted applied, and
/// the walk continues at `{2, 1}` -- whose `prev_epoch_seal` names the seal just consumed, which is what
/// makes the crossing provable rather than guessed.
TEST(CASGCArithmeticIntake, SealCrossesEpochAndIsAppliedAsNoOp)
{
    auto backend = std::make_shared<HintHoleBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    publishAt(*backend, layout, ns, RefTxnId{1, 2}, "ref_2", 2, DB::UInt128(2));
    writeSealAt(*backend, layout, ns, RefTxnId{1, 3});
    publishAt(*backend, layout, ns, RefTxnId{2, 1}, "ref_3", 3, DB::UInt128(3),
              /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{1, 3});
    publishAt(*backend, layout, ns, RefTxnId{2, 2}, "ref_4", 4, DB::UInt128(4));
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{2, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{1, 3},
    });

    /// The hint hides the seal AND the new epoch's first record: neither the epoch boundary nor its
    /// start may depend on the listing.
    backend->hide(layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 3}));
    backend->hide(layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{2, 1}));

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    ASSERT_GT(backend->holesServed(), 0u);

    EXPECT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{2, 2}));
    EXPECT_EQ(classificationOf(*backend, layout, ns), 2);
    for (uint64_t i = 1; i <= 4; ++i)
        EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(i)), 1) << "blob " << i;
}

/// Two chained seals in ONE round, the middle epoch entirely EMPTY (its seal is its only record, at
/// sequence 1, carrying `prev_epoch_seal`). The hint omits that whole epoch, so the only way to reach it
/// is the back-chain: the record the hint DOES show names the seal that must be consumed first.
TEST(CASGCArithmeticIntake, ChainedEmptyEpochSealsBothConsumedInOneRound)
{
    auto backend = std::make_shared<HintHoleBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    writeSealAt(*backend, layout, ns, RefTxnId{1, 2});
    writeSealAt(*backend, layout, ns, RefTxnId{2, 1}, /*prev_epoch_seal=*/RefTxnId{1, 2});
    publishAt(*backend, layout, ns, RefTxnId{3, 1}, "ref_2", 2, DB::UInt128(2),
              /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{2, 1});
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{3, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{2, 1},
    });

    backend->hide(layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{2, 1}));

    const auto intake = runRoundAndReadIntakeMetrics(store);
    ASSERT_FALSE(intake.empty()) << "no fold_ref_intake row";
    ASSERT_GT(backend->holesServed(), 0u);

    EXPECT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{3, 1}));
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(1)), 1);
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(2)), 1);

    /// THE ASSERTION THAT MAKES THIS TEST ABOUT THE BACK-CHAIN. Every check above is also satisfied by
    /// an id-ordered walk over the listed keys — which is why this case passed before the change. Two
    /// crossings can only happen by consuming `seal{1,2}` and then `seal{2,1}`, and `{2,1}` is reachable
    /// only through `{3,1}`'s `prev_epoch_seal`, since the hint never mentions it.
    EXPECT_EQ(intake.at("epoch_crossings"), 2u)
        << "the walk must cross TWICE, through a hidden epoch it can only reach by the seal chain";
    EXPECT_EQ(intake.at("logs_accounted"), intake.at("logs_applied"));
    EXPECT_EQ(intake.at("logs_applied"), 4u) << "two records and two seals, all applied";
    /// The checkpoint names the complete, authoritative frontier, so the bounded walk has no absent
    /// probes to perform. The hidden epoch remains reachable only through the seal chain.
    EXPECT_EQ(intake.at("absent_probes"), 0u);
}

/// A round that ends ON a seal (nothing above it yet) leaves the cursor there. The NEXT round must
/// still cross into the epoch that appears later -- the cursor sitting on a closed epoch's seal is the
/// ordinary steady state after a writer-epoch change, not a wedge.
TEST(CASGCArithmeticIntake, CursorRestingOnSealCrossesInALaterRound)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    writeSealAt(*backend, layout, ns, RefTxnId{1, 2});
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{1, 2},
    });

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    ASSERT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{1, 2})) << "the round consumed the seal";

    publishAt(*backend, layout, ns, RefTxnId{2, 1}, "ref_2", 2, DB::UInt128(2),
              /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{1, 2});
    advanceRecoverableCkptForRawFixture(*backend, layout, ns, RefTxnId{2, 1});

    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    EXPECT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{2, 1}));
    EXPECT_EQ(classificationOf(*backend, layout, ns), 2);
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(2)), 1);
}

/// ===================== IMPOSSIBLE SHAPES HOLD THE NAMESPACE =====================
///
/// `{1,3}` is genuinely absent while `{1,4}` is present AND listed. Contiguity says that cannot happen,
/// so whatever sits behind the gap may be an acked `+1`: the namespace is held at classification 4 with
/// its cursor UNMOVED, rather than sealing past the gap.
///
/// Listing-driven intake folded `{1,4}` and sealed the cursor at it -- permanently, since a record below
/// the cursor is never re-read.
TEST(CASGCArithmeticIntake, GapBelowWitnessHoldsNamespaceAtClassificationFour)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    publishAt(*backend, layout, ns, RefTxnId{1, 2}, "ref_2", 2, DB::UInt128(2));
    /// {1,3} is never written -- the record that vanished.
    publishAt(*backend, layout, ns, RefTxnId{1, 4}, "ref_4", 4, DB::UInt128(4));
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 4},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    EXPECT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{1, 2}))
        << "the cursor must not advance past a gap";
    EXPECT_EQ(classificationOf(*backend, layout, ns), 4);
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(4)), 0)
        << "the record above the gap was not folded";
}

/// A record of a LATER epoch reachable while the current epoch's seal was never consumed: the crossing
/// has no proof (the later epoch's `prev_epoch_seal` names a seal this cursor never reached), so the
/// namespace holds instead of jumping the boundary.
TEST(CASGCArithmeticIntake, UnconsumedSealCrossingHoldsNamespace)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    /// Epoch 1's missing `{1,2}` seal is the exact position epoch 2 claims to chain from. With no
    /// same-epoch witness above it, the later epoch is the only witness and the crossing must hold.
    publishAt(*backend, layout, ns, RefTxnId{2, 1}, "ref_2", 2, DB::UInt128(2),
              /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{1, 2});
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{2, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{1, 2},
    });

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    EXPECT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{1, 1}));
    EXPECT_EQ(classificationOf(*backend, layout, ns), 4);
    const auto coverage = coverageOf(*backend, layout, ns);
    ASSERT_TRUE(coverage && coverage->hold.has_value());
    EXPECT_EQ(coverage->hold->reason, HoldReason::UnconsumedSealCrossing);
    EXPECT_EQ(coverage->hold->offending_position, (RefTxnId{1, 2}));
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(2)), 0);
}

/// The back-chain proves the IDENTITY of the position an epoch chains from; it does not, by itself,
/// prove that position is a SEAL. Here a writer names an ordinary record (`{1,2}`, epoch 1's last
/// record, never sealed) as `{2,1}`'s `prev_epoch_seal`. Identity matches, so a chain-only check would
/// grant the crossing and declare epoch 1 closed while its writer may still be appending -- any later
/// `{1,k}` would then land permanently below the cursor, which is exactly the damage the seal exists to
/// prevent. The walk applied that record itself this round, so it knows its kind for free: refuse.
TEST(CASGCArithmeticIntake, CrossingFromANonSealRecordIsRefusedEvenWhenTheChainMatches)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    publishAt(*backend, layout, ns, RefTxnId{1, 2}, "ref_2", 2, DB::UInt128(2));
    /// `{1,2}` is an ordinary published record, NOT an `EpochSeal` -- and epoch 2 chains to it anyway.
    publishAt(*backend, layout, ns, RefTxnId{2, 1}, "ref_3", 3, DB::UInt128(3),
              /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{1, 2});
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{2, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{1, 2},
    });

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    EXPECT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{1, 2}))
        << "epoch 1 was never sealed, so the cursor may not leave it";
    EXPECT_EQ(classificationOf(*backend, layout, ns), 4);
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(1)), 1) << "epoch 1's records still fold";
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(2)), 1);
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(3)), 0)
        << "the record beyond the unsealed boundary must NOT be folded";
}

/// The crossing reads the epoch-start record to prove the chain, and the walk then reads it again to
/// fold it. A record that answers the first read and not the second would make the next iteration
/// re-derive the SAME crossing from the same unchanged cursor and resolve to the same position -- an
/// infinite spin inside one namespace's walk. The strict-progress guard turns that into a hold.
///
/// The fixture is the only shape that reaches it: a key that alternates present/absent across reads, so
/// `crossFromSeal` keeps succeeding while the walk's own GET keeps failing.
TEST(CASGCArithmeticIntake, EpochStartThatAnswersOnlyEveryOtherReadHoldsInsteadOfSpinning)
{
    /// Answers `flaky` on odd-numbered reads and 404s on even ones. Nothing else is disturbed.
    class AlternatingGetBackend : public InMemoryBackend
    {
    public:
        using DB::Cas::Backend::get;
        String flaky;
        size_t reads = 0;

        std::optional<GetResult> get(const String & key, Range range) override
        {
            if (key == flaky && ++reads % 2 == 0)
                return std::nullopt;
            return InMemoryBackend::get(key, range);
        }
    };

    auto backend = std::make_shared<AlternatingGetBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    writeSealAt(*backend, layout, ns, RefTxnId{1, 2});
    publishAt(*backend, layout, ns, RefTxnId{2, 1}, "ref_2", 2, DB::UInt128(2),
              /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{1, 2});
    /// A THIRD epoch is what keeps the unstable position from reading as a frontier: without a witness
    /// strictly above it, an absent `{2,1}` is just "the namespace ends here" and the walk stops
    /// normally. With `{3,1}` listed, the walk must keep trying to cross -- and the chain from `{3,1}`
    /// leads back to `{2,1}` every time, which is the spin.
    publishAt(*backend, layout, ns, RefTxnId{3, 1}, "ref_3", 3, DB::UInt128(3),
              /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{2, 1});
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{3, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{2, 1},
    });

    /// Arm only after seeding, so the fixture's own writes are undisturbed and the read counter starts
    /// at the round's first read of this key (`crossFromSeal`'s, which must succeed).
    backend->flaky = layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{2, 1});

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);   /// must RETURN -- the spin is the failure mode
    ASSERT_GE(backend->reads, 3u)
        << "the crossing must have re-proved the same position after the walk failed to read it; "
           "fewer reads means the guard was never reached";

    EXPECT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{1, 2}))
        << "the cursor stops on the seal it consumed and never enters the unstable epoch";
    EXPECT_EQ(classificationOf(*backend, layout, ns), 4);
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(2)), 0);
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(3)), 0)
        << "nothing above the unstable position may be folded either";
}

/// ===================== A PER-NAMESPACE FAILURE IS NOT A ROUND FAILURE =====================
///
/// Spec §5 narrows the whole-round abort to a key that cannot be attributed to any namespace. An
/// undecodable BODY belongs to exactly one namespace, so it clamps that namespace and nothing else:
/// `ns_a` holds at its last good record while `ns_b` folds and seals normally in the same round.
TEST(CASGCArithmeticIntake, CorruptBodyClampsOneNamespaceWhileAnotherFolds)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns_a{"00/aa@cas@"};
    fixture::admitLive(*backend, store->layout(), ns_a);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch
    const RootNamespace ns_b{"00/bb@cas@"};

    publishAt(*backend, layout, ns_a, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    backend->putIfAbsent(layout.refLogKey(fixture::fixtureLife(ns_a), RefTxnId{1, 2}), "this is not a cas_ref_log object");
    writeRecoverableCkptForRawFixture(*backend, layout, ns_a, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    publishAt(*backend, layout, ns_b, RefTxnId{1, 1}, "ref_1", 11, DB::UInt128(11), /*birth=*/true);
    publishAt(*backend, layout, ns_b, RefTxnId{1, 2}, "ref_2", 12, DB::UInt128(12));
    writeSealAt(*backend, layout, ns_b, RefTxnId{1, 3});
    writeRecoverableCkptForRawFixture(*backend, layout, ns_b, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 3},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{1, 3},
    });

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    EXPECT_EQ(cursorOf(*backend, layout, ns_a), (RefTxnId{1, 1}));
    EXPECT_EQ(classificationOf(*backend, layout, ns_a), 4);

    EXPECT_EQ(cursorOf(*backend, layout, ns_b), (RefTxnId{1, 3}))
        << "a sibling namespace's corrupt body must not stop this one";
    EXPECT_EQ(classificationOf(*backend, layout, ns_b), 2);
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(11)), 1);
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(12)), 1);
}

/// ===================== PROBE B1 OVER AN ARITHMETIC CUT =====================
///
/// `logs_accounted` is recomputed from the SEALED cut -- the arithmetic distance the round's cursors
/// claim to cover -- and compared with the count the walk incremented once per applied record. The two
/// can only differ if a cursor moved over a position nothing applied, which is precisely the damage
/// listing-driven intake used to do silently. The identity must survive both a hint hole (positions
/// applied that the listing never mentioned) and a seal crossing (an applied no-op, and a cut that
/// spans two epochs).
TEST(CASGCArithmeticIntake, B1IdentityHoldsOverAHoleyCutThatCrossesASeal)
{
    auto backend = std::make_shared<HintHoleBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    publishAt(*backend, layout, ns, RefTxnId{1, 2}, "ref_2", 2, DB::UInt128(2));
    writeSealAt(*backend, layout, ns, RefTxnId{1, 3});
    publishAt(*backend, layout, ns, RefTxnId{2, 1}, "ref_3", 3, DB::UInt128(3),
              /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{1, 3});
    publishAt(*backend, layout, ns, RefTxnId{2, 2}, "ref_4", 4, DB::UInt128(4));
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{2, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{1, 3},
    });

    backend->hide(layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 2}));

    const auto intake = runRoundAndReadIntakeMetrics(store);
    ASSERT_FALSE(intake.empty()) << "no fold_ref_intake row";
    ASSERT_GT(backend->holesServed(), 0u);

    EXPECT_EQ(intake.at("logs_accounted"), intake.at("logs_applied"));
    EXPECT_EQ(intake.at("logs_applied"), 5u)
        << "four records and the seal: the seal is APPLIED, as a no-op";
    EXPECT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{2, 2}));
}

/// A namespace the hint omits ENTIRELY still folds through the checkpoint's authoritative frontier.
/// Every log key remains readable by exact key, and the cursor reaches `{1,3}` despite the empty hint.
/// When the hint reappears, it changes neither the cursor nor the owner edges.
TEST(CASGCArithmeticIntake, WhollyOmittedNamespaceFoldsThroughAuthoritativeCheckpoint)
{
    auto backend = std::make_shared<HintHoleBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    for (uint64_t i = 1; i <= 3; ++i)
        publishAt(*backend, layout, ns, RefTxnId{1, i}, "ref_" + std::to_string(i), i,
                  DB::UInt128(i), /*birth=*/i == 1);
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 3},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });
    for (uint64_t i = 1; i <= 3; ++i)
        backend->hide(layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, i}));

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    const auto hidden_cov = coverageOf(*backend, layout, ns);
    ASSERT_TRUE(hidden_cov.has_value())
        << "the namespace is `Live` in the catalog, so it stays in the universe even fully hidden";
    EXPECT_EQ(hidden_cov->classification, 2) << "the checkpoint's frontier is folded by exact key";
    EXPECT_EQ(hidden_cov->last_folded_ref_id, (RefTxnId{1, 3}));

    /// The store stops lying: the already folded namespace reappears.
    backend->revealAll();
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    EXPECT_EQ(cursorOf(*backend, layout, ns), (RefTxnId{1, 3}));
    for (uint64_t i = 1; i <= 3; ++i)
        EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(i)), 1) << "blob " << i;
}
