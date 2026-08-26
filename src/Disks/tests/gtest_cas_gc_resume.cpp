#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include "cas_test_helpers.h"

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace DB::ErrorCodes
{
extern const int ABORTED;
}

namespace
{
const UInt128 kGc = hexToU128("00000000000000000000000000000001");
ManifestRef ref(uint64_t seq, uint64_t inst)
{
    return ManifestRef{.writer_epoch = 1, .build_sequence = seq, .manifest_ordinal = static_cast<uint32_t>(inst)};
}
bool blobExists(InMemoryBackend & b, const Layout & layout, const UInt128 & hash)
{
    return b.head(layout.blobKey(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hash)})).exists;
}

/// Whether the CURRENT retired list (any gc-shard) still holds an entry.
bool anyRetiredPending(const PoolPtr & s)
{
    /// Retired-in-snapshot (T4): condemned state rides the adopted fold seal's kCondemned rows, not a
    /// separate retired list — reconstruct the in-flight set from the seal.
    return anyCondemnedInSeal(s->backend(), s->layout());
}

/// Drive regular GC to a fixpoint over the ACK-FLOOR round (renew the store's mount ack after each round;
/// stay alive while any work counter is nonzero OR an in-flight retired entry remains).
size_t runGcToFixpoint(const PoolPtr & s, Gc & gc, size_t max_rounds = 64)
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
        if (no_work && !anyRetiredPending(s))
            break;
    }
    return rounds;
}

/// A backend that denies ONCE the SINGLE round-commit `gc/state` CAS — the casPut that advances
/// snap_generation (the one-pass round has exactly one such CAS; the lease-acquire CAS does not advance
/// snap_generation). A denied round leaves only never-adopted attempt-scoped debris (fold seal / retired
/// list under an attempt gc/state never adopted); a fresh-attempt rerun is idempotent.
class InterruptRoundCasBackend : public InMemoryBackend
{
public:
    explicit InterruptRoundCasBackend(String gc_state_key_) : gc_state_key(std::move(gc_state_key_)) {}

    CasResult casPut(const String & key, const String & bytes, const std::optional<Token> & expected,
                     const ObjectMeta & meta) override
    {
        if (arm_interrupt && key == gc_state_key)
        {
            const auto stored = get(key);
            const uint64_t stored_gen = stored ? decodeGcState(stored->bytes).snap_generation : 0;
            const uint64_t next_gen = decodeGcState(bytes).snap_generation;
            if (next_gen > stored_gen)
            {
                arm_interrupt = false;   /// one-shot: only depose the first round-commit CAS
                throw DB::Exception(DB::ErrorCodes::ABORTED,
                    "test-injected: round-commit gc/state CAS denied (leader deposed mid-round)");
            }
        }
        return InMemoryBackend::casPut(key, bytes, expected, meta);
    }

    bool arm_interrupt = false;

private:
    String gc_state_key;
};
}

/// (`CASGCRound.TrimDropsFoldedOwnerEvents` was removed with the snapshot+log ref model: it asserted GC
/// trims folded owner events out of a MUTABLE shard journal in place. Immutable `_log` objects are never
/// trimmed in place; the new-model equivalent -- ref-object cleanup deletes a covered `_log`/`_snap` key
/// once BOTH the durable cursor AND a checkpoint-named validated recovery triple cover it -- is exercised in
/// `gtest_cas_ref_gc.cpp` (`RefObjectCleanupRetainsCheckpointNamedTriple`).)

/// A crashed round leaves only never-adopted attempt-scoped debris (there is no resume machinery in the
/// one-pass round). A fresh Gc simply re-runs the round under a fresh attempt and the deletion pipeline
/// converges idempotently — a delete that already landed replays onto NotFound.
TEST(CASGCReplay, FreshAttemptRerunCompletes)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);
    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);
    store->renewWatermarkOnce();
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);

    // Drive the ack-floor pipeline to a fixpoint: the blob condemns, graduates, then is deleted. Every
    // step is exact-token / write-once, so a replay is idempotent (the unit oracle for the crash-replay rule).
    runGcToFixpoint(store, gc);
    EXPECT_FALSE(blobExists(*backend, store->layout(), DB::UInt128(1)));

    // Re-running again is a clean no-op (idempotent): the blob stays gone, no throw.
    EXPECT_NO_THROW(runRegularRoundReclaiming(gc));
    EXPECT_FALSE(blobExists(*backend, store->layout(), DB::UInt128(1)));
}

/// Crash-replay idempotence: a round is deposed at its SINGLE round-commit CAS (lease lost mid-round),
/// leaving only never-adopted attempt-scoped debris (a fold seal + retired list under an attempt gc/state
/// never adopted). A SECOND leader (different id => a fresh lease.seq, hence a fresh attempt) re-runs the
/// round from scratch and completes: no wedge, no CORRUPTED_DATA, and the prior-round artifacts under the
/// old (unadopted) attempt are simply unreferenced. The pool drains to a fixpoint.
TEST(CASGCReplay, DeposedRoundRerunsUnderFreshAttempt)
{
    auto backend = std::make_shared<InterruptRoundCasBackend>(/*gc_state_key*/ "p/gc/state");
    auto store = openPoolForTest(backend);
    ASSERT_EQ(store->layout().gcStateKey(), "p/gc/state");   // guard the injected key against layout drift
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    // First leader folds + adopts the first (snap_generation, snap_attempt).
    Gc gc1(store, hexToU128("00000000000000000000000000000001"));
    runRegularRoundReclaiming(gc1);
    store->renewWatermarkOnce();
    const auto after_fold = decodeGcState(backend->get(store->layout().gcStateKey())->bytes);
    ASSERT_EQ(after_fold.snap_attempt, after_fold.lease.seq);
    ASSERT_GT(after_fold.snap_generation, 0u);

    // Drop the only ref, then drive the round whose single commit CAS is DENIED (leader deposed mid-round).
    // The round folded under a FRESH attempt and published its fold seal + retired list under that attempt,
    // but the commit never adopted them — pure unadopted debris. gc/state is unchanged.
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    backend->arm_interrupt = true;
    EXPECT_THROW(runRegularRoundReclaiming(gc1), DB::Exception);
    backend->arm_interrupt = false;

    const auto after_interrupt = decodeGcState(backend->get(store->layout().gcStateKey())->bytes);
    EXPECT_EQ(after_interrupt.snap_generation, after_fold.snap_generation)
        << "the denied round-commit CAS must NOT advance the adopted generation";
    EXPECT_EQ(after_interrupt.snap_attempt, after_fold.snap_attempt)
        << "the denied round-commit CAS must NOT advance the adopted attempt";
    // The deposed round's fold seal is durable under its OWN (unadopted) attempt — unreferenced by gc/state.
    const uint64_t deposed_attempt = after_fold.lease.seq + 1;   // round 2 renewed the lease once
    const uint64_t deposed_gen = after_fold.snap_generation + 1;
    EXPECT_TRUE(backend->head(store->layout().foldSealKey(deposed_gen, deposed_attempt)).exists)
        << "the deposed round's fold seal is durable under its own unadopted attempt (harmless debris)";

    // A DIFFERENT leader takes over. The lease steal protocol observes the stalled lease twice before
    // stealing: the first round only observes and defers; the second steals and re-runs the round from
    // scratch under its own fresh attempt.
    Gc gc2(store, hexToU128("00000000000000000000000000000002"));
    EXPECT_NO_THROW(runRegularRoundReclaiming(gc2));   // observe-and-defer (lease not yet provably stalled)
    store->renewWatermarkOnce();

    // From here gc2 owns the lease; drive it to a fixpoint. It must drain the unreachable blob WITHOUT
    // wedging on the deposed attempt's debris (attempt-scoping keeps that debris invisible).
    EXPECT_NO_THROW(runGcToFixpoint(store, gc2));
    EXPECT_FALSE(blobExists(*backend, store->layout(), DB::UInt128(1)));

    const auto after_drain = decodeGcState(backend->get(store->layout().gcStateKey())->bytes);
    EXPECT_GT(after_drain.snap_generation, after_fold.snap_generation) << "the round completed under gc2";
    EXPECT_NE(after_drain.snap_attempt, deposed_attempt) << "the drained round never adopted the deposed attempt";

    // A further round is a clean no-op.
    EXPECT_NO_THROW(runRegularRoundReclaiming(gc2));
    EXPECT_FALSE(blobExists(*backend, store->layout(), DB::UInt128(1)));
}
