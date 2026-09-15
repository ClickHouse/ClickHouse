#include <gtest/gtest.h>

#include <optional>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcShardPlan.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Common/ProfileEvents.h>
#include "cas_test_helpers.h"
#include "config.h"

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CORRUPTED_DATA;
extern const int ABORTED;
extern const int NETWORK_ERROR;
}

namespace ProfileEvents
{
extern const Event CASGCMetaOps;
extern const Event CASGCEnumerationPages;
extern const Event CASMountExclusivityViolation;
extern const Event CASGCRetiredSpared;
}

using namespace DB::Cas;
using namespace DB::Cas::tests;

/// ROUND-LEVEL end-to-end GC tests over the root-local part-manifest model (one-pass ack-floor round:
/// heartbeat floor -> fold with the three-cursor merge -> pre-CAS exact-token deletes -> single CAS -> trim).
///
/// This file is the survivor of the old snap/cascade-based `gtest_cas_gc_round.cpp`. The per-STEP
/// behaviours it used to cover have moved to the dedicated GC-core suites and are intentionally NOT
/// re-tested here:
///   - fold edge dispatch (committed/precommit/promote/removal +/-1, 404 clamp/anomaly, fold barrier,
///     ref-mismatch fail-closed) -> gtest_cas_gc_fold.cpp
///   - condemn/graduate/delete + spare (manifest body deferred delete, publish racing the pass is spared,
///     unreferenced blob exact-token delete) -> gtest_cas_gc_ack_floor.cpp
///   - trim of folded owner events + idempotent crash replay -> gtest_cas_gc_resume.cpp
/// What remains here is what those step suites do NOT cover: the LEASE/leadership protocol (the round's
/// only stateful concurrency), the cursor-key codec, and the multi-round END-TO-END reclaim scenarios
/// driven to fixpoint (publish->drop->reclaim, multi-ref sharing, spare-on-recheck race, idempotent
/// fixpoint, split-brain duplicate-work-only). Every kept test keeps STRONG no-loss / no-dangle / no-leak
/// assertions. No test sleeps or reads a clock — "time" is the order of `runRegularRound` calls.

namespace
{

const UInt128 kGc = hexToU128("00000000000000000000000000000001");
const UInt128 kGcA = hexToU128("0000000000000000000000000000000a");
const UInt128 kGcB = hexToU128("0000000000000000000000000000000b");
const UInt128 kGcC = hexToU128("0000000000000000000000000000000c");

ManifestRef ref(uint64_t seq, uint64_t inst)
{
    return ManifestRef{.writer_epoch = 1, .build_sequence = seq, .manifest_ordinal = static_cast<uint32_t>(inst)};
}

std::optional<DB::Cas::Object> readOf(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    return (*op).read(key, Retry::standard());
}

bool headExists(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    return (*op).head(key, Retry::standard()).has_value();
}

ListPage listOf(Backend & backend, const String & prefix, const String & cursor, size_t limit)
{
    OperationForTest op(backend);
    return (*op).list(prefix, cursor, limit, Retry::standard());
}

void createRaw(Backend & backend, const String & key, const String & bytes)
{
    OperationForTest op(backend);
    (*op).create(key, bytes, Retry::standard());
}

bool blobExists(InMemoryBackend & b, const Layout & layout, const UInt128 & hash)
{
    return headExists(b, layout.blobKey(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hash)}));
}

bool manifestExists(InMemoryBackend & b, const Layout & layout, const ManifestId & id)
{
    return headExists(b, layout.manifestKey(id));
}

PoolPtr openTestPool(std::shared_ptr<InMemoryBackend> & out_backend)
{
    out_backend = std::make_shared<InMemoryBackend>();
    return Pool::open(out_backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

PoolPtr openTestPoolWithConfig(std::shared_ptr<InMemoryBackend> & out_backend, PoolConfig config)
{
    out_backend = std::make_shared<InMemoryBackend>();
    return Pool::open(out_backend, std::move(config));
}

/// Fault decorator for triage #5's regression test (`CASGCRetention.LosingRoundNeverDestroysParentSealGeneration`
/// below): the `fail_at_call`-th `casPut` against `faulted_key` returns `Conflict` instead of committing —
/// deterministically and single-threaded reproducing "this round's own gc/state CAS lost the race to a
/// concurrent leader," which is the only condition under which the pre-CAS wholesale prune's choice of
/// `referenced_generations` is externally observable (a round whose own CAS SUCCEEDS reclaims the same
/// generation moments later via the existing, unrelated post-CAS hand-off delete regardless of this fix,
/// so faulting the CAS is required, not optional, to pin the production call site). A call count, not a
/// one-shot arm flag: `Gc::acquireOrRenewLease` issues its OWN earlier `casPut` on the very same gc/state
/// key to renew the lease BEFORE a round folds — that renewal must SUCCEED (so the round actually reaches
/// the fold/prune it's meant to exercise), and only the round's LATER, final round-commit `casPut` must
/// be the one that loses. `fail_at_call` is 1-indexed and lets the test target that specific call exactly,
/// computed from `calls_to_faulted_key` observed so far rather than hardcoded.
class GcStateCasFaultBackend : public InMemoryBackend
{
public:
    std::expected<String, RawConflict> write(
        const String & key, const String & bytes, const std::optional<String> & expected_value,
        TransportAccess & access) override
    {
        if (expected_value && key == faulted_key)
        {
            ++calls_to_faulted_key;
            if (fail_at_call != 0 && calls_to_faulted_key == fail_at_call)
                return std::unexpected(RawConflict{});
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }

    String faulted_key;
    size_t calls_to_faulted_key = 0;
    /// 0 = never fault; else refuse exactly the Nth conditional write to `faulted_key`.
    size_t fail_at_call = 0;
};

GcState readState(InMemoryBackend & b, const Pool & s)
{
    const auto got = readOf(b, s.layout().gcStateKey());
    if (!got)
    {
        ADD_FAILURE() << "gc/state absent";
        return {};
    }
    return decodeGcState(got->bytes);
}

/// Whether ANY gc-shard's adopted-seal run still holds a `RunMarker::Condemned` row (the
/// retired state rides the snapshot run, not a separate retired-list object) — the ack-floor deletion
/// pipeline is still in flight while this is true.
bool anyRetiredPending(InMemoryBackend & b, const Pool & s)
{
    return anyCondemnedInSeal(b, s.layout());
}

/// Drive a Gc to fixpoint over the round-paced retired-cursor pipeline: run rounds, renewing the store's
/// own heartbeat after each (`renewWatermarkOnce` — keeps the lease + build-watermark floor current;
/// graduation itself paces on rounds alone). A condemned blob traverses the multi-round condemn ->
/// graduate -> delete pipeline, so "fixpoint" is reached only when a round did NO work AND the current
/// retired list is empty (nothing still in flight). Returns the number of rounds that held the lease and
/// did work. Bounded so a non-converging core fails downstream assertions rather than hanging.
size_t driveToFixpoint(InMemoryBackend & backend, const PoolPtr & store, Gc & gc)
{
    size_t working_rounds = 0;
    for (size_t r = 0; r < 64; ++r)
    {
        const RoundReport rep = runRegularRoundReclaiming(gc);
        if (!rep.acquired_lease)
            continue;
        store->renewWatermarkOnce();
        const bool no_work = rep.candidates == 0 && rep.deleted == 0 && rep.absent == 0
            && rep.replaced == 0 && rep.spared == 0;
        if (no_work && !anyRetiredPending(backend, *store))
            break;
        if (!no_work)
            ++working_rounds;
    }
    return working_rounds;
}

/// A full key -> incarnation snapshot of the backend, for the previewDeletes write-free invariant: any
/// write mints a fresh incarnation (or adds a key) and any removal drops one, so an unchanged map
/// across a call proves it performed NO writes.
std::map<String, String> snapshotKeyTokens(CasOperation & op)
{
    std::map<String, String> out;
    String cursor;
    while (true)
    {
        const ListPage page = op.list("", cursor, 100000, Retry::once());
        for (const ListedKey & k : page.keys)
            out[k.key] = k.etag ? k.etag->render() : String{};
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }
    return out;
}

}

/// ---- LEASE / leadership protocol (the round's only stateful concurrency) ----
///
/// The lease steal window is observation-based and deterministic (see CasGc.h): a contender becomes
/// steal-eligible when it observes the SAME (owner, seq) across two of its own consecutive round
/// attempts. The new model keeps gc/state {round, snap_generation, lease}, so these tests
/// are model-agnostic and were ported verbatim from the pre-redesign suite.

TEST(CASGCLease, FreshPoolAcquiresAndRenews)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    Gc gc(s, kGc);

    EXPECT_TRUE(gc.runRegularRound().acquired_lease);
    const GcState st1 = readState(*b, *s);
    EXPECT_EQ(st1.lease.owner, kGc);
    const uint64_t seq1 = st1.lease.seq;
    EXPECT_GE(seq1, 1u);

    EXPECT_TRUE(gc.runRegularRound().acquired_lease);            /// renew
    const GcState st2 = readState(*b, *s);
    EXPECT_EQ(st2.lease.owner, kGc);
    EXPECT_GT(st2.lease.seq, seq1);                              /// seq strictly advanced
}

TEST(CASGCLease, ContenderBacksOffWhileIncumbentRenews)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    Gc gc1(s, kGcA);
    Gc gc2(s, kGcB);

    ASSERT_TRUE(gc1.runRegularRound().acquired_lease);
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);          /// first sight: record observation
    ASSERT_TRUE(gc1.runRegularRound().acquired_lease);           /// incumbent renews (seq advances)
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);          /// gc2 sees a NEW seq => incumbent alive
    ASSERT_TRUE(gc1.runRegularRound().acquired_lease);
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);          /// alive again - never steals while renewing
    EXPECT_EQ(readState(*b, *s).lease.owner, kGcA);
}

TEST(CASGCLease, StealAfterObservedNonRenewalAdvancesLease)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    Gc gc1(s, kGcA);
    Gc gc2(s, kGcB);

    ASSERT_TRUE(gc1.runRegularRound().acquired_lease);
    const GcState st0 = readState(*b, *s);
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);          /// observation recorded; gc1 then DIES
    EXPECT_TRUE(gc2.runRegularRound().acquired_lease);           /// same (owner, seq) observed twice => steal
    const GcState st = readState(*b, *s);
    EXPECT_EQ(st.lease.owner, kGcB);
    EXPECT_GT(st.lease.seq, st0.lease.seq);
}

TEST(CASGCLease, HeartbeatBlocksFalseStealOfAliveLeader)
{
    /// B160: a slow-but-alive incumbent whose lease.seq is frozen for its (long) round must NOT be
    /// stolen from, because its advisory heartbeat keeps advancing.
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    Gc gc1(s, kGcA);
    Gc gc2(s, kGcB);

    ASSERT_TRUE(gc1.runRegularRound().acquired_lease);          /// gc1 leads (seq frozen for its round)
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);         /// gc2 observes (gc/hb absent yet)

    Gc::pulseHeartbeat(*s, kGcA);                              /// gc1 mid-round but heartbeating (hb 0->1)
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);         /// hb advanced => alive => NO steal
    Gc::pulseHeartbeat(*s, kGcA);                              /// hb 1->2
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);         /// still no steal while heartbeating
    EXPECT_EQ(readState(*b, *s).lease.owner, kGcA);            /// gc1 still owns the lease
}

/// A7-HIGH-fix follow-up (residual timing window): an allow_steal=false observation of a foreign
/// incumbent (the manual `SYSTEM ... GC` path) must NOT arm the frozen-tuple comparison that the loop's
/// very next (allow_steal=true) call uses to decide whether to steal. Without this, a manual round's
/// observation at time t, immediately followed by an unluckily-timed scheduled tick at t+epsilon (no
/// real chance for a live incumbent to heartbeat in between), would see the SAME (owner, seq, hb) twice
/// and steal a LIVE leader — exactly the hazard the allow_steal gate alone does not close, since it only
/// stops the MANUAL call itself from executing the steal CAS, not from contaminating the shared `Gc`
/// instance's observation state that the next allow_steal=true call reads.
TEST(CASGCLease, ManualObservationNeverArmsTheLoopsStealDecision)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    Gc gc1(s, kGcA);
    Gc gc2(s, kGcB);   /// plays the scheduler's ONE shared Gc, observed by both manual and loop calls

    ASSERT_TRUE(gc1.runRegularRound().acquired_lease);           /// gc1 leads; never renews, never heartbeats

    /// Manual observation "at t": allow_steal=false. Would normally be obs #1, but must NOT record it.
    EXPECT_FALSE(gc2.runRegularRound({}, /*allow_steal=*/false).acquired_lease);

    /// Loop-path call "immediately after" (allow_steal=true, the default): with the fix, gc2's
    /// last_seen_* is UNTOUCHED by the manual call above, so this is still effectively obs #1 (first
    /// sight) => must NOT steal. Pre-fix (manual observations armed the state), this would see the same
    /// frozen tuple as "twice observed" and steal gc1's still-live lease.
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);
    EXPECT_EQ(readState(*b, *s).lease.owner, kGcA);               /// gc1 keeps the lease

    /// The loop still recovers a genuinely dead incumbent across its OWN two spaced observations: the
    /// call above was the loop's real obs #1 (now armed, since allow_steal=true); this one is obs #2 of
    /// the same still-frozen tuple => steal-eligible => steals. Recovery is delayed, not disabled.
    EXPECT_TRUE(gc2.runRegularRound().acquired_lease);
    EXPECT_EQ(readState(*b, *s).lease.owner, kGcB);
}

/// P3-B1 (2026-07-11 mid-switch soak wedge): CasGcScheduler used to flip its `i_am_leader` flag (which
/// gates the heartbeat thread's pulses) only AFTER `runRegularRound` RETURNS, while the lease is
/// acquired INSIDE the round, before the (potentially long) fold. A brand-new leader's FIRST round
/// therefore ran the whole fold with no heartbeat cover: a follower observing the frozen (owner, seq)
/// across two of its own ticks steals deterministically once that first round outlasts ~2 ticks -
/// mutual-steal livelock under a slow fold. The fix moves the "start heartbeating" action to the
/// INSTANT the lease is acquired (`Gc::runRegularRound`'s new `on_lease_acquired` hook), fired before
/// the fold begins. These two tests pin the protocol both ways at the `Gc` level (the scheduler itself
/// only wires `i_am_leader.store(true, ...)` + one `pulseHeartbeat` call into that hook - a thread-pacing
/// wire-up not practically unit-testable without sleeps; verified by code review + the full gtest run).

TEST(CASGCLease, WithoutAcquireTimePulseFirstRoundStealsDeterministically)
{
    /// RED-before-the-fix scenario: gc1 acquires the lease and (simulating a long first round) never
    /// pulses `gc/hb` and never renews - exactly what happened before `on_lease_acquired` existed.
    /// gc2's SECOND observation of the same frozen (owner, seq, hb) steals, per the documented protocol.
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    Gc gc1(s, kGcA);
    Gc gc2(s, kGcB);

    ASSERT_TRUE(gc1.runRegularRound().acquired_lease);    /// gc1 becomes leader; NO pulse follows (the bug)
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);   /// obs #1: records (owner=A, seq, hb=absent)
    EXPECT_TRUE(gc2.runRegularRound().acquired_lease);    /// obs #2: unchanged => steal-eligible => STEALS
    EXPECT_EQ(readState(*b, *s).lease.owner, kGcB);
}

TEST(CASGCLease, AcquireTimePulseProtectsNewLeadersFirstRound)
{
    /// GREEN-after-the-fix scenario: with the fix, `i_am_leader` flips true and the FIRST pulse fires
    /// the instant gc1 acquires the lease - before B's first observation even happens - and the
    /// (separately-threaded, out of scope here) `heartbeatLoop` keeps landing further pulses on its own
    /// cadence for as long as `i_am_leader` stays true, i.e. for the whole duration of gc1's first round.
    /// The net effect proven here is the one that matters: SOME pulse lands between B's two
    /// observations (not just before both, and not only after both), so B's second observation sees hb
    /// advanced relative to its first and backs off instead of stealing - exactly what never happened
    /// pre-fix, when `i_am_leader` (and hence every pulse) was gated on the round having already returned.
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    Gc gc1(s, kGcA);
    Gc gc2(s, kGcB);

    ASSERT_TRUE(gc1.runRegularRound().acquired_lease);    /// gc1 becomes leader (still mid-fold, seq frozen)
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);   /// obs #1: records (owner=A, seq, hb=absent)
    Gc::pulseHeartbeat(*s, kGcA);                          /// a heartbeatLoop tick lands mid-round (hb 0->1)
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);   /// obs #2: hb advanced since obs #1 => alive => NO steal

    EXPECT_EQ(readState(*b, *s).lease.owner, kGcA);       /// gc1 keeps the lease through its whole first round
}

TEST(CASGCLease, StaleOwnerHeartbeatDoesNotEnableFalseSteal)
{
    /// A deposed leader's heartbeat thread keeps pulsing until its next round notices the lost lease
    /// (`i_am_leader` is only reset there), and `pulseHeartbeat` stamps `owner = self` while a losing
    /// CAS write silently vanishes — so a zombie old leader can keep `gc/hb.owner` pointing at ITSELF
    /// even while the live new leader is pulsing too. The liveness gate must therefore treat ANY
    /// movement of the observed (owner, hb_seq) pair between a follower's two ticks as "someone is
    /// alive": comparing hb_seq is only meaningful against the SAME remembered hb owner. The old
    /// predicate compared `hb.owner` with the LEASE owner instead, so a zombie-owned hb read as
    /// "not the leader's heartbeat" on both ticks and a live, pulsing new leader got its lease stolen.
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    Gc gc1(s, kGcA);
    Gc gc2(s, kGcB);
    Gc gc3(s, kGcC);

    /// gc1 leads, beats, then dies mid-round; gc2 legitimately steals the lease.
    ASSERT_TRUE(gc1.runRegularRound().acquired_lease);
    Gc::pulseHeartbeat(*s, kGcA);
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);   /// obs #1 of gc1's frozen tuple
    EXPECT_TRUE(gc2.runRegularRound().acquired_lease);    /// obs #2: frozen lease + frozen hb => steal
    ASSERT_EQ(readState(*b, *s).lease.owner, kGcB);

    /// gc2 is now mid-long-round (lease tuple frozen) and PULSING — but gc1's zombie heartbeat
    /// thread interleaves after every gc2 pulse, so the follower gc3 only ever OBSERVES gc1-owned
    /// heartbeats. The pair keeps moving, which is proof of life.
    Gc::pulseHeartbeat(*s, kGcB);
    Gc::pulseHeartbeat(*s, kGcA);                          /// zombie masks gc2's pulse
    EXPECT_FALSE(gc3.runRegularRound().acquired_lease);   /// obs #1: records (hb owner=A, seq)
    Gc::pulseHeartbeat(*s, kGcB);
    Gc::pulseHeartbeat(*s, kGcA);                          /// zombie masks again
    EXPECT_FALSE(gc3.runRegularRound().acquired_lease);   /// obs #2: hb pair MOVED => alive => NO steal
    EXPECT_EQ(readState(*b, *s).lease.owner, kGcB);       /// the live leader keeps its lease

    /// Liveness is preserved: once everything genuinely freezes (gc2 dead, zombie gone), the next
    /// tick completes the window — obs #2 above already re-armed on the now-frozen (lease, hb) pair.
    EXPECT_TRUE(gc3.runRegularRound().acquired_lease);    /// still frozen a full tick later => steal
    EXPECT_EQ(readState(*b, *s).lease.owner, kGcC);
}

TEST(CASGCLease, FailoverStealOnceHeartbeatStops)
{
    /// B160: once the incumbent stops heartbeating (it died), a follower observing the now-frozen
    /// heartbeat steals — automatic failover is preserved.
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    Gc gc1(s, kGcA);
    Gc gc2(s, kGcB);

    ASSERT_TRUE(gc1.runRegularRound().acquired_lease);
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);         /// obs #1
    Gc::pulseHeartbeat(*s, kGcA);                              /// one last pulse (hb 0->1)
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);         /// hb advanced => no steal; records hb=1
    /// gc1 now DEAD: no renew, no further pulse. hb stays at 1 == gc2's last observation.
    EXPECT_TRUE(gc2.runRegularRound().acquired_lease);          /// hb frozen + seq frozen => STEAL
    EXPECT_EQ(readState(*b, *s).lease.owner, kGcB);
}

TEST(CASGCLease, DeadIncumbentThenRevivedIncumbentWinsRace)
{
    /// A stalled incumbent that revives and renews BEFORE the contender's second look resets the
    /// contender's window: gc2's second observation sees a NEW seq => NOT steal-eligible => backs off.
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    Gc gc1(s, kGcA);
    Gc gc2(s, kGcB);

    ASSERT_TRUE(gc1.runRegularRound().acquired_lease);
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);          /// obs #1
    ASSERT_TRUE(gc1.runRegularRound().acquired_lease);           /// gc1 revives and renews
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);          /// new seq seen => window resets
    EXPECT_EQ(readState(*b, *s).lease.owner, kGcA);
}

/// The steal's refused precondition, case one of two: the re-decide sees the SAME frozen incumbent.
/// `readModifyWrite` does not treat a refusal as terminal -- it re-decides against what the refused
/// write's own resolve read observed and sends another attempt -- so a contender that was steal-eligible
/// still is, and the steal lands inside the SAME round. The two cases are separate tests because they
/// differ only in what the store holds at the re-decide, and that is the whole decision.
TEST(CASGCLease, RefusedStealAgainstAFrozenIncumbentRetriesAndLands)
{
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    Gc gc1(s, kGcA);
    Gc gc2(s, kGcB);

    ASSERT_TRUE(gc1.runRegularRound().acquired_lease);
    const GcState st0 = readState(*b, *s);
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);          /// obs #1; gc1 stalls now
    /// `refuseNextWrite` refuses without touching the object, which is what "the incumbent is still
    /// frozen" looks like to the re-decide.
    b->refuseNextWrite(s->layout().gcStateKey());
    EXPECT_TRUE(gc2.runRegularRound().acquired_lease)
        << "a refused steal against an unmoved tuple is retried inside the same call and lands";
    const GcState st1 = readState(*b, *s);
    EXPECT_EQ(st1.lease.owner, kGcB);
    EXPECT_GT(st1.lease.seq, st0.lease.seq);
}

/// Case two: the re-decide sees a MOVED tuple. This is what a real refused precondition means -- a
/// store refuses only what changed -- and the incumbent's own renewal is the change. The contender must
/// then decline rather than steal, because a moved tuple is proof of life, and it must leave the
/// incumbent's lease exactly as the incumbent wrote it.
TEST(CASGCLease, RefusedStealWhoseRedecideSeesAMovedTupleDeclines)
{
    class StealRaceBackend : public InMemoryBackend
    {
    public:
        std::expected<String, RawConflict> write(
            const String & key, const String & bytes, const std::optional<String> & expected_value,
            TransportAccess & access) override
        {
            if (arm && expected_value && key == gc_state_key)
            {
                const auto stored = InMemoryBackend::read(key, access);
                if (stored)
                {
                    arm = false;
                    /// The incumbent renews while this contender is deciding: bump its own `seq` under
                    /// its own owner, then refuse. Written before the refusal is returned, so the
                    /// resolve read the engine makes next observes the moved tuple.
                    GcState renewed = decodeGcState(stored->bytes);
                    ++renewed.lease.seq;
                    (void)InMemoryBackend::write(key, encodeGcState(renewed), stored->value, access);
                    return std::unexpected(RawConflict{});
                }
            }
            return InMemoryBackend::write(key, bytes, expected_value, access);
        }

        String gc_state_key;
        bool arm = false;
    };

    auto b = std::make_shared<StealRaceBackend>();
    auto s = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    b->gc_state_key = s->layout().gcStateKey();
    Gc gc1(s, kGcA);
    Gc gc2(s, kGcB);

    ASSERT_TRUE(gc1.runRegularRound().acquired_lease);
    const GcState st0 = readState(*b, *s);
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);          /// obs #1; gc1 stalls now
    b->arm = true;
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease)
        << "the re-decide sees a renewed incumbent, which is proof of life: decline, never steal";
    const GcState st1 = readState(*b, *s);
    EXPECT_EQ(st1.lease.owner, kGcA)
        << "gc2 wrote nothing that landed: a steal would have put kGcB here";
    EXPECT_EQ(st1.lease.seq, st0.lease.seq + 1)
        << "the only write that landed is the incumbent's injected renewal";
}

TEST(CASGCLease, CreateConflictReReadsWithinTheBound)
{
    /// The create-Conflict branch: a fresh pool where the create-if-absent write is refused (one-shot).
    /// `readModifyWrite` re-decides against the refused write's own resolve read, which still finds the
    /// key absent, so the second attempt creates and acquires. The retry policy's deadline is the bound.
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    Gc gc(s, hexToU128("0000000000000000000000000000000c"));

    b->refuseNextWrite(s->layout().gcStateKey());
    EXPECT_TRUE(gc.runRegularRound().acquired_lease);
    const GcState st = readState(*b, *s);
    EXPECT_EQ(st.lease.owner, hexToU128("0000000000000000000000000000000c"));
    EXPECT_EQ(st.lease.seq, 1u);
}

TEST(CASGCLease, CtorFailsClosedOnBadArguments)
{
    /// Guards: a null store and gc_id == 0 (reserved for "lease never held") are caller bugs.
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&] { Gc(nullptr, kGc); });
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&] { Gc(s, DB::UInt128(0)); });
}

TEST(CASGCLease, IncumbentRenewConflictRetriesOnceAndAcquires)
{
    /// The incumbent's own renew write is refused (one-shot). The re-decide sees our own ownership, so
    /// the renew is retried and acquires; the retry policy's deadline is the bound. Never
    /// acquired=true without a committed write — storage must carry the seq the SECOND attempt wrote.
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    Gc gc(s, hexToU128("0000000000000000000000000000000d"));

    ASSERT_TRUE(gc.runRegularRound().acquired_lease);            /// create: seq 1
    b->refuseNextWrite(s->layout().gcStateKey());                 /// inject: the renew CAS conflicts
    EXPECT_TRUE(gc.runRegularRound().acquired_lease);            /// re-read (still us) => retried once
    const GcState st = readState(*b, *s);
    EXPECT_EQ(st.lease.owner, hexToU128("0000000000000000000000000000000d"));
    EXPECT_EQ(st.lease.seq, 2u);                                 /// the committed retry's seq
}

TEST(CASGCLease, VanishedStateAfterObservationFailsClosed)
{
    /// gc/state is never legally deleted - absent AFTER a recorded observation proves an out-of-model
    /// deletion. Recreating a default state would reset round/cursors; the lease protocol
    /// must fail closed (CORRUPTED_DATA) instead.
    std::shared_ptr<InMemoryBackend> b;
    auto s = openTestPool(b);
    Gc gc1(s, kGcA);
    Gc gc2(s, kGcB);

    ASSERT_TRUE(gc1.runRegularRound().acquired_lease);
    EXPECT_FALSE(gc2.runRegularRound().acquired_lease);          /// gc2 records an observation

    OperationForTest wipe_op(*b);   /// out-of-model wipe (raw delete)
    const auto meta = (*wipe_op).head(s->layout().gcStateKey(), Retry::standard());
    ASSERT_TRUE(meta.has_value());
    ASSERT_EQ((*wipe_op).remove(s->layout().gcStateKey(), meta->etag, Retry::standard()), Removal::Removed);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { gc2.runRegularRound(); });
}

/// ---- END-TO-END round scenarios driven to fixpoint (the headline value of this file) ----

/// publish -> drop -> GC-to-fixpoint reclaim: a committed ref names a blob; after the ref is dropped,
/// the round protocol collects the blob (exact-token delete) AND the owner-removed manifest body, and a
/// further round is a clean no-op. The strongest no-loss/no-leak oracle: while the ref is live the blob
/// is NEVER touched; once dropped, BOTH the blob and the manifest are gone and nothing dangles.
TEST(CASGCRound, PublishDropReclaimsBlobAndManifestToFixpoint)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);
    const ManifestId id{ns, r};

    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    driveToFixpoint(*backend, store, gc);
    /// While live: the blob's in-degree is 1 and NOTHING is collected (no-loss).
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);
    EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(1)));
    EXPECT_TRUE(manifestExists(*backend, store->layout(), id));

    dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    driveToFixpoint(*backend, store, gc);
    /// After drop + fixpoint: the blob's only edge is gone, the blob is collected, the owner-removed
    /// manifest body is collected, and the in-degree generation reflects zero (no-leak / no-dangle).
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 0);
    EXPECT_FALSE(blobExists(*backend, store->layout(), DB::UInt128(1)));
    EXPECT_FALSE(manifestExists(*backend, store->layout(), id));

    /// Idempotent: re-running to fixpoint changes nothing and never throws.
    EXPECT_NO_THROW(driveToFixpoint(*backend, store, gc));
    EXPECT_FALSE(blobExists(*backend, store->layout(), DB::UInt128(1)));
}

/// After a round condemns one blob, the ADOPTED fold seal's per-shard
/// condemned_summary reflects it (condemned_total == 1, pending_total == 0) — distilled zero-I/O from the
/// RunMarker::Condemned rows the fold sealed into the snapshot run.
TEST(CASGCRound, CondemnRoundSealSummaryCountsCondemned)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    gc.runRegularRound();                 /// folds the +1
    store->renewWatermarkOnce();
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);   /// the -1 condemns it

    /// Drive rounds until the blob shows up condemned in the adopted-seal run; capture that seal.
    bool condemned = false;
    CasFoldSeal seal;
    for (int i = 0; i < 6 && !condemned; ++i)
    {
        gc.runRegularRound();
        store->renewWatermarkOnce();
        const GcState st = readState(*backend, *store);
        seal = decodeFoldSeal(
            readOf(*backend, store->layout().foldSealKey(st.snap_generation, st.snap_attempt))->bytes);
        for (const RetiredEntry & e : currentRetiredSet(*backend, store->layout(), /*shard*/0))
            if (e.ref == DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(DB::UInt128(1))})
                condemned = true;
    }
    ASSERT_TRUE(condemned) << "blob never condemned into the snapshot run";
    ASSERT_TRUE(seal.condemned_summary.contains(0)) << "seal summary must be total over gc_shards";
    EXPECT_EQ(seal.condemned_summary.at(0).condemned_total, 1u);
    EXPECT_EQ(seal.condemned_summary.at(0).pending_total, 0u)
        << "a freshly condemned entry is not yet delete_pending";
    EXPECT_LT(seal.condemned_summary.at(0).oldest_nonpending_condemn_round,
              std::numeric_limits<uint64_t>::max())
        << "a non-pending condemned entry records its condemn round";
}

/// retired-in-snapshot T5: `previewDeletes` streams the adopted seal's `RunMarker::Condemned` rows and reports each
/// with the STORED condemn-time token — `awaiting_graduation` while newly condemned, then `delete_pending`
/// once graduated, and NOTHING once the exact-token redelete has removed the blob. The preview performs no
/// HEAD on the condemned rows (the token is durable in-run) and is WRITE-FREE throughout (spec §5 req 1).
TEST(CASGCRound, PreviewReportsCondemnedRowsAndIsWriteFree)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);
    const UInt128 blob = DB::UInt128(1);
    writeBlobBody(*backend, store->layout(), blob);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);                 /// round 1: folds the +1; blob referenced
    EXPECT_TRUE(gc.previewDeletes().empty()) << "a live-referenced blob is never previewed for deletion";

    dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    runRegularRoundReclaiming(gc);                 /// condemning round: -1 => in-degree 0 => RunMarker::Condemned row (not pending)

    /// Write-free contract: a full key->incarnation snapshot must be identical across the call.
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const auto before = snapshotKeyTokens(op);
    const std::vector<Gc::PreviewEntry> awaiting = gc.previewDeletes();
    const auto after = snapshotKeyTokens(op);
    EXPECT_EQ(before, after) << "previewDeletes must perform NO writes";

    ASSERT_EQ(awaiting.size(), 1u) << "exactly the one condemned blob is previewed";
    EXPECT_EQ(awaiting[0].ref, (DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(blob)}));
    EXPECT_EQ(awaiting[0].key, store->layout().blobKey(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(blob)}));
    EXPECT_EQ(awaiting[0].reason, "awaiting_graduation");
    EXPECT_FALSE(awaiting[0].token.value.empty()) << "must carry the stored condemn-time incarnation";
    EXPECT_GT(awaiting[0].condemn_round, 0u) << "must carry the stored condemn round";

    runRegularRoundReclaiming(gc);                 /// graduation round: entry becomes delete_pending (blob still present)
    const std::vector<Gc::PreviewEntry> pending = gc.previewDeletes();
    ASSERT_EQ(pending.size(), 1u);
    EXPECT_EQ(pending[0].ref, (DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(blob)}));
    EXPECT_EQ(pending[0].reason, "delete_pending");
    EXPECT_FALSE(pending[0].token.value.empty());

    runRegularRoundReclaiming(gc);                 /// redelete round: exact-token delete; entry dropped; blob gone
    EXPECT_FALSE(blobExists(*backend, store->layout(), blob));
    EXPECT_TRUE(gc.previewDeletes().empty()) << "nothing to preview once the blob is redeleted";
}

/// A fully idle fold pure-carries every shard's authoritative rows verbatim. The parent is first made
/// non-vacuous with one live blob in each of two shards; the forced no-delta successor must preserve
/// both `blob_run` rows and the total `condemned` domain byte-for-byte.
TEST(CASGCRound, PureCarryRoundPreservesAuthoritativeShardRowsVerbatim)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .gc_shards = 2, .gc_fold_max_defer_rounds = 0});
    const RootNamespace ns{"00/aa@cas@"};

    Gc gc(store, kGc);

    const UInt128 shard0_blob{1};
    const UInt128 shard1_blob = (UInt128{1} << 64) | UInt128{1};
    ASSERT_EQ(blobShard(legacyMetaTestRef(shard0_blob), 2), 0u);
    ASSERT_EQ(blobShard(legacyMetaTestRef(shard1_blob), 2), 1u);

    const ManifestRef r0 = ref(1, 0xAA);
    const ManifestRef r1 = ref(2, 0xBB);
    writeBlobBody(*backend, store->layout(), shard0_blob);
    writeBlobBody(*backend, store->layout(), shard1_blob);
    writeManifestRaw(*backend, store->layout(), ns, r0, {blobEntryFor("a", shard0_blob)});
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("b", shard1_blob)});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl0", std::nullopt, r0);
    publishCommittedTransition(*backend, store->layout(), ns, "tbl1", std::nullopt, r1);
    gc.runRegularRound();
    const GcState st1 = readState(*backend, *store);
    const CasFoldSeal seal1 = decodeFoldSeal(
        readOf(*backend, store->layout().foldSealKey(st1.snap_generation, st1.snap_attempt))->bytes,
        store->layout(), /*gc_shards=*/2);

    /// No state changes after the parent. The zero defer bound forces an actual fold rather than DEFER,
    /// so every shard takes the production pure-carry path.
    gc.runRegularRound();
    const GcState st2 = readState(*backend, *store);
    const CasFoldSeal seal2 = decodeFoldSeal(
        readOf(*backend, store->layout().foldSealKey(st2.snap_generation, st2.snap_attempt))->bytes,
        store->layout(), /*gc_shards=*/2);

    /// TOTALITY: both seals carry a summary entry for every gc-shard.
    ASSERT_EQ(seal1.condemned_summary.size(), 2u);
    ASSERT_EQ(seal2.condemned_summary.size(), 2u);
    EXPECT_TRUE(seal1.condemned_summary.contains(0) && seal1.condemned_summary.contains(1));
    EXPECT_TRUE(seal2.condemned_summary.contains(0) && seal2.condemned_summary.contains(1));

    /// Capacity reserves one widest `blob_run` row per shard. Pin the production pure-carry seal to the
    /// authoritative grammar that makes that bound sufficient: at most one in-range canonical seq-0
    /// run per shard, beside exactly one `condemned` row for every shard.
    bool run_seen[2] = {false, false};
    ASSERT_EQ(seal1.blob_target_runs.size(), 2u);
    ASSERT_EQ(seal2.blob_target_runs.size(), 2u);
    for (const RunRef & run : seal2.blob_target_runs)
    {
        ASSERT_LT(run.shard, 2u);
        EXPECT_FALSE(run_seen[run.shard]);
        run_seen[run.shard] = true;
        const auto parsed = store->layout().parseBlobTargetRunKey(run.key);
        ASSERT_TRUE(parsed.has_value());
        EXPECT_EQ(parsed->shard, run.shard);
        EXPECT_EQ(parsed->generation, run.key_generation);
        EXPECT_EQ(parsed->seq, 0u);
    }
    EXPECT_TRUE(run_seen[0]);
    EXPECT_TRUE(run_seen[1]);
    for (uint64_t shard = 0; shard < 2; ++shard)
    {
        const auto parent_run = std::find_if(
            seal1.blob_target_runs.begin(), seal1.blob_target_runs.end(),
            [shard](const RunRef & run) { return run.shard == shard; });
        const auto carried_run = std::find_if(
            seal2.blob_target_runs.begin(), seal2.blob_target_runs.end(),
            [shard](const RunRef & run) { return run.shard == shard; });
        ASSERT_NE(parent_run, seal1.blob_target_runs.end());
        ASSERT_NE(carried_run, seal2.blob_target_runs.end());
        EXPECT_EQ(*carried_run, *parent_run);
    }

    /// VERBATIM CARRY: nothing was ever condemned, so every shard's summary is the zero entry, carried
    /// unchanged from parent to child across the fully idle fold.
    for (uint64_t shard = 0; shard < 2; ++shard)
    {
        EXPECT_EQ(seal2.condemned_summary.at(shard), seal1.condemned_summary.at(shard))
            << "shard " << shard << " summary must be carried verbatim from the parent seal";
        EXPECT_EQ(seal2.condemned_summary.at(shard).condemned_total, 0u);
    }
}

/// Attempt-scoping (B2): a fold seal planted under a NON-adopted attempt at the adopted generation
/// must be INVISIBLE to every reader. A deposed leader writes its fold seal under its own (unadopted)
/// `lease.seq`; that artifact lives at `foldSealKey(snap_generation, snap_attempt + k)` and no decision
/// path may resolve it. `previewDeletes` reads the in-degree generation strictly at the adopted
/// `(snap_generation, snap_attempt)`, so the decoy must not change its output and must not throw. This
/// is the implementation-level complement to the TLA+ `INV_ONLY_ADOPTED_VIEWABLE` gate.
TEST(CASGCRound, NonAdoptedAttemptSealIgnored)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    gc.runRegularRound();
    const GcState st = readState(*backend, *store);
    ASSERT_GT(st.snap_generation, 0u);

    /// Control preview BEFORE the decoy (previewDeletes is write-free, so the result is deterministic).
    const auto control = gc.previewDeletes();

    /// Plant a decoy fold seal under a DIFFERENT attempt at the SAME generation (a deposed leader's
    /// unadopted artifact). It must be invisible to the adopted-attempt readers.
    createRaw(*backend, store->layout().foldSealKey(st.snap_generation, st.snap_attempt + 999),
                         "decoy-seal-bytes");

    /// No reader resolves the non-adopted attempt: no throw, and the preview is unchanged by the decoy.
    std::vector<Gc::PreviewEntry> after;
    EXPECT_NO_THROW(after = gc.previewDeletes());
    EXPECT_EQ(after.size(), control.size())
        << "a non-adopted attempt's fold seal must not influence previewDeletes";

    /// A further full round must still proceed without throwing and without the decoy wedging it.
    EXPECT_NO_THROW(gc.runRegularRound());
}

/// B11: the round summary must count manifest-body (tree) deletes separately from blob deletes. A drop
/// that reclaims one manifest body must report manifests_deleted >= 1 in the RoundReport of the
/// reclaiming round, while blobs and manifests remain separately countable.
TEST(CASGCRound, RoundSummaryCountsManifestBodyDeletes)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xCC);
    const ManifestId id{ns, r};

    writeBlobBody(*backend, store->layout(), DB::UInt128(3));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(3))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    driveToFixpoint(*backend, store, gc);   /// fold the publish; no delete yet

    dropRefTransition(*backend, store->layout(), ns, "tbl", r);

    /// §0 introspection: both counters are captured BEFORE the condemn+delete pipeline below, which
    /// drives the round's meta pool (condemn/spare/delete) and its own orphan-sweep cursor pass.
    const auto meta_ops_before = ProfileEvents::global_counters[ProfileEvents::CASGCMetaOps].load();
    const auto pages_before = ProfileEvents::global_counters[ProfileEvents::CASGCEnumerationPages].load();

    /// Ack-floor drift: the owner-removed manifest body is deleted in the CONDEMNING round (post-CAS,
    /// after its -1 is adopted), while the blob's exact-token delete happens a few rounds later once the
    /// ack floor graduates its retired entry. So the two deletes fall in DIFFERENT reports now — accumulate
    /// across the pipeline (renewing the ack each round so the floor advances) and assert both were counted.
    uint64_t total_manifests_deleted = 0;
    uint64_t total_blob_deleted = 0;
    for (size_t i = 0; i < 64; ++i)
    {
        const RoundReport rep = runRegularRoundReclaiming(gc);
        if (!rep.acquired_lease)
            continue;
        store->renewWatermarkOnce();
        total_manifests_deleted += rep.manifests_deleted;
        total_blob_deleted += rep.deleted;
        if (total_manifests_deleted > 0 && total_blob_deleted > 0)
            break;
    }

    /// B11: the manifest-body delete must be counted separately from the blob delete.
    EXPECT_GE(total_manifests_deleted, 1u)
        << "round summary must count the owner-removed manifest body delete (B11 — manifests_deleted)";
    /// Blobs and manifests are separately countable: the blob delete (deleted >= 1) is independent.
    EXPECT_GE(total_blob_deleted, 1u)
        << "the blob exact-token delete must still be counted in deleted";
    /// The manifest body is gone and the blob is gone — no-leak / no-dangle.
    EXPECT_FALSE(manifestExists(*backend, store->layout(), id));
    EXPECT_FALSE(blobExists(*backend, store->layout(), DB::UInt128(3)));

    /// §0 introspection: the exact-token blob delete above scheduled at least one per-hash freshness-meta
    /// op on the round's bounded meta pool, and every round ran its own orphan-manifest-sweep cursor pass
    /// (default `manifest_sweep_list_budget_keys` is nonzero), fetching at least one LIST page directly.
    EXPECT_GE(ProfileEvents::global_counters[ProfileEvents::CASGCMetaOps].load() - meta_ops_before, 1);
    EXPECT_GE(ProfileEvents::global_counters[ProfileEvents::CASGCEnumerationPages].load() - pages_before, 1);
}

/// Manifest-body cleanup (post-CAS `manifest_deletes` phase) has no cap: the ref-log intake cursor that
/// discovers each owner-removed manifest commits in the SAME round's CAS that produces `mf_cleanup`, so an
/// entry a cap declined would never be re-derived by this pipeline -- a bounded burst would become a
/// permanent leak. Five tables' manifests are all owner-removed in one fold; the round must delete all
/// five bodies in the same round, with nothing left un-deleted.
TEST(CASGCRound, ManifestCleanupDrainsEntireRoundWithNoSkips)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "gc-runner",
        .gc_fold_max_defer_rounds = 0});
    const RootNamespace ns{"00/aa@cas@"};
    constexpr int kManifests = 5;

    std::vector<ManifestId> ids;
    for (int i = 0; i < kManifests; ++i)
    {
        const ManifestRef r = ref(1, 0xD0 + i);
        writeBlobBody(*backend, store->layout(), DB::UInt128(100 + i));
        writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(100 + i))});
        publishCommittedTransition(*backend, store->layout(), ns, "tbl" + std::to_string(i), std::nullopt, r);
        ids.push_back(ManifestId{ns, r});
    }

    Gc gc(store, kGc);
    driveToFixpoint(*backend, store, gc);   /// fold all five +1s; no manifest owner-removed yet
    for (const ManifestId & id : ids)
        ASSERT_TRUE(manifestExists(*backend, store->layout(), id));

    /// Remove all five owners in one window; the next fold's intake sees all five `-1` edges together.
    for (int i = 0; i < kManifests; ++i)
        dropRefTransition(*backend, store->layout(), ns, "tbl" + std::to_string(i), ref(1, 0xD0 + i));

    const RoundReport rep = runRegularRoundReclaiming(gc);
    ASSERT_TRUE(rep.acquired_lease);
    EXPECT_EQ(rep.manifests_deleted, kManifests)
        << "manifest_deletes must drain the entire mf_cleanup vector in one round, not cap it";

    for (const ManifestId & id : ids)
        EXPECT_FALSE(manifestExists(*backend, store->layout(), id))
            << "an unbudgeted cleanup must leave nothing surviving the round it was discovered in";
}

/// §0 introspection follow-up: `CASGCEnumerationPages` must not depend on the orphan-manifest sweep alone
/// (`manifest_sweep_list_budget_keys` zeroed below disables that pass entirely). The mandatory per-round
/// `cas/ns/stream/` scan -- `listRefPrefix`'s pre-fold DEFER signal and the fold share its result --
/// must still land at least one page each round.
TEST(CASGCRound, EnumerationPagesCountedEvenWithSweepBudgetZeroed)
{
    std::shared_ptr<InMemoryBackend> backend;
    PoolConfig config;
    config.pool_prefix = "p";
    config.server_root_id = "test";
    config.manifest_sweep_list_budget_keys = 0;   /// disables the orphan sweep's own LIST entirely
    config.gc_fold_max_defer_rounds = 0;          /// force fold-every-round (Phase-4 Lever A would defer)
    auto store = openTestPoolWithConfig(backend, config);

    Gc gc(store, kGc);
    const auto pages_before = ProfileEvents::global_counters[ProfileEvents::CASGCEnumerationPages].load();
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    EXPECT_GE(ProfileEvents::global_counters[ProfileEvents::CASGCEnumerationPages].load() - pages_before, 1)
        << "the round's own cas/ns/stream/ enumeration must count pages independent of "
           "the orphan sweep";
}

/// M1 REGRESSION (cross-round fold cursor must survive independent of trim): a folded-but-untrimmed owner
/// event must NOT be re-folded by the next round. With eager trim the folded event is removed so the bug
/// (sealedCursorOf resetting to 0 after a completed round, because snap_generation points at the COMPLETION
/// generation whose fold_seal lives at the parent) is MASKED. Disable trim to expose it: the publish event
/// stays in the journal, so a round that re-folds from 0 emits a SECOND +1 and drives the blob's in-degree
/// to 2 (a silent over-pin => leak). The fix carries the per-shard fold cursor into the completion seal so
/// the next round recovers the exact cursor. Asserts in-degree stays EXACTLY 1 across >= 2 re-folds.
TEST(CASGCRound, FoldCursorSurvivesAcrossRoundsWithoutTrim)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);

    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    gc.setTrimEnabledForTest(false);   /// keep the folded publish event in the journal across rounds

    /// Round 1 folds the +1 edge: in-degree 1, blob pinned.
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);

    /// Several more rounds. The publish event is STILL in the journal (trim off). Each round must
    /// recover the exact sealed cursor and re-fold NOTHING for this shard — in-degree stays exactly 1.
    for (int round = 0; round < 3; ++round)
    {
        ASSERT_TRUE(gc.runRegularRound().acquired_lease);
        EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1)
            << "round " << round << ": a folded-but-untrimmed event was re-folded => blob in-degree double-counted";
    }

    /// No-loss throughout: the live blob and its owner body are intact.
    EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(1)));
    EXPECT_TRUE(manifestExists(*backend, store->layout(), ManifestId{ns, r}));
}

/// Multi-ref sharing (INV-NO-LOSS): one blob referenced by TWO committed refs is spared until BOTH
/// drop. Dropping the first ref must NOT collect the blob (the second ref still pins it); only after the
/// second ref drops does the round collect it.
TEST(CASGCRound, SharedBlobSparedUntilBothRefsDrop)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r1 = ref(1, 0xA1);
    const ManifestRef r2 = ref(2, 0xA2);

    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    /// Two distinct manifests at two distinct refs, BOTH referencing the same shared blob 1.
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl1", std::nullopt, r1);
    publishCommittedTransition(*backend, store->layout(), ns, "tbl2", std::nullopt, r2);

    Gc gc(store, kGc);
    driveToFixpoint(*backend, store, gc);
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 2);   /// two source edges
    EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(1)));

    /// Drop the FIRST ref: in-degree falls to 1, blob STILL pinned by tbl2 (spared).
    dropRefTransition(*backend, store->layout(), ns, "tbl1", r1);
    driveToFixpoint(*backend, store, gc);
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);
    EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(1)))
        << "shared blob must survive while a second ref still names it";

    /// Drop the SECOND ref: in-degree reaches 0, blob is finally collected.
    dropRefTransition(*backend, store->layout(), ns, "tbl2", r2);
    driveToFixpoint(*backend, store, gc);
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 0);
    EXPECT_FALSE(blobExists(*backend, store->layout(), DB::UInt128(1)));
}

/// `gc_round_outcome_entry_budget` bounds only the `GcOutcomes` AUDIT row per spared decision, never the
/// decision itself. Five blobs are condemned (owner dropped, indegree 0, durable retired rows), then --
/// BEFORE graduation -- a fresh manifest re-references all five (the `CASThreeCursorMerge.RecoverySpares`
/// shape, scaled up and driven through the real round path): recovery wins unconditionally for every one
/// of them. `CASGCRetiredSpared` and blob survival prove all five decisions happened regardless of the
/// budget, but with a budget of 2, only 2 of the 5 get a row in the round's `GcOutcomes` log, so
/// `RoundReport::spared` (tallied from that log) reports 2, not 5.
TEST(CASGCRound, OutcomeEntryBudgetCapsSparedLogRowsWithoutRecondemning)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "test",
        .gc_round_outcome_entry_budget = 2,
        .gc_fold_max_defer_rounds = 0});
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r1 = ref(1, 0xC1);
    const ManifestRef r2 = ref(2, 0xC2);
    constexpr int kBlobs = 5;

    std::vector<ManifestEntry> entries;
    for (int i = 0; i < kBlobs; ++i)
    {
        writeBlobBody(*backend, store->layout(), DB::UInt128(i + 1));
        entries.push_back(blobEntryFor("p" + std::to_string(i), DB::UInt128(i + 1)));
    }
    writeManifestRaw(*backend, store->layout(), ns, r1, entries);
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r1);

    Gc gc(store, kGc);
    driveToFixpoint(*backend, store, gc);
    for (int i = 0; i < kBlobs; ++i)
        ASSERT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(i + 1)), 1);

    /// Drop the only ref: one round later all five blobs are condemned (indegree 0, durable retired rows).
    dropRefTransition(*backend, store->layout(), ns, "tbl", r1);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    for (int i = 0; i < kBlobs; ++i)
        ASSERT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(i + 1)), 0);
    store->renewWatermarkOnce();

    /// BEFORE graduation, a fresh manifest re-references all five: the NEXT fold recomputes indegree 1 for
    /// every one of them -- recovery wins over graduation for every entry, unconditionally.
    writeManifestRaw(*backend, store->layout(), ns, r2, entries);
    publishCommittedTransition(*backend, store->layout(), ns, "tbl2", std::nullopt, r2);

    const auto spared_events_before = ProfileEvents::global_counters[ProfileEvents::CASGCRetiredSpared].load();
    const RoundReport rep = runRegularRoundReclaiming(gc);
    ASSERT_TRUE(rep.acquired_lease);
    const uint64_t total_spared_reported = rep.spared;

    /// THE LOAD-BEARING ASSERTION: the audit log under-reports (capped at the budget) while every
    /// decision it under-reports still happened correctly.
    EXPECT_EQ(total_spared_reported, 2u)
        << "GcOutcomes rows must be capped at gc_round_outcome_entry_budget, not one per spared entry";
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASGCRetiredSpared].load() - spared_events_before, kBlobs)
        << "every spared decision must still happen even when its audit row is capped";
    for (int i = 0; i < kBlobs; ++i)
    {
        EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(i + 1)))
            << "blob " << i << " must survive -- the cap must never re-condemn a spared entry";
        EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(i + 1)), 1);
    }
}

/// Spare-during-the-pass, multi-blob discrimination: a drop condemns two blobs; in the SAME window
/// between rounds (before the next pass folds), one of them is re-referenced under a fresh ref. The pass
/// folds the racing publish and SPARES the re-referenced blob (recovery wins in the pass merge, dropping
/// its retired entry), while the genuinely-unreferenced blob proceeds through the condemn -> graduate ->
/// delete pipeline. The discriminating assertion: at fixpoint, one is spared (kept) and the other gone.
TEST(CASGCRound, RepublishDuringFenceWindowSparesOnlyReReferencedBlob)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r1 = ref(1, 0xB1);
    const ManifestRef r2 = ref(2, 0xB2);

    writeBlobBody(*backend, store->layout(), DB::UInt128(1));   /// kept (will be re-referenced)
    writeBlobBody(*backend, store->layout(), DB::UInt128(2));   /// genuinely dropped
    writeManifestRaw(*backend, store->layout(), ns, r1,
        {blobEntryFor("a", DB::UInt128(1)), blobEntryFor("b", DB::UInt128(2))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r1);

    Gc gc(store, kGc);
    driveToFixpoint(*backend, store, gc);
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(2)), 1);

    /// Repoint the ref from r1 to r2 between rounds: ONE event {old=committed(r1), new=committed(r2)}.
    /// The -1 (r1's body: blobs 1 AND 2) and +1 (r2's body: blob 1 only) net to in-degree 1 for blob 1
    /// (re-referenced => SPARED in the pass merge) and 0 for blob 2 (genuinely unreferenced => condemned,
    /// then reclaimed by the ack-floor pipeline). (A separate drop THEN repoint would double-count the -1
    /// on r1's blobs and drive blob 2 to -1 — an undercount the in-degree fold fails closed on.)
    writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", r1, r2);

    driveToFixpoint(*backend, store, gc);
    /// Blob 1 is re-referenced (net in-degree 1) => SPARED; blob 2 is genuinely unreferenced => GONE.
    EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(1)))
        << "the racing republish must spare blob 1";
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);
    EXPECT_FALSE(blobExists(*backend, store->layout(), DB::UInt128(2)))
        << "the genuinely-unreferenced blob 2 must be collected";
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(2)), 0);
}

/// Idempotent fixpoint: once a pool is quiescent (all live refs folded, nothing to collect), repeated
/// rounds are pure no-ops — no blob is collected, no manifest disappears, the in-degree generation is
/// stable, and no round throws. The split-brain-safety bedrock: every step is idempotent.
TEST(CASGCRound, IdempotentRerunAtFixpointIsNoOp)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);
    const ManifestId id{ns, r};

    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    driveToFixpoint(*backend, store, gc);
    const uint64_t gen0 = currentGenerationOf(*backend, store->layout());

    /// At quiescence: a fresh round does NO work (no candidates/deletes/spares) and changes nothing.
    const RoundReport quiescent = gc.runRegularRound();
    EXPECT_TRUE(quiescent.acquired_lease);
    EXPECT_EQ(quiescent.candidates, 0u);
    EXPECT_EQ(quiescent.deleted, 0u);
    EXPECT_EQ(quiescent.spared, 0u);

    EXPECT_NO_THROW(driveToFixpoint(*backend, store, gc));
    EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(1)));   /// no-loss
    EXPECT_TRUE(manifestExists(*backend, store->layout(), id));          /// no-loss
    /// The CONTENT no-op invariant: the live blob's durable in-degree is unchanged (still pinned). The
    /// generation POINTER advances every round by design (each fold seals a fresh generation for durable
    /// cursor coverage, and recheck seals the completion generation), even when no edges change — so the
    /// quiescence guarantee is "no candidates/deletes/spares + nothing lost", NOT a frozen generation.
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);
    EXPECT_GE(currentGenerationOf(*backend, store->layout()), gen0)
        << "the generation pointer is monotone; a quiescent round never moves it backward";
}

/// Split-brain: two leaders racing the same pool only DUPLICATE WORK, never double-delete or lose data.
/// gc1 leads and folds the live publish; the ref is then dropped; gc2 steals the lease (stale leader)
/// and both contend to collect the now-unreferenced blob. The exact-token delete is the only destructive
/// authority, so the blob is removed exactly once and a losing/duplicate attempt is a harmless 404/412 —
/// no exception escapes, and the blob ends up gone exactly once with no dangling owner.
TEST(CASGCRound, SplitBrainLeadersOnlyDuplicateWork)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);
    const ManifestId id{ns, r};

    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc1(store, kGcA);
    Gc gc2(store, kGcB);

    /// gc1 leads; fold the publish edge.
    ASSERT_TRUE(runRegularRoundReclaiming(gc1).acquired_lease);
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);

    /// The ref is dropped; gc1 stalls. gc2 observes the frozen lease twice and STEALS (new epoch).
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    EXPECT_FALSE(runRegularRoundReclaiming(gc2).acquired_lease);   /// obs #1
    ASSERT_TRUE(runRegularRoundReclaiming(gc2).acquired_lease);    /// obs #2 => steal

    /// Both leaders now drive rounds. The blob is collected exactly once; duplicate attempts are
    /// harmless. No round throws.
    EXPECT_NO_THROW(driveToFixpoint(*backend, store, gc2));
    EXPECT_NO_THROW(driveToFixpoint(*backend, store, gc1));   /// the revived stale leader backs off / duplicates harmlessly

    EXPECT_FALSE(blobExists(*backend, store->layout(), DB::UInt128(1)))
        << "the dropped blob must be collected exactly once across both leaders";
    EXPECT_FALSE(manifestExists(*backend, store->layout(), id));
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 0);
}

/// (`CASGCRound.TrimOnlyBelowSealedCoverage` and the B12 lazy/batched-trim tests
/// `LazyTrimSkipsSmallJournalAndKeepsTokenStable`, `LazyTrimCompactsAtThresholdOrSoftLimit`,
/// `MaintenanceTrimCompactsEverythingOnce` were removed with the snapshot+log ref model. They asserted
/// GC compacts a MUTABLE shard journal in place (INV-JOURNAL-COVERAGE / `gc_trim_min_events` gates).
/// Immutable `_log` objects are never trimmed in place: covered `_log`/`_snap` keys are DELETED by
/// ref-object cleanup once BOTH the durable cursor AND a checkpoint-named validated recovery triple
/// cover them -- exercised in `gtest_cas_ref_gc.cpp` (`RefObjectCleanupRetainsCheckpointNamedTriple`).)

/// ---- INTENTIONALLY NOT PORTED (covered elsewhere or obsolete in the manifest model) ----
///
/// The removed snap/cascade/tree cases and where their behaviour now lives:
///   - CasGcFold.{FreshUploadsAreNeverCandidates, DropZeroesTreeButChildStaysPinned,
///     RepublishSameRefIsLastOpWins, ExpansionIsOncePerTree, IncrementalSecondFoldOnlyNewRecords,
///     DurableSnapBeforeCursorAdvance, ForeignDivergentGenerationIsProbedPast,
///     GenerationProbeRecoversAfterLostCursorCas, SnapShardsOtherThanOneIsNotImplemented,
///     AbsentTree*, NoChurnRound*} — fold-step behaviour now in gtest_cas_gc_fold.cpp
///     (CommittedAdd/Removal/Precommit/FoldBarrier/Clamp+anomaly/RefMismatch).
///   - CasGcCorruptCommittedTree.MissingTreeOfLiveRefDoesNotHaltGc — now
///     CASGCFold.CommittedMissingBodyClampsCursorAndRecordsAnomaly.
///   - CASGCRetire.{Observes*, AbsentCandidate*, DeletedCandidate*, DeleteTimePrune*, BlobOnlyPrune*,
///     RetireForgets*, RetireSetsDurable*, Diverged*, BlobHeaderUnderflow*, RetireUsesFoldCommitted*,
///     RetireReplayAdoptsOwnCrashedAttempt} — retire-step behaviour now split between
///     gtest_cas_gc_ack_floor.cpp and the retire-view suite.
///   - CASGCRecheck.{SparedWhenPublishRacesTheFence, ReplacedWhenResurrectionWins, AbsentWhenAlreadyGone}
///     — now CASGCRecheck.{PublishRacingFenceSparesBlob, UnreferencedBlobDeletedExactToken}.
///   - CasGcFence.* / CasGcDiscovery.UsesRegistryNotList — the fence machinery is retired; the equivalent
///     no-op-round-does-not-mutate-ref-shards property is gtest_cas_gc_ack_floor.cpp::
///     CASGCAckFloor.NoOpRoundDoesNotMutateRefShards (+ helper registerNamespaceRaw discovery is
///     exercised by every fold test).
///   - CasGcCascade.* — the cascade/closure model is REMOVED; in-degree is per-blob, so a shared
///     child surviving one parent's deletion is now CASGCRound.SharedBlobSparedUntilBothRefsDrop above,
///     and "never cascades on replaced" is CASGCRound.RepublishDuringFenceWindowSparesOnlyReReferencedBlob.
///   - CasGcTrim.* — now gtest_cas_gc_resume.cpp::CASGCRound.TrimDropsFoldedOwnerEvents.
///   - CasGcResume.{CompletesRoundAfterCrashBeforeFencePersist, AdoptsOutcomesAfterCrashBeforeCascadePersist}
///     — now gtest_cas_gc_resume.cpp::CasGcResume.ResumeFromDurableFoldSealCompletesRound.
///   - CasGcScenario.ZombieDeleteAfterResurrectIs412 — relied on the snap/tree publish path + held
///     in-flight deletes; the in-degree-spare equivalent is RepublishDuringFenceWindowSparesOnly...
///     above (exact-token delete is the sole authority; a zombie carrying a stale token 412s).
///   - CASGCRound.PreviewDeletesIsWriteFreeAndSubsetOfUnreachable — previewDeletes survives, but it is
///     covered by the fsck/preview suite; not duplicated here.
///   - CasGcWatermark.LiveBuildPrecommitHonoredAcrossGcRounds /
///     CASGCRetire.ReclaimsAbandonedPrecommitWhenFloorPasses — precommit removal is now the WRITER's job
///     (an exact `owner_transition` on abandon, or a fenced successor's stale-precommit sweep); GC no
///     longer reclaims abandoned precommits. Exercised by the orphan-manifest-sweep / build-root suites.

/// B9 snap-generation retention, reimplemented over the run/generation model: after a generation is
/// adopted the GC prunes the per-generation seal/run/cleanup objects of generations at or below the
/// retention floor (snap_generation - gc_snapshot_generations_to_keep), advancing snap_pruned_through. This
/// test drives enough rounds to accumulate several generations, then asserts that everything at or below
/// the floor is GONE while the last `keep` generations (and the live current one) remain.
TEST(CASGCSnapRetention, PrunesOldGenerationsKeepingLastThree)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// keep the default 3 generations; one root shard so cursor keys are "ns/0".
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_snapshot_generations_to_keep = 3, .gc_fold_max_defer_rounds = 0});
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);

    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    /// Several quiescent rounds, each advancing the generation pointer (fold + completion). Enough to
    /// push generations below the floor.
    for (int i = 0; i < 8; ++i)
        ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    const GcState st = readState(*backend, *store);
    const uint64_t keep = 3;
    ASSERT_GT(st.snap_generation, keep);
    const uint64_t floor = st.snap_generation - keep;

    /// snap_pruned_through reached the floor (bounded burst is large enough for this generation count).
    EXPECT_EQ(st.snap_pruned_through, floor)
        << "retention cursor must reach the floor (snap_generation - keep)";

    /// Every generation at or below the floor is fully gone (fold seal absent).
    for (uint64_t g = 1; g <= floor; ++g)
    {
        EXPECT_FALSE(headExists(*backend, store->layout().foldSealKey(g, st.snap_attempt)))
            << "fold seal of pruned generation " << g << " must be gone";
        EXPECT_FALSE(headExists(*backend, store->layout().blobTargetRunKey(g, st.snap_attempt, /*shard*/0, /*seq*/0)))
            << "blob-target run of pruned generation " << g << " must be gone";
    }

    /// The fold seal at the current generation survives (the live in-degree view).
    EXPECT_TRUE(headExists(*backend, store->layout().foldSealKey(st.snap_generation, st.snap_attempt)))
        << "the current generation's seal must NOT be pruned";

    /// No-loss: the live blob and owner body are intact throughout retention pruning.
    EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(1)));
    EXPECT_TRUE(manifestExists(*backend, store->layout(), ManifestId{ns, r}));
}

/// Task 9 (wholesale generation-retention): a generation may hold artifacts under MULTIPLE attempts
/// (each round mints a fresh `lease.seq`, and a deposed leader may have written debris under its own
/// unadopted attempt). When a generation ages past the retention floor it must be reclaimed WHOLESALE
/// — every attempt's artifacts (incl. the attempt-scoped `retired/` and `outcomes/` sets that now live
/// under `gc/gen/<g>/attempt/<a>/`), not just the final adopted attempt's. This test plants a retired
/// set AND a decoy fold seal under a NON-adopted attempt at an old generation, ages that generation out,
/// and asserts the whole `gc/gen/<g>/` subtree is gone (the per-key single-attempt prune leaked it).
TEST(CASGCSnapRetention, WholesalePruneReclaimsAllAttemptsIncludingRetiredOutcomes)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_snapshot_generations_to_keep = 3, .gc_fold_max_defer_rounds = 0});
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);

    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    /// One round to establish the first completed generation and learn its adopted attempt (derive both
    /// from gc/state — never hardcode a generation; the round folds and completes, so it is > 1).
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    const GcState st1 = readState(*backend, *store);
    ASSERT_GT(st1.snap_generation, 0u);
    const uint64_t old_gen = st1.snap_generation;
    const uint64_t adopted_attempt_g1 = st1.snap_attempt;

    /// Plant debris under a NON-adopted attempt of generation 1: a retired set, an outcomes log, a fold
    /// seal, and a blob-target run — exactly the families a deposed leader would have written before its
    /// CAS failed. The per-key single-attempt prune (keyed on the FINAL snap_attempt) never touches them.
    const uint64_t decoy_attempt = adopted_attempt_g1 + 777;
    const String decoy_outcomes = store->layout().outcomesKey(old_gen, decoy_attempt, /*round*/0, /*shard*/0);
    const String decoy_seal = store->layout().foldSealKey(old_gen, decoy_attempt);
    const String decoy_run = store->layout().blobTargetRunKey(old_gen, decoy_attempt, /*shard*/0, /*seq*/0);
    createRaw(*backend, decoy_outcomes, "decoy-outcomes");
    createRaw(*backend, decoy_seal, "decoy-seal");
    createRaw(*backend, decoy_run, "decoy-run");

    /// Drop the ref so the next fold writes a FRESH run under a newer generation and the adopted seal's
    /// blob_target ref moves OFF `old_gen`. Under T0 reference-parent carry, a still-referenced generation
    /// is deliberately retained (its run is live), so `old_gen` can only age out once nothing references
    /// its run anymore — which the drop guarantees.
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);

    /// Age generation 1 well past the retention floor (keep=3): several more quiescent rounds.
    for (int i = 0; i < 8; ++i)
        ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    const GcState st = readState(*backend, *store);
    ASSERT_GT(st.snap_generation, old_gen + 3) << "generation 1 must be below the retention floor";

    /// The ENTIRE gc/gen/<old_gen>/ subtree — across ALL attempts — must be reclaimed.
    EXPECT_FALSE(headExists(*backend, decoy_outcomes)) << "non-adopted outcomes log leaked past retention";
    EXPECT_FALSE(headExists(*backend, decoy_seal)) << "non-adopted fold seal leaked past retention";
    EXPECT_FALSE(headExists(*backend, decoy_run)) << "non-adopted blob-target run leaked past retention";

    /// Nothing remains under the old generation prefix at all.
    const ListPage residue = listOf(*backend, store->layout().gcGenPrefix(old_gen), "", 1000);
    EXPECT_TRUE(residue.keys.empty()) << "old generation prefix must be fully reclaimed; left "
                                      << residue.keys.size() << " objects";

    /// The drop was necessary to move the seal's blob_target ref off `old_gen` so it could age out (under
    /// T0 a still-referenced generation is deliberately retained — see the comment above). The blob is
    /// condemned by the drop and, being round-paced, graduates and is physically deleted well within the
    /// 8 quiescent rounds above — retire drain is not the property under test here (generation retention
    /// is), so this only asserts the reclaim pipeline is not itself broken by the retention plumbing.
    EXPECT_FALSE(blobExists(*backend, store->layout(), DB::UInt128(1)));
    /// The owner-removed manifest body IS reclaimed by the part-manifest cleanup pass over the aging rounds.
    EXPECT_FALSE(manifestExists(*backend, store->layout(), ManifestId{ns, r}));
}

/// `deletePrefixWholesale`'s callers now draw from the round's shared
/// object-count budget instead of `UINT64_MAX`, and `snap_pruned_through` must advance only past a
/// FULLY drained generation -- never past one the budget cut short, or its undeleted remainder would
/// be stranded behind a cursor this loop never revisits. A tiny budget (2 objects/round) against a
/// generation carrying far more debris than that forces multiple rounds to fully drain it; the
/// invariant under test is that AT EVERY ROUND, `snap_pruned_through >= old_gen` implies the old
/// generation's prefix is already empty -- the cursor never claims completion it has not earned.
TEST(CASGCSnapRetention, PruneRespectsPrefixWholesaleBudgetAndNeverStrandsAPartialGeneration)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "test",
        .gc_snapshot_generations_to_keep = 3,
        .gc_round_prefix_wholesale_budget = 2,
        .gc_fold_max_defer_rounds = 0});
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);

    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    const GcState st1 = readState(*backend, *store);
    const uint64_t old_gen = st1.snap_generation;

    /// Ten extra debris objects under generation 1's prefix -- far more than the 2-object round budget
    /// can wholesale-delete in a single pass, regardless of whatever real fold artifacts already live
    /// there.
    for (int i = 0; i < 10; ++i)
        createRaw(*backend, store->layout().gcGenPrefix(old_gen) + "debris" + std::to_string(i), "x");

    /// Move the ref off `old_gen`'s run (as `WholesalePruneReclaimsAllAttemptsIncludingRetiredOutcomes`
    /// does) so the WHOLESALE RETENTION PRUNE -- not the one-shot post-CAS hand-off -- is what
    /// eventually processes this generation once the cursor reaches it.
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);

    size_t previous_residue = listOf(*backend, store->layout().gcGenPrefix(old_gen), "", 1000).keys.size();
    std::optional<int> drain_start_round;   /// first round the residue count actually DROPS
    std::optional<int> drain_done_round;    /// first round the residue reaches zero
    for (int i = 0; i < 40 && !drain_done_round; ++i)
    {
        ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
        const GcState st = readState(*backend, *store);
        const ListPage residue = listOf(*backend, store->layout().gcGenPrefix(old_gen), "", 1000);

        if (st.snap_pruned_through >= old_gen)
            EXPECT_TRUE(residue.keys.empty())
                << "round " << i << ": snap_pruned_through (" << st.snap_pruned_through
                << ") claims generation " << old_gen << " is behind it, but " << residue.keys.size()
                << " object(s) remain -- the cursor advanced past a partially-drained prefix";

        if (!drain_start_round && residue.keys.size() < previous_residue)
            drain_start_round = i;
        if (residue.keys.empty())
            drain_done_round = i;
        previous_residue = residue.keys.size();
    }

    ASSERT_TRUE(drain_start_round.has_value()) << "the round loop never even started draining the debris";
    ASSERT_TRUE(drain_done_round.has_value())
        << "the budget-limited generation must eventually fully drain within a generous round bound";
    /// THE LOAD-BEARING ASSERTION: with a 2-object budget against 10+ debris objects, draining cannot
    /// finish the SAME round it starts -- it must take several rounds. An unbounded
    /// `deletePrefixWholesale` call (the mutation this pins) drains everything the round it starts,
    /// collapsing this gap to zero.
    EXPECT_GT(*drain_done_round, *drain_start_round)
        << "draining finished the same round it started -- the per-round budget is not load-bearing";
}

/// Reclaim-VIA-RETENTION of a non-adopted current-generation attempt orphan (KISS prune model). A
/// deposed leader can write its fold seal under an attempt that lost CAS #1 to a higher-seq adopter —
/// debris at the FOLD generation under a NON-adopted attempt. There is NO per-round current-generation
/// attempt-sweep anymore (it cost a per-round LIST for a rare collision); the wholesale
/// generation-retention prune is the SOLE reclaimer. So such an orphan is NOT reclaimed within one
/// round; instead it waits until its generation ages past `keep` and the wholesale prefix-delete
/// reclaims the whole `gc/gen/<g>/` subtree — every attempt at once, including this orphan. This test
/// plants the orphan at a fold generation, ages that generation out, and asserts retention reclaims it.
TEST(CASGCSnapRetention, ReclaimsNonAdoptedCurrentGenAttemptViaRetention)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// keep=3 retention floor (matches WholesalePruneReclaimsAllAttemptsIncludingRetiredOutcomes).
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_snapshot_generations_to_keep = 3, .gc_fold_max_defer_rounds = 0});
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    /// Drive a couple of rounds so snap_attempt is comfortably above 0 (a low orphan seq exists below it).
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    const GcState st = readState(*backend, *store);
    ASSERT_GT(st.snap_attempt, 0u) << "need snap_attempt > 0 so a strictly-older orphan attempt exists";

    /// Plant a deposed competitor's debris at the next round's FOLD generation under an attempt strictly
    /// older than that round's adopted attempt — exactly the orphan the old per-round sweep targeted.
    const uint64_t orphan_gen = st.snap_generation + 1;
    const uint64_t orphan_attempt = st.snap_attempt - 1;
    const String orphan_seal = store->layout().foldSealKey(orphan_gen, orphan_attempt);
    const String orphan_run = store->layout().blobTargetRunKey(orphan_gen, orphan_attempt, 0, 0);
    createRaw(*backend, orphan_seal, "orphan-seal");
    createRaw(*backend, orphan_run, "orphan-run");

    /// One more round folds into `orphan_gen` and completes. The orphan must SURVIVE this round — there
    /// is no current-generation sweep; retention has not yet reached `orphan_gen`.
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    EXPECT_TRUE(headExists(*backend, orphan_seal))
        << "orphan must survive its own round — there is no per-round current-gen sweep";

    /// Age `orphan_gen` well past the retention floor (keep=3): several more quiescent rounds. The
    /// wholesale generation-retention prune then reclaims the WHOLE `gc/gen/<orphan_gen>/` subtree,
    /// including this non-adopted attempt's debris.
    for (int i = 0; i < 8; ++i)
        ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    const GcState st_after = readState(*backend, *store);
    ASSERT_GT(st_after.snap_generation, orphan_gen + 3) << "orphan_gen must be below the retention floor";

    EXPECT_FALSE(headExists(*backend, orphan_seal))
        << "non-adopted attempt orphan must be reclaimed by wholesale retention once its generation ages out";
    EXPECT_FALSE(headExists(*backend, orphan_run))
        << "the whole orphan subtree must be reclaimed by wholesale retention";

    /// No-loss: the live data is intact throughout.
    EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(1)));
    EXPECT_TRUE(manifestExists(*backend, store->layout(), ManifestId{ns, r}));
}

/// ---- Task 7 (2026-07-02 snapshot-streaming): ref-aware retention + post-CAS hand-off delete ----

/// Retention must NOT reclaim a generation whose run the live seal still references, EVEN once the
/// retention cursor (`snap_pruned_through`) has advanced past that generation. With `keep=1` and a live
/// ref that idle-carries across generations, `pruneSupersededGenerations` SKIPS gen-1's prefix every
/// round while advancing the cursor over it. The gen-1 run object (physically holding the seal's ref)
/// must survive, and folding/in-degree resolution THROUGH the carried ref must keep working.
TEST(CASGCRetention, PruneRetainsLiveReferencedRun)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// keep=1: the retention floor is aggressive so the cursor reaches gen-1's neighbourhood fast.
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_snapshot_generations_to_keep = 1, .gc_fold_max_defer_rounds = 0});
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);

    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);   // gen 1: the blob's run is sealed under gen-1's key namespace
    const GcState st1 = readState(*backend, *store);
    const uint64_t ref_gen = st1.snap_generation;

    /// The gen-1 seal's ref names gen-1's physical run key — capture it so we can assert the OBJECT
    /// (not just the generation number) survives retention.
    const auto seal1 = decodeFoldSeal(
        readOf(*backend, store->layout().foldSealKey(st1.snap_generation, st1.snap_attempt))->bytes);
    ASSERT_EQ(seal1.blob_target_runs.size(), 1u);
    const String referenced_run_key = seal1.blob_target_runs.front().key;
    ASSERT_EQ(seal1.blob_target_runs.front().key_generation, ref_gen);
    ASSERT_TRUE(headExists(*backend, referenced_run_key));

    /// Several idle rounds: no delta, no retired => pure ref-carry. Each round advances the generation
    /// and, once adopted_generation > keep, drives the retention prune forward. gen-1 is referenced every
    /// round, so it is SKIPPED (retained) even as `snap_pruned_through` climbs past it.
    for (int i = 0; i < 6; ++i)
        ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    const GcState st = readState(*backend, *store);
    /// The cursor has advanced strictly past the referenced generation (the retention prune SKIPPED it
    /// but still moved the high-water cursor forward) — this is the exact window Task 7 guards.
    ASSERT_GT(st.snap_pruned_through, ref_gen)
        << "the retention cursor must have advanced past the still-referenced generation";

    /// The referenced run object is STILL ALIVE despite the cursor passing its generation.
    EXPECT_TRUE(headExists(*backend, referenced_run_key))
        << "a run referenced by the live seal must be retained even after the cursor passes its generation";

    /// The current seal still references that same physical gen-1 object (carried, not reconstructed),
    /// and in-degree resolution THROUGH the carried ref still works.
    const auto seal_now = decodeFoldSeal(
        readOf(*backend, store->layout().foldSealKey(st.snap_generation, st.snap_attempt))->bytes);
    ASSERT_EQ(seal_now.blob_target_runs.size(), 1u);
    EXPECT_EQ(seal_now.blob_target_runs.front().key, referenced_run_key);
    EXPECT_EQ(seal_now.blob_target_runs.front().key_generation, ref_gen);
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1)
        << "folding still resolves in-degree through the retained, carried parent ref";

    /// No-loss end-to-end: the live blob is intact.
    EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(1)));
}

/// When a later delta finally REPLACES the carried ref with a fresh run, the superseded old-generation
/// run — whose generation the retention cursor already passed while it was retained — is reclaimed by the
/// post-CAS HAND-OFF delete in `runRegularRound` (the wholesale prune never revisits a generation behind
/// its cursor, so the ordinary prune would leak it). The whole `gc/gen/<old>/` prefix must be gone.
TEST(CASGCRetention, HandOffDeletesSupersededRef)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_snapshot_generations_to_keep = 1, .gc_fold_max_defer_rounds = 0});
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r1 = ref(1, 0xAA);

    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r1);

    Gc gc(store, kGc);
    runRegularRoundReclaiming(gc);   // gen 1: run sealed under gen-1
    const GcState st1 = readState(*backend, *store);
    const uint64_t old_gen = st1.snap_generation;
    const String old_prefix = store->layout().gcGenPrefix(old_gen);
    ASSERT_FALSE(listOf(*backend, old_prefix, "", 1000).keys.empty()) << "gen-1 prefix must be populated";

    /// Idle-carry the gen-1 ref until the retention cursor has advanced strictly PAST gen-1. Until it
    /// does, a normal prune could still reclaim gen-1 when the ref moves — the hand-off is only load-
    /// bearing once gen-1 is BEHIND the cursor.
    for (int i = 0; i < 6; ++i)
        ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    ASSERT_GT(readState(*backend, *store).snap_pruned_through, old_gen)
        << "gen-1 must be behind the retention cursor before the hand-off is exercised";
    /// gen-1 is retained (referenced) even though the cursor passed it.
    ASSERT_FALSE(listOf(*backend, old_prefix, "", 1000).keys.empty())
        << "the referenced gen-1 prefix must still exist before the ref moves off it";

    /// A real delta: swap the ref to a new manifest naming a different blob. The next fold writes a FRESH
    /// run under the new generation and the seal's shard-0 ref moves OFF gen-1.
    const ManifestRef r2 = ref(2, 0xBB);
    writeBlobBody(*backend, store->layout(), DB::UInt128(2));
    writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("b", DB::UInt128(2))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", r1, r2);

    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);   // folds through the carried ref; ref leaves gen-1

    /// The seal no longer references gen-1 ...
    const GcState st_after = readState(*backend, *store);
    const auto seal_after = decodeFoldSeal(
        readOf(*backend, store->layout().foldSealKey(st_after.snap_generation, st_after.snap_attempt))->bytes);
    for (const RunRef & rr : seal_after.blob_target_runs)
        EXPECT_NE(rr.key_generation, old_gen) << "the live seal must have moved its ref off gen-1";

    /// ... and the post-CAS hand-off delete reclaimed gen-1's WHOLE prefix (not just the single run
    /// object): seal, attempt subtree, run — all gone. The ordinary prune would have leaked it because its
    /// cursor is already past gen-1.
    const ListPage residue = listOf(*backend, old_prefix, "", 1000);
    EXPECT_TRUE(residue.keys.empty())
        << "the superseded gen-1 prefix must be hand-off deleted; left " << residue.keys.size() << " objects";

    /// The now-referenced blob 2 is intact; folding through the fresh run resolves it.
    EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(2)));
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(2)), 1);
}

/// The post-CAS hand-off draws from its OWN reserve, so a prune that spends its ENTIRE (separate, tiny)
/// budget in a round can never leave the hand-off with zero. Combines the two existing shapes: a
/// debris-heavy generation only the ordinary PRUNE ever touches (like
/// `PruneRespectsPrefixWholesaleBudgetAndNeverStrandsAPartialGeneration`, mid-drain over several rounds on
/// a starvation-small prune budget), running CONCURRENTLY with an idle-carried ref that finally moves off
/// its generation (like `HandOffDeletesSupersededRef`) in one of those very same mid-drain rounds.
TEST(CASGCRetention, HandoffOwnBudgetSurvivesAPruneHeavyRound)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// `gc_shards = 2` with the "keep" and "debris" blobs routed to DIFFERENT shards is load-bearing: with
    /// the default single shard, ANY delta anywhere rewrites the pool's one shared run object every round,
    /// which would drag the "keep" ref's physical run forward the moment the debris table is touched --
    /// destroying the idle-carry this test depends on. Two independent shards keep debris activity from
    /// disturbing the kept ref's generation at all until its ref is explicitly moved.
    /// `keep=5` (not the more aggressive `keep=1` other hand-off tests use) is ALSO load-bearing: the
    /// debris generation must still be numerically AHEAD of the cursor at the moment its own drop folds,
    /// or that fold's post-CAS hand-off phase -- not the ordinary prune -- would claim it (the same
    /// one-round "parent-seal protects, then hand-off claims" shape `HandOffDeletesSupersededRef` relies
    /// on, which this test must deliberately avoid for the debris generation).
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "test",
        .gc_snapshot_generations_to_keep = 5,
        .gc_shards = 2,
        .gc_round_prefix_wholesale_budget = 2,             /// prune: starvation-small, shared by nothing else
        .gc_round_handoff_prefix_wholesale_budget = 5,      /// hand-off: its own separate reserve
        .gc_fold_max_defer_rounds = 0});
    const RootNamespace ns{"00/aa@cas@"};
    Gc gc(store, kGc);
    /// `blobShard` uses only the digest's high 64 bits, so a small integer's `UInt128` (high bits zero)
    /// always routes to shard 0 regardless of `gc_shards` -- these two differ in the high half so they
    /// land in different shards of a 2-shard pool.
    const DB::UInt128 blob_keep_1 = hexToU128("00000000000000010000000000000000");
    const DB::UInt128 blob_keep_2 = hexToU128("00000000000000010000000000000001");
    const DB::UInt128 blob_debris = hexToU128("00000000000000020000000000000000");

    /// The HAND-OFF generation: ns "keep" idle-carries this ref for several rounds until the cursor has
    /// advanced strictly past it (referenced generations are skipped for free -- no budget spent).
    const ManifestRef r_keep_1 = ref(1, 0xE1);
    writeBlobBody(*backend, store->layout(), blob_keep_1);
    writeManifestRaw(*backend, store->layout(), ns, r_keep_1, {blobEntryFor("a", blob_keep_1)});
    publishCommittedTransition(*backend, store->layout(), ns, "keep", std::nullopt, r_keep_1);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    const uint64_t handoff_gen = readState(*backend, *store).snap_generation;
    const String handoff_prefix = store->layout().gcGenPrefix(handoff_gen);
    ASSERT_FALSE(listOf(*backend, handoff_prefix, "", 1000).keys.empty());

    for (int i = 0; i < 20; ++i)
        ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    ASSERT_GT(readState(*backend, *store).snap_pruned_through, handoff_gen)
        << "the hand-off generation must be behind the cursor before this test is meaningful";
    ASSERT_FALSE(listOf(*backend, handoff_prefix, "", 1000).keys.empty())
        << "still referenced -- must survive despite the cursor having passed it";

    /// The PRUNE-DEBRIS generation: a second table, on the OTHER shard, unreferenced from the start,
    /// carrying far more debris than the tiny prune budget can drain in one round. Minted well AHEAD of
    /// the current cursor (see the `keep=5` note above), so its own drop-fold is NOT immediately
    /// hand-off-eligible.
    const ManifestRef r_debris = ref(1, 0xE2);
    writeBlobBody(*backend, store->layout(), blob_debris);
    writeManifestRaw(*backend, store->layout(), ns, r_debris, {blobEntryFor("b", blob_debris)});
    publishCommittedTransition(*backend, store->layout(), ns, "debris", std::nullopt, r_debris);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    const uint64_t debris_gen = readState(*backend, *store).snap_generation;
    ASSERT_GT(debris_gen, readState(*backend, *store).snap_pruned_through)
        << "the debris generation must still be ahead of the cursor when its drop folds, or the hand-off "
           "(not the prune) would claim it";
    for (int i = 0; i < 10; ++i)
        createRaw(*backend, store->layout().gcGenPrefix(debris_gen) + "debris" + std::to_string(i), "x");
    dropRefTransition(*backend, store->layout(), ns, "debris", r_debris);

    /// Drive rounds until the debris generation is MID-DRAIN (prune has started but not yet finished it --
    /// the round-budget of 2 against 10+ objects guarantees several such rounds exist).
    bool mid_drain = false;
    for (int i = 0; i < 20 && !mid_drain; ++i)
    {
        ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
        const size_t residue = listOf(*backend, store->layout().gcGenPrefix(debris_gen), "", 1000).keys.size();
        mid_drain = residue > 0 && residue < 10;
    }
    ASSERT_TRUE(mid_drain) << "the debris generation never reached a partially-drained state to test against";
    ASSERT_FALSE(listOf(*backend, handoff_prefix, "", 1000).keys.empty())
        << "the hand-off generation must still be intact (untouched) going into the contended round";

    /// NOW, in a round where the prune is busy mid-drain on the debris generation (spending its entire
    /// small budget there), move the kept ref off the hand-off generation -- a fresh manifest replaces it.
    const ManifestRef r_keep_2 = ref(2, 0xE3);
    writeBlobBody(*backend, store->layout(), blob_keep_2);
    writeManifestRaw(*backend, store->layout(), ns, r_keep_2, {blobEntryFor("a", blob_keep_2)});
    publishCommittedTransition(*backend, store->layout(), ns, "keep", r_keep_1, r_keep_2);
    const size_t debris_residue_before = listOf(*backend, store->layout().gcGenPrefix(debris_gen), "", 1000).keys.size();
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);

    /// THE LOAD-BEARING ASSERTIONS: the prune spent its whole (separate) budget on the debris generation
    /// this very round (proving the two really contended for I/O in the same round) ...
    const size_t debris_residue_after = listOf(*backend, store->layout().gcGenPrefix(debris_gen), "", 1000).keys.size();
    EXPECT_EQ(debris_residue_before - debris_residue_after, 2u)
        << "the prune must have spent its entire per-round budget on the debris generation this round";
    /// ... and the hand-off, drawing from its OWN reserve, still fully reclaimed the generation the ref
    /// just moved off -- zero, not starved to zero by the prune's consumption.
    EXPECT_TRUE(listOf(*backend, handoff_prefix, "", 1000).keys.empty())
        << "the hand-off must not be starved by a prune-heavy round that exhausted a SEPARATE budget";
}

/// triage #5, driven through the REAL call site (`Gc::runRegularRound`, not a test seam): a losing
/// leader's pre-CAS wholesale generation-retention prune must never destroy a generation the PARENT
/// (currently-adopted, pre-fold) seal still references, even when the round's own PROPOSED seal has
/// already moved off it and the round's own `gc/state` CAS then loses. `GcStateCasFaultBackend` makes
/// this round's own round-commit CAS return `Conflict` — deterministically and single-threaded standing
/// in for a concurrent leader winning first — which is the only condition under which the fix is
/// externally observable: a round whose own CAS SUCCEEDS reclaims the same generation moments later via
/// the existing (unrelated, unchanged) post-CAS hand-off delete regardless of this fix, so a plain
/// successful round cannot tell bug from fix apart.
TEST(CASGCRetention, LosingRoundNeverDestroysParentSealGeneration)
{
    auto backend = std::make_shared<GcStateCasFaultBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_snapshot_generations_to_keep = 1});
    const Layout & layout = store->layout();
    backend->faulted_key = layout.gcStateKey();
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r1 = ref(1, 0xAA);

    writeBlobBody(*backend, layout, DB::UInt128(1));
    writeManifestRaw(*backend, layout, ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, r1);

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);   // round 1: gen=1 adopted, referencing blob 1's run.
    const GcState st1 = readState(*backend, *store);
    const uint64_t g_parent = st1.snap_generation;

    const auto seal1 = decodeFoldSeal(readOf(*backend, layout.foldSealKey(st1.snap_generation, st1.snap_attempt))->bytes);
    ASSERT_EQ(seal1.blob_target_runs.size(), 1u);
    const String parent_run_key = seal1.blob_target_runs.front().key;
    const String parent_gen_prefix = layout.gcGenPrefix(g_parent);
    ASSERT_FALSE(listOf(*backend, parent_gen_prefix, "", 1000).keys.empty());

    /// A real delta: swap the ref to a new manifest naming a different blob. The next fold will move
    /// shard 0's run OFF `g_parent` onto a fresh generation.
    const ManifestRef r2 = ref(2, 0xBB);
    writeBlobBody(*backend, layout, DB::UInt128(2));
    writeManifestRaw(*backend, layout, ns, r2, {blobEntryFor("b", DB::UInt128(2))});
    publishCommittedTransition(*backend, layout, ns, "tbl", r1, r2);

    /// Arm the fault for the NEXT round's SECOND casPut on gc/state, not its first: the first is
    /// `acquireOrRenewLease`'s own lease-renewal CAS (must SUCCEED, so the round actually folds), and the
    /// second is the round's final round-commit CAS (the one that must LOSE, exactly as if a concurrent
    /// leader had already committed a different seal first).
    const size_t calls_before = backend->calls_to_faulted_key;
    backend->fail_at_call = calls_before + 2;
    bool threw_aborted = false;
    try
    {
        gc.runRegularRound();
    }
    catch (const DB::Exception & e)
    {
        threw_aborted = (e.code() == DB::ErrorCodes::ABORTED);
        if (!threw_aborted)
            throw;
    }
    ASSERT_TRUE(threw_aborted) << "the losing round's own gc/state CAS must fail and propagate ABORTED";
    EXPECT_EQ(backend->calls_to_faulted_key, calls_before + 2)
        << "the round must have made exactly the expected two gc/state casPut attempts (renew + commit)";

    /// GREEN evidence: the losing round's pre-CAS prune must NOT have destroyed `g_parent` — it is still
    /// exactly what the (unreplaced, still-adopted) parent seal references.
    EXPECT_FALSE(listOf(*backend, parent_gen_prefix, "", 1000).keys.empty())
        << "a losing round must never destroy the generation the still-adopted parent seal references";
    EXPECT_TRUE(headExists(*backend, parent_run_key))
        << "the parent seal's exact run object must survive a losing round's pre-CAS prune";

    /// GC is NOT wedged: gc/state is unchanged (the CAS never committed) and the original blob still
    /// resolves cleanly through the surviving parent run — no `CORRUPTED_DATA` from a dangling reference.
    EXPECT_EQ(readState(*backend, *store).snap_generation, g_parent);
    EXPECT_TRUE(blobExists(*backend, layout, DB::UInt128(1)));
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(1)), 1);

    /// A subsequent round (fault already disarmed) must succeed normally, AND must still reclaim the
    /// losing round's own abandoned attempt debris — a generation referenced by NEITHER the parent nor
    /// the new proposed seal — proving the fix does not turn pruning off altogether.
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    const uint64_t g_after = readState(*backend, *store).snap_generation;
    ASSERT_GT(g_after, g_parent);
    for (uint64_t g = g_parent + 1; g < g_after; ++g)
        EXPECT_TRUE(listOf(*backend, layout.gcGenPrefix(g), "", 1000).keys.empty())
            << "generation " << g << " (the losing round's own abandoned attempt debris, referenced by "
               "neither the parent nor the new proposed seal) must still be reclaimed on a successful "
               "round — the fix must not disable pruning";

    EXPECT_TRUE(blobExists(*backend, layout, DB::UInt128(2)));
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(2)), 1);
}

/// keep == 0 is the forensics "keep ALL" mode: NO generation is pruned, snap_pruned_through stays 0.
TEST(CASGCSnapRetention, KeepZeroPrunesNothing)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .gc_snapshot_generations_to_keep = 0});
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);

    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    for (int i = 0; i < 6; ++i)
        ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    const GcState st = readState(*backend, *store);
    EXPECT_EQ(st.snap_pruned_through, 0u) << "keep==0 must prune nothing";

    /// Every seal from generation 1 up to the current one remains. Each generation was sealed under the
    /// attempt of the round that produced it (attempt == that round's lease.seq, which bumps every round),
    /// so a historical generation's seal lives under an earlier attempt than the final snap_attempt — scan
    /// all attempts up to snap_attempt and require the seal to survive under one of them.
    for (uint64_t g = 1; g <= st.snap_generation; ++g)
    {
        bool seal_present = false;
        for (uint64_t a = 0; a <= st.snap_attempt && !seal_present; ++a)
            seal_present = headExists(*backend, store->layout().foldSealKey(g, a));
        EXPECT_TRUE(seal_present) << "keep==0: seal of generation " << g << " must remain";
    }
}

TEST(CASGCRound, OrphanManifestCursorSweepDeletesAndPersistsCursor)
{
    std::shared_ptr<InMemoryBackend> backend;
    PoolConfig config;
    config.pool_prefix = "p";
    /// The GC runner owns a different mount from the synthetic `test` watermark below. This keeps the
    /// cursor-sweep assertions in the parent process without replacing its live renewer incarnation.
    config.server_root_id = "gc-runner";
    config.manifest_sweep_list_budget_keys = 1;
    config.manifest_sweep_delete_budget_keys = 1;
    /// This test drives MANY consecutive rounds expecting each to sweep + persist the cursor; force
    /// fold-every-round (Phase-4 Lever A would otherwise defer once the pool quiesces).
    config.gc_fold_max_defer_rounds = 0;
    auto store = openTestPoolWithConfig(backend, config);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();

    const RootNamespace ns{"test/aa@cas@"};
    registerNamespaceRaw(*backend, store->layout(), ns);
    const ManifestRef r1 = ref(5, 0xCA01);
    const ManifestRef r2 = ref(5, 0xCA02);
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("b", DB::UInt128(2))});
    setWatermarkMinActive(*backend, store->layout(), "test", r1.writer_epoch, /*min_active_build_sequence*/6);

    /// The §6 deletion premise is a second precondition on every sweep deletion: a manifest of an
    /// epoch-`E` build is deletable only once the namespace's sealed fold cursor sits in an epoch
    /// STRICTLY above `E`. The debris above is epoch 1, so the namespace's own ref log has to cross
    /// into epoch 2 and the ROUND has to fold that crossing.
    ///
    /// The crossing is written record by record and folded by the round's own arithmetic intake, which
    /// makes this the composition of the whole chain in one test: a real `EpochSeal` is minted at
    /// `{1,2}`, `RefTableState::apply` consumes it as INV-2's chain link when the epoch-2 record names
    /// it in `prev_epoch_seal`, the walk CROSSES on that back-chain, the round seals a cursor in epoch
    /// 2, and the premise then admits a deletion for the crossed epoch. Nothing here is seeded: an
    /// injected cursor would prove only that the premise reads a number, not that the number can be
    /// produced.
    ///
    /// The live publications use build sequences ABOVE the watermark's `min_active_build_sequence`, so the only
    /// sweep-ELIGIBLE manifests in the namespace remain the two debris bodies -- the premise, not the
    /// watermark, is what this test varies.
    publishAt(*backend, store->layout(), ns, RefTxnId{1, 1}, "tbl", /*build_sequence=*/7,
              DB::UInt128(0xB10B1), /*birth=*/true);
    writeRecoverableCkptForRawFixture(*backend, store->layout(), ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    Gc gc(store, kGc);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease);
    EXPECT_TRUE(manifestExists(*backend, store->layout(), ManifestId{ns, r1}))
        << "a cursor still INSIDE epoch 1 proves nothing about epoch 1's closing seal: the premise retains";
    EXPECT_TRUE(manifestExists(*backend, store->layout(), ManifestId{ns, r2}));

    /// The round above already persisted a mid-circuit cursor while deleting nothing, which is the
    /// cursor half of this test's subject: the sweep examined a key, retained it, and durably recorded
    /// where it got to.
    EXPECT_FALSE(readState(*backend, *store).manifest_sweep_cursor.empty())
        << "the sweep persisted the cursor it examined to, even having deleted nothing";

    /// Close epoch 1 and open epoch 2 over the seal it consumed.
    writeSealAt(*backend, store->layout(), ns, RefTxnId{1, 2});
    publishAt(*backend, store->layout(), ns, RefTxnId{2, 1}, "tbl2", /*build_sequence=*/7,
              DB::UInt128(0xB10B2), /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{1, 2});
    const std::optional<NamespaceLifeId> life = CasRefCatalog::lifeIfCataloged(op, store->layout(), ns);
    ASSERT_TRUE(life.has_value());
    const String ckpt_key = store->layout().refCkptKey(*life);
    const auto old_ckpt = readOf(*backend, ckpt_key);
    ASSERT_TRUE(old_ckpt.has_value());
    {
        OperationForTest ckpt_op(*backend);
        ASSERT_TRUE(std::holds_alternative<Committed>((*ckpt_op).replace(ckpt_key, encodeRefCkpt(RefCkpt{
            .life_epoch = 1,
            .committed_through = RefTxnId{2, 1},
            .checkpoint_snapshot_id = std::nullopt,
            .last_epoch_seal = RefTxnId{1, 2},
        }), old_ckpt->etag, Retry::standard())));
    }

    /// The list budget is one key per round, so reclaiming both debris bodies takes a circuit.
    for (int round = 0; round < 12; ++round)
    {
        ASSERT_TRUE(runRegularRoundReclaiming(gc).acquired_lease) << "round " << round;
        if (!manifestExists(*backend, store->layout(), ManifestId{ns, r1})
            && !manifestExists(*backend, store->layout(), ManifestId{ns, r2}))
            break;
    }
    EXPECT_FALSE(manifestExists(*backend, store->layout(), ManifestId{ns, r1}));
    EXPECT_FALSE(manifestExists(*backend, store->layout(), ManifestId{ns, r2}));

    /// What ADMITTED those deletions, stated rather than inferred: the round folded the crossing itself
    /// and sealed a cursor in the epoch above the debris. Without this the two expectations above would
    /// still pass if the premise ever stopped consulting the cursor at all.
    {
        const GcState st = readState(*backend, *store);
        const CasFoldSeal seal = decodeFoldSeal(
            readOf(*backend, store->layout().foldSealKey(st.snap_generation, st.snap_attempt))->bytes);
        const auto it = seal.ref_lives.find(catalogLifeIdForTest(*backend, store->layout(), ns));
        ASSERT_NE(it, seal.ref_lives.end()) << "the round must have sealed a coverage row";
        EXPECT_FALSE(it->second.coverage.hold.has_value()) << "a held namespace can never reach the premise";
        EXPECT_EQ(it->second.coverage.last_folded_ref_id, (RefTxnId{2, 1}))
            << "the cursor must sit in the epoch ABOVE the debris, reached by folding the seal at {1,2}";
    }

    /// Replacing a live Pool's own mount with a synthetic foreign watermark must make release fail
    /// CLOSED. This was an `EXPECT_DEATH` pinning a `LOGICAL_ERROR` abort; the abort was the defect
    /// (it fires from `~Pool`, defeating `finishTeardown`'s own catch by aborting at exception
    /// construction, and it fires in ASan builds on any deposed writer's shutdown). What it was really
    /// protecting is asserted directly now: the runtime never had a failed renewal, so it still
    /// believed it owned the mount, which makes this the exclusivity-violation arm — refuse, leave the
    /// occupant byte-for-byte untouched, and SURVIVE the teardown.
    std::shared_ptr<InMemoryBackend> foreign_backend;
    PoolConfig foreign_config = config;
    foreign_config.server_root_id = "test";
    auto invalid_store = openTestPoolWithConfig(foreign_backend, std::move(foreign_config));
    const String foreign_mount_key = invalid_store->layout().mountKey("test");
    setWatermarkMinActive(*foreign_backend, invalid_store->layout(), "test", r1.writer_epoch, /*min_active_build_sequence*/6);
    const auto occupant_before = readOf(*foreign_backend, foreign_mount_key);
    ASSERT_TRUE(occupant_before.has_value());
    const uint64_t violations_before
        = ProfileEvents::global_counters[ProfileEvents::CASMountExclusivityViolation].load();

    invalid_store.reset();   /// must not abort, must not terminate

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountExclusivityViolation].load(),
              violations_before + 1)
        << "a runtime that never observed a deposition must report the foreign occupant as a broken "
           "single-writer guarantee";
    const auto occupant_after = readOf(*foreign_backend, foreign_mount_key);
    ASSERT_TRUE(occupant_after.has_value()) << "the release must never delete another incarnation's lease";
    EXPECT_EQ(occupant_after->bytes, occupant_before->bytes)
        << "the release must leave the slot byte-for-byte untouched, never stamp our farewell over it";
}

/// Source-edge idempotency: re-folding the same blob activation does not double-count.
/// A blob activated twice from the SAME source edge (same ManifestId + path) has in-degree 1, not 2.
TEST(CASGCRound, FoldManifestEdgesEmitsOnePlusEdgePerBlob)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};

    const ManifestRef r = ref(1, 0xAA);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    driveToFixpoint(*backend, store, gc);

    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1)
        << "a single published manifest must contribute exactly one source edge per blob";
    EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(1)))
        << "the blob must still exist (in-degree > 0)";
}

/// Re-fold of a removal is idempotent: the fold barrier + source-edge set model ensure that
/// folding the same removal twice (the H1b scenario) does NOT drive the in-degree below zero.
TEST(CASGCRound, ReFoldOfRemovalIsIdempotent)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};

    const ManifestRef r = ref(1, 0xAA);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    Gc gc(store, kGc);
    driveToFixpoint(*backend, store, gc);
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1);

    /// Drop the ref and run to fixpoint. The blob should be reclaimed.
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);
    EXPECT_NO_THROW(driveToFixpoint(*backend, store, gc))
        << "re-fold of a removal must be idempotent (source-edge set, never underflows)";

    EXPECT_FALSE(blobExists(*backend, store->layout(), DB::UInt128(1)))
        << "the blob must be reclaimed after the only reference is dropped";
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 0);
}

/// Two distinct manifests referencing the same blob contribute TWO independent source edges.
/// Dropping one manifest leaves the other's edge intact (in-degree stays 1, blob is spared).
TEST(CASGCRound, TwoManifestsTwoSourceEdgesDropOneSpares)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};

    const ManifestRef r1 = ref(1, 0xAA);
    const ManifestRef r2 = ref(2, 0xBB);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r1, {blobEntryFor("a", DB::UInt128(1))});
    writeManifestRaw(*backend, store->layout(), ns, r2, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl1", std::nullopt, r1);
    publishCommittedTransition(*backend, store->layout(), ns, "tbl2", std::nullopt, r2);

    Gc gc(store, kGc);
    driveToFixpoint(*backend, store, gc);
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 2)
        << "two distinct manifests referencing the same blob must each contribute one source edge";
    EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(1)));

    /// Drop one of the two references; the other still pins the blob.
    dropRefTransition(*backend, store->layout(), ns, "tbl1", r1);
    driveToFixpoint(*backend, store, gc);
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 1)
        << "after dropping one of two references the in-degree must be 1";
    EXPECT_TRUE(blobExists(*backend, store->layout(), DB::UInt128(1)))
        << "the blob must survive — the second reference still pins it";
}

/// ===================== THE REQUEST CONTRACT AT THE ROUND'S OWN WRITES =====================

/// The advisory pulse sends AT MOST ONE write, and neither of the two ways it can fail reaches the
/// caller: the next pulse comes on cadence, so a deposed leader must never spend a retry budget
/// fighting for this key. Both halves are asserted by the write COUNT, because a policy that reissued
/// would be invisible in the outcome.
TEST(CASGc, HeartbeatPulseIsOnceAndAConflictIsIgnored)
{
    class HeartbeatFaultBackend : public InMemoryBackend
    {
    public:
        std::expected<String, RawConflict> write(
            const String & key, const String & bytes, const std::optional<String> & expected_value,
            TransportAccess & access) override
        {
            if (key == hb_key)
            {
                ++hb_writes;
                if (throw_next)
                {
                    throw_next = false;
                    throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "injected heartbeat write outage");
                }
                if (refuse_next)
                {
                    refuse_next = false;
                    return std::unexpected(RawConflict{});
                }
            }
            return InMemoryBackend::write(key, bytes, expected_value, access);
        }

        String hb_key;
        size_t hb_writes = 0;
        bool throw_next = false;
        bool refuse_next = false;
    };

    auto backend = std::make_shared<HeartbeatFaultBackend>();
    auto store = openPoolForTest(backend);
    backend->hb_key = store->layout().gcHbKey();

    Gc::pulseHeartbeat(*store, kGcA);
    ASSERT_EQ(backend->hb_writes, 1u) << "one pulse is one write";

    /// AN UNRESOLVED ATTEMPT IS NOT REISSUED. The key is removed first so the write's own resolving
    /// read finds nothing: with nothing at the key the attempt's fate is genuinely unknown, which is
    /// the only state a reissuing policy would act on. Under `once` the pulse ends there.
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ASSERT_EQ(op.removeCurrent(backend->hb_key, Retry::once()), Removal::Removed);
    backend->throw_next = true;
    const size_t before_unresolved = backend->hb_writes;
    EXPECT_NO_THROW(Gc::pulseHeartbeat(*store, kGcA));
    EXPECT_EQ(backend->hb_writes, before_unresolved + 1)
        << "an unresolved pulse is abandoned, never reissued";

    /// A REFUSED PRECONDITION is the ordinary race with another pulser: ignored, never thrown.
    Gc::pulseHeartbeat(*store, kGcA);
    backend->refuse_next = true;
    const size_t before_refused = backend->hb_writes;
    EXPECT_NO_THROW(Gc::pulseHeartbeat(*store, kGcA));
    EXPECT_EQ(backend->hb_writes, before_refused + 1);
}

/// The steal is the one destructive decision the lease machine makes, and it is a CONJUNCTION: the
/// lease tuple unchanged across two of this contender's own observations, the heartbeat pair unchanged
/// across the same window, and a caller allowed to steal. Each conjunct is falsified on its own here,
/// against the same frozen incumbent, so a build that dropped any one of them fails exactly one line.
TEST(CASGc, LeaseDecideStealsOnlyWithAllThreeConjuncts)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);

    Gc incumbent(store, kGcA);
    ASSERT_TRUE(incumbent.runRegularRound().acquired_lease);

    Gc contender(store, kGcB);
    EXPECT_FALSE(contender.runRegularRound({}, /*allow_steal=*/true).acquired_lease)
        << "the first tick has no earlier observation to freeze against";

    Gc::pulseHeartbeat(*store, kGcA);
    EXPECT_FALSE(contender.runRegularRound({}, /*allow_steal=*/true).acquired_lease)
        << "a moved heartbeat pair is proof of life even with the lease tuple frozen";

    ASSERT_TRUE(incumbent.runRegularRound().acquired_lease);
    EXPECT_FALSE(contender.runRegularRound({}, /*allow_steal=*/true).acquired_lease)
        << "a moved lease tuple is proof of life even with the heartbeat frozen";

    EXPECT_FALSE(contender.runRegularRound({}, /*allow_steal=*/false).acquired_lease)
        << "both observations are frozen, but this caller may not steal";

    EXPECT_TRUE(contender.runRegularRound({}, /*allow_steal=*/true).acquired_lease)
        << "frozen tuple, frozen heartbeat and a caller allowed to steal";
}

/// The round commits everything it did in ONE conditional write of `gc/state`. A refused precondition
/// there means another leader advanced the key, so the round is dropped whole: it throws `ABORTED` and
/// adopts no generation. The lease renewal that OPENED the round is a separate, earlier write and
/// stays committed.
TEST(CASGc, RoundCommitConflictDropsTheRound)
{
    class RoundCommitConflictBackend : public InMemoryBackend
    {
    public:
        std::expected<String, RawConflict> write(
            const String & key, const String & bytes, const std::optional<String> & expected_value,
            TransportAccess & access) override
        {
            if (arm && expected_value && key == gc_state_key)
            {
                const auto stored = InMemoryBackend::read(key, access);
                if (stored
                    && decodeGcState(bytes).snap_generation > decodeGcState(stored->bytes).snap_generation)
                {
                    arm = false;
                    return std::unexpected(RawConflict{});
                }
            }
            return InMemoryBackend::write(key, bytes, expected_value, access);
        }

        String gc_state_key;
        bool arm = false;
    };

    auto backend = std::make_shared<RoundCommitConflictBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    backend->gc_state_key = store->layout().gcStateKey();

    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(1, 0xAA);
    writeBlobBody(*backend, store->layout(), DB::UInt128(1));
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);

    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    /// Asserts presence rather than dereferencing: `gc/state` does not exist until a round's own lease
    /// acquire creates it, and an empty optional here is undefined behaviour, not a failing assertion.
    const auto readState = [&]
    {
        const auto got = op.read(backend->gc_state_key, Retry::once());
        EXPECT_TRUE(got) << "gc/state must exist once a round has acquired the lease";
        return got ? decodeGcState(got->bytes) : GcState{};
    };

    Gc gc(store, kGc);
    /// One honest round first, so the comparison below is against a committed round rather than
    /// against a bootstrap: the double is disarmed, so this round's own commit lands.
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    const GcState before = readState();
    backend->arm = true;
    try
    {
        gc.runRegularRound();
        FAIL() << "a refused round commit must end the round";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::ABORTED);
    }

    const GcState after = readState();
    EXPECT_EQ(after.snap_generation, before.snap_generation)
        << "a refused commit adopts no generation";
    EXPECT_GT(after.lease.seq, before.lease.seq)
        << "the lease renewal that opened the round is a separate, earlier write and stays committed";
}

#if USE_AWS_S3
/// A refused `gc/state` precondition whose resolve read was ITSELF refused: nothing observed the key,
/// so the round must not report a competing leader nobody saw. It still aborts, and the next round
/// re-reads -- the behaviour is unchanged, only the claim the message makes.
///
/// The S3 gate is the fault's, not the site's: the definitive-refusal classification that makes a
/// resolve read settle nothing rather than be reissued exists only for S3 errors.
TEST(CASGc, RoundCommitUnobservedConflictNamesNoCompetingLeader)
{
    class UnobservedCommitBackend : public InMemoryBackend
    {
    public:
        std::expected<String, RawConflict> write(
            const String & key, const String & bytes, const std::optional<String> & expected_value,
            TransportAccess & access) override
        {
            /// Only the ROUND COMMIT advances `round`; the lease renewal writes the same key and must
            /// pass through untouched.
            if (arm && expected_value && key == gc_state_key)
            {
                const auto stored = InMemoryBackend::read(key, access);
                if (stored && decodeGcState(bytes).round > decodeGcState(stored->bytes).round)
                {
                    arm = false;
                    refuse_read = true;
                    return std::unexpected(RawConflict{});
                }
            }
            return InMemoryBackend::write(key, bytes, expected_value, access);
        }

        std::optional<Raw> read(const String & key, TransportAccess & access) override
        {
            if (refuse_read && key == gc_state_key)
            {
                refuse_read = false;
                throw DB::S3Exception("UnobservedCommitBackend: the settling read is definitively refused",
                                      Aws::S3::S3Errors::UNKNOWN, "MalformedXML");
            }
            return InMemoryBackend::read(key, access);
        }

        String gc_state_key;
        bool arm = false;
        bool refuse_read = false;
    };

    auto backend = std::make_shared<UnobservedCommitBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds*/ 0);
    backend->gc_state_key = store->layout().gcStateKey();

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    backend->arm = true;
    try
    {
        gc.runRegularRound();
        FAIL() << "a refused round commit must end the round";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::ABORTED);
        EXPECT_NE(e.message().find("resolve read observed nothing"), String::npos) << e.message();
        EXPECT_EQ(e.message().find("another leader advanced it"), String::npos)
            << "nothing observed the key, so no competing leader may be named: " << e.message();
    }
}
#endif
