#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>

#include <atomic>
#include <limits>
#include <thread>

namespace DB::ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int ABORTED;
}

using namespace DB::Cas;


/// MountLeaseKeeper behavior: the per-server mount lease and the merged build-watermark floor ride the
/// SAME slot, renewed by one beat. The keeper anchors durably before return, adopts a slot already
/// written by `claimMount` (same uuid+epoch), re-reads the callback on each renew and bumps `seq`,
/// stamps the farewell sentinel (`min_active = UINT64_MAX`, `expires_at_ms <= now`) on `stop`, and
/// fails closed on any foreign touch (`renewOnce` throws).

namespace
{
/// The normal steady-state flow: `claimMount` writes the live (uuid, epoch) mount, THEN the keeper
/// adopts it. Seed that claim so `start` adopts instead of self-tripping the double-start guard.
void seedOwnClaim(Backend & b, const Layout & l, const String & srid, UInt128 uuid, uint64_t epoch,
                  uint64_t now_ms, uint64_t ttl_ms)
{
    ASSERT_EQ(claimMount(b, l, srid, uuid, epoch, now_ms, ttl_ms).kind, MountClaimResult::Claimed);
}

/// Fix #37 phase 1: `shouldFenceOnTransientRenewFailure` is `protected` on `MountLeaseKeeper` (it is an
/// internal decision hook, not part of the public keeper API) -- promote it to `public` here so these
/// tests can drive it directly, without needing a real background thread.
class TestableMountLeaseKeeper : public MountLeaseKeeper
{
public:
    using MountLeaseKeeper::MountLeaseKeeper;
    using MountLeaseKeeper::shouldFenceOnTransientRenewFailure;
    using MountLeaseKeeper::onRenewSucceeded;
};
}

TEST(CASHeartbeat, AnchorCarriesFloor)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    uint64_t min_active_now = 5;
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    MountLeaseKeeper keeper(backend, layout, srid, uuid, /*writer_epoch=*/9, std::chrono::milliseconds(100),
                            [&] { return now_ms; }, [&] { return min_active_now; });
    keeper.start();

    auto hr = backend->head(layout.mountKey(srid));
    ASSERT_TRUE(hr.exists);
    auto m = decodeMountLease(backend->get(layout.mountKey(srid))->bytes);
    EXPECT_EQ(m.writer_epoch, 9u);
    EXPECT_EQ(m.min_active, 5u);
    EXPECT_EQ(m.seq, 1u);
    EXPECT_FALSE(m.gc_fenced);
}

TEST(CASHeartbeat, RenewRereadsCallbackAndBumpsSeq)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    uint64_t min_active_now = 5;
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    MountLeaseKeeper keeper(backend, layout, srid, uuid, /*writer_epoch=*/9, std::chrono::milliseconds(100),
                            [&] { return now_ms; }, [&] { return min_active_now; });
    keeper.start();

    /// The dynamic field moves; the renewal re-reads it off the callback and bumps seq.
    now_ms = 1500;
    min_active_now = 8;
    keeper.renewOnce();

    auto m = decodeMountLease(backend->get(layout.mountKey(srid))->bytes);
    EXPECT_EQ(m.min_active, 8u);
    EXPECT_EQ(m.seq, 2u);
    EXPECT_EQ(m.expires_at_ms, 1500u + 100u);
}

TEST(CASHeartbeat, StopStampsExpiredAndFarewellSentinel)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    MountLeaseKeeper keeper(backend, layout, srid, uuid, /*writer_epoch=*/9, std::chrono::milliseconds(100),
                            [&] { return now_ms; }, [] { return uint64_t{5}; });
    keeper.start();

    now_ms = 2000;
    keeper.stop();

    auto m = decodeMountLease(backend->get(layout.mountKey(srid))->bytes);
    /// Terminal body stamps the lease already-expired (so a same-server reopen reclaims immediately)
    /// AND folds the watermark farewell into it (min_active = UINT64_MAX).
    EXPECT_LE(m.expires_at_ms, now_ms);
    EXPECT_EQ(m.min_active, std::numeric_limits<uint64_t>::max());
}

/// Phase A (spec rev.4 2026-07-24): a confirmed renewal mismatch whose re-read shows OUR OWN
/// (uuid, epoch), unfenced, is state UNCERTAINTY (an ambiguous landed renewal of ours, or a
/// same-pair twin after epoch-state loss) — fail closed via fence + self-remount, never an
/// exception that aborts debug/ASan builds at construction.
TEST(CASHeartbeat, SameEpochUnfencedTouchIsUncertainNotFatal)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    MountLeaseKeeper keeper(backend, layout, srid, uuid, /*writer_epoch=*/9, std::chrono::milliseconds(100),
                            [&] { return now_ms; }, [] { return uint64_t{5}; });
    keeper.start();

    /// The slot advances past our held token under our own pair (the ambiguous-landed-renewal shape).
    const HeadResult h = backend->head(layout.mountKey(srid));
    ASSERT_TRUE(h.exists);
    MountLease advanced;
    advanced.server_uuid = uuid;
    advanced.writer_epoch = 9;
    advanced.seq = 99;
    backend->putOverwrite(layout.mountKey(srid), encodeMountLease(advanced), h.token);

    try
    {
        keeper.renewOnce();
        FAIL() << "renewOnce must throw on a confirmed mismatch";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::ABORTED) << e.message();
        EXPECT_NE(e.message().find("state uncertain"), String::npos) << e.message();
        /// Forensics must ride in the message: the observed seq and our local seq.
        EXPECT_NE(e.message().find("seq=99"), String::npos) << e.message();
        /// The local-seq fragment specifically -- not just any "seq=99" substring (which the
        /// OBSERVED holder's own describeMountHolder text could also satisfy on its own).
        EXPECT_NE(e.message().find("vs our seq="), String::npos) << e.message();
    }
}

/// A body under our own uuid but a NEWER writer_epoch is proven supersession — a normal fencing
/// outcome (the TLA model's localLost), fail closed but never an abort.
TEST(CASHeartbeat, SupersededTouchIsFailClosedNotFatal)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    MountLeaseKeeper keeper(backend, layout, srid, uuid, /*writer_epoch=*/9, std::chrono::milliseconds(100),
                            [&] { return now_ms; }, [] { return uint64_t{5}; });
    keeper.start();

    const HeadResult h = backend->head(layout.mountKey(srid));
    ASSERT_TRUE(h.exists);
    MountLease successor;
    successor.server_uuid = uuid;
    successor.writer_epoch = 10;
    successor.seq = 1;
    backend->putOverwrite(layout.mountKey(srid), encodeMountLease(successor), h.token);

    try
    {
        keeper.renewOnce();
        FAIL() << "renewOnce must throw on a confirmed mismatch";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::ABORTED) << e.message();
        EXPECT_NE(e.message().find("superseded by a newer incarnation"), String::npos) << e.message();
    }
}

/// A foreign server holding our mount slot must FAIL CLOSED — and must not take the process with it.
///
/// This test used to be `ForeignUuidTouchStillDies`, an `EXPECT_DEATH` that pinned the abort. The abort
/// was the defect: the arm raised `LOGICAL_ERROR`, which aborts at CONSTRUCTION in debug/ASan builds,
/// and it does so on the keeper's BACKGROUND thread — so an environment-reachable condition (clear the
/// prefix, recreate under a different server id, and the survivor's next renewal lands there; see
/// `CASRefContiguousAlloc.SurvivingWriterIsFencedByTheRecreatedPoolsMount`, which drives exactly that)
/// took the whole server down, and took the ASan gate down with it.
///
/// What must NOT change is the outcome, which is what this test now pins: `renewOnce` throws, the throw
/// carries the foreign holder's identity, and it is classified `ABORTED` — the same mount-lost class the
/// sibling fencing arms use, which the background loop turns into a latched write fence. The
/// `abort_on_logical_error` arming is deliberately kept: with it ON, a `LOGICAL_ERROR` would still abort,
/// so reaching the `EXPECT_THROW` at all is the proof that this condition is no longer classified as one.
TEST(CASHeartbeat, ForeignUuidTouchFailsClosedWithoutAborting)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    MountLeaseKeeper keeper(backend, layout, srid, uuid, /*writer_epoch=*/9, std::chrono::milliseconds(100),
                            [&] { return now_ms; }, [] { return uint64_t{5}; });
    keeper.start();

    const HeadResult h = backend->head(layout.mountKey(srid));
    ASSERT_TRUE(h.exists);
    MountLease foreign;
    foreign.server_uuid = UInt128(0x9999);
    foreign.writer_epoch = 1;
    foreign.seq = 1;
    backend->putOverwrite(layout.mountKey(srid), encodeMountLease(foreign), h.token);

    /// Restored on every exit: this flag is process-global and every later test in this binary would
    /// inherit it.
    const bool armed_before = DB::abort_on_logical_error.load(std::memory_order_relaxed);
    DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
    SCOPE_EXIT({ DB::abort_on_logical_error.store(armed_before, std::memory_order_relaxed); });

    String message;
    int code = 0;
    try
    {
        keeper.renewOnce();
        FAIL() << "a foreign holder must fail the renewal closed, not be silently taken over";
    }
    catch (const DB::Exception & e)
    {
        message = e.message();
        code = e.code();
    }
    EXPECT_NE(message.find("held by a foreign server"), String::npos) << message;
    EXPECT_EQ(code, DB::ErrorCodes::ABORTED)
        << "the mount-lost class the background loop latches the write fence on -- and, critically, not "
           "LOGICAL_ERROR, which would abort this keeper thread and the whole process with it";
}

/// Mount-slot writer audit (the P1 "foreign writer" instrument): every mount-slot WRITE and every
/// OBSERVED foreign/conflicting body becomes an event, carrying the conflicting body's identity —
/// the payload the chronic "touched by a foreign writer" collisions need to be diagnosable.
TEST(CASMountAudit, ClaimReleaseAndForeignConflictEmitEvents)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    std::vector<CasEvent> seen;
    CasEventSink sink = [&](const CasEvent & e) { seen.push_back(e); };

    const uint64_t now_ms = 1'000'000;
    /// mint for uuid 1 -> one mount_claim
    ASSERT_EQ(claimMount(*backend, layout, "a", UInt128{1}, 1, now_ms, /*ttl_ms=*/10'000, {}, sink).kind,
              MountClaimResult::Claimed);
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0].type, CasEventType::MountClaim);
    EXPECT_EQ(seen[0].detail.at("server_root_id"), "a");
    EXPECT_EQ(seen[0].detail.at("branch"), "mint");

    /// a FOREIGN uuid claiming a live slot -> mount_conflict carrying the current holder's identity
    seen.clear();
    (void)claimMount(*backend, layout, "a", UInt128{2}, 1, now_ms, /*ttl_ms=*/10'000, {}, sink);
    ASSERT_FALSE(seen.empty());
    EXPECT_EQ(seen.back().type, CasEventType::MountConflict);
    EXPECT_EQ(seen.back().detail.at("server_root_id"), "a");
    /// The conflict must carry the ORIGINAL holder's identity (uuid 1, the minter) — not the
    /// foreign claimer's (uuid 2).
    EXPECT_EQ(seen.back().detail.at("holder_uuid"), u128ToHex(UInt128{1}));
    EXPECT_NE(seen.back().detail.at("holder_uuid"), u128ToHex(UInt128{2}));
}

/// The MountLeaseKeeper wiring: `start` adopting an already-claimed slot emits mount_claim, `stop`
/// (the farewell write) emits mount_release.
TEST(CASMountAudit, KeeperAdoptEmitsClaimAndTerminateEmitsRelease)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    std::vector<CasEvent> seen;
    CasEventSink sink = [&](const CasEvent & e) { seen.push_back(e); };
    MountLeaseKeeper keeper(backend, layout, srid, uuid, /*writer_epoch=*/9, std::chrono::milliseconds(100),
                            [&] { return now_ms; }, [] { return uint64_t{5}; }, sink);
    keeper.start();

    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0].type, CasEventType::MountClaim);
    EXPECT_EQ(seen[0].detail.at("branch"), "adopt");

    seen.clear();
    now_ms = 2000;
    keeper.stop();

    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0].type, CasEventType::MountRelease);
    EXPECT_EQ(seen[0].detail.at("branch"), "farewell");
}

/// Keeper-level foreign-conflict refusal: the mount slot is already held by a FOREIGN uuid (X) when
/// a keeper for a DIFFERENT uuid (Y) tries to claim it. This must fail closed and — since the
/// mount-audit sink is not yet installed at first-open — name X in the exception's message text
/// (the only identity carrier in err.log at that point). MountConflict payload coverage is above.
TEST(CASMountAudit, KeeperForeignConflictRefusesAndNamesHolder)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid_x(0x1111);
    const UInt128 uuid_y(0x2222);
    uint64_t now_ms = 1000;

    /// Foreign holder X claims the slot first.
    ASSERT_EQ(claimMount(*backend, layout, srid, uuid_x, /*our_epoch=*/1, now_ms, /*ttl_ms=*/100).kind,
              MountClaimResult::Claimed);

    MountLeaseKeeper keeper(backend, layout, srid, uuid_y, /*writer_epoch=*/1, std::chrono::milliseconds(100),
                            [&] { return now_ms; }, [] { return uint64_t{5}; });

    /// The enriched refusal message must name the OBSERVED holder (X), not the caller (Y).
    const String holder_uuid = u128ToHex(uuid_x);
    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            keeper.start();
        },
        holder_uuid);
}

/// `Pool::open` can fail before/inside `doStart` (e.g. a foreign-conflict refusal, see
/// `KeeperForeignConflictRefusesAndNamesHolder` above) — the keeper is destroyed without ever having
/// claimed anything. Teardown must not throw "release before start"; there is nothing to release. A
/// stop AFTER a successful start still performs the farewell (covered by
/// `StopStampsExpiredAndFarewellSentinel` above); a genuinely-started DOUBLE terminate stays loud.
TEST(CASHeartbeat, StopBeforeStartIsQuietNoOp)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    uint64_t now_ms = 1000;
    MountLeaseKeeper keeper(backend, layout, "a", UInt128{1}, /*writer_epoch=*/1, std::chrono::milliseconds(10'000),
                            [&] { return now_ms; }, [] { return uint64_t{0}; });
    /// start() never called.
    EXPECT_NO_THROW(keeper.stop());
    EXPECT_NO_THROW(keeper.stop());
}

/// "A fence costs an epoch" at the keeper layer: the GC fenced our fresh lease before we adopted it
/// (the lease expired mid-open — e.g. a slow first beat). This must fail closed with a TYPED,
/// recoverable `MountFencedException`, distinct from the generic "touched by a foreign writer"
/// `LOGICAL_ERROR` — the open path (Task 4) tells "re-open with a fresh epoch" apart from "fail hard"
/// by this code, not by parsing message text.
TEST(CASMountAudit, KeeperAdoptRefusesFencedSelfWithTypedError)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;

    /// mint (uuid, epoch 9), then fence it in place (what computeHeartbeatFloor does on expiry):
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);
    {
        auto got = backend->get(layout.mountKey(srid));
        MountLease fenced = decodeMountLease(got->bytes);
        fenced.gc_fenced = true;
        fenced.seq += 1;
        ASSERT_EQ(backend->putOverwrite(layout.mountKey(srid), encodeMountLease(fenced), got->token).outcome,
                  PutOutcome::Done);
    }

    std::vector<CasEvent> seen;
    CasEventSink sink = [&](const CasEvent & e) { seen.push_back(e); };
    /// A keeper for the SAME (uuid, epoch) tries to adopt the now-fenced slot.
    MountLeaseKeeper keeper(backend, layout, srid, uuid, /*writer_epoch=*/9, std::chrono::milliseconds(100),
                            [&] { return now_ms; }, [] { return uint64_t{5}; }, sink);

    bool threw = false;
    try
    {
        keeper.start();
    }
    catch (const MountFencedException & e)
    {
        threw = true;
        EXPECT_NE(e.message().find("fenced by GC"), String::npos) << e.message();
        EXPECT_EQ(e.message().find("foreign writer"), String::npos) << e.message();
    }
    EXPECT_TRUE(threw);

    ASSERT_FALSE(seen.empty());
    EXPECT_EQ(seen.back().type, CasEventType::MountConflict);
    EXPECT_EQ(seen.back().detail.at("branch"), "fenced_by_gc");
}

/// A renew mismatch is classified by BODY, not blamed on "a foreign writer" by default: the GC can
/// fence our OWN (uuid, epoch) mount slot after our lease expires (a late renewal beat racing the
/// GC's fence-out). The keeper must re-read and recognize this as its OWN incarnation being fenced —
/// a recoverable `MountFencedException`, not the generic single-writer-violation text.
TEST(CASHeartbeat, RenewOverFencedOwnSlotIsClassifiedNotForeign)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    std::vector<CasEvent> seen;
    CasEventSink sink = [&](const CasEvent & e) { seen.push_back(e); };
    MountLeaseKeeper keeper(backend, layout, srid, uuid, /*writer_epoch=*/9, std::chrono::milliseconds(100),
                            [&] { return now_ms; }, [] { return uint64_t{5}; }, sink);
    keeper.start();
    seen.clear();

    /// Mid-run: the GC fences our own (uuid, epoch) mount slot in place (as `computeHeartbeatFloor`
    /// does on an expired lease), preserving the whole body — a token-guarded putOverwrite, exactly
    /// as the GC's own fence-out does it.
    {
        const auto got = backend->get(layout.mountKey(srid));
        MountLease fenced = decodeMountLease(got->bytes);
        fenced.gc_fenced = true;
        fenced.seq += 1;
        ASSERT_EQ(backend->putOverwrite(layout.mountKey(srid), encodeMountLease(fenced), got->token).outcome,
                  PutOutcome::Done);
    }

    /// The renewal must classify the fence honestly — not "foreign writer":
    try
    {
        keeper.renewOnce();
        FAIL() << "renewOnce over a fenced slot must throw";
    }
    catch (const MountFencedException & e)
    {
        EXPECT_TRUE(e.message().find("fenced by GC") != String::npos);
        EXPECT_TRUE(e.message().find("foreign writer") == String::npos);
    }
    /// and the capture sink saw mount_conflict branch=fenced_by_gc with the fenced body's identity.
    ASSERT_FALSE(seen.empty());
    EXPECT_EQ(seen.back().type, CasEventType::MountConflict);
    EXPECT_EQ(seen.back().detail.at("branch"), "fenced_by_gc");
    EXPECT_EQ(seen.back().detail.at("holder_uuid"), u128ToHex(uuid));
}

/// Fix #37 phase 1: a TRANSIENT renewal failure (the background loop's `renewOnce` threw, but NOT via a
/// confirmed `onRenewMismatch`) must not fence while the last confirmed lease still has more than
/// `lease_safety_margin` left before it would expire -- the mount-lease protocol guarantees no other
/// writer can claim the slot before that deadline, so riding it out is safe.
TEST(CASHeartbeat, TransientRetryStaysWithinLeaseDeadline)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/1000);

    TestableMountLeaseKeeper keeper(backend, layout, srid, uuid, /*writer_epoch=*/9,
                                     std::chrono::milliseconds(1000), [&] { return now_ms; },
                                     [] { return uint64_t{0}; }, CasEventSink{},
                                     /*lease_safety_margin=*/std::chrono::milliseconds(100));
    keeper.start();   /// claim() anchors confirmed_deadline_ms = 1000 (now) + 1000 (ttl) = 2000

    /// Well before the deadline's safety margin (2000 - 100 = 1900): must NOT fence.
    now_ms = 1500;
    EXPECT_FALSE(keeper.shouldFenceOnTransientRenewFailure());

    /// At/after the safety-margin boundary: must fence.
    now_ms = 1900;
    EXPECT_TRUE(keeper.shouldFenceOnTransientRenewFailure());
    now_ms = 2000;
    EXPECT_TRUE(keeper.shouldFenceOnTransientRenewFailure());
}

/// A successful renew extends the confirmed deadline -- the boundary that WOULD have tripped against
/// the OLD deadline no longer does against the refreshed one. `confirmed_deadline_ms` is refreshed by
/// `onRenewSucceeded` (the hook the real background loop calls after a successful beat -- see
/// `CasPool.cpp`'s note that unit tests drive `renewOnce` directly and never through the loop, so this
/// test calls the promoted `onRenewSucceeded` itself to model exactly what one successful real beat
/// does), not by a bare `renewOnce` call in isolation.
TEST(CASHeartbeat, SuccessfulRenewExtendsTransientRetryDeadline)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/1000);

    TestableMountLeaseKeeper keeper(backend, layout, srid, uuid, /*writer_epoch=*/9,
                                     std::chrono::milliseconds(1000), [&] { return now_ms; },
                                     [] { return uint64_t{0}; }, CasEventSink{},
                                     /*lease_safety_margin=*/std::chrono::milliseconds(100));
    keeper.start();   /// confirmed_deadline_ms = 2000

    now_ms = 1900;
    ASSERT_TRUE(keeper.shouldFenceOnTransientRenewFailure()) << "sanity: 1900 trips the OLD deadline";

    /// A renew at now_ms=1900 succeeds; onRenewSucceeded (as the background loop would call it)
    /// refreshes confirmed_deadline_ms to 1900 + 1000 = 2900.
    keeper.renewOnce();
    keeper.onRenewSucceeded();
    EXPECT_FALSE(keeper.shouldFenceOnTransientRenewFailure())
        << "the refreshed deadline (2900, margin 100) must not trip at now_ms=1900 any more";
}

/// Fence-not-rescue round follow-up #1: the redo site in `CasPool.cpp`'s `mountWritable` (the
/// claim-consumed-the-TTL branch) calls `renewOnce` DIRECTLY -- never through
/// `onRenewSucceeded` (that hook is only ever invoked by `backgroundLoop`). Before this fix, only
/// `onRenewSucceeded` refreshed `confirmed_deadline_ms`, so a direct `renewOnce` call left the wall
/// deadline stale at the pre-redo anchor. A successful renewal must refresh the deadline regardless
/// of who called `renewOnce` -- background loop or a direct caller alike.
TEST(CASHeartbeat, DirectRenewOnceRefreshesConfirmedDeadlineWithoutOnRenewSucceeded)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/1000);

    TestableMountLeaseKeeper keeper(backend, layout, srid, uuid, /*writer_epoch=*/9,
                                     std::chrono::milliseconds(1000), [&] { return now_ms; },
                                     [] { return uint64_t{0}; }, CasEventSink{},
                                     /*lease_safety_margin=*/std::chrono::milliseconds(100));
    keeper.start();   /// confirmed_deadline_ms = 2000

    now_ms = 1900;
    ASSERT_TRUE(keeper.shouldFenceOnTransientRenewFailure()) << "sanity: 1900 trips the OLD deadline";

    /// A DIRECT renewOnce -- exactly what the redo site calls, with `onRenewSucceeded` never invoked
    /// anywhere near it -- must ALSO refresh confirmed_deadline_ms to 1900 + 1000 = 2900.
    keeper.renewOnce();
    EXPECT_FALSE(keeper.shouldFenceOnTransientRenewFailure())
        << "a direct renewOnce (with no onRenewSucceeded call at all) must refresh the confirmed "
           "deadline too -- the redo's wall anchor must not go stale";
}

namespace
{
/// Wraps an `InMemoryBackend` so `putOverwrite` throws a TRANSIENT (non-mismatch) exception for the
/// first `fault_count` calls, then delegates normally. Models a `putOverwrite` that fails before any
/// outcome is observed (timeout / 5xx / connection reset) -- exactly the case fix #37 phase 1 targets,
/// as opposed to a `PreconditionFailed` (a CONFIRMED, backend-observed mismatch).
class TransientPutOverwriteFaultBackend final : public InMemoryBackend
{
public:
    int fault_count = 0;

    PutResult putOverwrite(const String & k, const String & b, const Token & e, const ObjectMeta & m) override
    {
        if (fault_count > 0)
        {
            --fault_count;
            throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "injected transient putOverwrite fault");
        }
        return InMemoryBackend::putOverwrite(k, b, e, m);
    }
};
}

/// Real background thread: two transient faults, then the third beat lands. The loop must NOT stop and
/// must NOT fence (on_lost never fires) -- it just keeps retrying at the normal period.
TEST(CASHeartbeat, BackgroundLoopRetriesTransientFailureWithoutFencingOrStopping)
{
    auto backend = std::make_shared<TransientPutOverwriteFaultBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/30000);

    std::atomic<bool> lost{false};
    MountLeaseKeeper keeper(backend, layout, srid, uuid, /*writer_epoch=*/9, std::chrono::milliseconds(30000),
                            [&] { return now_ms; }, [] { return uint64_t{0}; }, CasEventSink{},
                            std::chrono::milliseconds(2000));
    keeper.setFenceCallbacks([](uint64_t) {}, [&] { lost = true; });
    keeper.start();   /// the adopt-path putOverwrite must land BEFORE the faults are armed below.

    /// Arm the faults only for the BACKGROUND renewals under test -- `start`'s own adopt-path
    /// putOverwrite above must not be faulted, or it throws straight out of this test body instead of
    /// exercising the loop's transient-retry path.
    backend->fault_count = 2;
    keeper.startBackground(std::chrono::milliseconds(20));

    /// Bounded poll (not a blind sleep): waits for the REAL background thread to land a renewal past
    /// the two faults. Generous 5s timeout; a background-thread test cannot be made synchronous without
    /// a dedicated test seam this codebase does not have (see gtest_cas_pool.cpp's preference for
    /// synchronous renewOnce-driven tests elsewhere -- not applicable here, since the loop-continuation
    /// behavior under test only exists inside backgroundLoop itself).
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    uint64_t seq = 1;
    while (std::chrono::steady_clock::now() < deadline)
    {
        seq = decodeMountLease(backend->get(layout.mountKey(srid))->bytes).seq;
        if (seq >= 2)
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    keeper.stopBackground();

    EXPECT_GE(seq, 2u) << "background loop never recovered from the transient faults";
    EXPECT_FALSE(lost.load()) << "a transient putOverwrite failure must not trip the fence";
}

/// A CONFIRMED mismatch (a same-(uuid, epoch) unfenced touch lands on the slot -- state
/// uncertainty, spec rev.4) must fence immediately, even with the deadline nowhere near expiry --
/// the other half of fix #37 phase 1's distinction. Phase A repartition: this shape is no longer
/// fatal (it throws `ABORTED`, not `LOGICAL_ERROR`), so the loop must recover via `on_lost` instead
/// of dying -- observe the fence the same way `BackgroundLoopRetriesTransientFailureWithoutFencingOrStopping`
/// observes non-fencing.
TEST(CASHeartbeat, BackgroundLoopFencesImmediatelyOnConfirmedMismatch)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/30000);

    std::atomic<bool> lost{false};
    MountLeaseKeeper keeper(
        backend, layout, srid, uuid, /*writer_epoch=*/9, std::chrono::milliseconds(30000),
        [&] { return now_ms; }, [] { return uint64_t{0}; }, CasEventSink{}, std::chrono::milliseconds(2000));
    keeper.setFenceCallbacks([](uint64_t) {}, [&] { lost = true; });
    keeper.start();

    /// A same-(uuid, epoch) unfenced touch overwrites the slot BEFORE the first background beat.
    const HeadResult h = backend->head(layout.mountKey(srid));
    MountLease advanced;
    advanced.server_uuid = uuid;
    advanced.writer_epoch = 9;
    advanced.seq = 99;
    ASSERT_EQ(backend->putOverwrite(layout.mountKey(srid), encodeMountLease(advanced), h.token).outcome,
              PutOutcome::Done);

    keeper.startBackground(std::chrono::milliseconds(20));
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!lost.load() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    keeper.stopBackground();

    EXPECT_TRUE(lost.load()) << "background loop must fence immediately on a confirmed same-epoch mismatch";
}

namespace
{
/// The landed-but-unacked case: the putOverwrite APPLIES to the in-memory state, THEN throws a
/// transient exception — the exact CI shape (a client-side timeout whose PUT landed server-side).
/// The existing TransientPutOverwriteFaultBackend throws BEFORE applying and cannot model this.
class ApplyThenThrowPutOverwriteFaultBackend final : public InMemoryBackend
{
public:
    int fault_count = 0;

    PutResult putOverwrite(const String & k, const String & b, const Token & e, const ObjectMeta & m) override
    {
        if (fault_count > 0)
        {
            --fault_count;
            InMemoryBackend::putOverwrite(k, b, e, m);   /// the write LANDS...
            throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR,
                "injected ambiguous fault: applied, ack lost");   /// ...the ack does not.
        }
        return InMemoryBackend::putOverwrite(k, b, e, m);
    }
};
}

/// End-to-end reproduction of the CI crash (Altinity PR#2073, asan CAS-s3 stateless): beat 1's
/// renewal lands but its ack is lost (transient -> the loop retries, deadline permitting); beat 2
/// renews with the now-stale token, gets a CONFIRMED mismatch, re-reads our own advanced body ->
/// the Phase A uncertain branch -> the loop stops, on_lost fires (fence latches; in production the
/// Pool self-remounts from there). No process death anywhere.
TEST(CASHeartbeat, BackgroundLoopSurvivesAmbiguousLandedRenewal)
{
    auto backend = std::make_shared<ApplyThenThrowPutOverwriteFaultBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/30000);

    std::atomic<bool> lost{false};
    MountLeaseKeeper keeper(backend, layout, srid, uuid, /*writer_epoch=*/9, std::chrono::milliseconds(30000),
                            [&] { return now_ms; }, [] { return uint64_t{0}; }, CasEventSink{},
                            std::chrono::milliseconds(2000));
    keeper.setFenceCallbacks([](uint64_t) {}, [&] { lost = true; });
    keeper.start();   /// the adopt-path putOverwrite must land unfaulted.

    backend->fault_count = 1;   /// beat 1: lands + throws (ambiguous); beat 2: confirmed mismatch.
    keeper.startBackground(std::chrono::milliseconds(20));

    /// Bounded poll for on_lost (never a blind sleep): the deadline is generous; the loop needs
    /// two ~20ms beats. abort_on_logical_error stays ON to prove no branch constructs one.
    DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!lost.load() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    /// Restore the default before returning: unlike the EXPECT_DEATH-wrapped uses above (which flip
    /// this flag only inside a forked child), this test sets it in the actual test process — leaving
    /// it ON would make every LOGICAL_ERROR raised by any LATER test in this binary abort instead of
    /// throwing, regardless of that test's own intent.
    DB::abort_on_logical_error.store(false, std::memory_order_relaxed);
    EXPECT_TRUE(lost.load()) << "the confirmed mismatch after an ambiguous landed renewal must "
                                "latch the fence via on_lost (and must not abort the process)";
    keeper.stopBackground();
}

/// Phase B: the confirmed-lease deadline anchors at the ATTEMPT-START instant, not the response
/// instant — a slow ack must not extend the local fence past what the durable body authorizes.
TEST(CASHeartbeat, RenewDeadlineAnchorsAtAttemptStartNotResponseTime)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    seedOwnClaim(*backend, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/1000);

    TestableMountLeaseKeeper keeper(backend, layout, srid, uuid, /*writer_epoch=*/9,
                                    std::chrono::milliseconds(1000),
                                    [&] { return now_ms; }, [] { return uint64_t{0}; }, CasEventSink{},
                                    std::chrono::milliseconds(100));
    keeper.start();   /// claim at now=1000 -> anchored confirmed deadline 2000

    /// Beat at now=1500; the "ack" (onRenewSucceeded) arrives late, at now=2400 — after the
    /// durable expiry stamped by THIS beat's payload (1500+1000=2500 durable; anchor 1500).
    now_ms = 1500;
    keeper.renewOnce();
    now_ms = 2400;
    keeper.onRenewSucceeded();

    /// Anchored: deadline = 1500 + 1000 = 2500. At now=2401 with margin 100 the boundary check is
    /// 2401 + 100 >= 2500 -> must fence. (Response-time behavior — the bug — would give
    /// 2400 + 1000 = 3400 and NOT fence.)
    now_ms = 2401;
    EXPECT_TRUE(keeper.shouldFenceOnTransientRenewFailure())
        << "a late ack must not extend the confirmed deadline past attempt-start + TTL";
}
