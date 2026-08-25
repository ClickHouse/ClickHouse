#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>

#include <atomic>
#include <deque>
#include <limits>
#include <mutex>
#include <thread>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int ABORTED;
}

using namespace DB::Cas;


/// MountLeaseKeeper behavior: the per-server mount lease and the merged build-watermark floor ride the
/// SAME slot, renewed by one beat. The keeper anchors durably before return, adopts a slot already
/// written by `claimMount` (same uuid+epoch), re-reads the callback on each renew and bumps `seq`,
/// stamps the farewell sentinel (`min_active = UINT64_MAX`, `expires_at_ms <= now`) on `release`, and
/// returns typed terminal results on any foreign touch.

namespace
{
/// The normal steady-state flow: `claimMount` writes the live (uuid, epoch) mount, THEN the keeper
/// adopts it. Seed that claim so `start` adopts instead of self-tripping the double-start guard.
void seedOwnClaim(Backend & b, const Layout & l, const String & srid, UInt128 uuid, uint64_t epoch,
                  uint64_t now_ms, uint64_t ttl_ms)
{
    ASSERT_EQ(claimMount(b, l, srid, uuid, epoch, now_ms, ttl_ms).kind, MountClaimResult::Claimed);
}

class RenewalScriptBackend final : public InMemoryBackend
{
public:
    enum class Action : uint8_t
    {
        Delegate,
        ThrowBefore,
        LandThenThrow,
        ReturnThenCancel,
        ThrowBeforeThenLandAfterResolve,
    };

    struct Attempt
    {
        String key;
        String bytes;
        Token expected;
    };

    using InMemoryBackend::get;
    using InMemoryBackend::putOverwrite;

    std::deque<Action> actions;
    std::vector<Attempt> attempts;
    std::function<void()> cancel_after_write;
    uint64_t get_calls = 0;

    PutResult putOverwrite(const String & key, const String & bytes, const Token & expected, const ObjectMeta & meta) override
    {
        attempts.push_back({key, bytes, expected});
        const Action action = actions.empty() ? Action::Delegate : actions.front();
        if (!actions.empty())
            actions.pop_front();

        if (action == Action::ThrowBefore || action == Action::ThrowBeforeThenLandAfterResolve)
        {
            if (action == Action::ThrowBeforeThenLandAfterResolve)
                pending = Attempt{key, bytes, expected};
            throw Poco::TimeoutException("injected renewal response uncertainty before a result");
        }

        PutResult result = InMemoryBackend::putOverwrite(key, bytes, expected, meta);
        if (action == Action::LandThenThrow)
        {
            if (cancel_after_write)
                cancel_after_write();
            throw Poco::TimeoutException("injected renewal response loss after commit");
        }
        if (action == Action::ReturnThenCancel && cancel_after_write)
            cancel_after_write();
        return result;
    }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        ++get_calls;
        std::optional<GetResult> result = InMemoryBackend::get(key, range);
        if (pending && pending->key == key)
        {
            const Attempt delayed = *pending;
            pending.reset();
            const PutResult landed = InMemoryBackend::putOverwrite(delayed.key, delayed.bytes, delayed.expected, {});
            if (landed.outcome != PutOutcome::Done)
                throw DB::Exception(DB::ErrorCodes::ABORTED, "injected delayed renewal did not land");
        }
        return result;
    }

private:
    std::optional<Attempt> pending;
};

CasRequestBudget renewalBudget(uint32_t max_attempts = 3)
{
    return CasRequestBudget{
        .attempt_timeout_ms = 10,
        .operation_deadline_ms = 500,
        .max_attempts = max_attempts,
        .lease_safety_margin_ms = 20,
        .retry_initial_backoff_ms = 0,
        .retry_max_backoff_ms = 0,
    };
}

MountRenewOperationEnvironment renewalEnvironment(
    uint64_t & boot_ms,
    const std::function<CasOverwriteStopCause()> & stop_cause = {})
{
    return MountRenewOperationEnvironment{
        .boot_ms = [&boot_ms] { return boot_ms; },
        .stop_cause = stop_cause ? stop_cause : [] { return CasOverwriteStopCause::Continue; },
        .wait_before_retry = [](uint64_t) { return true; },
        .observe = {},
    };
}

DB::Exception terminalException(const MountRenewResult & result)
{
    EXPECT_EQ(result.outcome, MountRenewOutcome::Terminal);
    EXPECT_NE(result.failure, nullptr);
    try
    {
        std::rethrow_exception(result.failure);
    }
    catch (const DB::Exception & e)
    {
        return e;
    }
    catch (...)
    {
        ADD_FAILURE() << "terminal keeper failure was not a typed DB::Exception";
    }
    return DB::Exception(DB::ErrorCodes::ABORTED, "missing terminal exception");
}

void renewKeeperOrThrow(MountLeaseKeeper & keeper)
{
    const MountRenewResult result = keeper.renew(renewalBudget(), MountRenewOperationEnvironment{});
    if (result.outcome == MountRenewOutcome::Terminal)
        std::rethrow_exception(result.failure);
    if (result.outcome != MountRenewOutcome::Committed)
        throw DB::Exception(DB::ErrorCodes::ABORTED, "keeper renewal was not attempted");
}
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
                            [&] { return now_ms; }, [&] { return min_active_now; }, {}, std::chrono::milliseconds(0));
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
                            [&] { return now_ms; }, [&] { return min_active_now; }, {}, std::chrono::milliseconds(0));
    keeper.start();

    /// The dynamic field moves; the renewal re-reads it off the callback and bumps seq.
    now_ms = 1500;
    min_active_now = 8;
    renewKeeperOrThrow(keeper);

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
                            [&] { return now_ms; }, [] { return uint64_t{5}; }, {}, std::chrono::milliseconds(0));
    keeper.start();

    now_ms = 2000;
    keeper.release();

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
                            [&] { return now_ms; }, [] { return uint64_t{5}; }, {}, std::chrono::milliseconds(0));
    keeper.start();

    /// The slot advances past our held token under our own pair (the ambiguous-landed-renewal shape).
    const HeadResult h = backend->head(layout.mountKey(srid));
    ASSERT_TRUE(h.exists);
    MountLease advanced;
    advanced.server_uuid = uuid;
    advanced.writer_epoch = 9;
    advanced.seq = 99;
    advanced.write_attempt_id = UInt128{99};
    backend->putOverwrite(layout.mountKey(srid), encodeMountLease(advanced), h.token);

    try
    {
        renewKeeperOrThrow(keeper);
        FAIL() << "renew must return a terminal conflict";
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
                            [&] { return now_ms; }, [] { return uint64_t{5}; }, {}, std::chrono::milliseconds(0));
    keeper.start();

    const HeadResult h = backend->head(layout.mountKey(srid));
    ASSERT_TRUE(h.exists);
    MountLease successor;
    successor.server_uuid = uuid;
    successor.writer_epoch = 10;
    successor.seq = 1;
    successor.write_attempt_id = UInt128{1};
    backend->putOverwrite(layout.mountKey(srid), encodeMountLease(successor), h.token);

    try
    {
        renewKeeperOrThrow(keeper);
        FAIL() << "renew must return a terminal conflict";
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
/// and the runtime consumes it on its renewal worker — so an environment-reachable condition (clear the
/// prefix, recreate under a different server id, and the survivor's next renewal lands there; see
/// `CASRefContiguousAlloc.SurvivingWriterIsFencedByTheRecreatedPoolsMount`, which drives exactly that)
/// took the whole server down, and took the ASan gate down with it.
///
/// What must NOT change is the outcome, which is what this test now pins: synchronous renewal returns
/// a terminal failure that, when propagated, throws; the exception
/// carries the foreign holder's identity, and it is classified `ABORTED` — the same mount-lost class the
/// sibling fencing arms use, which the runtime terminal consumer turns into a latched write fence. The
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
                            [&] { return now_ms; }, [] { return uint64_t{5}; }, {}, std::chrono::milliseconds(0));
    keeper.start();

    const HeadResult h = backend->head(layout.mountKey(srid));
    ASSERT_TRUE(h.exists);
    MountLease foreign;
    foreign.server_uuid = UInt128(0x9999);
    foreign.writer_epoch = 1;
    foreign.seq = 1;
    foreign.write_attempt_id = UInt128{1};
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
        renewKeeperOrThrow(keeper);
        FAIL() << "a foreign holder must fail the renewal closed, not be silently taken over";
    }
    catch (const DB::Exception & e)
    {
        message = e.message();
        code = e.code();
    }
    EXPECT_NE(message.find("held by a foreign server"), String::npos) << message;
    EXPECT_EQ(code, DB::ErrorCodes::ABORTED)
        << "the mount-lost class the runtime terminal consumer latches the write fence on -- and, critically, not "
           "LOGICAL_ERROR, which would abort the renewal worker and the whole process with it";
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
                            [&] { return now_ms; }, [] { return uint64_t{5}; }, sink, std::chrono::milliseconds(0));
    keeper.start();

    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0].type, CasEventType::MountClaim);
    EXPECT_EQ(seen[0].detail.at("branch"), "adopt");

    seen.clear();
    now_ms = 2000;
    keeper.release();

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
    DB::Cas::tests::expectThrowsCodeWithMessage(
        DB::ErrorCodes::ABORTED,
        holder_uuid,
        [&] { keeper.start(); });
}

/// `Pool::open` can fail before/inside `doStart` (e.g. a foreign-conflict refusal, see
/// `KeeperForeignConflictRefusesAndNamesHolder` above) — the keeper is destroyed without ever having
/// claimed anything. Teardown must not throw "release before start"; there is nothing to release. A
/// stop AFTER a successful start still performs the farewell (covered by
/// `StopStampsExpiredAndFarewellSentinel` above); a genuinely-started DOUBLE terminate stays loud.
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
                            [&] { return now_ms; }, [] { return uint64_t{5}; }, sink, std::chrono::milliseconds(0));
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
        renewKeeperOrThrow(keeper);
        FAIL() << "renew over a fenced slot must be terminal";
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

TEST(CASHeartbeat, KeeperStateAllowsOnlyActiveReleaseOrTerminal)
{
#if defined(DEBUG_OR_SANITIZER_BUILD)
#define EXPECT_KEEPER_STATE_REJECTION(statement) EXPECT_DEATH({ statement; }, "allowed only in")
#else
#define EXPECT_KEEPER_STATE_REJECTION(statement) EXPECT_THROW(statement, DB::Exception)
#endif

    Layout layout("pool");
    const UInt128 uuid{0x1234};

    {
        auto backend = std::make_shared<RenewalScriptBackend>();
        uint64_t wall_ms = 1000;
        uint64_t boot_ms = 100;
        seedOwnClaim(*backend, layout, "released", uuid, 9, wall_ms, 1000);
        MountLeaseKeeper keeper(
            backend, layout, "released", uuid, 9, std::chrono::milliseconds(1000),
            [&] { return wall_ms; }, [] { return uint64_t{7}; }, {}, std::chrono::milliseconds(20),
            [&] { return boot_ms; });
        EXPECT_EQ(keeper.state(), MountLeaseKeeperState::New);
        EXPECT_KEEPER_STATE_REJECTION(keeper.renew(renewalBudget(), renewalEnvironment(boot_ms)));
        EXPECT_KEEPER_STATE_REJECTION(keeper.release());
        EXPECT_EQ(keeper.start(), 100u);
        EXPECT_KEEPER_STATE_REJECTION(keeper.start());
        EXPECT_EQ(keeper.state(), MountLeaseKeeperState::Active);
        keeper.release();
        EXPECT_EQ(keeper.state(), MountLeaseKeeperState::Released);
        EXPECT_KEEPER_STATE_REJECTION(keeper.start());
        EXPECT_KEEPER_STATE_REJECTION(keeper.renew(renewalBudget(), renewalEnvironment(boot_ms)));
        EXPECT_KEEPER_STATE_REJECTION(keeper.release());
    }

    {
        auto backend = std::make_shared<RenewalScriptBackend>();
        uint64_t wall_ms = 1000;
        uint64_t boot_ms = 100;
        seedOwnClaim(*backend, layout, "terminal", uuid, 9, wall_ms, 1000);
        MountLeaseKeeper keeper(
            backend, layout, "terminal", uuid, 9, std::chrono::milliseconds(1000),
            [&] { return wall_ms; }, [] { return uint64_t{7}; }, {}, std::chrono::milliseconds(20),
            [&] { return boot_ms; });
        keeper.start();
        backend->actions = {RenewalScriptBackend::Action::ThrowBefore};
        const MountRenewResult result = keeper.renew(renewalBudget(1), renewalEnvironment(boot_ms));
        EXPECT_EQ(result.outcome, MountRenewOutcome::Terminal);
        EXPECT_NE(result.failure, nullptr);
        EXPECT_EQ(keeper.state(), MountLeaseKeeperState::RenewalTerminal);
        EXPECT_KEEPER_STATE_REJECTION(keeper.start());
        EXPECT_KEEPER_STATE_REJECTION(keeper.renew(renewalBudget(), renewalEnvironment(boot_ms)));
        EXPECT_KEEPER_STATE_REJECTION(keeper.release());
    }

#undef EXPECT_KEEPER_STATE_REJECTION
}

TEST(CASHeartbeat, RenewalRetriesOneImmutableBodyAndAdoptsLostResponse)
{
    auto backend = std::make_shared<RenewalScriptBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid{0x1234};
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    seedOwnClaim(*backend, layout, srid, uuid, 9, wall_ms, 1000);
    MountLeaseKeeper keeper(
        backend, layout, srid, uuid, 9, std::chrono::milliseconds(1000),
        [&] { return wall_ms; }, [] { return uint64_t{7}; }, {}, std::chrono::milliseconds(20),
        [&] { return boot_ms; });
    keeper.start();

    backend->attempts.clear();
    backend->actions = {RenewalScriptBackend::Action::ThrowBefore, RenewalScriptBackend::Action::Delegate};
    MountRenewResult retried = keeper.renew(renewalBudget(), renewalEnvironment(boot_ms));
    ASSERT_EQ(retried.outcome, MountRenewOutcome::Committed);
    ASSERT_EQ(backend->attempts.size(), 2u);
    EXPECT_EQ(backend->attempts[0].key, backend->attempts[1].key);
    EXPECT_EQ(backend->attempts[0].bytes, backend->attempts[1].bytes);
    EXPECT_EQ(backend->attempts[0].expected, backend->attempts[1].expected);
    const MountLease retry_body = decodeMountLease(backend->attempts[0].bytes);
    EXPECT_NE(retry_body.write_attempt_id, UInt128{});

    backend->attempts.clear();
    backend->actions = {RenewalScriptBackend::Action::LandThenThrow};
    MountRenewResult adopted = keeper.renew(renewalBudget(1), renewalEnvironment(boot_ms));
    EXPECT_EQ(adopted.outcome, MountRenewOutcome::Committed);
    EXPECT_TRUE(adopted.diagnostics.resolved_by_get);
    EXPECT_EQ(adopted.diagnostics.attempts_sent, 1u);
    EXPECT_EQ(decodeMountLease(backend->get(layout.mountKey(srid))->bytes).write_attempt_id,
              decodeMountLease(backend->attempts.front().bytes).write_attempt_id);
}

TEST(CASHeartbeat, DeadlineBeforeSendTerminalizesWithTypedFailure)
{
    auto backend = std::make_shared<RenewalScriptBackend>();
    Layout layout("pool");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    seedOwnClaim(*backend, layout, "test", UInt128{1}, 9, wall_ms, 100);
    MountLeaseKeeper keeper(
        backend, layout, "test", UInt128{1}, 9, std::chrono::milliseconds(100),
        [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
        [&] { return boot_ms; });
    keeper.start();
    backend->attempts.clear();
    backend->get_calls = 0;
    boot_ms = 180;
    const MountRenewResult result = keeper.renew(renewalBudget(), renewalEnvironment(boot_ms));
    const DB::Exception failure = terminalException(result);
    EXPECT_EQ(failure.code(), DB::ErrorCodes::NETWORK_ERROR);
    EXPECT_NE(failure.message().find("no attempt was sent"), String::npos) << failure.message();
    EXPECT_EQ(result.diagnostics.unresolved_reason, CasUnresolvedReason::NoAttemptSent);
    EXPECT_TRUE(backend->attempts.empty());
    EXPECT_EQ(backend->get_calls, 0u) << "a pre-send terminal deadline must perform no diagnostic GET";
}

TEST(CASHeartbeat, CancellationBeforeSendIsNotAttemptedAndAllowsRelease)
{
    auto backend = std::make_shared<RenewalScriptBackend>();
    Layout layout("pool");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    seedOwnClaim(*backend, layout, "test", UInt128{1}, 9, wall_ms, 1000);
    MountLeaseKeeper keeper(
        backend, layout, "test", UInt128{1}, 9, std::chrono::milliseconds(1000),
        [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
        [&] { return boot_ms; });
    keeper.start();
    backend->attempts.clear();
    backend->get_calls = 0;
    const auto cancelled = [] { return CasOverwriteStopCause::Cancelled; };
    const MountRenewResult result = keeper.renew(renewalBudget(), renewalEnvironment(boot_ms, cancelled));
    EXPECT_EQ(result.outcome, MountRenewOutcome::NotAttempted);
    EXPECT_EQ(result.failure, nullptr);
    EXPECT_EQ(keeper.state(), MountLeaseKeeperState::Active);
    EXPECT_TRUE(backend->attempts.empty());
    EXPECT_NO_THROW(keeper.release());
    EXPECT_EQ(keeper.state(), MountLeaseKeeperState::Released);
}

TEST(CASHeartbeat, CancellationAfterSendIsTerminalAndForbidsRelease)
{
    auto backend = std::make_shared<RenewalScriptBackend>();
    Layout layout("pool");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    bool cancelled = false;
    seedOwnClaim(*backend, layout, "test", UInt128{1}, 9, wall_ms, 1000);
    MountLeaseKeeper keeper(
        backend, layout, "test", UInt128{1}, 9, std::chrono::milliseconds(1000),
        [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
        [&] { return boot_ms; });
    keeper.start();
    backend->attempts.clear();
    backend->get_calls = 0;
    backend->cancel_after_write = [&] { cancelled = true; };
    backend->actions = {RenewalScriptBackend::Action::ReturnThenCancel};
    const MountRenewResult result = keeper.renew(
        renewalBudget(), renewalEnvironment(boot_ms, [&] {
            return cancelled ? CasOverwriteStopCause::Cancelled : CasOverwriteStopCause::Continue;
        }));
    const DB::Exception failure = terminalException(result);
    EXPECT_EQ(failure.code(), DB::ErrorCodes::NETWORK_ERROR);
    EXPECT_EQ(result.diagnostics.unresolved_reason, CasUnresolvedReason::FenceLostPostWrite);
    EXPECT_EQ(backend->get_calls, 0u) << "post-write cancellation must not start a diagnostic GET";
    EXPECT_EQ(keeper.state(), MountLeaseKeeperState::RenewalTerminal);
    const String bytes_before = backend->get(layout.mountKey("test"))->bytes;
    EXPECT_FALSE(keeper.canRelease());
    EXPECT_EQ(backend->get(layout.mountKey("test"))->bytes, bytes_before);
}

TEST(CASHeartbeat, SlowResolvedSuccessKeepsAttemptStartAnchor)
{
    auto backend = std::make_shared<RenewalScriptBackend>();
    Layout layout("pool");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    seedOwnClaim(*backend, layout, "test", UInt128{1}, 9, wall_ms, 1000);
    MountLeaseKeeper keeper(
        backend, layout, "test", UInt128{1}, 9, std::chrono::milliseconds(1000),
        [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
        [&] { return boot_ms; });
    keeper.start();
    boot_ms = 150;
    backend->cancel_after_write = [&] { boot_ms = 400; };
    backend->actions = {RenewalScriptBackend::Action::LandThenThrow};
    const MountRenewResult result = keeper.renew(renewalBudget(), renewalEnvironment(boot_ms));
    EXPECT_EQ(result.outcome, MountRenewOutcome::Committed);
    EXPECT_EQ(result.attempt_start_boot_ms, 150u);
    EXPECT_EQ(keeper.lastCommittedAttemptStartBootMs(), 150u);
}

TEST(CASHeartbeat, SamePairTwinAndForeignOrSuccessorStayTerminal)
{
    const auto run_case = [](UInt128 current_uuid, uint64_t current_epoch, UInt128 current_attempt)
    {
        auto backend = std::make_shared<RenewalScriptBackend>();
        Layout layout("pool");
        uint64_t wall_ms = 1000;
        uint64_t boot_ms = 100;
        const UInt128 uuid{1};
        seedOwnClaim(*backend, layout, "test", uuid, 9, wall_ms, 1000);
        MountLeaseKeeper keeper(
            backend, layout, "test", uuid, 9, std::chrono::milliseconds(1000),
            [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
            [&] { return boot_ms; });
        keeper.start();
        auto got = backend->get(layout.mountKey("test"));
        MountLease current = decodeMountLease(got->bytes);
        current.server_uuid = current_uuid;
        current.writer_epoch = current_epoch;
        current.write_attempt_id = current_attempt;
        ++current.seq;
        ASSERT_EQ(backend->putOverwrite(layout.mountKey("test"), encodeMountLease(current), got->token).outcome,
                  PutOutcome::Done);
        backend->get_calls = 0;
        const MountRenewResult result = keeper.renew(renewalBudget(), renewalEnvironment(boot_ms));
        const DB::Exception failure = terminalException(result);
        EXPECT_NE(failure.code(), DB::ErrorCodes::LOGICAL_ERROR);
        EXPECT_EQ(keeper.state(), MountLeaseKeeperState::RenewalTerminal);
        EXPECT_EQ(backend->get_calls, 1u) << "the controller's resolving GET must be the only terminal read";
    };

    run_case(UInt128{1}, 9, UInt128{0xAAAA});
    run_case(UInt128{2}, 9, UInt128{0xBBBB});
    run_case(UInt128{1}, 10, UInt128{0xCCCC});
}

TEST(CASHeartbeat, ExpectedPredecessorThenLateLandingIsAdoptedExactly)
{
    auto backend = std::make_shared<RenewalScriptBackend>();
    Layout layout("pool");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    seedOwnClaim(*backend, layout, "test", UInt128{1}, 9, wall_ms, 1000);
    MountLeaseKeeper keeper(
        backend, layout, "test", UInt128{1}, 9, std::chrono::milliseconds(1000),
        [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
        [&] { return boot_ms; });
    keeper.start();
    backend->attempts.clear();
    backend->actions = {
        RenewalScriptBackend::Action::ThrowBeforeThenLandAfterResolve,
        RenewalScriptBackend::Action::Delegate,
    };
    const MountRenewResult result = keeper.renew(renewalBudget(), renewalEnvironment(boot_ms));
    EXPECT_EQ(result.outcome, MountRenewOutcome::Committed);
    EXPECT_TRUE(result.diagnostics.resolved_by_get);
    ASSERT_EQ(backend->attempts.size(), 2u);
    EXPECT_EQ(backend->attempts[0].bytes, backend->attempts[1].bytes);
    EXPECT_EQ(decodeMountLease(backend->get(layout.mountKey("test"))->bytes).write_attempt_id,
              decodeMountLease(backend->attempts[0].bytes).write_attempt_id);
}

TEST(CASHeartbeat, GcFenceAndVanishedMountStayTerminal)
{
    const auto run_case = [](bool vanish)
    {
        auto backend = std::make_shared<RenewalScriptBackend>();
        Layout layout("pool");
        uint64_t wall_ms = 1000;
        uint64_t boot_ms = 100;
        seedOwnClaim(*backend, layout, "test", UInt128{1}, 9, wall_ms, 1000);
        MountLeaseKeeper keeper(
            backend, layout, "test", UInt128{1}, 9, std::chrono::milliseconds(1000),
            [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
            [&] { return boot_ms; });
        keeper.start();
        const String key = layout.mountKey("test");
        auto got = backend->get(key);
        if (vanish)
            ASSERT_EQ(backend->deleteExact(key, got->token).kind, DeleteOutcome::Kind::Deleted);
        else
        {
            MountLease fenced = decodeMountLease(got->bytes);
            fenced.gc_fenced = true;
            ++fenced.seq;
            ASSERT_EQ(backend->putOverwrite(key, encodeMountLease(fenced), got->token).outcome, PutOutcome::Done);
        }
        const DB::Exception failure = terminalException(keeper.renew(renewalBudget(), renewalEnvironment(boot_ms)));
        EXPECT_NE(failure.code(), DB::ErrorCodes::LOGICAL_ERROR);
        EXPECT_EQ(keeper.state(), MountLeaseKeeperState::RenewalTerminal);
    };
    run_case(false);
    run_case(true);
}

TEST(CASHeartbeat, LateDeliveryAfterTerminalCannotRearmOrOverwriteSuccessor)
{
    const auto make_terminal = [](const std::shared_ptr<RenewalScriptBackend> & backend,
                                  const Layout & layout, const String & srid,
                                  uint64_t & wall_ms, uint64_t & boot_ms)
    {
        seedOwnClaim(*backend, layout, srid, UInt128{1}, 9, wall_ms, 1000);
        auto keeper = std::make_unique<MountLeaseKeeper>(
            backend, layout, srid, UInt128{1}, 9, std::chrono::milliseconds(1000),
            [&] { return wall_ms; }, [] { return uint64_t{0}; }, CasEventSink{}, std::chrono::milliseconds(20),
            [&] { return boot_ms; });
        keeper->start();
        backend->actions = {RenewalScriptBackend::Action::ThrowBeforeThenLandAfterResolve};
        const MountRenewResult result = keeper->renew(renewalBudget(1), renewalEnvironment(boot_ms));
        EXPECT_EQ(result.outcome, MountRenewOutcome::Terminal);
        EXPECT_EQ(keeper->state(), MountLeaseKeeperState::RenewalTerminal);
        return keeper;
    };

    Layout layout("pool");
    {
        auto backend = std::make_shared<RenewalScriptBackend>();
        uint64_t wall_ms = 1000;
        uint64_t boot_ms = 100;
        auto keeper = make_terminal(backend, layout, "before-reclaim", wall_ms, boot_ms);
        const MountLease landed = decodeMountLease(backend->get(layout.mountKey("before-reclaim"))->bytes);
        EXPECT_EQ(landed.writer_epoch, 9u);
        EXPECT_EQ(keeper->state(), MountLeaseKeeperState::RenewalTerminal);
    }
    {
        auto backend = std::make_shared<RenewalScriptBackend>();
        uint64_t wall_ms = 1000;
        uint64_t boot_ms = 100;
        seedOwnClaim(*backend, layout, "after-successor", UInt128{1}, 9, wall_ms, 1000);
        MountLeaseKeeper keeper(
            backend, layout, "after-successor", UInt128{1}, 9, std::chrono::milliseconds(1000),
            [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
            [&] { return boot_ms; });
        keeper.start();
        backend->actions = {RenewalScriptBackend::Action::ThrowBefore};
        const MountRenewResult result = keeper.renew(renewalBudget(1), renewalEnvironment(boot_ms));
        ASSERT_EQ(result.outcome, MountRenewOutcome::Terminal);
        ASSERT_FALSE(backend->attempts.empty());
        const auto delayed = backend->attempts.back();
        auto current = backend->get(delayed.key);
        MountLease fenced = decodeMountLease(current->bytes);
        fenced.gc_fenced = true;
        ++fenced.seq;
        ASSERT_EQ(backend->InMemoryBackend::putOverwrite(delayed.key, encodeMountLease(fenced), current->token, {}).outcome,
                  PutOutcome::Done);
        ASSERT_EQ(claimMount(*backend, layout, "after-successor", UInt128{1}, 10, wall_ms, 1000).kind,
                  MountClaimResult::Claimed);
        MountLeaseKeeper successor(
            backend, layout, "after-successor", UInt128{1}, 10, std::chrono::milliseconds(1000),
            [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
            [&] { return boot_ms; });
        successor.start();
        EXPECT_EQ(backend->InMemoryBackend::putOverwrite(delayed.key, delayed.bytes, delayed.expected, {}).outcome,
                  PutOutcome::PreconditionFailed);
        EXPECT_EQ(decodeMountLease(backend->get(delayed.key)->bytes).writer_epoch, 10u);
    }
}

TEST(CASHeartbeat, WallClockStepsAndBootSuspendCannotExtendAuthority)
{
    auto backend = std::make_shared<RenewalScriptBackend>();
    Layout layout("pool");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    seedOwnClaim(*backend, layout, "test", UInt128{1}, 9, wall_ms, 1000);
    MountLeaseKeeper keeper(
        backend, layout, "test", UInt128{1}, 9, std::chrono::milliseconds(1000),
        [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
        [&] { return boot_ms; });
    keeper.start();

    wall_ms = 9'000'000;
    EXPECT_EQ(keeper.renew(renewalBudget(), renewalEnvironment(boot_ms)).outcome, MountRenewOutcome::Committed);
    wall_ms = 1;
    EXPECT_EQ(keeper.renew(renewalBudget(), renewalEnvironment(boot_ms)).outcome, MountRenewOutcome::Committed);

    backend->attempts.clear();
    boot_ms += 10'000;
    const MountRenewResult suspended = keeper.renew(renewalBudget(), renewalEnvironment(boot_ms));
    const DB::Exception failure = terminalException(suspended);
    EXPECT_EQ(failure.code(), DB::ErrorCodes::NETWORK_ERROR);
    EXPECT_TRUE(backend->attempts.empty()) << "suspend-sized BOOTTIME overshoot must close admission";
}
