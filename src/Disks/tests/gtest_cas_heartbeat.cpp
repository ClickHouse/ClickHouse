#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>

#include "config.h"
#include <IO/S3Common.h>

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


/// MountLeaseRenewer behavior: the per-server mount lease and the merged build-watermark floor ride the
/// SAME slot, renewed by one beat. The renewer anchors durably before return, adopts a slot already
/// written by `claimMount` (same uuid+epoch), re-reads the callback on each renew and bumps `seq`,
/// stamps the farewell sentinel (`min_active_build_sequence = UINT64_MAX`, `expires_at_ms <= now`) on `release`, and
/// returns typed terminal results on any foreign touch.

namespace
{
/// The two request planes this file's renewers run on. Both are open-fence -- the exclusivity these
/// tests exercise is the mount protocol's own, not a fence's -- on the same injected boot clock the
/// renewer's lease deadline is expressed on, so the two never disagree about how much budget is left.
/// `sleep_step_ms`, when set, makes one inter-attempt pause jump the clock past the lease bound: that
/// is how a test asks for exactly one physical attempt without a per-call attempt cap. It depends on
/// the engine checking the bound, sleeping, then checking again -- a reissue that slept first would
/// send a second attempt. `tests::OperationForTest` covers a fixture needing one operation, but
/// neither the two planes a renewer takes nor this clock, which is why this stays local.
class Ops
{
public:
    Ops(std::shared_ptr<Backend> backend, uint64_t * boot_ms, uint64_t sleep_step_ms = 0)
        : mount(openRequestsForTest(backend))
        , farewell(openRequestsForTest(std::move(backend)))
        , op(mount.admit())
    {
        for (CasRequests * requests : {&mount, &farewell})
        {
            requests->setNowFnForTest([boot_ms] { return *boot_ms; });
            requests->setSleepFnForTest(
                [boot_ms, sleep_step_ms](uint64_t ms) { *boot_ms += sleep_step_ms ? sleep_step_ms : ms; });
        }
    }

    Ops(const Ops &) = delete;
    Ops & operator=(const Ops &) = delete;

    CasRequests mount;
    CasRequests farewell;
    CasOperation op;
};

/// A fixture write that must land, so a mis-seeded fixture fails where it is written rather than in
/// the assertion it silently invalidated.
void mustCommit(WriteResult && result, const String & what)
{
    if (!std::holds_alternative<Committed>(result))
        throw DB::Exception(DB::ErrorCodes::ABORTED, "test fixture write '{}' did not commit", what);
}

/// The normal steady-state flow: `claimMount` writes the live (uuid, epoch) mount, THEN the renewer
/// adopts it. Seed that claim so `start` adopts instead of self-tripping the double-start guard.
void seedOwnClaim(CasOperation & op, const Layout & l, const String & srid, UInt128 uuid, uint64_t epoch,
                  uint64_t now_ms, uint64_t ttl_ms)
{
    ASSERT_EQ(claimMount(op, l, srid, uuid, epoch, now_ms, ttl_ms).kind, MountClaimResult::Claimed);
}

/// Not `final`: `EnvelopeEatingBackend` (the envelope-cutoff test below) derives from it to
/// reuse its `Attempt`/`attempts` bookkeeping while overriding `write`/`read` with its own always-fail
/// behavior instead of the scripted-action queue.
class RenewalScriptBackend : public InMemoryBackend
{
public:
    enum class Action : uint8_t
    {
        Delegate,
        ThrowBefore,
        LandThenThrow,
        ReturnThenCancel,
        ThrowBeforeThenLandAfterResolve,
        ThrowConnectHint,
    };

    struct Attempt
    {
        String key;
        String bytes;
        std::optional<String> expected;
    };

    std::deque<Action> actions;
    std::vector<Attempt> attempts;
    std::function<void()> cancel_after_write;
    uint64_t read_calls = 0;

    /// Only a GUARDED write of a mount slot is scripted; the fixture's own seeding and every other
    /// key reach the store untouched.
    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value,
                                             TransportAccess & access) override
    {
        if (!expected_value || !key.ends_with("/mount"))
            return InMemoryBackend::write(key, bytes, expected_value, access);

        attempts.push_back({key, bytes, expected_value});
        const Action action = actions.empty() ? Action::Delegate : actions.front();
        if (!actions.empty())
            actions.pop_front();

        if (action == Action::ThrowConnectHint)
        {
#if USE_AWS_S3
            throw DB::S3Exception("Poco::Exception. Code: 1000, e.code() = 99, Cannot assign requested address: 10.0.0.1:9000",
                                  Aws::S3::S3Errors::NETWORK_CONNECTION);
#else
            throw Poco::TimeoutException("connect timed out");
#endif
        }

        if (action == Action::ThrowBefore || action == Action::ThrowBeforeThenLandAfterResolve)
        {
            if (action == Action::ThrowBeforeThenLandAfterResolve)
                pending = Attempt{key, bytes, expected_value};
            throw Poco::TimeoutException("injected renewal response uncertainty before a result");
        }

        auto result = InMemoryBackend::write(key, bytes, expected_value, access);
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

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        ++read_calls;
        std::optional<Raw> result = InMemoryBackend::read(key, access);
        if (pending && pending->key == key)
        {
            const Attempt delayed = *pending;
            pending.reset();
            const auto landed = InMemoryBackend::write(delayed.key, delayed.bytes, delayed.expected, access);
            if (!landed.has_value())
                throw DB::Exception(DB::ErrorCodes::ABORTED, "injected delayed renewal did not land");
        }
        return result;
    }

private:
    std::optional<Attempt> pending;
};

MountRenewOperationEnvironment renewalEnvironment(
    uint64_t & boot_ms,
    const std::function<bool()> & live = {},
    const std::function<bool()> & cancelled = {})
{
    return MountRenewOperationEnvironment{
        .boot_ms = [&boot_ms] { return boot_ms; },
        .live = live,
        .cancelled = cancelled,
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
        ADD_FAILURE() << "terminal renewer failure was not a typed DB::Exception";
    }
    return DB::Exception(DB::ErrorCodes::ABORTED, "missing terminal exception");
}

void renewOrThrow(MountLeaseRenewer & renewer)
{
    const MountRenewResult result = renewer.renew(MountRenewOperationEnvironment{});
    if (result.outcome == MountRenewOutcome::Terminal)
        std::rethrow_exception(result.failure);
    if (result.outcome != MountRenewOutcome::Committed)
        throw DB::Exception(DB::ErrorCodes::ABORTED, "renewer renewal was not attempted");
}
}

TEST(CASHeartbeat, AnchorCarriesFloor)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    uint64_t min_active_build_sequence_now = 5;
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, srid, uuid, /*writer_epoch=*/9,
                            std::chrono::milliseconds(100), [&] { return now_ms; },
                            [&] { return min_active_build_sequence_now; }, {}, std::chrono::milliseconds(0),
                            [&] { return boot_ms; });
    renewer.start();

    ASSERT_TRUE(ops.op.head(layout.mountKey(srid), Retry::standard()).has_value());
    auto m = decodeMountLease(ops.op.read(layout.mountKey(srid), Retry::standard())->bytes);
    EXPECT_EQ(m.writer_epoch, 9u);
    EXPECT_EQ(m.min_active_build_sequence, 5u);
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
    uint64_t min_active_build_sequence_now = 5;
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, srid, uuid, /*writer_epoch=*/9,
                            std::chrono::milliseconds(100), [&] { return now_ms; },
                            [&] { return min_active_build_sequence_now; }, {}, std::chrono::milliseconds(0),
                            [&] { return boot_ms; });
    renewer.start();

    /// The dynamic field moves; the renewal re-reads it off the callback and bumps seq.
    now_ms = 1500;
    min_active_build_sequence_now = 8;
    renewOrThrow(renewer);

    auto m = decodeMountLease(ops.op.read(layout.mountKey(srid), Retry::standard())->bytes);
    EXPECT_EQ(m.min_active_build_sequence, 8u);
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
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, srid, uuid, /*writer_epoch=*/9,
                            std::chrono::milliseconds(100), [&] { return now_ms; },
                            [] { return uint64_t{5}; }, {}, std::chrono::milliseconds(0),
                            [&] { return boot_ms; });
    renewer.start();

    now_ms = 2000;
    renewer.release();

    auto m = decodeMountLease(ops.op.read(layout.mountKey(srid), Retry::standard())->bytes);
    /// Terminal body stamps the lease already-expired (so a same-server reopen reclaims immediately)
    /// AND folds the watermark farewell into it (min_active_build_sequence = UINT64_MAX).
    EXPECT_LE(m.expires_at_ms, now_ms);
    EXPECT_EQ(m.min_active_build_sequence, std::numeric_limits<uint64_t>::max());
}

namespace
{
/// Reports the SHIPPED PRODUCTION defaults (`attempt_timeout_ms=5000`, two `connect_timeout_cap_ms=1000`
/// caps -> `attemptEnvelopeMs()=7000`, `CasRequestBudget.cpp`'s own defaults) while landing every attempt
/// immediately: the write's own success is not what is under test here, only whether the farewell's
/// policy window is wide enough to admit one attempt in the first place.
struct DefaultEnvelopeBackend : InMemoryBackend
{
    uint64_t attemptTimeoutMs() const override { return 5000; }
    uint64_t attemptEnvelopeMs() const override { return 7000; }
};

/// A DIFFERENT envelope from `DefaultEnvelopeBackend`'s, for
/// `FarewellIsAdmittedUnderADifferentEnvelope` below: that test exists to pin the window's
/// ARITHMETIC, not just that some window admits the write, so it needs a reservation the
/// shipped-default window (16000 ms) could not have admitted by coincidence.
struct WiderEnvelopeBackend : InMemoryBackend
{
    uint64_t attemptTimeoutMs() const override { return 5000; }
    uint64_t attemptEnvelopeMs() const override { return 9000; }
};
}

/// A write reserves two attempt envelopes before it starts (`CasOperation::writeLoop`'s
/// `reservedFor(0, 2)`), so at the shipped defaults the farewell needs a policy window that admits
/// 2 * 7000 = 14000 ms. A fixed window that predates that reservation (`kFarewellBudgetMs` alone is
/// 10000 ms) refuses the write before its first attempt on every graceful shutdown: no farewell is
/// published, and the next start pays a full incarnation-stability observation instead of reclaiming
/// the slot instantly.
TEST(CASHeartbeat, FarewellIsAdmittedUnderTheDefaultBudget)
{
    auto backend = std::make_shared<DefaultEnvelopeBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/30000);

    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, srid, uuid, /*writer_epoch=*/9,
                            std::chrono::milliseconds(30000), [&] { return now_ms; },
                            [] { return uint64_t{5}; }, {}, std::chrono::milliseconds(2000),
                            [&] { return boot_ms; });
    renewer.start();

    now_ms = 2000;
    EXPECT_NO_THROW(renewer.release())
        << "the farewell's policy window must admit the write's own two-envelope reservation "
           "(2 * 7000 ms with the shipped defaults) -- otherwise a clean shutdown never hands the "
           "mount slot back and every restart pays a full incarnation-stability observation";

    auto m = decodeMountLease(ops.op.read(layout.mountKey(srid), Retry::standard())->bytes);
    EXPECT_LE(m.expires_at_ms, now_ms);
    EXPECT_EQ(m.min_active_build_sequence, std::numeric_limits<uint64_t>::max());
}

/// Pins the window's ARITHMETIC, not just that some fixed window happens to be wide enough: a
/// regression that hardcoded the shipped-default window (16000 ms) instead of deriving it from
/// `attemptReservationMs()` would still pass `FarewellIsAdmittedUnderTheDefaultBudget` above (16000
/// happens to equal what a 7000 ms envelope needs) but would refuse THIS write, whose reservation is
/// 2 * 9000 = 18000 ms -- strictly more than the shipped-default window.
TEST(CASHeartbeat, FarewellIsAdmittedUnderADifferentEnvelope)
{
    auto backend = std::make_shared<WiderEnvelopeBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/40000);

    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, srid, uuid, /*writer_epoch=*/9,
                            std::chrono::milliseconds(40000), [&] { return now_ms; },
                            [] { return uint64_t{5}; }, {}, std::chrono::milliseconds(2000),
                            [&] { return boot_ms; });
    renewer.start();

    now_ms = 2000;
    EXPECT_NO_THROW(renewer.release())
        << "the farewell's policy window must be DERIVED from this backend's own envelope "
           "(2 * 9000 ms), not hardcoded to the shipped-default window -- a window fixed at "
           "16000 ms would refuse this write's 18000 ms reservation";

    auto m = decodeMountLease(ops.op.read(layout.mountKey(srid), Retry::standard())->bytes);
    EXPECT_LE(m.expires_at_ms, now_ms);
    EXPECT_EQ(m.min_active_build_sequence, std::numeric_limits<uint64_t>::max());
}

/// The derived window alone is not the whole story: mount-control activity must also never run past
/// the point this node's own fence may already be gone. A 5000 ms TTL with a 2000 ms safety margin
/// leaves only 3000 ms of lease-safe remaining time at release -- far short of the 7000 ms envelope's
/// own 16000 ms derived window (2 * 7000 + 2000 slack) -- so the LEASE bound, not the derived window,
/// must be what refuses this write, and it must refuse it before any physical attempt: a write that
/// cannot land inside the lease-safe remainder gains nothing by being sent anyway.
TEST(CASHeartbeat, FarewellIsRefusedWhenTheLeaseExpiresBeforeItsDerivedWindow)
{
    auto backend = std::make_shared<DefaultEnvelopeBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/5000);

    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, srid, uuid, /*writer_epoch=*/9,
                            std::chrono::milliseconds(5000), [&] { return now_ms; },
                            [] { return uint64_t{5}; }, {}, std::chrono::milliseconds(2000),
                            [&] { return boot_ms; });
    renewer.start();

    now_ms = 2000;
    String message;
    int code = 0;
    bool threw = false;
    try
    {
        renewer.release();
    }
    catch (const DB::Exception & e)
    {
        threw = true;
        message = e.message();
        code = e.code();
    }
    EXPECT_TRUE(threw) << "a farewell whose reservation cannot fit inside the lease-safe remaining "
                           "time must be refused, not admitted past the point this node's fence may "
                           "already be gone";
    EXPECT_EQ(code, DB::ErrorCodes::NETWORK_ERROR) << message;
    EXPECT_NE(message.find("gave up at the lease deadline after zero attempt(s)"), String::npos) << message;

    auto m = decodeMountLease(ops.op.read(layout.mountKey(srid), Retry::standard())->bytes);
    EXPECT_NE(m.min_active_build_sequence, std::numeric_limits<uint64_t>::max())
        << "the refused write must not have landed";
}

/// The lease bound added above must not change what an ordinary Conflict outcome does: a successor
/// that took the slot (a different, unfenced incarnation) before this node's own shutdown could
/// publish its farewell must be left untouched, and the release must report the conflict rather than
/// silently succeeding or overwriting the successor's incarnation.
TEST(CASHeartbeat, ForeignIncarnationDuringFarewellLeavesTheSuccessorUntouchedAndReportsTheConflict)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, srid, uuid, /*writer_epoch=*/9,
                            std::chrono::milliseconds(100), [&] { return now_ms; },
                            [] { return uint64_t{5}; }, {}, std::chrono::milliseconds(0),
                            [&] { return boot_ms; });
    renewer.start();

    /// A successor (a different uuid/epoch, NOT gc_fenced) took the slot before this node's own
    /// clean shutdown could publish its farewell -- the exact shape a live double-start reclaim
    /// leaves behind.
    const auto observed = ops.op.read(layout.mountKey(srid), Retry::standard());
    ASSERT_TRUE(observed.has_value());
    MountLease successor;
    successor.server_uuid = UInt128(0x9999);
    successor.writer_epoch = 1;
    successor.seq = 1;
    successor.write_attempt_id = UInt128{1};
    mustCommit(ops.op.replace(layout.mountKey(srid), encodeMountLease(successor), observed->etag,
                              Retry::standard()), "successor slot");

    now_ms = 2000;
    String message;
    int code = 0;
    try
    {
        renewer.release();
        FAIL() << "a farewell that finds a foreign, unfenced incarnation must report the conflict, "
                  "not silently succeed or clobber the successor";
    }
    catch (const DB::Exception & e)
    {
        message = e.message();
        code = e.code();
    }
    EXPECT_EQ(code, DB::ErrorCodes::ABORTED) << message;
    EXPECT_NE(message.find("found a foreign incarnation"), String::npos) << message;

    auto m = decodeMountLease(ops.op.read(layout.mountKey(srid), Retry::standard())->bytes);
    EXPECT_EQ(m.server_uuid, successor.server_uuid)
        << "the successor's own incarnation must be untouched by the refused farewell";
    EXPECT_EQ(m.writer_epoch, successor.writer_epoch);
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
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, srid, uuid, /*writer_epoch=*/9,
                            std::chrono::milliseconds(100), [&] { return now_ms; },
                            [] { return uint64_t{5}; }, {}, std::chrono::milliseconds(0),
                            [&] { return boot_ms; });
    renewer.start();

    /// The slot advances past the incarnation we hold, under our own pair (the ambiguous-landed-renewal shape).
    const auto observed = ops.op.read(layout.mountKey(srid), Retry::standard());
    ASSERT_TRUE(observed.has_value());
    MountLease advanced;
    advanced.server_uuid = uuid;
    advanced.writer_epoch = 9;
    advanced.seq = 99;
    advanced.write_attempt_id = UInt128{99};
    mustCommit(ops.op.replace(layout.mountKey(srid), encodeMountLease(advanced), observed->etag,
                              Retry::standard()), "advanced slot");

    try
    {
        renewOrThrow(renewer);
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
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, srid, uuid, /*writer_epoch=*/9,
                            std::chrono::milliseconds(100), [&] { return now_ms; },
                            [] { return uint64_t{5}; }, {}, std::chrono::milliseconds(0),
                            [&] { return boot_ms; });
    renewer.start();

    const auto observed = ops.op.read(layout.mountKey(srid), Retry::standard());
    ASSERT_TRUE(observed.has_value());
    MountLease successor;
    successor.server_uuid = uuid;
    successor.writer_epoch = 10;
    successor.seq = 1;
    successor.write_attempt_id = UInt128{1};
    mustCommit(ops.op.replace(layout.mountKey(srid), encodeMountLease(successor), observed->etag,
                              Retry::standard()), "successor slot");

    try
    {
        renewOrThrow(renewer);
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
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, srid, uuid, /*writer_epoch=*/9,
                            std::chrono::milliseconds(100), [&] { return now_ms; },
                            [] { return uint64_t{5}; }, {}, std::chrono::milliseconds(0),
                            [&] { return boot_ms; });
    renewer.start();

    const auto observed = ops.op.read(layout.mountKey(srid), Retry::standard());
    ASSERT_TRUE(observed.has_value());
    MountLease foreign;
    foreign.server_uuid = UInt128(0x9999);
    foreign.writer_epoch = 1;
    foreign.seq = 1;
    foreign.write_attempt_id = UInt128{1};
    mustCommit(ops.op.replace(layout.mountKey(srid), encodeMountLease(foreign), observed->etag,
                              Retry::standard()), "foreign slot");

    /// Restored on every exit: this flag is process-global and every later test in this binary would
    /// inherit it.
    const bool armed_before = DB::abort_on_logical_error.load(std::memory_order_relaxed);
    DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
    SCOPE_EXIT({ DB::abort_on_logical_error.store(armed_before, std::memory_order_relaxed); });

    String message;
    int code = 0;
    try
    {
        renewOrThrow(renewer);
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

    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    const uint64_t now_ms = 1'000'000;
    /// mint for uuid 1 -> one mount_claim
    ASSERT_EQ(claimMount(ops.op, layout, "a", UInt128{1}, 1, now_ms, /*ttl_ms=*/10'000, {}, sink).kind,
              MountClaimResult::Claimed);
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0].type, CasEventType::MountClaim);
    EXPECT_EQ(seen[0].detail.at("server_root_id"), "a");
    EXPECT_EQ(seen[0].detail.at("branch"), "mint");

    /// a FOREIGN uuid claiming a live slot -> mount_conflict carrying the current holder's identity
    seen.clear();
    (void)claimMount(ops.op, layout, "a", UInt128{2}, 1, now_ms, /*ttl_ms=*/10'000, {}, sink);
    ASSERT_FALSE(seen.empty());
    EXPECT_EQ(seen.back().type, CasEventType::MountConflict);
    EXPECT_EQ(seen.back().detail.at("server_root_id"), "a");
    /// The conflict must carry the ORIGINAL holder's identity (uuid 1, the minter) — not the
    /// foreign claimer's (uuid 2).
    EXPECT_EQ(seen.back().detail.at("holder_uuid"), u128ToHex(UInt128{1}));
    EXPECT_NE(seen.back().detail.at("holder_uuid"), u128ToHex(UInt128{2}));
}

/// The MountLeaseRenewer wiring: `start` adopting an already-claimed slot emits mount_claim, `stop`
/// (the farewell write) emits mount_release.
TEST(CASMountAudit, RenewerAdoptEmitsClaimAndTerminateEmitsRelease)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    std::vector<CasEvent> seen;
    CasEventSink sink = [&](const CasEvent & e) { seen.push_back(e); };
    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, srid, uuid, /*writer_epoch=*/9,
                            std::chrono::milliseconds(100), [&] { return now_ms; },
                            [] { return uint64_t{5}; }, sink, std::chrono::milliseconds(0),
                            [&] { return boot_ms; });
    renewer.start();

    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0].type, CasEventType::MountClaim);
    EXPECT_EQ(seen[0].detail.at("branch"), "adopt");

    seen.clear();
    now_ms = 2000;
    renewer.release();

    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0].type, CasEventType::MountRelease);
    EXPECT_EQ(seen[0].detail.at("branch"), "farewell");
}

/// Renewer-level foreign-conflict refusal: the mount slot is already held by a FOREIGN uuid (X) when
/// a renewer for a DIFFERENT uuid (Y) tries to claim it. This must fail closed and — since the
/// mount-audit sink is not yet installed at first-open — name X in the exception's message text
/// (the only identity carrier in err.log at that point). MountConflict payload coverage is above.
TEST(CASMountAudit, RenewerForeignConflictRefusesAndNamesHolder)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid_x(0x1111);
    const UInt128 uuid_y(0x2222);
    uint64_t now_ms = 1000;

    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    /// Foreign holder X claims the slot first.
    ASSERT_EQ(claimMount(ops.op, layout, srid, uuid_x, /*our_epoch=*/1, now_ms, /*ttl_ms=*/100).kind,
              MountClaimResult::Claimed);

    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, srid, uuid_y, /*writer_epoch=*/1,
                            std::chrono::milliseconds(100), [&] { return now_ms; },
                            [] { return uint64_t{5}; }, {}, std::chrono::milliseconds(2000),
                            [&] { return boot_ms; });

    /// The enriched refusal message must name the OBSERVED holder (X), not the caller (Y).
    const String holder_uuid = u128ToHex(uuid_x);
    DB::Cas::tests::expectThrowsCodeWithMessage(
        DB::ErrorCodes::ABORTED,
        holder_uuid,
        [&] { renewer.start(); });
}

/// `Pool::open` can fail before/inside `doStart` (e.g. a foreign-conflict refusal, see
/// `RenewerForeignConflictRefusesAndNamesHolder` above) — the renewer is destroyed without ever having
/// claimed anything. Teardown must not throw "release before start"; there is nothing to release. A
/// stop AFTER a successful start still performs the farewell (covered by
/// `StopStampsExpiredAndFarewellSentinel` above); a genuinely-started DOUBLE terminate stays loud.
TEST(CASMountAudit, RenewerAdoptRefusesFencedSelfWithTypedError)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;

    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    /// mint (uuid, epoch 9), then fence it in place (what computeHeartbeatFloor does on expiry):
    seedOwnClaim(ops.op, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);
    {
        auto got = ops.op.read(layout.mountKey(srid), Retry::standard());
        MountLease fenced = decodeMountLease(got->bytes);
        fenced.gc_fenced = true;
        fenced.seq += 1;
        mustCommit(ops.op.replace(layout.mountKey(srid), encodeMountLease(fenced), got->etag,
                                  Retry::standard()), "fence-out");
    }

    std::vector<CasEvent> seen;
    CasEventSink sink = [&](const CasEvent & e) { seen.push_back(e); };
    /// A renewer for the SAME (uuid, epoch) tries to adopt the now-fenced slot.
    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, srid, uuid, /*writer_epoch=*/9,
                            std::chrono::milliseconds(100), [&] { return now_ms; },
                            [] { return uint64_t{5}; }, sink, std::chrono::milliseconds(2000),
                            [&] { return boot_ms; });

    bool threw = false;
    try
    {
        renewer.start();
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
/// GC's fence-out). The renewer must re-read and recognize this as its OWN incarnation being fenced —
/// a recoverable `MountFencedException`, not the generic single-writer-violation text.
TEST(CASHeartbeat, RenewOverFencedOwnSlotIsClassifiedNotForeign)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid(0x1234);
    uint64_t now_ms = 1000;
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, srid, uuid, /*epoch=*/9, now_ms, /*ttl_ms=*/100);

    std::vector<CasEvent> seen;
    CasEventSink sink = [&](const CasEvent & e) { seen.push_back(e); };
    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, srid, uuid, /*writer_epoch=*/9,
                            std::chrono::milliseconds(100), [&] { return now_ms; },
                            [] { return uint64_t{5}; }, sink, std::chrono::milliseconds(0),
                            [&] { return boot_ms; });
    renewer.start();
    seen.clear();

    /// Mid-run: the GC fences our own (uuid, epoch) mount slot in place (as `computeHeartbeatFloor`
    /// does on an expired lease), preserving the whole body — a guarded write against the incarnation
    /// it observed, exactly as the GC's own fence-out does it.
    {
        const auto got = ops.op.read(layout.mountKey(srid), Retry::standard());
        MountLease fenced = decodeMountLease(got->bytes);
        fenced.gc_fenced = true;
        fenced.seq += 1;
        mustCommit(ops.op.replace(layout.mountKey(srid), encodeMountLease(fenced), got->etag,
                                  Retry::standard()), "fence-out");
    }

    /// The renewal must classify the fence honestly — not "foreign writer":
    try
    {
        renewOrThrow(renewer);
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

TEST(CASHeartbeat, RenewerStateAllowsOnlyActiveReleaseOrTerminal)
{
#if defined(DEBUG_OR_SANITIZER_BUILD)
#define EXPECT_RENEWER_STATE_REJECTION(statement) EXPECT_DEATH({ statement; }, "allowed only in")
#else
#define EXPECT_RENEWER_STATE_REJECTION(statement) EXPECT_THROW(statement, DB::Exception)
#endif

    Layout layout("pool");
    const UInt128 uuid{0x1234};

    {
        auto backend = std::make_shared<RenewalScriptBackend>();
        uint64_t wall_ms = 1000;
        uint64_t boot_ms = 100;
        Ops ops(backend, &boot_ms);
        seedOwnClaim(ops.op, layout, "released", uuid, 9, wall_ms, 1000);
        MountLeaseRenewer renewer(
            ops.mount, ops.farewell, layout, "released", uuid, 9, std::chrono::milliseconds(1000),
            [&] { return wall_ms; }, [] { return uint64_t{7}; }, {}, std::chrono::milliseconds(20),
            [&] { return boot_ms; });
        EXPECT_EQ(renewer.state(), MountLeaseRenewerState::New);
        EXPECT_RENEWER_STATE_REJECTION(renewer.renew(renewalEnvironment(boot_ms)));
        EXPECT_RENEWER_STATE_REJECTION(renewer.release());
        EXPECT_EQ(renewer.start(), 100u);
        EXPECT_RENEWER_STATE_REJECTION(renewer.start());
        EXPECT_EQ(renewer.state(), MountLeaseRenewerState::Active);
        renewer.release();
        EXPECT_EQ(renewer.state(), MountLeaseRenewerState::Released);
        EXPECT_RENEWER_STATE_REJECTION(renewer.start());
        EXPECT_RENEWER_STATE_REJECTION(renewer.renew(renewalEnvironment(boot_ms)));
        EXPECT_RENEWER_STATE_REJECTION(renewer.release());
    }

    {
        auto backend = std::make_shared<RenewalScriptBackend>();
        uint64_t wall_ms = 1000;
        uint64_t boot_ms = 100;
        /// One pause jumps the clock past the lease bound, so the ambiguous first attempt is the only
        /// one this renewal ever sends and its verdict is the terminal one under test.
        Ops ops(backend, &boot_ms, /*sleep_step_ms=*/10'000);
        seedOwnClaim(ops.op, layout, "terminal", uuid, 9, wall_ms, 1000);
        MountLeaseRenewer renewer(
            ops.mount, ops.farewell, layout, "terminal", uuid, 9, std::chrono::milliseconds(1000),
            [&] { return wall_ms; }, [] { return uint64_t{7}; }, {}, std::chrono::milliseconds(20),
            [&] { return boot_ms; });
        renewer.start();
        backend->actions = {RenewalScriptBackend::Action::ThrowBefore};
        const MountRenewResult result = renewer.renew(renewalEnvironment(boot_ms));
        EXPECT_EQ(result.outcome, MountRenewOutcome::Terminal);
        EXPECT_NE(result.failure, nullptr);
        EXPECT_EQ(renewer.state(), MountLeaseRenewerState::RenewalTerminal);
        EXPECT_RENEWER_STATE_REJECTION(renewer.start());
        EXPECT_RENEWER_STATE_REJECTION(renewer.renew(renewalEnvironment(boot_ms)));
        EXPECT_RENEWER_STATE_REJECTION(renewer.release());
    }

#undef EXPECT_RENEWER_STATE_REJECTION
}

TEST(CASHeartbeat, RenewalRetriesOneImmutableBodyAndAdoptsLostResponse)
{
    auto backend = std::make_shared<RenewalScriptBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid{0x1234};
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, srid, uuid, 9, wall_ms, 1000);
    MountLeaseRenewer renewer(
        ops.mount, ops.farewell, layout, srid, uuid, 9, std::chrono::milliseconds(1000),
        [&] { return wall_ms; }, [] { return uint64_t{7}; }, {}, std::chrono::milliseconds(20),
        [&] { return boot_ms; });
    renewer.start();

    backend->attempts.clear();
    backend->actions = {RenewalScriptBackend::Action::ThrowBefore, RenewalScriptBackend::Action::Delegate};
    MountRenewResult retried = renewer.renew(renewalEnvironment(boot_ms));
    ASSERT_EQ(retried.outcome, MountRenewOutcome::Committed);
    ASSERT_EQ(backend->attempts.size(), 2u);
    EXPECT_EQ(backend->attempts[0].key, backend->attempts[1].key);
    EXPECT_EQ(backend->attempts[0].bytes, backend->attempts[1].bytes);
    EXPECT_EQ(backend->attempts[0].expected, backend->attempts[1].expected);
    const MountLease retry_body = decodeMountLease(backend->attempts[0].bytes);
    EXPECT_NE(retry_body.write_attempt_id, UInt128{});

    backend->attempts.clear();
    backend->actions = {RenewalScriptBackend::Action::LandThenThrow};
    MountRenewResult adopted = renewer.renew(renewalEnvironment(boot_ms));
    EXPECT_EQ(adopted.outcome, MountRenewOutcome::Committed);
    EXPECT_TRUE(adopted.resolved_by_read);
    EXPECT_EQ(adopted.attempts_sent, 1u);
    EXPECT_EQ(decodeMountLease(ops.op.read(layout.mountKey(srid), Retry::standard())->bytes).write_attempt_id,
              decodeMountLease(backend->attempts.front().bytes).write_attempt_id);
}

#if USE_AWS_S3
TEST(CASHeartbeat, RenewalOverConnectFailuresRecoversWithoutASettleRead)
{
    auto backend = std::make_shared<RenewalScriptBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid{0x1234};
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, srid, uuid, 9, wall_ms, 30000);
    MountLeaseRenewer renewer(
        ops.mount, ops.farewell, layout, srid, uuid, 9, std::chrono::milliseconds(30000),
        [&] { return wall_ms; }, [] { return uint64_t{7}; }, {}, std::chrono::milliseconds(2000),
        [&] { return boot_ms; });
    renewer.start();

    backend->attempts.clear();
    backend->read_calls = 0;
    /// Three seconds of "no free port" at 50 ms per hint, then the store answers.
    for (int i = 0; i < 60; ++i)
        backend->actions.push_back(RenewalScriptBackend::Action::ThrowConnectHint);
    backend->actions.push_back(RenewalScriptBackend::Action::Delegate);
    const MountRenewResult renewed = renewer.renew(renewalEnvironment(boot_ms));
    ASSERT_EQ(renewed.outcome, MountRenewOutcome::Committed);
    EXPECT_GT(renewed.attempts_sent, 1u);
    EXPECT_FALSE(renewed.resolved_by_read);            /// classification `committed_after_retry`
    EXPECT_EQ(backend->read_calls, 0u);
    EXPECT_EQ(backend->attempts.size(), 61u);
    for (const auto & attempt : backend->attempts)
        EXPECT_EQ(attempt.bytes, backend->attempts.front().bytes);
}
#endif

TEST(CASHeartbeat, DeadlineBeforeSendTerminalizesWithTypedFailure)
{
    auto backend = std::make_shared<RenewalScriptBackend>();
    Layout layout("pool");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, "test", UInt128{1}, 9, wall_ms, 100);
    MountLeaseRenewer renewer(
        ops.mount, ops.farewell, layout, "test", UInt128{1}, 9, std::chrono::milliseconds(100),
        [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
        [&] { return boot_ms; });
    renewer.start();
    backend->attempts.clear();
    backend->read_calls = 0;
    boot_ms = 180;
    const MountRenewResult result = renewer.renew(renewalEnvironment(boot_ms));
    const DB::Exception failure = terminalException(result);
    EXPECT_EQ(failure.code(), DB::ErrorCodes::NETWORK_ERROR);
    EXPECT_NE(failure.message().find("no attempt sent"), String::npos) << failure.message();
    EXPECT_NE(failure.message().find("external_lease_deadline"), String::npos) << failure.message();
    EXPECT_FALSE(result.sent_any);
    ASSERT_TRUE(result.deadline_source.has_value());
    EXPECT_EQ(*result.deadline_source, GaveUp::Source::Lease);
    EXPECT_TRUE(backend->attempts.empty());
    EXPECT_EQ(backend->read_calls, 0u) << "a pre-send terminal deadline must perform no diagnostic read";
}

TEST(CASHeartbeat, CancellationBeforeSendIsNotAttemptedAndAllowsRelease)
{
    auto backend = std::make_shared<RenewalScriptBackend>();
    Layout layout("pool");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, "test", UInt128{1}, 9, wall_ms, 1000);
    MountLeaseRenewer renewer(
        ops.mount, ops.farewell, layout, "test", UInt128{1}, 9, std::chrono::milliseconds(1000),
        [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
        [&] { return boot_ms; });
    renewer.start();
    backend->attempts.clear();
    backend->read_calls = 0;
    const MountRenewResult result = renewer.renew(renewalEnvironment(
        boot_ms, /*live=*/[] { return false; }, /*cancelled=*/[] { return true; }));
    EXPECT_EQ(result.outcome, MountRenewOutcome::NotAttempted);
    EXPECT_EQ(result.failure, nullptr);
    EXPECT_EQ(renewer.state(), MountLeaseRenewerState::Active);
    EXPECT_TRUE(backend->attempts.empty());
    EXPECT_NO_THROW(renewer.release());
    EXPECT_EQ(renewer.state(), MountLeaseRenewerState::Released);
}

TEST(CASHeartbeat, CancellationAfterSendIsTerminalAndForbidsRelease)
{
    auto backend = std::make_shared<RenewalScriptBackend>();
    Layout layout("pool");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    bool cancelled = false;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, "test", UInt128{1}, 9, wall_ms, 1000);
    MountLeaseRenewer renewer(
        ops.mount, ops.farewell, layout, "test", UInt128{1}, 9, std::chrono::milliseconds(1000),
        [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
        [&] { return boot_ms; });
    renewer.start();
    backend->attempts.clear();
    backend->read_calls = 0;
    backend->cancel_after_write = [&] { cancelled = true; };
    backend->actions = {RenewalScriptBackend::Action::ReturnThenCancel};
    const MountRenewResult result = renewer.renew(
        renewalEnvironment(boot_ms, /*live=*/[&] { return !cancelled; }, /*cancelled=*/[&] { return cancelled; }));
    const DB::Exception failure = terminalException(result);
    EXPECT_EQ(failure.code(), DB::ErrorCodes::NETWORK_ERROR);
    EXPECT_TRUE(result.sent_any);
    EXPECT_EQ(backend->read_calls, 0u) << "post-write cancellation must not start a diagnostic read";
    EXPECT_EQ(renewer.state(), MountLeaseRenewerState::RenewalTerminal);
    const String bytes_before = ops.op.read(layout.mountKey("test"), Retry::standard())->bytes;
    EXPECT_FALSE(renewer.canRelease());
    EXPECT_EQ(ops.op.read(layout.mountKey("test"), Retry::standard())->bytes, bytes_before);
}

TEST(CASHeartbeat, SlowResolvedSuccessKeepsAttemptStartAnchor)
{
    auto backend = std::make_shared<RenewalScriptBackend>();
    Layout layout("pool");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, "test", UInt128{1}, 9, wall_ms, 1000);
    MountLeaseRenewer renewer(
        ops.mount, ops.farewell, layout, "test", UInt128{1}, 9, std::chrono::milliseconds(1000),
        [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
        [&] { return boot_ms; });
    renewer.start();
    boot_ms = 150;
    backend->cancel_after_write = [&] { boot_ms = 400; };
    backend->actions = {RenewalScriptBackend::Action::LandThenThrow};
    const MountRenewResult result = renewer.renew(renewalEnvironment(boot_ms));
    EXPECT_EQ(result.outcome, MountRenewOutcome::Committed);
    EXPECT_EQ(result.attempt_start_boot_ms, 150u);
    EXPECT_EQ(renewer.lastCommittedAttemptStartBootMs(), 150u);
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
        Ops ops(backend, &boot_ms);
        seedOwnClaim(ops.op, layout, "test", uuid, 9, wall_ms, 1000);
        MountLeaseRenewer renewer(
            ops.mount, ops.farewell, layout, "test", uuid, 9, std::chrono::milliseconds(1000),
            [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
            [&] { return boot_ms; });
        renewer.start();
        auto got = ops.op.read(layout.mountKey("test"), Retry::standard());
        MountLease current = decodeMountLease(got->bytes);
        current.server_uuid = current_uuid;
        current.writer_epoch = current_epoch;
        current.write_attempt_id = current_attempt;
        ++current.seq;
        mustCommit(ops.op.replace(layout.mountKey("test"), encodeMountLease(current), got->etag,
                                  Retry::standard()), "competing slot");
        backend->read_calls = 0;
        const MountRenewResult result = renewer.renew(renewalEnvironment(boot_ms));
        const DB::Exception failure = terminalException(result);
        EXPECT_NE(failure.code(), DB::ErrorCodes::LOGICAL_ERROR);
        EXPECT_EQ(renewer.state(), MountLeaseRenewerState::RenewalTerminal);
        EXPECT_EQ(backend->read_calls, 1u) << "the write's own resolving read must be the only terminal read";
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
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, "test", UInt128{1}, 9, wall_ms, 1000);
    MountLeaseRenewer renewer(
        ops.mount, ops.farewell, layout, "test", UInt128{1}, 9, std::chrono::milliseconds(1000),
        [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
        [&] { return boot_ms; });
    renewer.start();
    backend->attempts.clear();
    backend->actions = {
        RenewalScriptBackend::Action::ThrowBeforeThenLandAfterResolve,
        RenewalScriptBackend::Action::Delegate,
    };
    const MountRenewResult result = renewer.renew(renewalEnvironment(boot_ms));
    EXPECT_EQ(result.outcome, MountRenewOutcome::Committed);
    EXPECT_TRUE(result.resolved_by_read);
    ASSERT_EQ(backend->attempts.size(), 2u);
    EXPECT_EQ(backend->attempts[0].bytes, backend->attempts[1].bytes);
    EXPECT_EQ(decodeMountLease(ops.op.read(layout.mountKey("test"), Retry::standard())->bytes).write_attempt_id,
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
        Ops ops(backend, &boot_ms);
        seedOwnClaim(ops.op, layout, "test", UInt128{1}, 9, wall_ms, 1000);
        MountLeaseRenewer renewer(
            ops.mount, ops.farewell, layout, "test", UInt128{1}, 9, std::chrono::milliseconds(1000),
            [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
            [&] { return boot_ms; });
        renewer.start();
        const String key = layout.mountKey("test");
        auto got = ops.op.read(key, Retry::standard());
        if (vanish)
            ASSERT_EQ(ops.op.remove(key, got->etag, Retry::standard()), Removal::Removed);
        else
        {
            MountLease fenced = decodeMountLease(got->bytes);
            fenced.gc_fenced = true;
            ++fenced.seq;
            mustCommit(ops.op.replace(key, encodeMountLease(fenced), got->etag, Retry::standard()),
                       "fence-out");
        }
        const DB::Exception failure = terminalException(renewer.renew(renewalEnvironment(boot_ms)));
        EXPECT_NE(failure.code(), DB::ErrorCodes::LOGICAL_ERROR);
        EXPECT_EQ(renewer.state(), MountLeaseRenewerState::RenewalTerminal);
    };
    run_case(false);
    run_case(true);
}

TEST(CASHeartbeat, LateDeliveryAfterTerminalCannotRearmOrOverwriteSuccessor)
{
    Layout layout("pool");
    {
        auto backend = std::make_shared<RenewalScriptBackend>();
        uint64_t wall_ms = 1000;
        uint64_t boot_ms = 100;
        /// One pause jumps the clock past the lease bound, so the ambiguous first attempt is the only
        /// one this renewal sends and the renewal ends terminal with that attempt still in flight.
        Ops ops(backend, &boot_ms, /*sleep_step_ms=*/10'000);
        seedOwnClaim(ops.op, layout, "before-reclaim", UInt128{1}, 9, wall_ms, 1000);
        MountLeaseRenewer renewer(
            ops.mount, ops.farewell, layout, "before-reclaim", UInt128{1}, 9, std::chrono::milliseconds(1000),
            [&] { return wall_ms; }, [] { return uint64_t{0}; }, CasEventSink{}, std::chrono::milliseconds(20),
            [&] { return boot_ms; });
        renewer.start();
        backend->actions = {RenewalScriptBackend::Action::ThrowBeforeThenLandAfterResolve};
        const MountRenewResult result = renewer.renew(renewalEnvironment(boot_ms));
        EXPECT_EQ(result.outcome, MountRenewOutcome::Terminal);

        /// The delayed write landed during the resolving read. It carries this renewer's own epoch, and
        /// it does not put the renewer back in business.
        const MountLease landed = decodeMountLease(
            ops.op.read(layout.mountKey("before-reclaim"), Retry::standard())->bytes);
        EXPECT_EQ(landed.writer_epoch, 9u);
        EXPECT_EQ(renewer.state(), MountLeaseRenewerState::RenewalTerminal);
    }
    {
        auto backend = std::make_shared<RenewalScriptBackend>();
        uint64_t wall_ms = 1000;
        uint64_t boot_ms = 100;
        Ops ops(backend, &boot_ms, /*sleep_step_ms=*/10'000);
        seedOwnClaim(ops.op, layout, "after-successor", UInt128{1}, 9, wall_ms, 1000);
        MountLeaseRenewer renewer(
            ops.mount, ops.farewell, layout, "after-successor", UInt128{1}, 9, std::chrono::milliseconds(1000),
            [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
            [&] { return boot_ms; });
        renewer.start();

        /// The incarnation the about-to-be-terminal renewal names as its precondition: a late delivery
        /// of that attempt can only ever be replayed against exactly this one.
        const Etag delayed_precondition
            = ops.op.read(layout.mountKey("after-successor"), Retry::standard())->etag;

        backend->actions = {RenewalScriptBackend::Action::ThrowBefore};
        const MountRenewResult result = renewer.renew(renewalEnvironment(boot_ms));
        ASSERT_EQ(result.outcome, MountRenewOutcome::Terminal);
        ASSERT_FALSE(backend->attempts.empty());
        const auto delayed = backend->attempts.back();

        /// The GC fences the slot, then a successor claims it at a fresh epoch and adopts it.
        auto current = ops.op.read(delayed.key, Retry::standard());
        MountLease fenced = decodeMountLease(current->bytes);
        fenced.gc_fenced = true;
        ++fenced.seq;
        mustCommit(ops.op.replace(delayed.key, encodeMountLease(fenced), current->etag, Retry::standard()),
                   "fence-out");
        ASSERT_EQ(claimMount(ops.op, layout, "after-successor", UInt128{1}, 10, wall_ms, 1000).kind,
                  MountClaimResult::Claimed);
        MountLeaseRenewer successor(
            ops.mount, ops.farewell, layout, "after-successor", UInt128{1}, 10, std::chrono::milliseconds(1000),
            [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
            [&] { return boot_ms; });
        successor.start();

        /// Replaying the delayed attempt against the incarnation it named is refused; the successor's
        /// body is what stands.
        EXPECT_TRUE(std::holds_alternative<Conflict>(
            ops.op.replace(delayed.key, delayed.bytes, delayed_precondition, Retry::once())));
        EXPECT_EQ(decodeMountLease(ops.op.read(delayed.key, Retry::standard())->bytes).writer_epoch, 10u);
    }
}

TEST(CASHeartbeat, WallClockStepsAndBootSuspendCannotExtendAuthority)
{
    auto backend = std::make_shared<RenewalScriptBackend>();
    Layout layout("pool");
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, "test", UInt128{1}, 9, wall_ms, 1000);
    MountLeaseRenewer renewer(
        ops.mount, ops.farewell, layout, "test", UInt128{1}, 9, std::chrono::milliseconds(1000),
        [&] { return wall_ms; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(20),
        [&] { return boot_ms; });
    renewer.start();

    wall_ms = 9'000'000;
    EXPECT_EQ(renewer.renew(renewalEnvironment(boot_ms)).outcome, MountRenewOutcome::Committed);
    wall_ms = 1;
    EXPECT_EQ(renewer.renew(renewalEnvironment(boot_ms)).outcome, MountRenewOutcome::Committed);

    backend->attempts.clear();
    boot_ms += 10'000;
    const MountRenewResult suspended = renewer.renew(renewalEnvironment(boot_ms));
    const DB::Exception failure = terminalException(suspended);
    EXPECT_EQ(failure.code(), DB::ErrorCodes::NETWORK_ERROR);
    EXPECT_TRUE(backend->attempts.empty()) << "suspend-sized BOOTTIME overshoot must close admission";
}

/// Every attempt costs the whole envelope (attempt 100 + 2 * cap 50 = 200 ms) and fails ambiguously.
/// Under a 1000 ms lease with a 100 ms margin the renewal must stop issuing before the cutoff rather
/// than start an attempt that cannot finish inside it.
namespace
{
/// Bypasses `RenewalScriptBackend`'s scripted-action queue for a guarded mount write and instead
/// always fails it (and every read) once armed, each failure costing the whole envelope on the
/// injected boot clock. Left unarmed during `seedOwnClaim` (an unconditional read then an unguarded
/// create -- neither is a guarded mount write, but the read would still hit the always-throwing
/// override below) and during `renewer.start()`'s adopt read, so the fixture itself can land.
struct EnvelopeEatingBackend : RenewalScriptBackend
{
    uint64_t * boot_ms = nullptr;
    bool armed = false;
    uint64_t attemptTimeoutMs() const override { return 100; }
    uint64_t attemptEnvelopeMs() const override { return 200; }
    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value, TransportAccess & access) override
    {
        if (armed && expected_value && key.ends_with("/mount"))
        {
            attempts.push_back({key, bytes, expected_value});
            *boot_ms += 200;
            throw Poco::TimeoutException("the whole envelope, gone");
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }
    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (armed)
        {
            *boot_ms += 200;
            throw Poco::TimeoutException("the read too");
        }
        return InMemoryBackend::read(key, access);
    }
};
}

TEST(CASHeartbeat, RenewalStopsBeforeTheCutoffWhenEveryAttemptConsumesTheEnvelope)
{
    auto backend = std::make_shared<EnvelopeEatingBackend>();
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    backend->boot_ms = &boot_ms;
    Layout layout("pool");
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, "test", UInt128{0x1234}, 9, wall_ms, 1000);
    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, "test", UInt128{0x1234}, 9, std::chrono::milliseconds(1000),
                              [&] { return wall_ms; }, [] { return uint64_t{7}; }, {}, std::chrono::milliseconds(100),
                              [&] { return boot_ms; });
    renewer.start();
    const uint64_t cutoff = renewer.lastCommittedAttemptStartBootMs() + 1000 - 100;
    backend->attempts.clear();
    backend->armed = true;
    const MountRenewResult result = renewer.renew(renewalEnvironment(boot_ms));
    EXPECT_EQ(result.outcome, MountRenewOutcome::Terminal);
    EXPECT_LE(boot_ms, cutoff) << "the last attempt started inside the cutoff and the engine did not start one that could not finish";
}
