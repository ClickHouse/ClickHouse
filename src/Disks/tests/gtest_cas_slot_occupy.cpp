#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestControl.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <Poco/Exception.h>

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::ChunkFaultBackend;
using DB::Cas::tests::LandedButAckLostOnceBackend;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// ================================================================================================
/// Task 2 (2026-07-28 CAS ref-chain Stage A streams, spec INV-2): CasRequestController::slotOccupy --
/// the dedicated RAW slot-occupy primitive every seal writer and wedge retry uses. ONE conditional
/// create; on conflict, ONE raw exact GET of the occupant -- NEVER retries internally, NEVER lists,
/// and NEVER composes putIfAbsentControlled (which retries the same (key, bytes) internally) or
/// resolveByExactGet (which compares against an expected body and throws CORRUPTED_DATA on a
/// mismatch) [codex finding 3]. Adjudicating whether an Occupied occupant is "mine" is entirely the
/// CALLER's job (Task 4/6, the CaCasMountCore `mine` contract) -- these tests only pin the
/// primitive's own three-way outcome and its op-count contract (Created=1, Occupied=2,
/// Unresolved<=2 backend ops).
/// ================================================================================================

namespace
{

/// Deletes the key the INSTANT its own conditional create conflicts, modelling "the occupant that
/// caused the conflict vanished before slotOccupy's single resolve GET" -- a race a real backend can
/// produce (e.g. GC reclaiming an already-condemned object) that the primitive must survive by
/// reporting Unresolved, NEVER a fabricated Created.
class VanishOnConflictBackend : public CountingBackend
{
public:
    using CountingBackend::putIfAbsent;

    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        PutResult result = CountingBackend::putIfAbsent(key, bytes, meta);
        if (result.outcome == PutOutcome::PreconditionFailed)
        {
            const HeadResult h = head(key);
            if (h.exists)
                deleteExact(key, h.token);
        }
        return result;
    }
};

/// Throws a deterministic LOCAL failure (BAD_ARGUMENTS, in isDeterministicLocalFailure's set) on the
/// first putIfAbsent -- models a backend-level programming bug, distinct from ChunkFaultBackend's
/// Mode::Definite below, which is a whitelisted SYNCHRONOUS REJECTION
/// (classifyConditionalWriteResult's DefiniteFailure). slotOccupy must rethrow both, unchanged, never
/// folding either into Unresolved (SlotOccupyResult::Kind has no DefiniteFailure member to carry it).
class LocalFailureOnceBackend : public CountingBackend
{
public:
    using CountingBackend::putIfAbsent;
    bool fail_once = true;

    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        if (fail_once)
        {
            fail_once = false;
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "scripted deterministic local failure");
        }
        return CountingBackend::putIfAbsent(key, bytes, meta);
    }
};

/// (`LandedButAckLostOnceBackend` -- "the write LANDS, then the ack is lost" -- was lifted into
/// `cas_test_helpers.h` for Task 4, whose wedge-adoption tests need the identical seam through a whole
/// Pool. Its `key_substr` defaults to empty, which is exactly this file's original behaviour: fault the
/// first `putIfAbsent` of any key.)
/// Delegates the FIRST putIfAbsent for a key to CountingBackend -- so the write actually LANDS -- and
/// only THEN throws an ambiguous exception, modelling "our own PUT committed but its response was lost"
/// (the Task-4 adoption input: plan's "Occupied + bytes == wedge.bytes -> an earlier attempt landed ->
/// adopt"). Distinct from InMemoryBackend::injectAmbiguousPutIfAbsent, which never touches the store at
/// all -- that hook models an attempt that did NOT land; this one models an attempt that DID.
/// One-shot per backend instance: review finding I2 asked specifically for a ~10-line local backend rather than
/// reusing ChunkFaultBackend::Mode::LandedThenLost, which also arms a one-shot lost-GET fault that would
/// obscure whether slotOccupy's OWN immediate resolve (not just a later caller's retry) is correct too.

}

/// ---- Step 1 required scenarios ----

TEST(CASSlotOccupy, AbsentKeyCreatesWithOneOp)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequestController controller(backend, CasRequestBudget{});

    const auto result = controller.slotOccupy("k", "payload", [] { return true; });
    EXPECT_EQ(result.kind, SlotOccupyResult::Kind::Created);
    EXPECT_TRUE(result.occupant_bytes.empty());
    EXPECT_TRUE(result.occupant_token.empty()) << "occupant_token is Occupied-only; must stay default on Created";
    EXPECT_EQ(result.unresolved_reason, CasUnresolvedReason::NotUnresolved);

    EXPECT_EQ(backend->putCount("k"), 1u);
    EXPECT_EQ(backend->getCount("k"), 0u);
    EXPECT_EQ(backend->headCount("k"), 0u);

    const auto landed = backend->get("k");
    ASSERT_TRUE(landed.has_value());
    EXPECT_EQ(landed->bytes, "payload");
}

TEST(CASSlotOccupy, PreExistingKeyOccupiedWithExactBytesAndTokenTwoOps)
{
    auto backend = std::make_shared<CountingBackend>();
    const PutResult seeded = backend->putIfAbsent("k", "occupant-bytes");
    ASSERT_EQ(seeded.outcome, PutOutcome::Done);
    backend->resetCounts();

    CasRequestController controller(backend, CasRequestBudget{});
    const auto result = controller.slotOccupy("k", "my-attempt-bytes", [] { return true; });
    EXPECT_EQ(result.kind, SlotOccupyResult::Kind::Occupied);
    EXPECT_EQ(result.occupant_bytes, "occupant-bytes");
    EXPECT_EQ(result.occupant_token, seeded.token);

    EXPECT_EQ(backend->putCount("k"), 1u);
    EXPECT_EQ(backend->getCount("k"), 1u);
    EXPECT_EQ(backend->headCount("k"), 0u) << "exactly PUT+GET -- a HEAD-then-GET implementation must fail this";

    /// A conflict never overwrites or appends -- the pre-existing object is untouched.
    const auto current = backend->get("k");
    ASSERT_TRUE(current.has_value());
    EXPECT_EQ(current->bytes, "occupant-bytes");
}

TEST(CASSlotOccupy, InjectedAmbiguousPutResolvesUnresolvedWhenGetFindsNothing)
{
    auto backend = std::make_shared<CountingBackend>();
    backend->injectAmbiguousPutIfAbsent("k");

    CasRequestController controller(backend, CasRequestBudget{});
    const auto result = controller.slotOccupy("k", "payload", [] { return true; });
    EXPECT_EQ(result.kind, SlotOccupyResult::Kind::Unresolved);
    /// An attempt WAS sent (the ambiguous PUT itself) -- this is never the pre-attempt NoAttemptSent
    /// case. Of the existing CasUnresolvedReason values, AttemptsExhausted is the one documented as
    /// "the genuine case the 'retry budget exhausted' wording describes" -- exactly this call's single
    /// (and only) attempt having nothing left to give once its resolve GET came up empty.
    EXPECT_EQ(result.unresolved_reason, CasUnresolvedReason::AttemptsExhausted);
    EXPECT_FALSE(unresolvedProvesNothingWasSent(result.unresolved_reason));

    EXPECT_EQ(backend->putCount("k"), 1u);
    EXPECT_EQ(backend->getCount("k"), 1u);
    EXPECT_FALSE(backend->head("k").exists) << "the injected fault must not actually create anything";
}

TEST(CASSlotOccupy, ConflictThenVanishResolvesUnresolved)
{
    auto backend = std::make_shared<VanishOnConflictBackend>();
    const auto seeded = backend->putIfAbsent("k", "occupant-bytes");
    ASSERT_EQ(seeded.outcome, PutOutcome::Done);
    backend->resetCounts();

    CasRequestController controller(backend, CasRequestBudget{});
    const auto result = controller.slotOccupy("k", "my-attempt-bytes", [] { return true; });
    EXPECT_EQ(result.kind, SlotOccupyResult::Kind::Unresolved);
    EXPECT_EQ(result.unresolved_reason, CasUnresolvedReason::AttemptsExhausted);

    EXPECT_EQ(backend->putCount("k"), 1u);
    EXPECT_EQ(backend->getCount("k"), 1u);
    /// No headCount assertion here (unlike the sibling Occupied test above): VanishOnConflictBackend's
    /// OWN fixture issues a HEAD internally (to fetch the token before deleteExact) -- that HEAD belongs
    /// to the test's vanish mechanism, not to slotOccupy, so asserting headCount==0 would be wrong, not
    /// stronger. slotOccupy itself never calls head(); only put+get are its own ops.
    EXPECT_FALSE(backend->head("k").exists) << "the occupant vanished between the conflict and the resolve GET";
}

TEST(CASSlotOccupy, FenceFlipMidCallRefusesPreAttemptNeverLiesCreated)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequestController controller(backend, CasRequestBudget{});

    const auto result = controller.slotOccupy("k", "payload", [] { return false; });
    EXPECT_EQ(result.kind, SlotOccupyResult::Kind::Unresolved);
    /// The pre-attempt reason: fence_ok refused before anything was sent to the backend.
    EXPECT_EQ(result.unresolved_reason, CasUnresolvedReason::NoAttemptSent);
    EXPECT_TRUE(unresolvedProvesNothingWasSent(result.unresolved_reason));

    EXPECT_EQ(backend->putTotal(), 0u);
    EXPECT_EQ(backend->getTotal(), 0u);
    EXPECT_FALSE(backend->head("k").exists) << "never a lie of Created -- the key must be untouched";
}

/// ---- Bonus coverage: the deadline pre-gate (the OTHER half of "fence/deadline-gated"), and the two
/// rethrow paths this primitive shares with its sibling controlled ops.  ----

/// The deadline gate is the SAME pre-attempt refusal as the fence gate above -- a fake clock proves it
/// fires from elapsed time alone, with a fence that always says yes.
TEST(CASSlotOccupy, OperationDeadlineExhaustedRefusesPreAttempt)
{
    auto backend = std::make_shared<CountingBackend>();
    uint64_t clock = 0;
    auto now_ms = [&clock]() -> uint64_t { const uint64_t t = clock; clock += 1000; return t; };

    CasRequestBudget budget;
    budget.attempt_timeout_ms = 50;
    budget.operation_deadline_ms = 500;   /// entry now_ms()==0 -> deadline_ms=500; the gate's OWN
                                           /// now_ms() call then returns 1000 -> 1000+50 > 500 -> refuse
    CasRequestController controller(backend, budget, now_ms);

    const auto result = controller.slotOccupy("k", "payload", [] { return true; });
    EXPECT_EQ(result.kind, SlotOccupyResult::Kind::Unresolved);
    EXPECT_EQ(result.unresolved_reason, CasUnresolvedReason::NoAttemptSent);
    EXPECT_EQ(backend->putTotal(), 0u);
    EXPECT_EQ(backend->getTotal(), 0u) << "zero ops total -- the deadline gate must refuse before any I/O, same as the fence gate";
}

/// A whitelisted synchronous rejection (classifyConditionalWriteResult's DefiniteFailure) PROVES the
/// request was never applied -- slotOccupy must surface it unchanged rather than resolving or folding
/// it into Unresolved. Guarded to USE_AWS_S3 builds ONLY [review M6]: DefiniteFailure classification is
/// structurally unreachable without it (classifyConditionalWriteResult's whitelist is entirely inside
/// its own `#if USE_AWS_S3`), so on a no-S3 build ChunkFaultBackend::Mode::Definite instead throws a
/// plain CORRUPTED_DATA DB::Exception -- which is in isDeterministicLocalFailure's set, meaning this
/// test would silently exercise the SAME slotOccupy branch as DeterministicLocalFailurePropagatesWithoutResolve
/// below rather than the DefiniteFailure branch it claims to cover. Better a visibly-absent test on that
/// config than a passing one that isn't testing what its name says.
#if USE_AWS_S3
TEST(CASSlotOccupy, DefiniteFailurePropagatesWithoutResolve)
{
    auto backend = std::make_shared<ChunkFaultBackend>();
    backend->fault_substr = "k";
    backend->mode = ChunkFaultBackend::Mode::Definite;
    backend->fault_count = 1;

    CasRequestController controller(backend, CasRequestBudget{});
    EXPECT_THROW(controller.slotOccupy("k", "payload", [] { return true; }), DB::Exception);
    /// ChunkFaultBackend's fault check throws BEFORE delegating to CountingBackend::putIfAbsent, so
    /// putCount stays 0 on this path -- fault_count reaching 0 is this backend's own proof the (one)
    /// attempt was made and consumed the fault.
    EXPECT_EQ(backend->fault_count, 0);
    EXPECT_EQ(backend->getCount("k"), 0u) << "a whitelisted definite rejection must never trigger a resolve GET";
}
#endif

/// A deterministic LOCAL failure (isDeterministicLocalFailure's set) is the OTHER rethrow path --
/// distinct from DefiniteFailure above, and checked first in the implementation, so it needs its own
/// backend-level fault to prove both branches are wired, not just one masking the other.
TEST(CASSlotOccupy, DeterministicLocalFailurePropagatesWithoutResolve)
{
    auto backend = std::make_shared<LocalFailureOnceBackend>();
    CasRequestController controller(backend, CasRequestBudget{});

    bool threw = false;
    try
    {
        controller.slotOccupy("k", "payload", [] { return true; });
    }
    catch (const DB::Exception & e)
    {
        threw = true;
        EXPECT_EQ(e.code(), DB::ErrorCodes::BAD_ARGUMENTS) << "the ORIGINAL exception must propagate unchanged";
    }
    EXPECT_TRUE(threw) << "a deterministic local failure must propagate, never return an outcome";
    /// LocalFailureOnceBackend throws BEFORE delegating to CountingBackend::putIfAbsent (same shape as
    /// ChunkFaultBackend above), so putCount stays 0 here too -- fail_once flipping is this backend's
    /// own proof the attempt was made.
    EXPECT_FALSE(backend->fail_once);
    EXPECT_EQ(backend->getCount("k"), 0u);
}

/// ---- Fix round 1 (review findings I1, I2): the two gaps the reviewer required landed before Task 4
/// consumes this primitive. Both guard the design decisions the review approved -- see
/// task-2-review.md concern (a) and finding I2's Task-4-adoption note. ----

/// I1: pins the single-fence_ok()-call design (concern (a)) so a future contributor cannot silently
/// "fix the inconsistency" by re-adding the sibling ops' post-write fence recheck -- that change would
/// break Task 4's old-generation-retry semantics (resolveWedgeOnce deliberately calls slotOccupy under
/// the wedge's ORIGINAL admitted_fence_generation, and relies on ITS OWN post-I/O checkFenceOrThrow,
/// not a second internal check here, to decide whether the result is still relevant). A counting
/// fence_ok that only answers true on its FIRST call: if slotOccupy ever called it again after the
/// write landed, this test would see Unresolved instead of Created, OR (if the outcome happened to
/// still read Created some other way) the call-count assertion below would catch the extra invocation
/// either way.
TEST(CASSlotOccupy, CreatedNeverRechecksFenceAfterTheWrite)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequestController controller(backend, CasRequestBudget{});

    int fence_calls = 0;
    const auto fence_ok = [&fence_calls]
    {
        ++fence_calls;
        return fence_calls == 1;
    };

    const auto result = controller.slotOccupy("k", "payload", fence_ok);
    EXPECT_EQ(result.kind, SlotOccupyResult::Kind::Created);
    EXPECT_EQ(fence_calls, 1) << "slotOccupy must call fence_ok() exactly ONCE (pre-attempt only) -- "
                                 "a post-write recheck would falsely report Unresolved here (fence_calls's "
                                 "SECOND answer is false) and would break Task 4's old-generation-retry design";
}

/// I2: proves Occupied is reachable for an occupant that is OUR OWN earlier ambiguous write, not only
/// for a foreign pre-seeded one (PreExistingKeyOccupiedWithExactBytesAndTokenTwoOps above always seeds
/// via a plain, unambiguous putIfAbsent). This is the exact input shape Task 4's resolveWedgeOnce
/// adjudicates: "Occupied + bytes == wedge.bytes -> an earlier attempt landed -> adopt" (plan :329).
TEST(CASSlotOccupy, OwnLandedAmbiguousWriteObservedAsOccupiedOnRetry)
{
    auto backend = std::make_shared<LandedButAckLostOnceBackend>();
    CasRequestController controller(backend, CasRequestBudget{});

    /// Call 1 -- the original attempt: the PUT's own response is lost, but the write DID commit, and
    /// THIS call's own resolve GET (unfaulted) observes it immediately -- Occupied with OUR bytes,
    /// proving the same-call resolve path works for a landed ambiguous write, not only a foreign one.
    const auto first = controller.slotOccupy("k", "my-bytes", [] { return true; });
    EXPECT_EQ(first.kind, SlotOccupyResult::Kind::Occupied);
    EXPECT_EQ(first.occupant_bytes, "my-bytes");
    EXPECT_EQ(backend->putCount("k"), 1u);
    EXPECT_EQ(backend->getCount("k"), 1u);

    /// Call 2 -- Task 4's resolveWedgeOnce pattern: a LATER caller's flush resolving the SAME logical
    /// attempt via a FRESH slotOccupy call. The fault is already consumed (one-shot), so this PUT
    /// conflicts cleanly (PreconditionFailed) and the resolve GET observes OUR OWN earlier bytes again --
    /// the exact adoption input Task 4 is built on, and the SAME incarnation both calls saw.
    const auto second = controller.slotOccupy("k", "my-bytes", [] { return true; });
    EXPECT_EQ(second.kind, SlotOccupyResult::Kind::Occupied);
    EXPECT_EQ(second.occupant_bytes, "my-bytes");
    EXPECT_EQ(second.occupant_token, first.occupant_token) << "both calls must observe the SAME landed incarnation";
    EXPECT_EQ(backend->putCount("k"), 2u);
    EXPECT_EQ(backend->getCount("k"), 2u);
}
