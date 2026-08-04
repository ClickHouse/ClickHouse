#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestControl.h>
#include <Common/ProfileEvents.h>

#include <limits>

#if USE_AWS_S3
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/tests/cas_test_helpers.h>
#include <IO/S3Common.h>
#include <Poco/Net/NetException.h>
#endif

using namespace DB::Cas;

namespace DB::ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int ABORTED;
}

namespace ProfileEvents
{
    extern const Event CASConditionalWriteAttempts;
    extern const Event CASConditionalWriteCommitted;
    extern const Event CASConditionalWriteDefiniteFailure;
    extern const Event CASConditionalWriteUnresolved;
}

#if USE_AWS_S3
namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CORRUPTED_DATA;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_EXCEPTION;
}
#endif

/// The success path (buf.finalize() returned without throwing) is always Committed. No exception
/// object is needed — the caller distinguishes success from failure before calling either overload.
TEST(CASRequestControl, SuccessIsAlwaysCommitted)
{
    EXPECT_EQ(classifyConditionalWriteResult(), CasWriteOutcome::Committed);
}

/// Fix #37 phase 2: the retry-later throw must be NETWORK_ERROR, never ABORTED -- ABORTED is silently
/// swallowed by ReplicatedMergeMutateTaskBase (no backoff, no last_exception), which is exactly the
/// defect this fix closes.
TEST(CASWriteRetryLater, ThrowsNetworkErrorNotAborted)
{
    bool threw = false;
    try
    {
        throwCasWriteRetryLater("test cause");
        FAIL() << "throwCasWriteRetryLater must always throw";
    }
    catch (const DB::Exception & e)
    {
        threw = true;
        EXPECT_EQ(e.code(), DB::ErrorCodes::NETWORK_ERROR);
        EXPECT_NE(e.code(), DB::ErrorCodes::ABORTED);
        EXPECT_NE(e.message().find("test cause"), String::npos) << e.message();
        EXPECT_NE(e.message().find("retrying later"), String::npos) << e.message();
    }
    EXPECT_TRUE(threw);
}

/// The exception_ptr twin (for call sites that fail a pending future/promise rather than throw
/// directly, e.g. CasRefLedger's queued-append completion paths) must carry the SAME classification.
TEST(CASWriteRetryLater, ExceptionPtrVariantCarriesSameClassification)
{
    const std::exception_ptr eptr = makeCasWriteRetryLaterExceptionPtr("another cause");
    bool threw = false;
    try
    {
        std::rethrow_exception(eptr);
        FAIL() << "expected a thrown exception";
    }
    catch (const DB::Exception & e)
    {
        threw = true;
        EXPECT_EQ(e.code(), DB::ErrorCodes::NETWORK_ERROR);
        EXPECT_NE(e.message().find("another cause"), String::npos) << e.message();
    }
    EXPECT_TRUE(threw);
}

#if USE_AWS_S3

/// One row per RFC cas-s3-timeout-retry-control §operation-classes classification. PreconditionFailed
/// is NEVER DefiniteFailure — it means the key exists, not that the request was rejected — and every
/// unrecognized/ambiguous error also falls to Unresolved, never to a false DefiniteFailure.
TEST(CASRequestControl, ClassifiesPreconditionFailedAsUnresolved)
{
    DB::S3Exception e("412 from backend", Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed");
    EXPECT_EQ(classifyConditionalWriteResult(e), CasWriteOutcome::Unresolved);
}

TEST(CASRequestControl, ClassifiesTimeoutAsUnresolved)
{
    Poco::TimeoutException e("simulated client-side receive timeout");
    EXPECT_EQ(classifyConditionalWriteResult(e), CasWriteOutcome::Unresolved);
}

TEST(CASRequestControl, ClassifiesConnectionResetAsUnresolved)
{
    Poco::Net::ConnectionResetException e("simulated connection reset");
    EXPECT_EQ(classifyConditionalWriteResult(e), CasWriteOutcome::Unresolved);
}

TEST(CASRequestControl, Classifies5xxAsUnresolved)
{
    DB::S3Exception e("simulated internal error", Aws::S3::S3Errors::INTERNAL_FAILURE, "InternalError");
    EXPECT_EQ(classifyConditionalWriteResult(e), CasWriteOutcome::Unresolved);
    /// SlowDown / ServiceUnavailable are also 5xx-class and equally Unresolved.
    DB::S3Exception slow_down("simulated throttle", Aws::S3::S3Errors::SLOW_DOWN, "SlowDown");
    EXPECT_EQ(classifyConditionalWriteResult(slow_down), CasWriteOutcome::Unresolved);
}

TEST(CASRequestControl, ClassifiesMalformedRequestAsDefiniteFailure)
{
    DB::S3Exception e("bad xml", Aws::S3::S3Errors::UNKNOWN, "MalformedXML");
    EXPECT_EQ(classifyConditionalWriteResult(e), CasWriteOutcome::DefiniteFailure);
    /// The modeled-enum path (no canonical name attached) must classify identically.
    DB::S3Exception by_code("bad argument", Aws::S3::S3Errors::INVALID_REQUEST);
    EXPECT_EQ(classifyConditionalWriteResult(by_code), CasWriteOutcome::DefiniteFailure);
}

TEST(CASRequestControl, ClassifiesEntityTooLargeAsDefiniteFailure)
{
    DB::S3Exception e("body exceeds the maximum object size", Aws::S3::S3Errors::UNKNOWN, "EntityTooLarge");
    EXPECT_EQ(classifyConditionalWriteResult(e), CasWriteOutcome::DefiniteFailure);
}

TEST(CASRequestControl, ClassifiesAccessDeniedAsDefiniteFailure)
{
    DB::S3Exception e("simulated 403", Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied");
    EXPECT_EQ(classifyConditionalWriteResult(e), CasWriteOutcome::DefiniteFailure);
    /// The modeled-enum path (no canonical name attached) must classify identically.
    DB::S3Exception by_code("simulated 403, no name", Aws::S3::S3Errors::ACCESS_DENIED);
    EXPECT_EQ(classifyConditionalWriteResult(by_code), CasWriteOutcome::DefiniteFailure);
}

/// Anything the classifier does not recognize (an unmodeled/unnamed S3 error, or an entirely
/// unrelated exception type) must fail toward Unresolved — never toward a false DefiniteFailure or a
/// false Committed (RFC §resolve-before-reissuing: ambiguity always resolves toward "resolve before
/// reissuing").
TEST(CASRequestControl, UnrecognizedErrorsFailSafeToUnresolved)
{
    DB::S3Exception unknown_named("weird service error", Aws::S3::S3Errors::UNKNOWN, "SomeFutureErrorCode");
    EXPECT_EQ(classifyConditionalWriteResult(unknown_named), CasWriteOutcome::Unresolved);

    /// UNKNOWN_EXCEPTION (not LOGICAL_ERROR): any arbitrary non-S3 exception type works here -- the
    /// point is that the classifier doesn't recognize it, not which specific code it carries.
    /// LOGICAL_ERROR would abort the whole process under debug/sanitizer builds merely by being
    /// constructed (Exception's constructor calls handle_error_code unconditionally).
    DB::Exception unrelated(DB::ErrorCodes::UNKNOWN_EXCEPTION, "not an S3 error at all");
    EXPECT_EQ(classifyConditionalWriteResult(unrelated), CasWriteOutcome::Unresolved);
}

/// recordConditionalWriteAttemptStarted / recordConditionalWriteOutcome bump the per-class counters
/// (RFC §observability): attempts, and exactly one of Committed/DefiniteFailure/Unresolved per call.
TEST(CASRequestControl, CountersHookupIncrementsPerClass)
{
    using ProfileEvents::global_counters;
    const auto attempts_before = global_counters[ProfileEvents::CASConditionalWriteAttempts].load();
    const auto committed_before = global_counters[ProfileEvents::CASConditionalWriteCommitted].load();
    const auto definite_before = global_counters[ProfileEvents::CASConditionalWriteDefiniteFailure].load();
    const auto unresolved_before = global_counters[ProfileEvents::CASConditionalWriteUnresolved].load();

    recordConditionalWriteAttemptStarted();
    recordConditionalWriteOutcome(CasWriteOutcome::Committed);
    recordConditionalWriteAttemptStarted();
    recordConditionalWriteOutcome(CasWriteOutcome::DefiniteFailure);
    recordConditionalWriteAttemptStarted();
    recordConditionalWriteOutcome(CasWriteOutcome::Unresolved);

#if !WITH_COVERAGE
    EXPECT_EQ(global_counters[ProfileEvents::CASConditionalWriteAttempts].load() - attempts_before, 3u);
    EXPECT_EQ(global_counters[ProfileEvents::CASConditionalWriteCommitted].load() - committed_before, 1u);
    EXPECT_EQ(global_counters[ProfileEvents::CASConditionalWriteDefiniteFailure].load() - definite_before, 1u);
    EXPECT_EQ(global_counters[ProfileEvents::CASConditionalWriteUnresolved].load() - unresolved_before, 1u);
#else
    (void)attempts_before; (void)committed_before; (void)definite_before; (void)unresolved_before;
#endif
}

/// Wiring smoke test: a real conditional write through ObjectStorageBackend (Native mode) counts one
/// attempt and one Committed outcome via the SAME instrumented call site nativeConditionalPut uses —
/// see finalizeConditionalWriteInstrumented in CasObjectStorageBackend.cpp.
TEST(CASRequestControl, NativeConditionalPutCountsOneAttemptAndCommitted)
{
    using ProfileEvents::global_counters;
    const auto attempts_before = global_counters[ProfileEvents::CASConditionalWriteAttempts].load();
    const auto committed_before = global_counters[ProfileEvents::CASConditionalWriteCommitted].load();

    auto b = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native);
    EXPECT_EQ(b->putIfAbsent("p/rc/one", "v1").outcome, PutOutcome::Done);

#if !WITH_COVERAGE
    EXPECT_EQ(global_counters[ProfileEvents::CASConditionalWriteAttempts].load() - attempts_before, 1u);
    EXPECT_EQ(global_counters[ProfileEvents::CASConditionalWriteCommitted].load() - committed_before, 1u);
#else
    (void)attempts_before; (void)committed_before;
#endif
}

/// Mechanism property (RFC §disable-transparent-conditional-write-retries), tested at the layer
/// actually reachable from a unit-test binary: NO live/fake S3 endpoint is available here (the Native
/// conditional-write path is exercised end-to-end only at M-W against RustFS — see the HONEST NOTE in
/// CasObjectStorageBackend.cpp), so driving a real socket-level retry against a real client is not
/// reachable from this binary. What IS reachable and asserted here: every Native conditional write
/// selects the SingleAttempt object-storage retry profile, and a non-S3 backend such as
/// LocalObjectStorage reports it as UNSUPPORTED via IObjectStorage::supportsRetryProfile — the property
/// checkConditionalWriteSingleAttemptSupport's fail-closed mount-time gate relies on.
TEST(CASRequestControl, SingleAttemptProfileRequestedAndLocalBackendRejected)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native);
    const auto ws = b->conditionalWriteSettingsForTest();
    EXPECT_EQ(ws.object_storage_retry_profile, DB::ObjectStorageRetryProfile::SingleAttempt);
    /// LocalObjectStorage does not implement the profile — the capability check must say no.
    EXPECT_FALSE(DB::Cas::tests::makeLocalObjectStorageForTest()->supportsRetryProfile(DB::ObjectStorageRetryProfile::SingleAttempt));
}

/// The SECOND retry-affecting layer above the S3 client (review finding): WriteBufferFromS3's OWN
/// makeSinglepartUpload/completeMultipartUpload retry loop reissues the identical conditional request
/// on a NO_SUCH_KEY response, driven by S3RequestSetting::max_unexpected_write_error_retries (default
/// 4) — a client-level override alone does not bound it (see WriteSettings::
/// s3_max_unexpected_write_error_retries_override). Asserted at the reachable seam: no live/fake S3
/// endpoint exists in this binary to drive the retry loop itself, so this proves the settings
/// plumbing conditionalWriteSettings() -> WriteSettings produces the override value that
/// S3ObjectStorage::writeObject then applies to request_settings — NOT a real single-attempt
/// assertion against a live wire attempt.
TEST(CASRequestControl, ConditionalWriteSettingsForceSingleUnexpectedWriteErrorRetry)
{
    auto b = std::make_shared<ObjectStorageBackend>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), ObjectStorageBackend::Mode::Native);
    const auto ws = b->conditionalWriteSettingsForTest();
    EXPECT_EQ(ws.s3_max_unexpected_write_error_retries_override, 1u);
}

/// ================================================================================================
/// Task 5: CasRequestController — retry controller (deadlines, fence gating, exact-key resolution)
/// ================================================================================================

namespace
{

/// A per-call scripted Backend for CasRequestController tests: `putIfAbsent` optionally throws a
/// caller-supplied exception (models one classified HTTP-attempt outcome) or returns a forced
/// `PutOutcome` directly (models a `PreconditionFailed` observed WITHOUT an exception); with neither
/// set it delegates to the real in-memory conditional-write semantics. `get` optionally returns a
/// forced result, independent of what `putIfAbsent` actually did, so a test can drive exact-key
/// resolution (identical / different / absent) without the scripted put and the resolve GET needing to
/// agree on a shared, real backing store.
class ScriptedControllerBackend : public InMemoryBackend
{
public:
    std::function<void()> put_thrower;
    std::optional<PutOutcome> put_forced_outcome;
    std::atomic<uint64_t> put_attempts{0};

    std::function<void()> put_overwrite_thrower;
    std::optional<PutOutcome> put_overwrite_forced_outcome;
    std::atomic<uint64_t> put_overwrite_attempts{0};

    bool get_overridden = false;
    std::optional<GetResult> get_override_value;   /// meaningful only when get_overridden

    void setGetOverride(std::optional<GetResult> value)
    {
        get_overridden = true;
        get_override_value = std::move(value);
    }

    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        ++put_attempts;
        if (put_thrower)
            put_thrower();
        if (put_forced_outcome)
            return {*put_forced_outcome, {}};
        return InMemoryBackend::putIfAbsent(key, bytes, meta);
    }

    PutResult putOverwrite(const String & key, const String & bytes, const Token & expected, const ObjectMeta & meta) override
    {
        ++put_overwrite_attempts;
        if (put_overwrite_thrower)
            put_overwrite_thrower();
        if (put_overwrite_forced_outcome)
            return {*put_overwrite_forced_outcome, {}};
        return InMemoryBackend::putOverwrite(key, bytes, expected, meta);
    }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        if (get_overridden)
            return get_override_value;
        return InMemoryBackend::get(key, range);
    }
};

GetResult resultWithBytes(const String & bytes)
{
    return GetResult{.bytes = bytes, .token = Token{"t", TokenType::Emulated}, .attributes = {}};
}

}

TEST(CASRequestController, UncertainResolvesIdenticalAsCommitted)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();
    backend->put_thrower = [] { throw Poco::TimeoutException("scripted: ambiguous"); };
    backend->setGetOverride(resultWithBytes("payload"));

    CasRequestController controller(backend, CasRequestBudget{});
    const auto outcome = controller.putIfAbsentControlled("k", "payload", [] { return true; });
    EXPECT_EQ(outcome, CasWriteOutcome::Committed);
    EXPECT_EQ(backend->put_attempts.load(), 1u);
}

TEST(CASRequestController, UncertainResolvesDifferentThrowsCorruption)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();
    backend->put_thrower = [] { throw Poco::TimeoutException("scripted: ambiguous"); };
    backend->setGetOverride(resultWithBytes("someone-elses-bytes"));

    CasRequestController controller(backend, CasRequestBudget{});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        controller.putIfAbsentControlled("k", "payload", [] { return true; });
    });
}

/// GET-absent NEVER yields DefiniteFailure (spec §writer-side-linearization): the SAME (key, bytes) is
/// retried up to `max_attempts`, and only THEN does the call give up with Unresolved.
TEST(CASRequestController, UncertainResolvesAbsentRetriesSameKeyWithinBudget)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();
    backend->put_thrower = [] { throw Poco::TimeoutException("scripted: ambiguous"); };
    backend->setGetOverride(std::nullopt);   /// absent on every resolve

    CasRequestBudget budget;
    budget.max_attempts = 3;
    budget.retry_initial_backoff_ms = 0;   /// backoff behavior is pinned by its own tests below
    CasRequestController controller(backend, budget);
    const auto outcome = controller.putIfAbsentControlled("k", "payload", [] { return true; });
    EXPECT_EQ(outcome, CasWriteOutcome::Unresolved);
    EXPECT_EQ(backend->put_attempts.load(), 3u);   /// every attempt targeted the SAME key/bytes
}

/// The operation deadline — not just the attempt-count budget — cuts a retry loop short: a fake clock
/// advances by a fixed step per now_ms() call (no sleeps), and max_attempts is generous enough that only
/// the deadline check can be what stops the loop.
TEST(CASRequestController, OperationDeadlineExhaustionReturnsUnresolvedBeforeMaxAttempts)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();
    backend->put_thrower = [] { throw Poco::TimeoutException("scripted: ambiguous"); };
    backend->setGetOverride(std::nullopt);   /// absent on every resolve

    uint64_t clock = 0;
    auto now_ms = [&clock]() -> uint64_t { const uint64_t t = clock; clock += 200; return t; };

    CasRequestBudget budget;
    budget.max_attempts = 10;
    budget.attempt_timeout_ms = 50;
    budget.operation_deadline_ms = 450;
    budget.retry_initial_backoff_ms = 0;   /// isolate the deadline check from the backoff's own deadline guard
    CasRequestController controller(backend, budget, now_ms);
    const auto outcome = controller.putIfAbsentControlled("k", "payload", [] { return true; });
    EXPECT_EQ(outcome, CasWriteOutcome::Unresolved);
    EXPECT_EQ(backend->put_attempts.load(), 2u);   /// cut off well before the 10-attempt budget
}

/// WHY `attempt_timeout_ms == operation_deadline_ms` IS REJECTED AT STARTUP, demonstrated on the
/// mechanism itself before the rejection is asserted below.
///
/// The deadline is captured as `now + operation_deadline_ms` and the pre-send gate asks
/// `now + attempt_timeout_ms > deadline`. Equal values collapse that to `now_2 > now_1`: ONE elapsed
/// millisecond between the capture and the gate refuses the whole operation with NOTHING SENT. That is
/// not a bounded operation, it is a coin flip on the scheduler -- "mostly works, occasionally refuses
/// having sent nothing", which is the flakiness class validation exists to prevent. Single-attempt
/// semantics is what `max_attempts = 1` is for; the equality contributes only the race.
///
/// The controller is constructed DIRECTLY here, bypassing `validateCasRequestBudget`, because the
/// point is to show the behaviour the validator now forbids. Three tests were flaky on exactly this
/// before it was forbidden: `8f9e63c7a19`'s sweep-interruption test,
/// `CASRefInstallSafety.UncertainPrecommitKeepsItsCleanupOwnerAndItsBody`, and
/// `CASRefWriterAppendLane.WedgedLaneBlocksSameTableWhileOtherTableProceeds`.
TEST(CASRequestController, EqualAttemptTimeoutAndDeadlineWouldRefuseAfterASingleTick)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();

    /// The smallest possible passage of time: one millisecond per clock read.
    uint64_t clock = 0;
    auto now_ms = [&clock]() -> uint64_t { const uint64_t t = clock; clock += 1; return t; };

    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = 100;
    CasRequestController razor(backend, budget, now_ms);
    EXPECT_EQ(razor.putIfAbsentControlled("k", "payload", [] { return true; }), CasWriteOutcome::Unresolved);
    EXPECT_EQ(backend->put_attempts.load(), 0u)
        << "the refusal came from the clock, not from the backend: nothing was sent at all";

    /// STRICTLY LESS -- the shape the validator now requires -- sends the request over the SAME one-tick
    /// clock. So what the inequality buys is the request actually happening, not merely a bigger number.
    clock = 0;
    budget.operation_deadline_ms = 5000;
    CasRequestController wide(backend, budget, now_ms);
    EXPECT_EQ(wide.putIfAbsentControlled("k", "payload", [] { return true; }), CasWriteOutcome::Committed);
    EXPECT_EQ(backend->put_attempts.load(), 1u);
}

/// And the same equality is refused at startup, so no budget can reach the controller in that shape.
/// The boundary is asserted from BOTH sides: equality throws, one millisecond more is accepted.
TEST(CASRequestController, ValidateBudgetRejectsAttemptTimeoutEqualToOperationDeadline)
{
    CasRequestBudget budget;
    budget.attempt_timeout_ms = 5000;
    budget.operation_deadline_ms = 5000;
    budget.lease_safety_margin_ms = 1000;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&]
    {
        validateCasRequestBudget(budget, /*mount_lease_ttl_ms=*/30000, /*mount_renew_period_ms=*/10000);
    });

    budget.operation_deadline_ms = 5001;
    EXPECT_NO_THROW(validateCasRequestBudget(budget, /*mount_lease_ttl_ms=*/30000, /*mount_renew_period_ms=*/10000))
        << "one millisecond of headroom is the whole requirement -- the rule is strictness, not size";
}

TEST(CASRequestController, OverwriteAmbiguousResolvesIntendedBytesAsCommitted)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();
    bool first_attempt = true;
    backend->put_overwrite_thrower = [&first_attempt]
    {
        if (first_attempt)
        {
            first_attempt = false;
            throw Poco::TimeoutException("scripted: ambiguous");
        }
    };
    backend->setGetOverride(resultWithBytes("new-payload"));

    CasRequestController controller(backend, CasRequestBudget{});
    const auto result = controller.putOverwriteControlled(
        "k", "new-payload", Token{"old", TokenType::Emulated}, [] { return true; });
    EXPECT_EQ(result.outcome, CasOverwriteOutcome::Committed);
    EXPECT_EQ(backend->put_overwrite_attempts.load(), 1u);
    EXPECT_EQ(result.token, (Token{"t", TokenType::Emulated}));
}

TEST(CASRequestController, OverwriteAmbiguousResolvesExpectedTokenAndRetriesWithinBudget)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();
    backend->put_overwrite_thrower = [] { throw Poco::TimeoutException("scripted: ambiguous"); };
    const Token expected{"old", TokenType::Emulated};
    backend->setGetOverride(GetResult{.bytes = "old-payload", .token = expected, .attributes = {}});

    CasRequestBudget budget;
    budget.max_attempts = 3;
    budget.retry_initial_backoff_ms = 0;
    CasRequestController controller(backend, budget);
    const auto result = controller.putOverwriteControlled("k", "new-payload", expected, [] { return true; });
    EXPECT_EQ(result.outcome, CasOverwriteOutcome::Unresolved);
    EXPECT_EQ(backend->put_overwrite_attempts.load(), 3u);
}

TEST(CASRequestController, OverwriteAmbiguousResolvesDifferentTokenAndBytesAsConflict)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();
    bool first_attempt = true;
    backend->put_overwrite_thrower = [&first_attempt]
    {
        if (first_attempt)
        {
            first_attempt = false;
            throw Poco::TimeoutException("scripted: ambiguous");
        }
    };
    backend->setGetOverride(GetResult{
        .bytes = "someone-elses-payload", .token = Token{"other", TokenType::Emulated}, .attributes = {}});

    CasRequestController controller(backend, CasRequestBudget{});
    const auto result = controller.putOverwriteControlled(
        "k", "new-payload", Token{"old", TokenType::Emulated}, [] { return true; });
    EXPECT_EQ(result.outcome, CasOverwriteOutcome::Conflict);
    EXPECT_EQ(backend->put_overwrite_attempts.load(), 1u);
}

TEST(CASRequestController, OverwriteOperationDeadlineExhaustionReturnsUnresolvedBeforeMaxAttempts)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();
    backend->put_overwrite_thrower = [] { throw Poco::TimeoutException("scripted: ambiguous"); };
    const Token expected{"old", TokenType::Emulated};
    backend->setGetOverride(GetResult{.bytes = "old-payload", .token = expected, .attributes = {}});

    uint64_t clock = 0;
    auto now_ms = [&clock]() -> uint64_t { const uint64_t t = clock; clock += 200; return t; };

    CasRequestBudget budget;
    budget.max_attempts = 10;
    budget.attempt_timeout_ms = 50;
    budget.operation_deadline_ms = 450;
    budget.retry_initial_backoff_ms = 0;
    CasRequestController controller(backend, budget, now_ms);
    const auto result = controller.putOverwriteControlled("k", "new-payload", expected, [] { return true; });
    EXPECT_EQ(result.outcome, CasOverwriteOutcome::Unresolved);
    EXPECT_EQ(backend->put_overwrite_attempts.load(), 2u);
}

TEST(CASRequestController, FenceLostBeforeAttemptSendsNoAttempt)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();
    CasRequestController controller(backend, CasRequestBudget{});
    const auto outcome = controller.putIfAbsentControlled("k", "payload", [] { return false; });
    EXPECT_EQ(outcome, CasWriteOutcome::Unresolved);
    EXPECT_EQ(backend->put_attempts.load(), 0u);
}

/// The write itself may have landed, but a fence lost between the write and this call's own final
/// check must never surface as Committed (RFC §ack-and-cache-rules: no ACK, no cache update on that
/// path) — the caller sees Unresolved and must not treat the operation as acknowledged.
TEST(CASRequestController, FenceLostAfterWriteNeverReturnsCommitted)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();   /// real in-memory commit path
    int fence_calls = 0;
    auto fence_ok = [&fence_calls] { return fence_calls++ == 0; };   /// true once, then false

    CasRequestController controller(backend, CasRequestBudget{});
    const auto outcome = controller.putIfAbsentControlled("k", "payload", fence_ok);
    EXPECT_EQ(outcome, CasWriteOutcome::Unresolved);
    EXPECT_EQ(backend->put_attempts.load(), 1u);   /// the write itself DID happen
    EXPECT_TRUE(backend->head("k").exists);        /// ...it is durable; never claimed as Committed here
}

TEST(CASRequestController, DefiniteFailurePropagatesImmediatelyWithoutResolve)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();
    backend->put_thrower = [] { throw DB::S3Exception("scripted: malformed", Aws::S3::S3Errors::UNKNOWN, "MalformedXML"); };

    CasRequestController controller(backend, CasRequestBudget{});
    const auto outcome = controller.putIfAbsentControlled("k", "payload", [] { return true; });
    EXPECT_EQ(outcome, CasWriteOutcome::DefiniteFailure);
    EXPECT_EQ(backend->put_attempts.load(), 1u);   /// no retry, no resolve GET issued
}

/// ================================================================================================
/// Inter-attempt backoff (chaos-tolerance-report §Task B follow-up / stagefix-review M3): the
/// controller paces reissues with a capped-exponential, fence-gated, deadline-aware sleep instead of
/// hammering a recovering store with immediate retries.
/// ================================================================================================

/// The full event-ordered schedule: fence checked before EVERY attempt AND before EVERY sleep, sleeps
/// strictly between attempts, capped exponential (initial 100ms, cap 200ms), no sleep after the final
/// attempt. The exact interleaving is the contract — a sleep served before its fence check would keep
/// a fenced writer dozing past its lease.
TEST(CASRequestControllerBackoff, CappedExponentialSleepsAreFenceCheckedAndOrdered)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();
    std::vector<String> events;
    backend->put_thrower = [&] { events.emplace_back("put"); throw Poco::TimeoutException("scripted: ambiguous"); };
    backend->setGetOverride(std::nullopt);   /// absent on every resolve

    CasRequestBudget budget;
    budget.max_attempts = 5;
    budget.attempt_timeout_ms = 1;
    budget.operation_deadline_ms = 1000000;   /// never the binding constraint here
    budget.retry_initial_backoff_ms = 100;
    budget.retry_max_backoff_ms = 200;
    CasRequestController controller(
        backend, budget,
        /*now_ms=*/[] { return static_cast<uint64_t>(0); },
        /*sleep_ms=*/[&](uint64_t ms) { events.push_back("sleep:" + std::to_string(ms)); });

    const auto fence_ok = [&] { events.emplace_back("fence"); return true; };
    const auto outcome = controller.putIfAbsentControlled("k", "payload", fence_ok);
    EXPECT_EQ(outcome, CasWriteOutcome::Unresolved);
    EXPECT_EQ(backend->put_attempts.load(), 5u);

    const std::vector<String> expected{
        "fence", "put", "fence", "sleep:100",
        "fence", "put", "fence", "sleep:200",
        "fence", "put", "fence", "sleep:200",
        "fence", "put", "fence", "sleep:200",
        "fence", "put"};   /// budget spent: no fence-for-sleep, no sleep after the last attempt
    EXPECT_EQ(events, expected);
}

/// A fence lost between an ambiguous attempt's resolve and its backoff sleep aborts INSTANTLY: no
/// sleep is served, no further attempt is sent, and the outcome is Unresolved (never a false
/// Committed, never a retry under a lost lease).
TEST(CASRequestControllerBackoff, FenceLostBeforeSleepAbortsWithoutSleeping)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();
    backend->put_thrower = [] { throw Poco::TimeoutException("scripted: ambiguous"); };
    backend->setGetOverride(std::nullopt);

    CasRequestBudget budget;
    budget.max_attempts = 5;
    budget.retry_initial_backoff_ms = 100;
    budget.retry_max_backoff_ms = 200;
    uint64_t sleeps = 0;
    int fence_calls = 0;
    CasRequestController controller(
        backend, budget, /*now_ms=*/[] { return static_cast<uint64_t>(0); },
        /*sleep_ms=*/[&](uint64_t) { ++sleeps; });

    /// True for the pre-attempt check (call 1), lost by the pre-sleep check (call 2).
    const auto fence_ok = [&fence_calls] { return ++fence_calls <= 1; };
    const auto outcome = controller.putIfAbsentControlled("k", "payload", fence_ok);
    EXPECT_EQ(outcome, CasWriteOutcome::Unresolved);
    EXPECT_EQ(backend->put_attempts.load(), 1u) << "no attempt may be sent after the fence is lost";
    EXPECT_EQ(sleeps, 0u) << "a fence lost mid-backoff must abort BEFORE the sleep, not after it";
    EXPECT_EQ(fence_calls, 2);
}

/// A backoff sleep the operation deadline cannot afford is never served: when sleep + one more
/// attempt would cross the deadline, the loop gives up immediately (Unresolved) instead of sleeping
/// into a guaranteed exhaustion.
TEST(CASRequestControllerBackoff, SleepThatWouldCrossOperationDeadlineIsSkipped)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();
    backend->put_thrower = [] { throw Poco::TimeoutException("scripted: ambiguous"); };
    backend->setGetOverride(std::nullopt);

    uint64_t clock = 0;
    CasRequestBudget budget;
    budget.max_attempts = 10;
    budget.attempt_timeout_ms = 10;
    budget.operation_deadline_ms = 100;
    budget.retry_initial_backoff_ms = 1000;   /// any sleep would blow the 100ms deadline
    budget.retry_max_backoff_ms = 1000;
    uint64_t sleeps = 0;
    CasRequestController controller(
        backend, budget, /*now_ms=*/[&clock] { return clock; },
        /*sleep_ms=*/[&](uint64_t) { ++sleeps; });

    const auto outcome = controller.putIfAbsentControlled("k", "payload", [] { return true; });
    EXPECT_EQ(outcome, CasWriteOutcome::Unresolved);
    EXPECT_EQ(backend->put_attempts.load(), 1u);
    EXPECT_EQ(sleeps, 0u) << "the deadline guard must refuse the sleep, not serve it and then fail";
}

/// THE ENVELOPE CONTRACT (chaos-tolerance-report §Task B follow-up): the DEFAULT budget rides a
/// simulated 60-second S3 outage — every conditional-write attempt fails (≈3s adaptive first-attempt
/// timeout each, the observed incident shape) until the store recovers at t=60s, then the next
/// attempt commits, all inside the default 90s operation deadline and 16-attempt budget. The fake
/// clock advances 3s per failed attempt and by each backoff sleep, so this test pins the arithmetic
/// documented on CasRequestBudget without any wall-clock waiting.
TEST(CASRequestControllerBackoff, DefaultBudgetRidesSixtySecondOutage)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();
    uint64_t clock = 0;
    backend->put_thrower = [&clock]
    {
        if (clock < 60000)
        {
            clock += 3000;   /// the failed attempt's own ~3s adaptive receive timeout
            throw Poco::TimeoutException("scripted: store paused");
        }
        /// store recovered: fall through to the real in-memory conditional write (Done)
    };
    backend->setGetOverride(std::nullopt);   /// nothing ever landed while the store was paused

    CasRequestController controller(
        backend, CasRequestBudget{}, /*now_ms=*/[&clock] { return clock; },
        /*sleep_ms=*/[&clock](uint64_t ms) { clock += ms; });

    const auto outcome = controller.putIfAbsentControlled("k", "payload", [] { return true; });
    EXPECT_EQ(outcome, CasWriteOutcome::Committed) << "the default budget must absorb a 60s outage";
    /// Schedule: attempts fail at 3s each with sleeps 0.2,0.4,0.8,1.6,3.2 then 5s (cap); the first
    /// attempt scheduled at clock >= 60000 (attempt 11, t=61.2s) commits — well inside 16 attempts
    /// and the 90s deadline.
    EXPECT_EQ(backend->put_attempts.load(), 11u);
    EXPECT_LT(clock, CasRequestBudget{}.operation_deadline_ms);
}

/// Availfix review M1: deterministic CALLER/local bugs — `LOGICAL_ERROR` (a broken source),
/// `NOT_IMPLEMENTED` (a mode/capability guard, e.g. promoteStaged on a backend without a native
/// conditional copy), `BAD_ARGUMENTS` (a deterministic encode rejection escaping buildHeader's second
/// encode), `CORRUPTED_DATA` (integrity) — propagate INSTANTLY from the create retry loop: exactly one
/// attempt, no occupancy resolve, no backoff sleep. Retrying a deterministic failure only replays it
/// (~12 minutes at the default budget through putBlob's outer loop) and buries the root cause behind a
/// retryable ABORTED — the exact class the `PutBlobWrongSizeFailsClosed` sweep regression exposed.
TEST(CASRequestControllerCreate, DeterministicLocalFailuresPropagateInstantly)
{
    /// LOGICAL_ERROR aborts the whole process in debug/sanitizer builds instead of behaving like a
    /// catchable exception, so it's excluded from this loop there --
    /// CASRequestControllerCreateDeathTest below proves the same instant-propagate contract for it
    /// positively via EXPECT_DEATH instead.
#ifdef DEBUG_OR_SANITIZER_BUILD
    const std::vector<int> codes = {DB::ErrorCodes::NOT_IMPLEMENTED, DB::ErrorCodes::BAD_ARGUMENTS, DB::ErrorCodes::CORRUPTED_DATA};
#else
    const std::vector<int> codes = {DB::ErrorCodes::LOGICAL_ERROR, DB::ErrorCodes::NOT_IMPLEMENTED,
                                     DB::ErrorCodes::BAD_ARGUMENTS, DB::ErrorCodes::CORRUPTED_DATA};
#endif
    for (const int code : codes)
    {
        SCOPED_TRACE("error code " + std::to_string(code));
        auto backend = std::make_shared<ScriptedControllerBackend>();
        uint64_t sleeps = 0;
        int attempts = 0;
        CasRequestController controller(
            backend, CasRequestBudget{}, /*now_ms=*/[] { return static_cast<uint64_t>(0); },
            /*sleep_ms=*/[&](uint64_t) { ++sleeps; });

        bool threw = false;
        try
        {
            controller.conditionalCreateControlled("k",
                [&]() -> PutResult
                {
                    ++attempts;
                    throw DB::Exception(code, "scripted deterministic local failure");
                },
                [] { return true; });
        }
        catch (const DB::Exception & e)
        {
            threw = true;
            EXPECT_EQ(e.code(), code) << "the ORIGINAL exception must propagate, not a mapped outcome";
        }
        EXPECT_TRUE(threw) << "a deterministic local failure must propagate, never return an outcome";
        EXPECT_EQ(attempts, 1) << "no reissue: retrying a deterministic failure only replays it";
        EXPECT_EQ(sleeps, 0u) << "no backoff sleep may be served for a deterministic failure";
    }
}

#if defined(DEBUG_OR_SANITIZER_BUILD)
/// Debug/sanitizer-build counterpart to DeterministicLocalFailuresPropagateInstantly's LOGICAL_ERROR
/// case, excluded from that loop above: LOGICAL_ERROR aborts the process here instead of throwing a
/// catchable exception, so the check must be a death test (same pattern as CASBlobDigestDeathTest in
/// gtest_cas_blob_digest.cpp).
TEST(CASRequestControllerCreateDeathTest, LogicalErrorPropagatesInstantlyAborts)
{
    auto backend = std::make_shared<ScriptedControllerBackend>();
    CasRequestController controller(
        backend, CasRequestBudget{}, /*now_ms=*/[] { return static_cast<uint64_t>(0); },
        /*sleep_ms=*/[](uint64_t) {});
    EXPECT_DEATH(
        {
            controller.conditionalCreateControlled("k",
                [&]() -> PutResult { throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "scripted deterministic local failure"); },
                [] { return true; });
        }, "");
}
#endif

/// Startup validation (RFC §required-timeout-model): a consistent default budget is accepted silently;
/// either inequality violated on its own is rejected with BAD_ARGUMENTS.
TEST(CASRequestController, ValidateBudgetAcceptsConsistentDefaults)
{
    EXPECT_NO_THROW(validateCasRequestBudget(CasRequestBudget{}, /*mount_lease_ttl_ms=*/30000, /*mount_renew_period_ms=*/10000));
}

TEST(CASRequestController, ValidateBudgetRejectsAttemptTimeoutPlusMarginAtOrAboveLeaseTtl)
{
    CasRequestBudget budget;
    budget.attempt_timeout_ms = 25000;
    budget.lease_safety_margin_ms = 5000;   /// sums to EXACTLY the lease TTL below — not strictly less
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&]
    {
        validateCasRequestBudget(budget, /*mount_lease_ttl_ms=*/30000, /*mount_renew_period_ms=*/10000);
    });
}

TEST(CASRequestController, ValidateBudgetRejectsAttemptTimeoutAboveOperationDeadline)
{
    CasRequestBudget budget;
    budget.attempt_timeout_ms = 6000;
    budget.operation_deadline_ms = 5000;
    budget.lease_safety_margin_ms = 1000;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&]
    {
        validateCasRequestBudget(budget, /*mount_lease_ttl_ms=*/30000, /*mount_renew_period_ms=*/10000);
    });
}

/// max_attempts == 0 would let putIfAbsentControlled return Unresolved without ever sending an
/// attempt — reject at startup rather than silently accepting a no-op budget.
TEST(CASRequestController, ValidateBudgetRejectsZeroMaxAttempts)
{
    CasRequestBudget budget;
    budget.max_attempts = 0;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&]
    {
        validateCasRequestBudget(budget, /*mount_lease_ttl_ms=*/30000, /*mount_renew_period_ms=*/10000);
    });
}

/// A capped-exponential backoff whose cap sits below its own starting value is inconsistent — reject
/// at startup (0/0 disables backoff and stays accepted, covered by the defaults test above since the
/// defaults are nonzero and consistent).
TEST(CASRequestController, ValidateBudgetRejectsInitialBackoffAboveMaxBackoff)
{
    CasRequestBudget budget;
    budget.retry_initial_backoff_ms = 500;
    budget.retry_max_backoff_ms = 100;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&]
    {
        validateCasRequestBudget(budget, /*mount_lease_ttl_ms=*/30000, /*mount_renew_period_ms=*/10000);
    });
}

/// attempt_timeout_ms + lease_safety_margin_ms must not be computed by a wrapping uint64 sum: absurd
/// config values near UINT64_MAX must fail validation (correctly, as inconsistent), never wrap around
/// to a spuriously small sum that would pass the "< lease TTL" check.
TEST(CASRequestController, ValidateBudgetRejectsOverflowingSumRatherThanWrapping)
{
    CasRequestBudget budget;
    budget.attempt_timeout_ms = std::numeric_limits<uint64_t>::max() - 10;
    budget.lease_safety_margin_ms = 20;   /// sum would wrap past UINT64_MAX to a tiny value
    budget.operation_deadline_ms = std::numeric_limits<uint64_t>::max();
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&]
    {
        validateCasRequestBudget(budget, /*mount_lease_ttl_ms=*/30000, /*mount_renew_period_ms=*/10000);
    });
}

#endif
