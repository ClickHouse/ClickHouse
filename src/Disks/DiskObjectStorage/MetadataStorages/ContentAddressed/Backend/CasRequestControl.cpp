#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestControl.h>

#include <Common/Exception.h>
#include <Common/LoggingHelpers.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include "config.h"

#if USE_AWS_S3
#include <IO/S3Common.h>
#endif

#include <algorithm>
#include <chrono>
#include <thread>
#include <utility>

namespace ProfileEvents
{
    extern const Event CASConditionalWriteAttempts;
    extern const Event CASConditionalWriteCommitted;
    extern const Event CASConditionalWriteDefiniteFailure;
    extern const Event CASConditionalWriteUnresolved;
    extern const Event CASConditionalWriteFenceLostPostWrite;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::Cas
{

CasWriteOutcome classifyConditionalWriteResult([[maybe_unused]] const std::exception & e)
{
#if USE_AWS_S3
    /// `PreconditionFailed`/`NoSuchKey` (a lost If-None-Match/If-Match — see
    /// ObjectStorageBackend::finalizeConditionalWrite for the exact matching), any 5xx
    /// (InternalError/ServiceUnavailable/SlowDown/RequestTimeout), and any S3 error this function does
    /// not recognize all fall through to the fail-safe default below: Unresolved. Only the WHITELIST
    /// below proves the request was never applied.
    if (const auto * s3e = dynamic_cast<const S3Exception *>(&e))
    {
        if (S3::isMalformedRequestError(*s3e) || S3::isEntityTooLargeError(*s3e) || S3::isAccessDeniedError(*s3e))
            return CasWriteOutcome::DefiniteFailure;
    }
#endif
    /// Poco::Net::NetException (connection loss) / Poco::TimeoutException (client-side timeout) and
    /// every other error type: the request's fate is unproven — fail toward "resolve before
    /// reissuing, never toward a false
    /// DefiniteFailure.
    return CasWriteOutcome::Unresolved;
}

void recordConditionalWriteAttemptStarted()
{
    ProfileEvents::increment(ProfileEvents::CASConditionalWriteAttempts);
}

void recordConditionalWriteOutcome(CasWriteOutcome outcome)
{
    switch (outcome)
    {
        case CasWriteOutcome::Committed:
            ProfileEvents::increment(ProfileEvents::CASConditionalWriteCommitted);
            return;
        case CasWriteOutcome::DefiniteFailure:
            ProfileEvents::increment(ProfileEvents::CASConditionalWriteDefiniteFailure);
            return;
        case CasWriteOutcome::Unresolved:
            ProfileEvents::increment(ProfileEvents::CASConditionalWriteUnresolved);
            return;
    }
}

namespace
{

uint64_t steadyClockNowMs()
{
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
}

/// The default inter-attempt backoff sleep. NOT a race-fix sleep: it is deliberate, bounded,
/// fence-gated pacing of reissues toward a recovering object store, and it is injectable so tests
/// never wait on it.
void threadSleepMs(uint64_t ms)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

/// Deterministic caller/local bugs the create retry loop must surface immediately:
/// reissuing only replays the same failure — up to ~12 minutes of budget × putBlob's outer loop at
/// the defaults — and buries the root cause behind a retryable ABORTED. The set:
///   LOGICAL_ERROR   — a local invariant violation (e.g. uploadFromSource's source-size check; pinned
///                     by `CasPartWriteTxn.PutBlobWrongSizeFailsClosed`, which caught exactly this class)
///   NOT_IMPLEMENTED — a mode/capability guard (e.g. `promoteStaged` on a backend without a native
///                     conditional server-side copy) — a deterministic configuration bug
///   BAD_ARGUMENTS   — a deterministic encode/argument rejection (e.g. BAD_ARGUMENTS escaping
///                     buildHeader's second, intended_ref-less encode)
///   CORRUPTED_DATA  — integrity failure; retrying re-reads/re-streams the same bad bytes (the same
///                     fail-fast rule the driver-side correctness markers enforce)
/// Fail-safe either way: a propagated exception is never a false Committed.
bool isDeterministicLocalFailure(int code)
{
    return code == ErrorCodes::LOGICAL_ERROR || code == ErrorCodes::NOT_IMPLEMENTED
        || code == ErrorCodes::BAD_ARGUMENTS || code == ErrorCodes::CORRUPTED_DATA;
}

}

void validateCasRequestBudget(const CasRequestBudget & budget, uint64_t mount_lease_ttl_ms, uint64_t mount_renew_period_ms)
{
    if (budget.max_attempts < 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "CAS request budget rejected: max_attempts must be at least 1 (got {}) — zero would let "
            "putIfAbsentControlled return Unresolved without ever sending an attempt.",
            budget.max_attempts);

    /// Overflow-safe: `attempt_timeout_ms + lease_safety_margin_ms` could wrap uint64 for absurd config
    /// values, which would make the sum spuriously small and the inequality below pass when it should
    /// fail closed. Compare via subtraction against the (unsigned, so already non-negative) TTL instead
    /// of computing the sum directly.
    if (!(budget.attempt_timeout_ms < mount_lease_ttl_ms
          && budget.lease_safety_margin_ms < mount_lease_ttl_ms - budget.attempt_timeout_ms))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "CAS request budget rejected: attempt_timeout_ms ({}) + lease_safety_margin_ms ({}) must be "
            "strictly less than the mount lease TTL ({} ms). A writable mount refuses to open with "
            "this budget.",
            budget.attempt_timeout_ms, budget.lease_safety_margin_ms, mount_lease_ttl_ms);
    /// STRICTLY less, and the strictness is the load-bearing half. `attempt_timeout_ms >
    /// operation_deadline_ms` is the obvious error — a single attempt cannot outlast the logical
    /// operation it belongs to. EQUALITY is the subtle one, and it is worse than useless: the deadline
    /// is captured as `now + operation_deadline_ms` and every pre-send gate below asks
    /// `now + attempt_timeout_ms > deadline_ms`, so equal values collapse that to `now_2 > now_1` and
    /// ONE elapsed millisecond between the two clock reads refuses the operation having sent NOTHING.
    /// The resulting behaviour is "mostly works, occasionally refuses with nothing sent", decided by
    /// the scheduler rather than by the budget — exactly the flakiness this validation exists to catch,
    /// and observed three times in tests before it was forbidden. A caller that wants one attempt says
    /// `max_attempts = 1`; the equality adds only the race.
    if (!(budget.attempt_timeout_ms < budget.operation_deadline_ms))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "CAS request budget rejected: attempt_timeout_ms ({}) must be strictly less than "
            "operation_deadline_ms ({}) — equality turns the pre-send gate into a wall-clock race that "
            "refuses after a single elapsed tick, having sent nothing. Use max_attempts to bound the "
            "number of attempts.",
            budget.attempt_timeout_ms, budget.operation_deadline_ms);
    if (!(budget.retry_initial_backoff_ms <= budget.retry_max_backoff_ms))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "CAS request budget rejected: retry_initial_backoff_ms ({}) must not exceed "
            "retry_max_backoff_ms ({}) — the capped-exponential backoff cap cannot sit below its own "
            "starting value. Set both to 0 to disable inter-attempt backoff.",
            budget.retry_initial_backoff_ms, budget.retry_max_backoff_ms);

    LOG_INFO(getLogger("CasRequestControl"),
        "CAS request budget in effect: attempt_timeout_ms={} operation_deadline_ms={} max_attempts={} "
        "lease_safety_margin_ms={} retry_initial_backoff_ms={} retry_max_backoff_ms={} "
        "(mount_lease_ttl_ms={} mount_renew_period_ms={})",
        budget.attempt_timeout_ms, budget.operation_deadline_ms, budget.max_attempts,
        budget.lease_safety_margin_ms, budget.retry_initial_backoff_ms, budget.retry_max_backoff_ms,
        mount_lease_ttl_ms, mount_renew_period_ms);
}

namespace
{
/// Shared by both public entry points below so the log line and the exception's message text can
/// never drift apart. Rate-limited (not per-distinct-`why` -- `LogSeriesLimiter` keys on the LOGGER
/// NAME only, so under a sustained outage where `why` keeps changing slightly, only the first message
/// in each window prints; this is the intended throttle, not a bug). Warning-level visibility is
/// intentional: this condition is expected to self-heal
/// (the caller retries), but an operator watching CAS logs directly should see it without having to
/// know to look at system.replication_queue.
void logCasWriteRetryLater(const String & why)
{
    LogSeriesLimiter log(getLogger("CasWriteRetryLater"), /*allowed_count=*/1, /*interval_s=*/30);
    LOG_WARNING(log, "CAS write could not be committed ({}); retrying later", why);
}
}

[[noreturn]] void throwCasWriteRetryLater(const String & why)
{
    logCasWriteRetryLater(why);
    throw Exception(ErrorCodes::NETWORK_ERROR, "CAS write could not be committed ({}); retrying later", why);
}

std::exception_ptr makeCasWriteRetryLaterExceptionPtr(const String & why)
{
    logCasWriteRetryLater(why);
    return std::make_exception_ptr(
        Exception(ErrorCodes::NETWORK_ERROR, "CAS write could not be committed ({}); retrying later", why));
}

[[noreturn]] void throwCasTransientUnavailable(const String & subject, const String & condition)
{
    /// The code is coarse (it shares a `system.errors` row with socket failures), so the MESSAGE must
    /// carry the whole truth: which CA condition refused, and that the refusal is a state rather than
    /// damage. Consumers key on the code; operators read this line.
    ///
    /// The shared suffix carries ONLY the classification, because that is the one claim true at every
    /// site: retry-later is right even where the condition may turn out terminal, since the next attempt
    /// re-decides against fresh state. Any promise about HOW the condition clears belongs in `condition`,
    /// where the site that can actually prove it makes it -- `checkFenceOrThrow` provably cannot.
    throw Exception(ErrorCodes::NETWORK_ERROR,
        "{} -- {}; TRANSIENT unavailability, not damage", subject, condition);
}

CasRequestController::CasRequestController(BackendPtr backend_, CasRequestBudget budget_, std::function<uint64_t()> now_ms_,
                                           std::function<void(uint64_t)> sleep_ms_)
    : backend(std::move(backend_))
    , budget(budget_)
    , now_ms(now_ms_ ? std::move(now_ms_) : std::function<uint64_t()>(steadyClockNowMs))
    , sleep_ms(sleep_ms_ ? std::move(sleep_ms_) : std::function<void(uint64_t)>(threadSleepMs))
{
}

void CasRequestController::setSleepFnForTest(std::function<void(uint64_t)> sleep_ms_)
{
    sleep_ms = sleep_ms_ ? std::move(sleep_ms_) : std::function<void(uint64_t)>(threadSleepMs);
}

uint64_t CasRequestController::backoffBeforeAttempt(uint32_t next_attempt) const
{
    const uint64_t initial = budget.retry_initial_backoff_ms;
    const uint64_t cap = budget.retry_max_backoff_ms;
    if (initial == 0 || next_attempt < 2)
        return 0;
    /// Saturating `initial << doublings`: `initial > cap >> doublings` implies the unshifted product
    /// already exceeds the cap, so return the cap without ever computing an overflowing shift.
    const uint32_t doublings = next_attempt - 2;
    if (doublings >= 63 || initial > (cap >> doublings))
        return cap;
    return std::min(initial << doublings, cap);
}

bool CasRequestController::pauseBeforeReissue(uint32_t completed_attempt, uint64_t deadline_ms,
                                              const std::function<bool()> & fence_ok, CasUnresolvedReason * out_reason)
{
    /// Fence BEFORE the sleep (the pre-attempt fence rule applies to the whole loop, not just the
    /// attempt): a fence lost mid-backoff aborts the operation instantly — sleeping first would keep a
    /// fenced writer alive for up to a full backoff cap after it lost its right to write.
    if (!fence_ok())
    {
        if (out_reason)
            *out_reason = CasUnresolvedReason::FenceLostMidWay;
        return false;
    }
    const uint64_t backoff = backoffBeforeAttempt(completed_attempt + 1);
    if (backoff == 0)
        return true;
    /// Never serve a sleep the operation cannot afford: if the backoff plus one more attempt would
    /// cross the operation deadline, give up NOW instead of sleeping into a guaranteed Unresolved.
    if (now_ms() + backoff + budget.attempt_timeout_ms > deadline_ms)
    {
        if (out_reason)
            *out_reason = CasUnresolvedReason::DeadlineMidWay;
        return false;
    }
    sleep_ms(backoff);
    return true;
}

CasWriteOutcome CasRequestController::resolveByExactGet(std::string_view key, std::string_view expected_bytes,
                                                        Token * out_token)
{
    const String key_s{key};
    std::optional<GetResult> got;
    try
    {
        got = backend->get(key_s);
    }
    catch (const std::exception &)
    {
        /// The GET itself failed (network, auth, ...): the object's identity cannot be proven either
        /// way — an unresolved read leaves this Unresolved, exactly like an absent read.
        return CasWriteOutcome::Unresolved;
    }

    if (!got)
        return CasWriteOutcome::Unresolved;   /// absent -> another attempt may still be legal

    if (got->bytes == expected_bytes)
    {
        if (out_token)
            *out_token = got->token;
        return CasWriteOutcome::Committed;    /// identical deterministic bytes -> the earlier attempt DID commit
    }

    /// A DIFFERENT valid object at the exact key this create intended: a real conflict, not a retryable
    /// ambiguity. Fail closed rather than silently treating it as Unresolved/DefiniteFailure.
    throw Exception(ErrorCodes::CORRUPTED_DATA,
        "CasRequestController: exact-key resolution at '{}' observed a DIFFERENT object than the one "
        "this attempt intended to create — a real conflict, not a retryable ambiguity", key_s);
}

CasWriteOutcome CasRequestController::putIfAbsentControlled(
    std::string_view key, std::string_view bytes, const std::function<bool()> & fence_ok, Token * out_token,
    CasUnresolvedReason * out_reason)
{
    const String key_s{key};
    const String bytes_s{bytes};
    const uint64_t deadline_ms = now_ms() + budget.operation_deadline_ms;
    /// Diagnostic bookkeeping only -- nothing below branches on it (finding #37 defect 3).
    uint32_t attempts_sent = 0;
    /// Does an EARLIER attempt of THIS call remain unresolved -- sent, and neither proven applied nor
    /// proven refused? Set on the one path that produces exactly that state: an attempt whose outcome
    /// was ambiguous and whose exact-key resolve came back absent or unreadable. The request may have
    /// been received; an absent read now proves nothing about what materializes later, which is the
    /// whole reason the reissue loop exists. This is NOT diagnostic: it decides the CALL's verdict at
    /// the DefiniteFailure arm below. A pre-attempt gate refusal never sets it -- those return without
    /// sending, so they leave nothing that could land.
    bool earlier_attempt_unresolved = false;
    const auto unresolved = [&](CasUnresolvedReason reason)
    {
        if (out_reason)
            *out_reason = reason;
        return CasWriteOutcome::Unresolved;
    };
    if (out_reason)
        *out_reason = CasUnresolvedReason::NotUnresolved;

    for (uint32_t attempt = 1; attempt <= budget.max_attempts; ++attempt)
    {
        /// Gate BEFORE every attempt: the
        /// local mount fence must still hold, and there must be enough of the operation's own deadline
        /// left for one more attempt to plausibly complete. Neither check sends anything to the backend.
        if (!fence_ok())
            return unresolved(attempts_sent == 0 ? CasUnresolvedReason::NoAttemptSent
                                                 : CasUnresolvedReason::FenceLostMidWay);
        if (now_ms() + budget.attempt_timeout_ms > deadline_ms)
            return unresolved(attempts_sent == 0 ? CasUnresolvedReason::NoAttemptSent
                                                 : CasUnresolvedReason::DeadlineMidWay);
        ++attempts_sent;

        /// The committed incarnation's token, filled by whichever leg proves Committed below.
        Token committed_token;
        CasWriteOutcome attempt_outcome{};
        try
        {
            const PutResult put = backend->putIfAbsent(key_s, bytes_s);
            /// PreconditionFailed here means only "the key already exists" — it does NOT prove who
            /// created it (possibly OUR earlier unresolved attempt). Collapse it onto Unresolved so it
            /// goes through the SAME resolve-before-reissue path as an ambiguous exception, never a
            /// false DefiniteFailure/Committed.
            attempt_outcome = put.outcome == PutOutcome::Done ? CasWriteOutcome::Committed : CasWriteOutcome::Unresolved;
            if (put.outcome == PutOutcome::Done)
                committed_token = put.token;
        }
        catch (const std::exception & e)
        {
            attempt_outcome = classifyConditionalWriteResult(e);
        }

        if (attempt_outcome == CasWriteOutcome::DefiniteFailure)
        {
            /// THIS attempt is proven never applied — but the verdict belongs to the CALL. An earlier
            /// attempt that is still unresolved may yet materialize at the key, and a caller reading
            /// `DefiniteFailure` acts on "the key is unwritten": `CasRefLedger::commitRefChunk` clears
            /// its apply-pending marker and reports the txn id never used, so the next append re-derives
            /// that id and a late-landing predecessor becomes an acked-then-lost transaction. Ambiguity
            /// dominates a definite refusal that came after it; the caller wedges and resolves the key
            /// instead. No resolve and no retry either way — this attempt has nothing left to settle.
            if (earlier_attempt_unresolved)
                return unresolved(CasUnresolvedReason::DefiniteFailureAfterAmbiguity);
            return CasWriteOutcome::DefiniteFailure;   /// every attempt of this call was proven never applied
        }

        if (attempt_outcome == CasWriteOutcome::Unresolved)
        {
            /// Resolve-before-reissue. May throw CORRUPTED_DATA (a real
            /// conflict) straight out of this call — that is never a retry signal.
            attempt_outcome = resolveByExactGet(key_s, bytes_s, &committed_token);
            if (attempt_outcome == CasWriteOutcome::Unresolved)
            {
                /// This attempt is now one that may still land: it was sent, and the resolve settled
                /// nothing. Recorded BEFORE the exhaustion checks below so it is set no matter which of
                /// them ends the loop, and read by the DefiniteFailure arm of every later attempt.
                earlier_attempt_unresolved = true;
                /// Absent/unreadable: another attempt of the SAME (key, bytes) may be legal — after the
                /// fence-gated capped-exponential backoff (pauseBeforeReissue). No pause after the LAST
                /// attempt: the budget is spent, sleeping would only delay the Unresolved verdict.
                ///
                /// Both refusals report through `unresolved`, never a bare `return Unresolved`: this is
                /// the ordinary way a busy lane exhausts itself, so leaving `out_reason` at its initial
                /// `NotUnresolved` here made the ref lane's wedge message read "is UNCERTAIN (not
                /// unresolved)" for the single most common wedge there is.
                if (attempt == budget.max_attempts)
                    return unresolved(CasUnresolvedReason::AttemptsExhausted);
                CasUnresolvedReason pause_reason = CasUnresolvedReason::AttemptsExhausted;
                if (!pauseBeforeReissue(attempt, deadline_ms, fence_ok, &pause_reason))
                    return unresolved(pause_reason);
                continue;
            }
        }

        /// attempt_outcome == Committed here (either the attempt's own 2xx, or resolution found
        /// identical bytes). Final fence check before reporting success: a fence lost here means the
        /// write may have landed but this call must never claim it did. Count this "response observed
        /// after the local fence" leg separately from the generic Unresolved classifier so a cross-epoch
        /// fence loss is visible rather than folded into ordinary retry-budget exhaustion.
        if (!fence_ok())
        {
            ProfileEvents::increment(ProfileEvents::CASConditionalWriteFenceLostPostWrite);
            return unresolved(CasUnresolvedReason::FenceLostPostWrite);
        }
        if (out_token)
            *out_token = committed_token;
        return CasWriteOutcome::Committed;
    }

    return unresolved(CasUnresolvedReason::AttemptsExhausted);   /// budget exhausted, no definite outcome
}

CasCreateResult CasRequestController::conditionalCreateControlled(
    std::string_view key, const std::function<PutResult()> & attempt, const std::function<bool()> & fence_ok)
{
    const String key_s{key};
    const uint64_t deadline_ms = now_ms() + budget.operation_deadline_ms;

    for (uint32_t attempt_no = 1; attempt_no <= budget.max_attempts; ++attempt_no)
    {
        /// Same pre-attempt gates as putIfAbsentControlled.
        if (!fence_ok())
            return {CasCreateOutcome::Unresolved, {}};
        if (now_ms() + budget.attempt_timeout_ms > deadline_ms)
            return {CasCreateOutcome::Unresolved, {}};

        std::optional<PutResult> put;
        try
        {
            put = attempt();
        }
        catch (const std::exception & e)
        {
            /// A deterministic LOCAL bug surfaced by the attempt itself — a caller/config error, never
            /// a wire ambiguity; reissuing would only replay it. Propagate unchanged: instant, loud,
            /// exactly the pre-controller behavior (see isDeterministicLocalFailure for the set and the
            /// per-code rationale). This deliberately differs from `putIfAbsentControlled`'s
            /// everything-Unresolved:
            /// that lane's byte-exact resolve makes retrying any unproven error harmless, while
            /// retrying a broken source/mode/encode here is pure noise. Fail-safe either way — a
            /// propagated exception is never a false Committed.
            if (const auto * db_e = dynamic_cast<const Exception *>(&e); db_e && isDeterministicLocalFailure(db_e->code()))
                throw;
            /// A whitelisted synchronous rejection PROVES the request was never applied: surface the
            /// original exception — the blob lane's callers always saw the raw storage error's root
            /// cause, and losing it behind an outcome enum here would only degrade diagnostics
            /// Anything else is ambiguous: fall through to the occupancy
            /// resolve below.
            if (classifyConditionalWriteResult(e) == CasWriteOutcome::DefiniteFailure)
                throw;
        }

        if (put)
        {
            if (put->outcome == PutOutcome::PreconditionFailed)
                return {CasCreateOutcome::Occupied, {}};

            /// Done. Final fence check before reporting success: a fence
            /// lost here means the write may have landed but this call must never claim it did.
            if (!fence_ok())
            {
                ProfileEvents::increment(ProfileEvents::CASConditionalWriteFenceLostPostWrite);
                return {CasCreateOutcome::Unresolved, {}};
            }
            return {CasCreateOutcome::Committed, put->token};
        }

        /// Ambiguous attempt: resolve by exact-key OCCUPANCY — one HEAD, never a body GET (the body
        /// may be multi-GB, and reading a possibly-condemned occupant would flirt with the resurrect
        /// invariant; the key's content-address IS the identity proof, see the header contract).
        bool occupied = false;
        bool head_answered = true;
        try
        {
            occupied = backend->head(key_s).exists;
        }
        catch (const std::exception &)
        {
            /// The HEAD itself failed: occupancy unproven either way. Reissuing is still safe — an
            /// occupant answers the reissued If-None-Match with PreconditionFailed (-> Occupied on
            /// the next round) — so treat exactly like "absent" and let the budget bound the loop.
            head_answered = false;
        }
        if (head_answered && occupied)
            return {CasCreateOutcome::Occupied, {}};

        if (attempt_no == budget.max_attempts || !pauseBeforeReissue(attempt_no, deadline_ms, fence_ok))
            return {CasCreateOutcome::Unresolved, {}};
    }

    return {CasCreateOutcome::Unresolved, {}};   /// attempt budget exhausted without a definite outcome
}

CasOverwriteResult CasRequestController::putOverwriteControlled(
    std::string_view key, std::string_view bytes, const Token & expected, const std::function<bool()> & fence_ok)
{
    const String key_s{key};
    const String bytes_s{bytes};
    const uint64_t deadline_ms = now_ms() + budget.operation_deadline_ms;

    for (uint32_t attempt_no = 1; attempt_no <= budget.max_attempts; ++attempt_no)
    {
        if (!fence_ok())
            return {CasOverwriteOutcome::Unresolved, {}};
        if (now_ms() + budget.attempt_timeout_ms > deadline_ms)
            return {CasOverwriteOutcome::Unresolved, {}};

        std::optional<PutResult> put;
        try
        {
            put = backend->putOverwrite(key_s, bytes_s, expected);
        }
        catch (const std::exception & e)
        {
            /// Same rethrow convention as conditionalCreateControlled: a deterministic local bug or
            /// a whitelisted synchronous rejection PROVES no retry can help -- surface it unchanged.
            if (const auto * db_e = dynamic_cast<const Exception *>(&e); db_e && isDeterministicLocalFailure(db_e->code()))
                throw;
            if (classifyConditionalWriteResult(e) == CasWriteOutcome::DefiniteFailure)
                throw;
            /// Else ambiguous -- fall through to resolve below.
        }

        if (put && put->outcome == PutOutcome::Done)
        {
            if (!fence_ok())
            {
                ProfileEvents::increment(ProfileEvents::CASConditionalWriteFenceLostPostWrite);
                return {CasOverwriteOutcome::Unresolved, {}};
            }
            return {CasOverwriteOutcome::Committed, put->token};
        }

        /// Ambiguous: either a caught transient exception, or PreconditionFailed (which alone does
        /// NOT prove a real conflict -- it may be our own earlier attempt's write landing under a
        /// concurrent resolve). Resolve with one GET.
        std::optional<GetResult> got;
        try
        {
            got = backend->get(key_s);
        }
        catch (const std::exception &)
        {
            got.reset();   /// GET failed: still ambiguous, fall through to retry below.
        }

        if (got && got->token == expected)
        {
            /// The token we CAS'd against is STILL current: our attempt never applied. Fall through
            /// to the pause-and-reissue gate below (same key, bytes, expected).
        }
        else if (got && got->bytes == bytes_s)
        {
            if (!fence_ok())
            {
                ProfileEvents::increment(ProfileEvents::CASConditionalWriteFenceLostPostWrite);
                return {CasOverwriteOutcome::Unresolved, {}};
            }
            return {CasOverwriteOutcome::Committed, got->token};
        }
        else if (got)
        {
            /// A DIFFERENT token AND different bytes: a genuine competing write. Real conflict --
            /// never collapsed into Unresolved/DefiniteFailure, never thrown.
            return {CasOverwriteOutcome::Conflict, {}};
        }
        /// else: the GET itself failed or the key vanished -- still ambiguous, fall through to retry.

        if (attempt_no == budget.max_attempts || !pauseBeforeReissue(attempt_no, deadline_ms, fence_ok))
            return {CasOverwriteOutcome::Unresolved, {}};
    }

    return {CasOverwriteOutcome::Unresolved, {}};   /// attempt budget exhausted without a definite outcome
}

CasOverwriteResult CasRequestController::putIfAbsentControlledMutable(
    std::string_view key, std::string_view bytes, const std::function<bool()> & fence_ok)
{
    const String key_s{key};
    const String bytes_s{bytes};
    const uint64_t deadline_ms = now_ms() + budget.operation_deadline_ms;

    for (uint32_t attempt_no = 1; attempt_no <= budget.max_attempts; ++attempt_no)
    {
        if (!fence_ok())
            return {CasOverwriteOutcome::Unresolved, {}};
        if (now_ms() + budget.attempt_timeout_ms > deadline_ms)
            return {CasOverwriteOutcome::Unresolved, {}};

        std::optional<PutResult> put;
        try
        {
            put = backend->putIfAbsent(key_s, bytes_s);
        }
        catch (const std::exception & e)
        {
            /// Same rethrow convention as putOverwriteControlled/conditionalCreateControlled.
            if (const auto * db_e = dynamic_cast<const Exception *>(&e); db_e && isDeterministicLocalFailure(db_e->code()))
                throw;
            if (classifyConditionalWriteResult(e) == CasWriteOutcome::DefiniteFailure)
                throw;
            /// Else ambiguous -- fall through to resolve below.
        }

        if (put && put->outcome == PutOutcome::Done)
        {
            if (!fence_ok())
            {
                ProfileEvents::increment(ProfileEvents::CASConditionalWriteFenceLostPostWrite);
                return {CasOverwriteOutcome::Unresolved, {}};
            }
            return {CasOverwriteOutcome::Committed, put->token};
        }

        /// Ambiguous: either a caught transient exception, or PreconditionFailed (which alone does
        /// NOT prove a real conflict -- it may be our own earlier attempt's write landing under a
        /// concurrent resolve, or a racing writer creating the identical value). Resolve with one GET.
        std::optional<GetResult> got;
        try
        {
            got = backend->get(key_s);
        }
        catch (const std::exception &)
        {
            got.reset();   /// GET failed: still ambiguous, fall through to retry below.
        }

        if (!got)
        {
            /// Still absent: our attempt never applied. Fall through to the pause-and-reissue gate
            /// below (same key, bytes).
        }
        else if (got->bytes == bytes_s)
        {
            if (!fence_ok())
            {
                ProfileEvents::increment(ProfileEvents::CASConditionalWriteFenceLostPostWrite);
                return {CasOverwriteOutcome::Unresolved, {}};
            }
            return {CasOverwriteOutcome::Committed, got->token};
        }
        else
        {
            /// Present with DIFFERENT bytes: something else already occupies the key with a
            /// different value. For a MUTABLE marker this is a normal outcome, not corruption --
            /// return it as a value, never thrown.
            return {CasOverwriteOutcome::Conflict, {}};
        }

        if (attempt_no == budget.max_attempts || !pauseBeforeReissue(attempt_no, deadline_ms, fence_ok))
            return {CasOverwriteOutcome::Unresolved, {}};
    }

    return {CasOverwriteOutcome::Unresolved, {}};   /// attempt budget exhausted without a definite outcome
}

SlotOccupyResult CasRequestController::slotOccupy(
    std::string_view key, std::string_view bytes, const std::function<bool()> & fence_ok)
{
    const String key_s{key};
    const String bytes_s{bytes};
    const uint64_t deadline_ms = now_ms() + budget.operation_deadline_ms;

    /// Pre-attempt gate ONLY -- the same two checks every controlled op runs before its first (here,
    /// only) attempt: the mount fence must still hold, and there must be enough of the operation's own
    /// deadline left for one attempt to plausibly complete. Neither check sends anything to the
    /// backend, so a refusal here PROVES the key is untouched by this call. UNLIKE every sibling
    /// controlled op, there is no post-write recheck below: fence_ok is evaluated once per attempt, and
    /// the stronger post-I/O consistency check (fence generation together with wedge/txn identity) is
    /// the CALLER's contract (Task 4/6's re-acquire-lock-and-checkFenceOrThrow step), not this raw
    /// primitive's.
    if (!fence_ok() || now_ms() + budget.attempt_timeout_ms > deadline_ms)
        return {.kind = SlotOccupyResult::Kind::Unresolved, .occupant_bytes = {}, .occupant_token = {},
                .unresolved_reason = CasUnresolvedReason::NoAttemptSent};

    std::optional<PutResult> put;
    try
    {
        put = backend->putIfAbsent(key_s, bytes_s);
    }
    catch (const std::exception & e)
    {
        /// Same rethrow convention as putOverwriteControlled/putIfAbsentControlledMutable: a
        /// deterministic local bug, or a whitelisted synchronous rejection that PROVES the request was
        /// never applied, surfaces unchanged -- SlotOccupyResult::Kind has no DefiniteFailure member to
        /// carry either one. Anything else is ambiguous: fall through to the raw resolve GET below,
        /// exactly like a clean PreconditionFailed -- this primitive cannot and does not distinguish
        /// the two.
        if (const auto * db_e = dynamic_cast<const Exception *>(&e); db_e && isDeterministicLocalFailure(db_e->code()))
            throw;
        if (classifyConditionalWriteResult(e) == CasWriteOutcome::DefiniteFailure)
            throw;
    }

    if (put && put->outcome == PutOutcome::Done)
        return {.kind = SlotOccupyResult::Kind::Created, .occupant_bytes = {}, .occupant_token = {},
                .unresolved_reason = CasUnresolvedReason::NotUnresolved};

    /// Ambiguous attempt or a clean conflict: resolve with exactly ONE raw exact GET -- no byte-compare,
    /// no throw on a different occupant [codex finding 3: this is a DEDICATED slot operation, not
    /// putIfAbsentControlled (which retries the same (key, bytes) internally) or resolveByExactGet
    /// (which compares against an expected body and throws CORRUPTED_DATA on a mismatch) composed
    /// together]. Adjudicating whether the occupant is "mine" is entirely the CALLER's job (the
    /// CaCasMountCore `mine` contract), never this primitive's.
    ///
    /// WHOLE-OBJECT read, unlike conditionalCreateControlled's occupancy resolve (see that method's doc
    /// in the header), which deliberately uses HEAD instead of GET because a blob body "may be
    /// multi-GB". That reasoning does not apply here: slotOccupy is scoped by its callers (Task 4/6,
    /// spec INV-2) to small, write-once CONTROL slots -- ref-log transactions and epoch seals -- whose
    /// size is bounded by their own format's registry cap (the strict-grammar object caps
    /// CasRefLogFormat/CasRefCkptFormat enforce on decode), never a data blob. slotOccupy itself stays
    /// format-agnostic (it takes a raw key/bytes pair, per the "Interface handed to Stage B" contract in
    /// the plan) and does not encode any format's cap here -- the size bound is a property of what
    /// callers are allowed to pass it, enforced where the returned bytes are decoded, not by this seam.
    std::optional<GetResult> got;
    try
    {
        got = backend->get(key_s);
    }
    catch (const std::exception &)
    {
        got.reset();   /// the GET itself failed: still unresolved -- a one-shot primitive never retries
    }

    if (!got)
        /// The occupant that caused the conflict vanished before this GET (or the GET itself failed):
        /// the outcome is unknowable right now -- NEVER a fabricated Created.
        return {.kind = SlotOccupyResult::Kind::Unresolved, .occupant_bytes = {}, .occupant_token = {},
                .unresolved_reason = CasUnresolvedReason::AttemptsExhausted};

    return {.kind = SlotOccupyResult::Kind::Occupied, .occupant_bytes = std::move(got->bytes),
            .occupant_token = got->token, .unresolved_reason = CasUnresolvedReason::NotUnresolved};
}

}
