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
#include <limits>
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

/// Deterministic caller/local bugs a mutable conditional retry loop must surface immediately:
/// reissuing only replays the same failure and buries the root cause behind a retryable exception.
/// The set:
///   LOGICAL_ERROR   — a local invariant violation
///   NOT_IMPLEMENTED — a deterministic mode or capability guard
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

enum class OverwriteGateClosure : uint8_t
{
    Open,
    FenceOrLifecycleLost,
    Cancelled,
    Deadline,
};

struct OverwriteGateSample
{
    OverwriteGateClosure closure;
    CasOverwriteStopCause stop_cause;
};

/// Sample the operation stop cause and clock exactly once. The returned closure has already applied
/// the protocol precedence; callers only map its position to `CasUnresolvedReason`.
OverwriteGateSample sampleOverwriteGate(
    const CasOverwriteOperationContext & context,
    const std::function<uint64_t()> & now_ms,
    uint64_t required_time_ms)
{
    const CasOverwriteStopCause stop_cause = context.stop_cause();
    const uint64_t now = now_ms();
    const bool deadline_closed
        = now >= context.absolute_deadline_ms || required_time_ms > context.absolute_deadline_ms - now;

    if (stop_cause == CasOverwriteStopCause::FenceOrLifecycleLost)
        return {OverwriteGateClosure::FenceOrLifecycleLost, stop_cause};
    if (stop_cause == CasOverwriteStopCause::Cancelled)
        return {OverwriteGateClosure::Cancelled, stop_cause};
    if (deadline_closed)
        return {OverwriteGateClosure::Deadline, stop_cause};
    return {OverwriteGateClosure::Open, stop_cause};
}

uint64_t saturatingAdd(uint64_t lhs, uint64_t rhs)
{
    if (rhs > std::numeric_limits<uint64_t>::max() - lhs)
        return std::numeric_limits<uint64_t>::max();
    return lhs + rhs;
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

CasOverwriteResult CasRequestController::putOverwriteControlled(
    std::string_view key, std::string_view bytes, const Token & expected, const std::function<bool()> & fence_ok)
{
    const uint64_t operation_start_ms = now_ms();
    const uint64_t deadline_ms = saturatingAdd(operation_start_ms, budget.operation_deadline_ms);
    const CasOverwriteOperationContext context{
        .absolute_deadline_ms = deadline_ms,
        .deadline_source = CasOverwriteDeadlineSource::RequestBudget,
        .stop_cause = [&fence_ok]
        {
            return fence_ok() ? CasOverwriteStopCause::Continue : CasOverwriteStopCause::FenceOrLifecycleLost;
        },
        .wait_before_retry = [this](uint64_t wait_ms)
        {
            sleep_ms(wait_ms);
            return true;
        },
        .observe = [](const CasOverwriteProgress &) {},
    };
    return putOverwriteControlledImpl(key, bytes, expected, context, /*preserve_legacy_gates=*/true);
}

CasOverwriteResult CasRequestController::putOverwriteControlled(
    std::string_view key,
    std::string_view bytes,
    const Token & expected,
    const CasOverwriteOperationContext & context)
{
    return putOverwriteControlledImpl(key, bytes, expected, context, /*preserve_legacy_gates=*/false);
}

CasOverwriteResult CasRequestController::putOverwriteControlledImpl(
    std::string_view key,
    std::string_view bytes,
    const Token & expected,
    const CasOverwriteOperationContext & context,
    bool preserve_legacy_gates)
{
    const String key_s{key};
    const String bytes_s{bytes};
    CasOverwriteDiagnostics diagnostics;
    diagnostics.deadline_source = context.deadline_source;
    bool ambiguity_observed = false;
    bool earlier_attempt_unresolved = false;
    bool observer_failure_reported = false;

    const auto observe = [&](CasOverwriteProgressKind kind, uint32_t attempt_no) noexcept
    {
        try
        {
            context.observe(CasOverwriteProgress{kind, attempt_no});
        }
        catch (...)
        {
            if (!observer_failure_reported)
            {
                observer_failure_reported = true;
                try
                {
                    LOG_DEBUG(getLogger("CasRequestControl"),
                        "CAS overwrite progress observer threw; suppressing this and further observer exceptions");
                }
                catch (...)
                {
                }
            }
        }
    };

    const auto unresolved = [&](CasUnresolvedReason reason, CasOverwriteStopCause stop_cause)
    {
        diagnostics.unresolved_reason = reason;
        diagnostics.stop_cause = stop_cause;
        return CasOverwriteResult{CasOverwriteOutcome::Unresolved, {}, diagnostics};
    };

    const auto resultForGate = [&](const OverwriteGateSample & sample, bool commit_proved)
        -> std::optional<CasOverwriteResult>
    {
        if (sample.closure == OverwriteGateClosure::Open)
            return std::nullopt;

        if (sample.closure == OverwriteGateClosure::Deadline)
        {
            return unresolved(
                diagnostics.attempts_sent == 0 ? CasUnresolvedReason::NoAttemptSent : CasUnresolvedReason::DeadlineMidWay,
                CasOverwriteStopCause::Continue);
        }

        const CasUnresolvedReason reason = diagnostics.attempts_sent == 0
            ? CasUnresolvedReason::NoAttemptSent
            : (commit_proved ? CasUnresolvedReason::FenceLostPostWrite : CasUnresolvedReason::FenceLostMidWay);
        if (commit_proved)
            ProfileEvents::increment(ProfileEvents::CASConditionalWriteFenceLostPostWrite);
        return unresolved(reason, sample.stop_cause);
    };

    const auto gate = [&](uint64_t required_time_ms, bool commit_proved = false)
        -> std::optional<CasOverwriteResult>
    {
        return resultForGate(sampleOverwriteGate(context, now_ms, required_time_ms), commit_proved);
    };

    /// The existing overload historically checked its fence before each `PUT`/sleep and after a
    /// proven commit, but did not add clock/fence samples around the resolving `GET`. Keep that exact
    /// schedule while adapting its callbacks into the context representation; otherwise an injected
    /// clock that advances per sample loses a physical attempt solely because the adapter observed it.
    const auto legacyGate = [&](uint64_t required_time_ms, bool commit_proved, bool check_deadline)
        -> std::optional<CasOverwriteResult>
    {
        const CasOverwriteStopCause stop_cause = context.stop_cause();
        if (stop_cause != CasOverwriteStopCause::Continue)
        {
            const OverwriteGateClosure closure = stop_cause == CasOverwriteStopCause::FenceOrLifecycleLost
                ? OverwriteGateClosure::FenceOrLifecycleLost
                : OverwriteGateClosure::Cancelled;
            return resultForGate({closure, stop_cause}, commit_proved);
        }
        if (!check_deadline)
            return std::nullopt;

        const uint64_t now = now_ms();
        if (now > context.absolute_deadline_ms || required_time_ms > context.absolute_deadline_ms - now)
            return resultForGate({OverwriteGateClosure::Deadline, CasOverwriteStopCause::Continue}, commit_proved);
        return std::nullopt;
    };

    while (true)
    {
        const auto pre_put_refusal = preserve_legacy_gates
            ? legacyGate(budget.attempt_timeout_ms, /*commit_proved=*/false, /*check_deadline=*/true)
            : gate(budget.attempt_timeout_ms);
        if (pre_put_refusal)
            return *pre_put_refusal;
        if (diagnostics.attempts_sent >= budget.max_attempts)
            return unresolved(CasUnresolvedReason::AttemptsExhausted, CasOverwriteStopCause::Continue);

        const uint32_t attempt_no = diagnostics.attempts_sent + 1;
        if (attempt_no > 1)
            observe(CasOverwriteProgressKind::RetryStarted, attempt_no);
        ++diagnostics.attempts_sent;
        observe(CasOverwriteProgressKind::PutStarted, attempt_no);

        std::optional<PutResult> put;
        bool attempt_may_still_land = false;
        try
        {
            put = backend->putOverwrite(key_s, bytes_s, expected);
        }
        catch (const std::exception & e)
        {
            /// A deterministic local bug or a whitelisted synchronous rejection proves no retry can
            /// help. It surfaces unchanged unless an earlier request from this logical operation is
            /// still ambiguous; that earlier request dominates the call-wide result because it may
            /// still land after this later attempt was refused.
            const auto * db_e = dynamic_cast<const Exception *>(&e);
            const bool definite_failure = (db_e && isDeterministicLocalFailure(db_e->code()))
                || classifyConditionalWriteResult(e) == CasWriteOutcome::DefiniteFailure;
            if (definite_failure)
            {
                if (!preserve_legacy_gates)
                {
                    if (auto refusal = gate(/*required_time_ms=*/0))
                        return *refusal;
                    if (earlier_attempt_unresolved)
                        return unresolved(
                            CasUnresolvedReason::DefiniteFailureAfterAmbiguity,
                            CasOverwriteStopCause::Continue);
                }
                throw;
            }
            attempt_may_still_land = true;
            /// Else ambiguous -- fall through to resolve below.
        }

        if (put && put->outcome == PutOutcome::Done)
        {
            const auto post_write_refusal = preserve_legacy_gates
                ? legacyGate(/*required_time_ms=*/0, /*commit_proved=*/true, /*check_deadline=*/false)
                : gate(/*required_time_ms=*/0, /*commit_proved=*/true);
            if (post_write_refusal)
                return *post_write_refusal;
            return {CasOverwriteOutcome::Committed, put->token, diagnostics};
        }

        /// Ambiguous: either a caught transient exception, or PreconditionFailed (which alone does
        /// NOT prove a real conflict -- it may be our own earlier attempt's write landing under a
        /// concurrent resolve). Resolve with one GET.
        if (!ambiguity_observed)
        {
            ambiguity_observed = true;
            observe(CasOverwriteProgressKind::BecameAmbiguous, attempt_no);
        }
        if (!preserve_legacy_gates)
        {
            if (auto refusal = gate(budget.attempt_timeout_ms))
                return *refusal;
        }

        observe(CasOverwriteProgressKind::ResolveStarted, attempt_no);
        std::optional<GetResult> got;
        try
        {
            got = backend->get(key_s);
            diagnostics.resolve_observation_completed = true;
            diagnostics.observed_bytes = got ? std::optional<String>{got->bytes} : std::nullopt;
        }
        catch (const std::exception &)
        {
            diagnostics.resolve_observation_completed = false;
            diagnostics.observed_bytes.reset();
            got.reset();   /// GET failed: still ambiguous, fall through to retry below.
        }

        if (got && got->token != expected && got->bytes == bytes_s)
        {
            diagnostics.resolved_by_get = true;
            observe(CasOverwriteProgressKind::ResolvedByGet, attempt_no);
            const auto post_write_refusal = preserve_legacy_gates
                ? legacyGate(/*required_time_ms=*/0, /*commit_proved=*/true, /*check_deadline=*/false)
                : gate(/*required_time_ms=*/0, /*commit_proved=*/true);
            if (post_write_refusal)
                return *post_write_refusal;
            return {CasOverwriteOutcome::Committed, got->token, diagnostics};
        }

        /// Apply stop/deadline precedence to the completed resolve before accepting a conflict,
        /// waiting, or letting attempt exhaustion decide whether another `PUT` is legal.
        if (!preserve_legacy_gates)
        {
            if (auto refusal = gate(/*required_time_ms=*/0))
                return *refusal;
        }

        if (got && got->token == expected)
        {
            /// The token we CAS'd against is STILL current: our attempt never applied. Fall through
            /// to the pause-and-reissue gate below (same key, bytes, expected).
        }
        else if (got)
        {
            /// A DIFFERENT token AND different bytes: a genuine competing write. Real conflict --
            /// never collapsed into Unresolved/DefiniteFailure, never thrown.
            return {CasOverwriteOutcome::Conflict, {}, diagnostics};
        }
        /// else: the GET itself failed or the key vanished -- still ambiguous, fall through to retry.

        if (attempt_may_still_land)
            earlier_attempt_unresolved = true;

        /// Attempt exhaustion participates only when another physical `PUT` would be sent. It is
        /// deliberately evaluated after the final attempt's resolving `GET` and after the gate above.
        if (diagnostics.attempts_sent >= budget.max_attempts)
        {
            if (!preserve_legacy_gates)
            {
                if (auto refusal = gate(budget.attempt_timeout_ms))
                    return *refusal;
            }
            return unresolved(CasUnresolvedReason::AttemptsExhausted, CasOverwriteStopCause::Continue);
        }

        const uint64_t backoff_ms = backoffBeforeAttempt(attempt_no + 1);
        if (backoff_ms == 0)
            continue;

        const uint64_t retry_reservation_ms = saturatingAdd(backoff_ms, budget.attempt_timeout_ms);
        const auto pre_wait_refusal = preserve_legacy_gates
            ? legacyGate(retry_reservation_ms, /*commit_proved=*/false, /*check_deadline=*/true)
            : gate(retry_reservation_ms);
        if (pre_wait_refusal)
            return *pre_wait_refusal;

        const bool wait_completed = context.wait_before_retry(backoff_ms);
        if (preserve_legacy_gates)
            continue;

        const OverwriteGateSample after_wait = sampleOverwriteGate(context, now_ms, budget.attempt_timeout_ms);
        if (!wait_completed && after_wait.stop_cause == CasOverwriteStopCause::Continue)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CasRequestController: wait_before_retry returned false while stop_cause remained Continue");
        if (auto refusal = resultForGate(after_wait, /*commit_proved=*/false))
            return *refusal;
    }
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
            return {CasOverwriteOutcome::Unresolved, {}, {}};
        if (now_ms() + budget.attempt_timeout_ms > deadline_ms)
            return {CasOverwriteOutcome::Unresolved, {}, {}};

        std::optional<PutResult> put;
        try
        {
            put = backend->putIfAbsent(key_s, bytes_s);
        }
        catch (const std::exception & e)
        {
            /// Same rethrow convention as `putOverwriteControlled`.
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
                return {CasOverwriteOutcome::Unresolved, {}, {}};
            }
            return {CasOverwriteOutcome::Committed, put->token, {}};
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
                return {CasOverwriteOutcome::Unresolved, {}, {}};
            }
            return {CasOverwriteOutcome::Committed, got->token, {}};
        }
        else
        {
            /// Present with DIFFERENT bytes: something else already occupies the key with a
            /// different value. For a MUTABLE marker this is a normal outcome, not corruption --
            /// return it as a value, never thrown.
            return {CasOverwriteOutcome::Conflict, {}, {}};
        }

        if (attempt_no == budget.max_attempts || !pauseBeforeReissue(attempt_no, deadline_ms, fence_ok))
            return {CasOverwriteOutcome::Unresolved, {}, {}};
    }

    return {CasOverwriteOutcome::Unresolved, {}, {}};   /// attempt budget exhausted without a definite outcome
}

SlotOccupyResult CasRequestController::slotOccupy(
    std::string_view key, std::string_view bytes, const std::function<bool()> & fence_ok)
{
    const String key_s{key};
    const String bytes_s{bytes};
    const uint64_t deadline_ms = now_ms() + budget.operation_deadline_ms;

    /// Pre-attempt gate -- the same two checks every controlled op runs before its first (here, only)
    /// attempt: the mount fence must still hold, and there must be enough of the operation's own
    /// deadline left for one attempt to plausibly complete. Neither check sends anything to the
    /// backend, so a refusal here PROVES the key is untouched by this call. UNLIKE every sibling
    /// controlled op, there is no post-write result recheck below: the stronger post-I/O consistency
    /// check (fence generation together with wedge/txn identity) is the CALLER's contract (Task 4/6's
    /// re-acquire-lock-and-checkFenceOrThrow step), not this raw primitive's. The admission predicate is
    /// checked again only if this attempt needs a second backend request to resolve its result; a
    /// `Created` result still performs no post-write recheck.
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
    /// Whole-object resolution is safe here because `slotOccupy` is scoped by its callers (Task 4/6,
    /// spec INV-2) to small, write-once control slots -- ref-log transactions and epoch seals -- whose
    /// size is bounded by their own format's registry cap (the strict-grammar object caps
    /// CasRefLogFormat/CasRefCkptFormat enforce on decode), never a data blob. slotOccupy itself stays
    /// format-agnostic (it takes a raw key/bytes pair, per the "Interface handed to Stage B" contract in
    /// the plan) and does not encode any format's cap here -- the size bound is a property of what
    /// callers are allowed to pass it, enforced where the returned bytes are decoded, not by this seam.
    if (!fence_ok())
        return {.kind = SlotOccupyResult::Kind::Unresolved, .occupant_bytes = {}, .occupant_token = {},
                .unresolved_reason = CasUnresolvedReason::AttemptsExhausted};

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
