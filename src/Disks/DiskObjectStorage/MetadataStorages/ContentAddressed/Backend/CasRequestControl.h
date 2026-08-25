#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <base/types.h>
#include <cstdint>
#include <exception>
#include <functional>
#include <optional>
#include <string_view>

namespace DB::Cas
{

/// Outcome of ONE HTTP attempt for a CAS conditional write (`If-None-Match`/`If-Match`), issued with
/// the generic S3 client's transparent retries disabled for that attempt. This is the seam the
/// `CasRequestController` is built on: it decides whether another attempt is legal and how an
/// uncertain result is resolved.
///   - Committed: the attempt's own request completed successfully (2xx) — the object is durable.
///   - DefiniteFailure: a synchronous rejection that PROVES the request was never applied server-side
///     — a WHITELISTED malformed-request / entity-too-large / access-denied error ONLY. Never
///     `PreconditionFailed`: a lost precondition means the key exists, not that the request failed.
///   - Unresolved: everything else — `PreconditionFailed`/`NoSuchKey`, a client-side timeout, a
///     connection loss, a 5xx, or any error this classifier does not recognize. The caller resolves
///     the exact key before deciding whether another attempt is legal; ambiguity always
///     resolves toward Unresolved, never toward a false DefiniteFailure or a false Committed.
enum class CasWriteOutcome : uint8_t
{
    Committed,
    DefiniteFailure,
    Unresolved,
};

/// WHY a controlled write came back `Unresolved`. It exists because `Unresolved` covers two materially
/// different states, and telling them apart is the difference between a five-minute triage and an hour
/// of it — and, since finding #37 defect 3, between a table that keeps its write availability and one
/// that loses it until remount.
///
/// `NoAttemptSent` is the one that carries real information: both pre-attempt gates (the mount fence
/// and the operation deadline) reject BEFORE anything reaches the backend, so on the very first
/// iteration the key is PROVABLY unwritten — there is no ambiguity to resolve, only a lost right to
/// write. Every other reason leaves an object that may or may not be durable, which is what the
/// wedge/resolve machinery exists for.
///
/// NOT purely diagnostic any more: `unresolvedProvesNothingWasSent` below turns this into the fact the
/// ref append lane acts on (`CasRefLedger::commitRefChunk`'s `Unresolved` arm), so ADDING A MEMBER HERE
/// IS A PROTOCOL DECISION — read that predicate before you do.
enum class CasUnresolvedReason : uint8_t
{
    NotUnresolved,     /// the call did not return Unresolved
    NoAttemptSent,     /// a pre-attempt gate rejected on the FIRST iteration: nothing was ever sent
    FenceLostMidWay,   /// >= 1 attempt was sent, then the mount fence dropped
    DeadlineMidWay,    /// >= 1 attempt was sent, then the operation deadline left no room for another
    FenceLostPostWrite,/// an attempt COMMITTED but the fence had dropped by the time it returned
    AttemptsExhausted, /// the genuine case the "retry budget exhausted" wording describes
    /// A LATER attempt was definitively refused while an EARLIER one of the same call is still
    /// unresolved. The refusal proves only its own attempt never applied; the earlier one may still
    /// materialize at the key, so the CALL cannot report `DefiniteFailure` (see
    /// `putIfAbsentControlled`). Reported instead of the definite verdict, never alongside it.
    DefiniteFailureAfterAmbiguity,
};

/// Does this `Unresolved` PROVE that no attempt ever reached the network — i.e. that the key is
/// unwritten and there is nothing for an exact-key resolution to settle?
///
/// True for exactly ONE value, and that is the whole design: `NoAttemptSent` is reported only when a
/// pre-attempt gate rejected while `attempts_sent == 0`, so `backend->putIfAbsent` was never called
/// (see `putIfAbsentControlled`). Every other value — including `NotUnresolved`, which a caller can
/// still observe if some path returns `Unresolved` without recording a reason — leaves an object that
/// MAY be durable, and callers that protect themselves against that (the ref lane's append wedge) must
/// keep doing so.
///
/// Written as an allow-list: a switch with no `default` and a trailing `return false`, so a member
/// added to `CasUnresolvedReason` later fails BOTH ways safely. The missing case is a `-Wswitch` build
/// error, which forces the contributor to classify it deliberately; and if that diagnostic is ever
/// silenced, the runtime answer for the unclassified member is "no, this does not prove anything",
/// which is the conservative side. Never turn this into a deny-list — a new reason must not be able to
/// claim "nothing was sent" by omission.
constexpr bool unresolvedProvesNothingWasSent(CasUnresolvedReason reason)
{
    switch (reason)
    {
        case CasUnresolvedReason::NoAttemptSent:
            return true;
        case CasUnresolvedReason::NotUnresolved:
        case CasUnresolvedReason::FenceLostMidWay:
        case CasUnresolvedReason::DeadlineMidWay:
        case CasUnresolvedReason::FenceLostPostWrite:
        case CasUnresolvedReason::AttemptsExhausted:
        /// The whole point of this value is that an earlier attempt WAS sent and may still land.
        case CasUnresolvedReason::DefiniteFailureAfterAmbiguity:
            return false;
    }
    return false;
}

/// Human-readable tail for an exception or log line, so the two states above stop reading alike.
constexpr std::string_view describeUnresolvedReason(CasUnresolvedReason reason)
{
    switch (reason)
    {
        case CasUnresolvedReason::NotUnresolved:      return "not unresolved";
        case CasUnresolvedReason::NoAttemptSent:      return "no attempt was sent (the mount fence or the "
                                                             "operation deadline rejected before the first "
                                                             "request) — the key is provably unwritten";
        case CasUnresolvedReason::FenceLostMidWay:    return "the mount fence dropped after at least one "
                                                             "attempt had been sent";
        case CasUnresolvedReason::DeadlineMidWay:     return "the operation deadline ran out after at least "
                                                             "one attempt had been sent";
        case CasUnresolvedReason::FenceLostPostWrite: return "an attempt committed but the mount fence had "
                                                             "dropped before it returned";
        case CasUnresolvedReason::AttemptsExhausted:  return "the attempt budget was exhausted without a "
                                                             "definite outcome";
        case CasUnresolvedReason::DefiniteFailureAfterAmbiguity:
                                                      return "a later attempt was definitively refused, but "
                                                             "an earlier attempt of the same call is still "
                                                             "unresolved and may yet land";
    }
    return "unspecified";
}

/// The success path: `buf.finalize()` returned without throwing. Always Committed — kept as a named,
/// counted entry point so both paths of a classify-then-record call site read the same way (see the
/// exception overload below).
constexpr CasWriteOutcome classifyConditionalWriteResult()
{
    return CasWriteOutcome::Committed;
}

/// The exception path: classify what `buf.finalize()` threw for ONE CAS conditional-write HTTP
/// attempt, according to the CAS conditional-write operation classes. Pure — never rethrows, never
/// touches counters; see recordConditionalWriteOutcome for the counters hookup.
CasWriteOutcome classifyConditionalWriteResult(const std::exception & e);

/// Records the start of one HTTP attempt for a CAS conditional write (the attempts counter).
void recordConditionalWriteAttemptStarted();

/// Records one attempt's terminal outcome (the per-class outcome counters). Callers pass the result of
/// whichever classifyConditionalWriteResult overload applies, or an outcome already known by
/// construction (e.g. the legacy `PutOutcome::PreconditionFailed` path, which today resolves without
/// throwing — see ObjectStorageBackend::nativeConditionalPut).
void recordConditionalWriteOutcome(CasWriteOutcome outcome);

/// The three separate limits a CAS-owned retry controller enforces for ONE logical conditional-write
/// operation. Never represented by a single `request_timeout_ms` value — see `validateCasRequestBudget`
/// for the relationship a writable mount enforces at startup, and `CasRequestController` for the
/// runtime use.
struct CasRequestBudget
{
    /// Maximum client wait budgeted for one HTTP attempt. `CasRequestController` uses this ONLY as a
    /// per-attempt scheduling check (an attempt is not started unless it could still finish inside the
    /// operation deadline) — the actual socket-level wait is configured on the object storage's client
    /// (the object storage backend's single-attempt client), not by this struct.
    uint64_t attempt_timeout_ms = 5000;
    /// Maximum wall-clock time for the COMPLETE logical operation — every attempt, every exact-key
    /// resolution, and every inter-attempt backoff sleep — counted from the first call to
    /// `putIfAbsentControlled`. A DURATION, not an absolute deadline: each call establishes its own
    /// `now + operation_deadline_ms` bound.
    ///
    /// This deadline is the authoritative bound on how long a CAS conditional write keeps riding an S3
    /// disruption server-side before the caller sees an abort. 90s absorbs a ~60s object-store outage
    /// with margin (see the arithmetic on `max_attempts` below) — PROVIDED the mount fence stays alive.
    /// The fence, not this deadline, is
    /// what binds under a TOTAL outage: lease renewals are conditional writes against the same store,
    /// so when everything is unreachable the fence deadline freezes at `last_renew + mount_lease_ttl`
    /// and `fence_ok` stops the loop ≈ TTL−attempt_timeout−margin (~23s) after the last successful
    /// renewal — the required fail-closed behavior (never an attempt past the lease), not a
    /// budget limitation. While renewals DO land (blips, throttling, partial outages — the runtime-owned
    /// renewal worker keeps extending the fence deadline), the op is NOT bounded by
    /// the lease TTL and rides the full deadline here.
    uint64_t operation_deadline_ms = 90000;
    /// Maximum number of controlled attempts for one logical operation (the first attempt counts as 1).
    /// Sized so the operation deadline above — never this count — is what binds under the observed
    /// failure shape (~3s adaptive first-attempt PUT timeout per failed attempt + capped-exponential
    /// backoff): 16 attempts × ~3s + Σ backoff (0.2+0.4+0.8+1.6+3.2 + 10×5 = 56.2s) ≈ 104s > 90s.
    uint32_t max_attempts = 16;
    /// Startup-only margin folded into `validateCasRequestBudget`'s inequality against the mount lease
    /// TTL. Not consulted at runtime by the controller itself — the caller's `fence_ok` callback (backed
    /// by the local write fence's own deadline) is what actually gates lease-relative timing per attempt.
    uint64_t lease_safety_margin_ms = 2000;
    /// Inter-attempt backoff (`cas_s3_retry_initial_backoff_ms` /
    /// `cas_s3_retry_max_backoff_ms`): the sleep before reissuing
    /// after an ambiguous attempt whose resolve observed the key absent, capped exponential —
    /// `initial · 2^(reissues-1)`, never above `retry_max_backoff_ms`. 0 disables backoff (immediate
    /// reissue — the pre-backoff behavior, and what most exhaustion-path unit tests configure). The
    /// controller checks the fence BEFORE every sleep and never sleeps past the operation deadline.
    uint64_t retry_initial_backoff_ms = 200;
    uint64_t retry_max_backoff_ms = 5000;

    /// Recovery-level retry (`CasRefLedger::ensureRefTableRecovered`): a whole ref-table recovery
    /// attempt (LIST + snapshot/log GETs + seal PUT) that fails with a transient NETWORK_ERROR is
    /// retried, with capped-exponential backoff, until this total wall-clock budget is spent — then the
    /// error propagates and the table's load fails for this touch (the `lazy_load_tables` database
    /// setting makes the NEXT touch retry). This sits ON TOP of the per-request `operation_deadline_ms`
    /// envelope above: one recovery attempt may itself burn ~90s inside a single seal PUT. Independent
    /// of the mount-lease invariants validated in `validateCasRequestBudget` — not part of that
    /// inequality set.
    uint64_t recovery_retry_budget_ms = 120000;
    uint64_t recovery_retry_initial_backoff_ms = 1000;
    uint64_t recovery_retry_max_backoff_ms = 30000;
};

/// Startup validation: a writable mount refuses to open with an inconsistent budget rather than
/// silently falling back to an unbounded or unsafe retry policy. Throws
/// `BAD_ARGUMENTS` unless ALL hold:
///   attempt_timeout_ms + lease_safety_margin_ms < mount_lease_ttl_ms
///   attempt_timeout_ms < operation_deadline_ms          (STRICTLY — see below)
///   retry_initial_backoff_ms <= retry_max_backoff_ms
///
/// The middle one is strict on purpose. Equality does not mean "one attempt's worth of budget": the
/// deadline is captured as `now + operation_deadline_ms` and each pre-send gate asks
/// `now + attempt_timeout_ms > deadline_ms`, so equal values reduce it to `now_2 > now_1` and a single
/// elapsed millisecond refuses the operation having sent NOTHING. Bound the attempt COUNT with
/// `max_attempts`, never by starving the deadline.
/// `mount_renew_period_ms` takes no part in the inequality (the renewer keeps the fence deadline
/// refreshed well ahead of the TTL by construction) — it is accepted only so the effective-values log
/// line records the full picture in one place.
///
/// A successor mounting over an unclean predecessor waits at least one lease TTL, plus its
/// materialization grace period, before trusting recovery listings. This is long enough for any
/// conditional PUT still in flight at the predecessor to either land or be abandoned by its own
/// exhausted retry budget. The predecessor's budget is constrained by
/// `attempt_timeout_ms + lease_safety_margin_ms < mount_lease_ttl_ms`, so no additional handover
/// check is needed here.
void validateCasRequestBudget(const CasRequestBudget & budget, uint64_t mount_lease_ttl_ms, uint64_t mount_renew_period_ms);

/// Throw the recoverable "CAS write could not be committed, retry later" condition.
///
/// WHY NETWORK_ERROR (this replaces an earlier ABORTED throw):
/// A content-addressed write can fail for a reason that is neither the caller's fault
/// nor permanent: the mount-lease / write fence was lost (e.g. a renewal PUT timed out
/// against a slow or throttling object store), or a conditional PUT exhausted its retry
/// budget mid-outage. The right response is "abandon this attempt, try again later" --
/// which is precisely what a transient error means.
///
/// It previously threw `ABORTED`, which was actively harmful to background merges:
/// `ReplicatedMergeMutateTaskBase` treats `ABORTED` as "merge deliberately cancelled
/// (shutdown / `DROP` / merges-blocker), not an error", so it neither records
/// `last_exception_time_ms` nor lets `ReplicatedMergeTreeQueue`'s exponential backoff
/// engage. Under a sustained store outage the queue re-executed the merge roughly every
/// 2 seconds, recomputing the whole (possibly multi-GiB) output part every time for the
/// entire outage -- hundreds of full recomputes, and invisible in system.replication_queue.
///
/// `NETWORK_ERROR` is the best-fitting EXISTING code:
///   - it is NOT in the merge "retry silently, no backoff" exemption set (only `ABORTED`
///     and `PART_IS_TEMPORARILY_LOCKED` are), so the existing backoff -- capped by
///     `max_postpone_time_for_failed_replicated_merges_ms` -- engages automatically;
///   - it is already in ClickHouse's transient/retryable taxonomy
///     (`checkDataPart::isRetryableException` lists it beside `ABORTED`), so a part under
///     verification is not misread as corrupted;
///   - nothing on the merge / insert / replication commit path special-cases it in a way
///     that would misfire (ZooKeeper retriability keys on `Coordination::Exception`, a
///     different type), and it is not caught specially on the CAS write path.
///
/// Honest caveat: `NETWORK_ERROR` is coarser than the true condition. For the
/// throttled-store / timed-out / lost-lease cases it is accurate; for a purely logical
/// fence loss (e.g. the namespace is being dropped) it slightly overstates "network".
/// The precise cause is always in the exception MESSAGE, never inferred from the code.
///
/// If that imprecision ever matters -- operator confusion, or a future upstream change
/// that attaches merge-path handling to `NETWORK_ERROR` and reintroduces a collision --
/// switch to a dedicated code (e.g. CAS_WRITE_RETRY_LATER) by changing the
/// single throw below. A dedicated code is honest and collision-proof by construction
/// (backoff still engages, since only `ABORTED` / `PART_IS_TEMPORARILY_LOCKED` are exempt);
/// the only extra work is one appended line in `ErrorCodes.cpp` and, optionally, adding it
/// to `checkDataPart::isRetryableException` and an HTTP-status mapping for the foreground
/// `INSERT` client. We deliberately kept `NETWORK_ERROR` for now to add zero new coupling to
/// generic ClickHouse code, consistent with the rest of the CAS layer.
///
/// SCOPE: only the ESCAPING retry-later throws route here (fence lost or a controlled write outcome
/// remaining uncertain). Startup/decommission and generic live-lock-brake `ABORTED` values keep
/// their meaning and are not rerouted here.
[[noreturn]] void throwCasWriteRetryLater(const String & why);

/// Same classification as `throwCasWriteRetryLater`, but returns the exception as a
/// `std::exception_ptr` for call sites that fail a pending future/promise (`CasRefLedger`'s
/// `complete_error`) rather than throw directly. Both entry points route through the SAME
/// construction internally, so the error code / message shape has exactly one place that decides it.
std::exception_ptr makeCasWriteRetryLaterExceptionPtr(const String & why);

/// Throw the recoverable "this content-addressed disk cannot serve the request right now" condition.
/// Sibling of `throwCasWriteRetryLater`, same class for the same reasons (see the long rationale above),
/// differing only in what it describes: that one names a WRITE whose commit did not land, this one names
/// a DISK STATE that refused the request before it started -- on either plane.
///
/// The class is load-bearing beyond CAS. `ReplicatedMergeTreePartCheckThread::checkPartImpl` rethrows
/// (leaving the part queued for a later check) exactly when `checkDataPart::isRetryableException`
/// recognises the error, and otherwise declares the part broken -- detach and re-fetch.
/// `INVALID_STATE` is absent from that classifier, so a lease blip used to read as part corruption
/// (BACKLOG `{#lease-blip-part-check-collapse}`). Re-coding the CA transients
/// was chosen over widening the upstream classifier because `INVALID_STATE` is broad: widening it would
/// also reclassify 18 unrelated TERMINAL sites, CA and non-CA alike.
///
/// SCOPE, narrow by design: a refusal routes here when it either names an AUTO-RECOVERING disk condition,
/// or CANNOT ESTABLISH that its condition is terminal. `checkFenceOrThrow` is the second kind -- one guard
/// trips for a lease blip and for a FORGET decommission alike and it cannot tell them apart -- and it is in
/// scope for write-plane uniformity: its 32 sibling write-transient sites already mint this class, and an
/// unproven condition must be retried rather than consumed as damage. What is NEVER in scope is a refusal
/// whose condition is PROVEN terminal: `IdentityLost`, both `Vanished` flavours, a storage that is not
/// started, an unbootstrappable prefix, a proven-absent pool identity, a closed writer epoch -- all keep
/// `INVALID_STATE`. A proven-terminal state that read as retryable would make every consumer retry forever
/// against a disk that is never coming back.
///
/// `subject` names the refusing disk or pool (e.g. "content-addressed disk 'ca'") and `condition` states
/// the CA condition truthfully, INCLUDING any promise about how it clears -- only the site knows whether
/// it can make one. What is appended HERE is the classification alone, so it cannot drift between call
/// sites. Unlike `throwCasWriteRetryLater` this deliberately does not log: these sites fire once per
/// refused operation (tens of thousands within a single observed lease gap) and every caller already
/// reports the exception it receives.
[[noreturn]] void throwCasTransientUnavailable(const String & subject, const String & condition);

/// Outcome of a controlled MUTABLE conditional overwrite (`putOverwriteControlled`) -- an If-Match
/// replace whose caller can, unlike a content-addressed create, supply the intended bytes for
/// GET-based resolution, because the payload here is deterministic (a pure function of the
/// caller's record), not freshly minted per attempt.
///   - Committed: an attempt's own request completed (2xx) and the final fence check held, or
///     resolution proved the intended bytes are already what's currently stored -- `token` names
///     that incarnation.
///   - Conflict: resolution proved the key's CURRENT token AND bytes both differ from what this
///     call intended -- a genuine competing write. Returned as a value, never thrown, never
///     collapsed into Unresolved/DefiniteFailure -- mirrors the existing uncontrolled
///     casMeta/CasResult contract (a conflict lets the caller reload and decide).
///   - Unresolved: budget exhausted, fence lost, or the current token still equals `expected` (the
///     attempt provably never applied) with the resolve unable to prove either outcome yet --
///     caller must not ACK.
enum class CasOverwriteOutcome : uint8_t
{
    Committed,
    Conflict,
    Unresolved,
};

enum class CasOverwriteDeadlineSource : uint8_t
{
    RequestBudget,
    ExternalLeaseSafety,
};

enum class CasOverwriteStopCause : uint8_t
{
    Continue,
    Cancelled,
    FenceOrLifecycleLost,
};

enum class CasOverwriteProgressKind : uint8_t
{
    PutStarted,
    BecameAmbiguous,
    ResolveStarted,
    RetryStarted,
    ResolvedByGet,
};

struct CasOverwriteProgress
{
    CasOverwriteProgressKind kind;
    uint32_t attempt_no;
};

/// Per-operation gates for a controlled mutable overwrite. `absolute_deadline_ms` uses the same
/// clock as the controller's injected `now_ms`; it is fixed by the caller before controller entry
/// and therefore cannot be re-anchored after preemption. `wait_before_retry` is interruptible and
/// must return false only after publishing a non-`Continue` stop cause. `observe` is diagnostic only:
/// an exception from it is contained and cannot affect the protocol result.
struct CasOverwriteOperationContext
{
    uint64_t absolute_deadline_ms;
    CasOverwriteDeadlineSource deadline_source;
    std::function<CasOverwriteStopCause()> stop_cause;
    std::function<bool(uint64_t)> wait_before_retry;
    std::function<void(const CasOverwriteProgress &)> observe;
};

struct CasOverwriteDiagnostics
{
    uint32_t attempts_sent = 0;
    bool resolved_by_get = false;
    CasUnresolvedReason unresolved_reason = CasUnresolvedReason::NotUnresolved;
    CasOverwriteDeadlineSource deadline_source = CasOverwriteDeadlineSource::RequestBudget;
    CasOverwriteStopCause stop_cause = CasOverwriteStopCause::Continue;
    /// The last exact resolving GET completed by this controller. `resolve_observation_completed`
    /// distinguishes a confirmed absence (`observed_bytes == nullopt`) from a failed/not-run read.
    /// Terminal protocol owners use this snapshot instead of starting diagnostic I/O after the
    /// controller has closed its deadline/cancellation gate.
    bool resolve_observation_completed = false;
    std::optional<String> observed_bytes;
};

/// Result of one `CasRequestController::putOverwriteControlled` operation. `token` is meaningful
/// only when `outcome` is `Committed`.
struct CasOverwriteResult
{
    CasOverwriteOutcome outcome = CasOverwriteOutcome::Unresolved;
    Token token;   /// set ONLY on Committed
    CasOverwriteDiagnostics diagnostics;
};

/// Result of one `CasRequestController::slotOccupy` operation — a WRITE-ONCE conditional create whose
/// body is content-addressed or otherwise not byte-comparable across separate CALLS the way
/// `putOverwriteControlled`'s deterministic marker is (each caller of `slotOccupy` — an epoch seal, a
/// wedge retry — mints its own attempt and decides for itself, from `Occupied`'s bytes, whether the
/// occupant is its own earlier write or something else entirely; see the adjudication note below).
///   - Created:    this call's OWN conditional create committed — the key held nothing before it.
///   - Occupied:   the key already holds an object, observed by ONE raw exact `GET` after the create
///     conflicted — `occupant_bytes`/`occupant_token` name exactly what is there NOW. The primitive
///     never compares these bytes against what this call attempted and never throws on a mismatch:
///     unlike `resolveByExactGet` (whose caller supplies ONE expected body across every retry of the
///     SAME logical attempt), `slotOccupy` never retries, so there is no "our earlier attempt" to
///     distinguish from a genuine foreign occupant — that adjudication (the `CaCasMountCore` `mine`
///     contract: an occupant is this caller's write only if the BYTES match, never a generation/shape
///     match alone) is entirely the CALLER's job.
///   - Unresolved: the outcome is unknowable right now — a pre-attempt gate refused (fence lost /
///     deadline exhausted, `unresolved_reason == NoAttemptSent`, nothing was sent — `unresolvedProvesNothingWasSent`
///     is TRUE only for this case), admission was lost after the create but before its resolution GET,
///     or the conditional create was itself ambiguous (a transient exception) and the follow-up resolve
///     GET found nothing (the occupant that caused the conflict vanished before the GET, or the GET
///     itself failed). Both post-create cases report `unresolved_reason == AttemptsExhausted`, for which
///     `unresolvedProvesNothingWasSent` is FALSE — NEVER fabricated into
///     a false `Created`. CALLERS: do not log a bare `describeUnresolvedReason(AttemptsExhausted)` for
///     this case — it reads "the attempt budget was exhausted", which is misleading for a primitive
///     with no retry budget, and it silently folds admission loss together with "the resolve GET found
///     nothing" and "the occupant that caused the conflict was DELETED under a live epoch" (a
///     GC-invariant alarm, not routine contention) into the same generic wording. `SlotOccupyResult`
///     carries no discriminator between these sub-cases; the day a caller NEEDS the split is the trigger
///     for adding a dedicated `CasUnresolvedReason` value (a gated protocol decision, not a drive-by).
struct SlotOccupyResult
{
    enum class Kind : uint8_t { Created, Occupied, Unresolved };
    Kind kind = Kind::Unresolved;
    /// Occupied only: the occupant, fetched by exact GET after the conditional create conflicted.
    String occupant_bytes;
    Token occupant_token;
    /// Unresolved only: why the attempt outcome is unknowable right now.
    CasUnresolvedReason unresolved_reason{};
};

/// CAS-owned retry controller: the only place that decides whether a conditional-write attempt may be
/// reissued. It does not touch a writer cache or return ACK. Callers update their cache and acknowledge
/// the operation only after this controller has resolved the outcome and performed its final fence
/// check, using the returned `CasWriteOutcome`.
class CasRequestController
{
public:
    /// `now_ms_`: monotonic-ish clock, defaulting to `std::chrono::steady_clock`; tests inject a fake
    /// one to drive deadline behavior deterministically (no sleeps).
    /// `sleep_ms_`: the inter-attempt backoff sleep, defaulting to a real `std::this_thread::sleep_for`;
    /// tests inject a recorder/no-op to assert the backoff schedule without wall-clock waits. The
    /// controller only ever sleeps BETWEEN attempts of one logical operation, on the calling thread,
    /// with no Pool mutex held (every call site — the ref append lane's leader, `stageManifest`, and
    /// snapshot publishes — invokes the controller outside its locks; the append lane's
    /// LEADERSHIP is deliberately held across the sleep: same-table appends must queue behind an
    /// unresolved predecessor PUT anyway, preserving the writer's per-table ordering.
    CasRequestController(BackendPtr backend_, CasRequestBudget budget_, std::function<uint64_t()> now_ms_ = {},
                         std::function<void(uint64_t)> sleep_ms_ = {});

    /// Controlled `putIfAbsent` with resolve-before-reissue. Performs at
    /// most `budget.max_attempts` attempts of the exact SAME (key, bytes) — never a different key, never
    /// a different body — bounded by `budget.operation_deadline_ms` measured from this call's own start,
    /// with capped-exponential inter-attempt backoff (`retry_initial_backoff_ms`/`retry_max_backoff_ms`).
    /// `fence_ok` is consulted before EVERY attempt (a false answer sends no further attempt), before
    /// EVERY backoff sleep (a fence lost mid-loop aborts instantly, never after a pointless sleep), and
    /// once more before a `Committed` return (a false answer there means the write may have landed but
    /// this call reports `Unresolved`, never a false `Committed`). A sleep is
    /// never entered when it (plus one more attempt) could not fit the operation deadline. An uncertain
    /// attempt is resolved via `resolveByExactGet` before deciding whether to reissue.
    /// Throws `CORRUPTED_DATA` if resolution ever observes DIFFERENT valid bytes at `key` — a real
    /// conflict, never collapsed into `Unresolved`/`DefiniteFailure`. Returns `Unresolved` (never
    /// throws) when the fence is lost or the budget is exhausted before a definite outcome is reached.
    ///
    /// THE VERDICT IS THE CALL'S, NOT THE LAST ATTEMPT'S. `DefiniteFailure` is returned only when EVERY
    /// attempt this call sent was itself proven never applied. One attempt's whitelisted rejection
    /// proves nothing about an EARLIER attempt of the same call that went ambiguous: that request may
    /// have been received and may still materialize at `key` (an absent resolve GET is not evidence —
    /// `unresolvedProvesNothingWasSent`). Any such attempt therefore dominates the result, which becomes
    /// `Unresolved`/`DefiniteFailureAfterAmbiguity` — the wedge path — because a caller acting on
    /// `DefiniteFailure` declares the key unwritten and reuses the id (`CasRefLedger::commitRefChunk`),
    /// which an ambiguous predecessor can turn into an acked-then-lost transaction. Attempts a
    /// pre-attempt gate refused never reach the backend, so they never make this call ambiguous.
    /// `out_token` (optional): set ONLY on a `Committed` return, to the committed incarnation's token —
    /// the attempt's own `PutResult` token, or the token the resolve GET observed when it proved an
    /// earlier ambiguous attempt landed. Lets audit emitters (e.g. `PartWriteTxn::stageManifest`'s
    /// `ManifestPut` event) keep the token without a follow-up HEAD. Untouched on any other return.
    /// `out_reason` (optional): WHY an `Unresolved` was returned. Diagnostic only — the returned
    /// outcome is unchanged, so no caller's decision depends on it. It exists because `Unresolved`
    /// currently conflates two very different situations, and the resulting message
    /// ("retry budget exhausted") is printed even where NOTHING was ever sent: finding #37 defect 3,
    /// whose own note records that the opacity "plausibly fed 3 prior wrong analyses" — and it did so
    /// again on 2026-07-24, when a sanitizer-slow unit test fenced itself and the text sent the CI
    /// triage looking for a retry problem that did not exist. `NoAttemptSent` is the load-bearing
    /// distinction: it means the key was provably never written, whereas the other reasons leave a
    /// possibly-durable object behind.
    CasWriteOutcome putIfAbsentControlled(std::string_view key, std::string_view bytes,
                                          const std::function<bool()> & fence_ok, Token * out_token = nullptr,
                                          CasUnresolvedReason * out_reason = nullptr);

    /// One-shot exact-key resolution of an uncertain immutable create:
    ///   - identical bytes observed at `key`  -> Committed (the earlier attempt DID commit)
    ///   - DIFFERENT bytes observed at `key`  -> throws CORRUPTED_DATA (a real conflict, not a retry
    ///     signal — never silently treated as ambiguous)
    ///   - absent, or the GET itself fails    -> Unresolved (another attempt may still be legal)
    /// NEVER returns DefiniteFailure: an absent or unreadable key proves nothing about whether the
    /// original request will eventually be provably non-applied, so resolution alone can never produce
    /// that verdict. `out_token` (optional): set ONLY on `Committed`, to the observed incarnation's token.
    CasWriteOutcome resolveByExactGet(std::string_view key, std::string_view expected_bytes,
                                      Token * out_token = nullptr);

    /// Controlled If-Match overwrite with resolve-before-reissue, for a MUTABLE marker whose bytes
    /// are deterministic so GET-based resolution can compare them (unlike a content-addressed
    /// create's freshly-minted-per-attempt body). Performs at most `budget.max_attempts` attempts of
    /// the exact SAME (key, bytes, expected token), bounded by `budget.operation_deadline_ms`, with
    /// the same fence/backoff/deadline gates as `putIfAbsentControlled`. An ambiguous attempt
    /// (`PreconditionFailed`, or a transient exception classified `Unresolved`) is resolved with ONE
    /// GET at `key`:
    ///   - the current token still equals `expected`  -> the attempt provably never applied; another
    ///     attempt of the SAME (key, bytes, expected) is legal (fence/backoff/deadline-gated)
    ///   - the current bytes equal `bytes`             -> Committed (an earlier ambiguous attempt of
    ///     THIS call already landed); `token` is the observed incarnation
    ///   - neither                                      -> Conflict: a genuine competing write
    ///     landed; returned as a value, never thrown
    ///   - the GET itself fails                         -> still ambiguous; reissue is safe
    /// A whitelisted `DefiniteFailure` classification, or a deterministic local failure
    /// (`isDeterministicLocalFailure`), rethrows the original exception rather than collapsing it
    /// into an outcome.
    CasOverwriteResult putOverwriteControlled(std::string_view key, std::string_view bytes,
                                              const Token & expected, const std::function<bool()> & fence_ok);

    /// Controlled overwrite with a caller-owned absolute deadline, cancellation/lifecycle cause,
    /// interruptible retry wait, and contained diagnostic observer. Stop and deadline gates run
    /// before every backend request, before and after every wait, and before accepting a proven
    /// commit. The physical-attempt limit is considered only when another `PUT` would be sent, so the
    /// exact resolving `GET` for the final sent attempt is never suppressed.
    CasOverwriteResult putOverwriteControlled(
        std::string_view key,
        std::string_view bytes,
        const Token & expected,
        const CasOverwriteOperationContext & context);

    /// Controlled put-if-absent for a MUTABLE marker whose bytes are deterministic, where an
    /// EXISTING DIFFERENT value at the key is a normal outcome (Conflict), not corruption. This is
    /// the create-side sibling of `putOverwriteControlled` and deliberately does NOT reuse
    /// `putIfAbsentControlled`: that method's resolve (`resolveByExactGet`) throws `CORRUPTED_DATA`
    /// on any different bytes at the key, which is correct for the ref-log lane's immutable,
    /// content-addressed keys (a different value there truly is impossible-by-construction) but
    /// wrong for a mutable state marker (e.g. a blob's freshness-meta sidecar), where a
    /// pre-existing DIFFERENT value is an expected, non-corrupt state a racing writer or GC pass
    /// left behind. Performs at most `budget.max_attempts` attempts of the exact SAME (key, bytes),
    /// bounded by `budget.operation_deadline_ms`, with the same fence/backoff/deadline gates as
    /// `putIfAbsentControlled`. An ambiguous attempt (`PreconditionFailed`, or a transient exception
    /// classified `Unresolved`) is resolved with ONE GET at `key`:
    ///   - absent                          -> the attempt provably never applied; another attempt of
    ///     the SAME (key, bytes) is legal (fence/backoff/deadline-gated)
    ///   - present, bytes equal `bytes`     -> Committed (an earlier ambiguous attempt of THIS call,
    ///     or a racing writer creating the identical value, already landed); `token` is the observed
    ///     incarnation
    ///   - present, bytes differ           -> Conflict: something else already occupies the key with
    ///     a different value; returned as a value, never thrown
    ///   - the GET itself fails            -> still ambiguous; reissue is safe
    /// Same DefiniteFailure/deterministic-local-failure rethrow convention as `putOverwriteControlled`.
    CasOverwriteResult putIfAbsentControlledMutable(std::string_view key, std::string_view bytes,
                                                    const std::function<bool()> & fence_ok);

    /// A DEDICATED RAW slot-occupy primitive [codex finding 3]: exactly ONE fence/deadline-gated
    /// conditional create of `bytes` at `key`; on conflict, exactly ONE raw exact `GET` of the
    /// occupant. NEVER retries internally, NEVER lists, and NEVER composes `putIfAbsentControlled`
    /// (which retries the SAME (key, bytes) internally) or `resolveByExactGet` (which compares against
    /// an expected body and throws `CORRUPTED_DATA` on a mismatch) — both contradict "one conditional
    /// create" and `Occupied(bytes, token)` respectively. This is the primitive every seal writer and
    /// wedge retry uses (spec INV-2): each CALL is one bounded attempt, and a caller that wants to keep
    /// trying calls this again later, under its OWN fence/deadline/backoff discipline.
    ///
    /// `fence_ok` and the operation deadline are checked before the (only) create attempt — a refusal
    /// there sends nothing and reports `Unresolved`/`NoAttemptSent`, exactly like every other
    /// controlled op's first iteration. If the create conflicts or is ambiguous, `fence_ok` is checked
    /// once more immediately before its resolution GET; refusal starts no GET and reports
    /// `Unresolved`/`AttemptsExhausted`, because the create was already sent. There is deliberately no
    /// post-I/O fence recheck after either request: verifying that a `Created`/`Occupied` result is still
    /// relevant remains the caller's contract (Task 4/6's recheck under its own state lock).
    ///
    /// CONSEQUENCE, stated bluntly because it is the OPPOSITE of every sibling op's behavior: a
    /// `Created` or `Occupied` returned here may come from a call whose fence was lost WHILE the PUT or
    /// GET was in flight — this primitive does not know and does not check. Acting on either result
    /// (adopting, acknowledging, installing) without the caller's OWN post-I/O
    /// `checkFenceOrThrow(admitted_generation)` under its own lock is a correctness bug, not a missed
    /// diagnostic — see Task 4's `resolveWedgeOnce` and Task 6's recovery CAS-walk in the plan for the
    /// exact recheck shape.
    ///
    /// A whitelisted synchronous rejection (`classifyConditionalWriteResult`'s `DefiniteFailure`) or a
    /// deterministic local failure (`isDeterministicLocalFailure`) RETHROWS the original exception
    /// unchanged — the same convention as `putOverwriteControlled`/
    /// `putIfAbsentControlledMutable` (`SlotOccupyResult::Kind` has no `DefiniteFailure` member to carry
    /// it). Any other exception, or a clean `PreconditionFailed`, is ambiguous and falls through to the
    /// resolve GET identically — this primitive cannot and does not distinguish the two.
    ///
    /// Op-count contract (asserted by every `gtest_cas_slot_occupy.cpp` test): `Created` costs exactly
    /// one backend op (the create); `Occupied` costs exactly two (the create, then the resolve GET);
    /// `Unresolved` costs at most two (zero when a pre-attempt gate refuses, one when admission is lost
    /// before resolution, otherwise the create plus a resolve GET that came up empty or failed).
    SlotOccupyResult slotOccupy(std::string_view key, std::string_view bytes,
                                const std::function<bool()> & fence_ok);

    /// Test-only: replace the inter-attempt backoff sleep (e.g. with a no-op) on an already-constructed
    /// controller — for tests that reach the controller only through a fully-wired Pool/disk and cannot
    /// pass the ctor parameter (see `Pool::setCasRetrySleepForTest`). Passing an empty function restores
    /// the real sleep. Not thread-safe: call before driving any traffic through the controller.
    void setSleepFnForTest(std::function<void(uint64_t)> sleep_ms_);

private:
    CasOverwriteResult putOverwriteControlledImpl(
        std::string_view key,
        std::string_view bytes,
        const Token & expected,
        const CasOverwriteOperationContext & context,
        bool preserve_legacy_gates);

    /// The gate between a completed ambiguous attempt and its reissue: fence check FIRST (a fence lost
    /// mid-loop must abort before any sleep), then the capped-exponential backoff sleep — skipped
    /// entirely (returning false, no sleep served) when the sleep plus one more attempt could not fit
    /// the operation deadline. Returns true when the loop may proceed to the next attempt; the loop
    /// top's own pre-attempt fence/deadline checks re-run AFTER the sleep.
    ///
    /// `out_reason` (optional) receives WHICH of the two refusals returned false, so a caller reporting
    /// an `Unresolved` from here does not have to guess between them. Both are mid-way by construction:
    /// this gate is only reached once an attempt has been sent.
    bool pauseBeforeReissue(uint32_t completed_attempt, uint64_t deadline_ms, const std::function<bool()> & fence_ok,
                            CasUnresolvedReason * out_reason = nullptr);
    /// The backoff scheduled before attempt `next_attempt` (attempt 2 sleeps `retry_initial_backoff_ms`,
    /// doubling per reissue), saturating at `retry_max_backoff_ms`. 0 when backoff is disabled.
    uint64_t backoffBeforeAttempt(uint32_t next_attempt) const;

    BackendPtr backend;
    CasRequestBudget budget;
    std::function<uint64_t()> now_ms;
    std::function<void(uint64_t)> sleep_ms;
};

}
