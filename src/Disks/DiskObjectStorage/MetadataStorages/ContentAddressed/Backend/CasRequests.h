#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasFence.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasEtag.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasHotKeys.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRetry.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasWriteResult.h>
#include <IO/ReadBuffer.h>
#include <base/types.h>

#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace DB::Cas
{

/// TRUE when the store's own answer proves this write never applied: a malformed request, an entity
/// too large, an access denial, or a credential failure. Whether a STALE CREDENTIAL explains it is not
/// asked here -- the engine asks the backend for fresh credentials first, and refuses only when no
/// refresh HELPED: the error was outside the credential class, the one refresh this call is allowed
/// installed nothing, or there is no reissue left to sign with what it did install. A refresh that
/// helps re-sends the attempt, which is known not to have applied. A non-S3 exception is never a
/// refusal: an unmodeled error may have landed.
bool isDefinitelyRefusedWrite(const std::exception & e);

/// TRUE when a transport failure's text says the CONNECTION itself failed: no free local port, a
/// refused or unreachable peer, or the connect poll's own timeout. A hint, not a verdict: the same
/// errno can be reported after `send` or `recv`, so a hinted attempt keeps every property of an
/// ambiguous one; what the hint changes is only that the engine reissues before spending a read.
bool isConnectFailureHint(const std::exception & e);

/// TRUE when a FIRST physical attempt (`attempt_no == 1`) failed with the adaptive first-attempt
/// timeout: an `S3Exception` naming `NETWORK_CONNECTION` whose text is the generic transport-timeout
/// one, not a connect-failure hint (checked first, so a hinted attempt stays hinted -- the failed
/// connection it names is a different condition from the fuse, and must not be claimed by it). A
/// connection-quality answer about a fresh connection, not a store fault -- attempt 2 runs under the
/// full attempt budget, so the right response is to re-send at once rather than pace it like a fault.
bool isFirstAttemptFuseTimeout(const std::exception & e, size_t attempt_no);

/// Deterministic caller/local bugs, surfaced unchanged by every loop here: reissuing only replays the
/// same failure and buries the root cause behind a retryable exception. The set is `LOGICAL_ERROR`,
/// `NOT_IMPLEMENTED`, `BAD_ARGUMENTS` and `CORRUPTED_DATA`.
bool isDeterministicLocalFailure(int code);

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
/// SCOPE: only the ESCAPING retry-later throws route here (fence lost or a controlled write outcome
/// remaining uncertain). Startup/decommission and generic live-lock-brake `ABORTED` values keep
/// their meaning and are not rerouted here.
[[noreturn]] void throwCasWriteRetryLater(const String & why);

/// Same classification as `throwCasWriteRetryLater`, but returns the exception as a
/// `std::exception_ptr` for call sites that fail a pending future/promise rather than throw directly.
/// Both entry points route through the SAME construction internally, so the error code / message shape
/// has exactly one place that decides it.
std::exception_ptr makeCasWriteRetryLaterExceptionPtr(const String & why);

/// Throw the recoverable "this content-addressed disk cannot serve the request right now" condition.
/// Sibling of `throwCasWriteRetryLater`, same class for the same reasons, differing only in what it
/// describes: that one names a WRITE whose commit did not land, this one names a DISK STATE that
/// refused the request before it started -- on either plane.
///
/// `subject` names the refusing disk or pool (e.g. "content-addressed disk 'ca'") and `condition` states
/// the CA condition truthfully, INCLUDING any promise about how it clears -- only the site knows whether
/// it can make one. What is appended HERE is the classification alone, so it cannot drift between call
/// sites. Unlike `throwCasWriteRetryLater` this deliberately does not log: these sites can fire
/// repeatedly per refused operation and every caller already reports the exception it receives.
[[noreturn]] void throwCasTransientUnavailable(const String & subject, const String & condition);

/// The failure class a FRESH CREDENTIAL could fix -- a subset of `isDefinitelyRefusedWrite`, exposed
/// because a caller whose own loop makes the next physical attempt must not treat it as terminal: the
/// engine refreshes once before it gives the answer, and the caller's next attempt signs with what the
/// refresh installed.
bool isRefreshableCredentialError(const std::exception & e);

/// Facts the fence cannot see, sampled by the caller. Non-throwing; FALSE ends the operation exactly
/// like a lost fence, because the engine does not need to know which of the two refused.
using Liveness       = std::function<bool()>;
/// `decide` sees the current object (or absence) and returns the bytes to write, or nullopt for
/// "nothing to do". It may throw: the exception is the caller's control flow and propagates unchanged.
using DecideOnObject = std::function<std::optional<String>(const std::optional<Object> &)>;
using DecideOnMeta   = std::function<std::optional<String>(const std::optional<Meta> &)>;

/// One key returned by `list`. `etag` is present only on a backend that surfaces per-key
/// incarnations through LIST -- see `Backend::supportsListTokens`.
struct ListedKey
{
    String key;
    uint64_t size;
    std::optional<Etag> etag;
};
/// One page of an enumeration. `next_cursor` resumes strictly after the last returned key; empty
/// marks the end.
struct ListPage
{
    std::vector<ListedKey> keys;
    String next_cursor;
};
/// The walk's callback: FALSE stops the walk.
using ListedKeyFn = std::function<bool(const ListedKey &)>;

namespace detail
{
/// The engine's per-attempt counters, behind functions so the header need not declare the events.
void recordAttempt();
void recordReissue();
void recordConflictPause();
void recordFirstAttemptFuse();
}

class CasOperation;

/// The only caller of `Backend`. It owns the three things a physical request must be measured
/// against -- the transport, the mount fence, and the clock -- and it is the sole minter of
/// `Etag`, so a caller can hold one only by way of a request this class admitted.
///
/// It is constructed with a fence because a fence is a property of whoever holds the lease, not of a
/// call: the mount plane passes the mount fence, the GC plane and the offline tools an open one. A
/// verb is never called here; verbs live on `CasOperation`, which carries the generation the caller
/// was admitted under.
class CasRequests
{
public:
    /// `now_ms` defaults to `CLOCK_BOOTTIME` milliseconds -- the same clock a mount lease deadline is
    /// expressed on, so `Retry::untilLeaseSafe` and this engine compare like with like. `sleep_ms`
    /// defaults to a real sleep. `attempt_reservation_ms` is taken from the backend's own attempt
    /// envelope (attempt timeout plus its connect caps): it is what the engine reserves before it
    /// starts anything. `hot_keys` is the pool's
    /// write lane, shared by its planes; without one this object owns a private lane with no cache,
    /// so a write through it costs today's read and write.
    CasRequests(BackendPtr backend_, Fence fence_,
                std::function<uint64_t()> now_ms_ = {}, std::function<void(uint64_t)> sleep_ms_ = {},
                CasHotKeys * hot_keys_ = nullptr);

    /// Both may be called concurrently on one `CasRequests`: neither writes a member, and the only
    /// state either reads is the backend and the fence -- whose closures must therefore be thread-safe
    /// too. The test setters below DO write members, and belong to setup, before any operation runs.
    ///
    /// Admitted now, under the fence's current generation.
    CasOperation admit(Liveness liveness = {});
    /// Admitted earlier: the generation came from a persisted runtime record, and an operation that
    /// resumes under a generation the fence has since moved past gives up rather than writing.
    CasOperation resume(uint64_t admitted_generation, Liveness liveness = {});

    /// The capability predicates and `dialect()`.
    Backend & backendForCapabilityPredicates() { return *backend; }

    /// A caller's own inter-iteration wait, paced through the same clock the engine's own sleeps use --
    /// so a test that replaces the sleep sees no real time pass in either. `CasOperation` carries the
    /// same call for the loops that hold an operation rather than the plane it was admitted on.
    void pause(uint64_t ms) { sleep_ms(ms); }

    void setNowFnForTest(std::function<uint64_t()> now_ms_);
    void setSleepFnForTest(std::function<void(uint64_t)> sleep_ms_);
    void setAttemptReservationForTest(uint64_t ms) { attempt_reservation_ms = ms; }

    /// What every write on this plane reserves before it starts an attempt (the backend's own attempt
    /// envelope). A caller that derives its OWN policy window from a write's cost -- rather than from a
    /// constant that predates this reservation -- reads it here instead of duplicating the backend call.
    uint64_t attemptReservationMs() const { return attempt_reservation_ms; }

private:
    friend class CasOperation;
    /// `CasOperation::owner` is a `CasRequests &`: `CasOperation`'s own friendship with `CasHotKeys`
    /// does not extend to what that reference points at, so the lane needs its own grant to reach the
    /// clock and sleep it reads and paces through `op.owner`.
    friend class CasHotKeys;

    /// The one place a transport key is created. Every verb reaches the store through this, so no
    /// engine code -- and nothing outside it -- can name the key's type, let alone construct one.
    /// `attempt_no` is the caller's own 1-based physical-attempt count, threaded to the transport
    /// through `TransportAccess::attemptNo()` so a reissue is seen as attempt >= 2.
    template <typename Fn>
    auto withTransportAccess(size_t attempt_no, Fn && fn)
    {
        TransportAccess access(attempt_no);
        return std::forward<Fn>(fn)(access);
    }

    /// The store's answer for `key`, as an incarnation. Throws `CORRUPTED_DATA` naming the key when
    /// the value fails this backend's dialect grammar.
    Etag mint(const String & key, String value) const;
    /// `mint` without the verdict, for the one caller that must treat a malformed value as an
    /// ambiguity to settle by reading rather than as corruption: a write's own 2xx response.
    std::optional<Etag> tryMint(const String & key, String value) const;
    /// The transport value to send as a precondition. Throws `LOGICAL_ERROR` when the incarnation
    /// names another key or another backend -- a precondition built from it would silently mean
    /// something else.
    const String & valueFor(const String & key, const Etag & inc) const;

    BackendPtr backend;
    Fence fence;
    std::function<uint64_t()> now_ms;
    std::function<void(uint64_t)> sleep_ms;
    uint64_t attempt_reservation_ms;
    /// The private lane of a `CasRequests` built without a pool; null when `hot_keys` is the pool's.
    std::unique_ptr<CasHotKeys> own_hot_keys;
    CasHotKeys * hot_keys;
};

/// The cap on one `removeManyWriteOnce` chunk -- also the ceiling a batch-delete request can carry.
inline constexpr size_t kBulkDeleteMaxKeys = 1000;

/// One admitted operation: the unit a policy, a fence generation and a liveness predicate apply to.
/// Move-only, and every request it makes re-checks its admission -- before each attempt, before each
/// sleep, and once more after a proven commit, so a write whose fence was lost while it was in flight
/// is never reported as committed.
///
/// SINGLE-THREADED: it carries mutable per-call state, so one operation belongs to one task. A caller
/// that fans work out gives each task its own, built from `generation()` through `CasRequests::resume`.
class CasOperation
{
public:
    CasOperation(CasOperation &&) = default;
    CasOperation(const CasOperation &) = delete;
    CasOperation & operator=(const CasOperation &) = delete;

    uint64_t generation() const { return admitted_generation; }
    /// A caller's own inter-iteration wait, paced through the same clock the engine's own sleeps use --
    /// so a test that replaces the sleep sees no real time pass in either. For the hand-written loops
    /// that reissue something the engine must not reissue for them.
    void pause(uint64_t ms) { owner.sleep_ms(ms); }
    /// The verdict point: is this operation still admitted? For the sites that guard a decision rather
    /// than a request.
    bool admitted() const { return gate(0) == Gate::Ok; }

    /// The write lane for keys several writers of this pool share.
    CasHotKeys & hotKeys() const { return *owner.hot_keys; }

    /// `policy` with its window turned into an absolute deadline on this operation's clock, taken NOW.
    /// A hand-written loop freezes its policy once before it starts and passes the frozen value to
    /// every call it makes, so the loop ends when the window it was given ends -- rather than granting
    /// each verb of each iteration a fresh one, which is how a bounded document promise became hours
    /// of paced retrying. A policy that already carries a deadline is returned unchanged, so freezing
    /// twice cannot extend it.
    Retry freeze(const Retry & policy) const;

    std::optional<Object> read(const String & key, const Retry & policy);
    std::optional<Meta>   head(const String & key, const Retry & policy);
    ListPage               list(const String & prefix, const String & cursor, size_t limit, const Retry & policy);
    /// Walks every key under `prefix` exactly once. The policy governs EACH PAGE, not the walk: a walk
    /// is an unbounded number of requests, and a silently truncated enumeration is the error a
    /// coverage record exists to prevent. `on_page_fetched` fires once per page DELIVERED; a page that
    /// took several reissues still fires once, and `CASRequestAttempt` is the physical count.
    void forEachListedKey(const String & prefix, const ListedKeyFn & fn, const Retry & per_page,
                          size_t page_limit = 1000, const std::function<void()> & on_page_fetched = {});
    Removal remove(const String & key, const Etag & seen, const Retry & policy);
    /// `head` then `remove` of what it saw, repeating on `Mismatch`. `Gone` when the key is already
    /// absent; never returns `Mismatch` -- under `once`, where there is no reissue to resolve one, a
    /// `Mismatch` is the retry-later throw the read verbs use when their policy is exhausted.
    Removal removeCurrent(const String & key, const Retry & policy);
    /// Deletes ONE chunk of up to `kBulkDeleteMaxKeys` write-once keys as one request under the
    /// policy: admission, fence, budget, deadline, backoff and reissue exactly as `remove`. A reissue
    /// resends the whole chunk; a key the failed attempt already deleted is absent, and absence is
    /// success. Throws when the policy is exhausted. More keys than the cap is a caller bug: the
    /// consumer chunks, so that every chunk that succeeded is recorded before a later one can fail.
    void removeManyWriteOnce(const std::vector<WriteOnceKey> & keys, const Retry & policy);
    /// The one primitive that reports failure as a value, so the policy reissues on the OUTCOME:
    /// `Indeterminate` is retried, the four authoritative outcomes return at once, and an
    /// `Indeterminate` that outlives the bound is returned rather than thrown. Admission refused before
    /// the first attempt still throws -- nothing was probed.
    SentinelProbeResult probeSentinel(const String & key, const Retry & policy);
    /// The OPEN is under the policy; the body is the SDK's.
    std::unique_ptr<ReadBuffer> stream(const String & key, const Retry & policy);
    /// The INITIATION is under the policy; the transfer is the SDK's.
    void publish(const BlobPublishRequest & request, const Retry & policy);

    WriteResult create(const String & key, const String & bytes, const Retry & policy);
    WriteResult replace(const String & key, const String & bytes, const Etag & seen, const Retry & policy);
    /// Read, decide, write, and re-decide on conflict against what the write's own resolve read
    /// already observed. `decide` returning nullopt is `Declined`.
    WriteResult readModifyWrite(const String & key, const DecideOnObject & decide, const Retry & policy);
    /// The same loop over `head`, settling a refused precondition with a `head` too. Proving that an
    /// ambiguous attempt landed needs the bytes, so that one path does read a body; either way the verb
    /// reports a `Meta` and never an `Object`.
    WriteResult readModifyWriteOnPresence(const String & key, const DecideOnMeta & decide, const Retry & policy);

private:
    friend class CasRequests;
    /// The lane waits on this operation's own admission and reads and writes through its verbs; it
    /// needs the gate, the reservation, the resolve read and the give-up helpers, never the transport.
    friend class CasHotKeys;

    CasOperation(CasRequests & owner_, uint64_t admitted_generation_, Liveness liveness_)
        : owner(owner_), admitted_generation(admitted_generation_), liveness(std::move(liveness_))
    {
    }

    enum class Gate : uint8_t { Ok, FenceLost, NoBudget };
    /// The admission point: the fence for `needed_ms` from now, then the caller's own facts.
    Gate gate(uint64_t needed_ms) const;

    /// Everything ONE logical write call accumulates. `attempts_sent`, `sent_any`, `last_seen`,
    /// `reissues` and `refresh_attempted` outlive each attempt and, for `readModifyWrite`, each inner
    /// write, so a `GaveUp` reports what the whole call did rather than what its last attempt did.
    struct WriteState
    {
        uint32_t attempts_sent = 0;
        bool sent_any = false;
        /// The one field that belongs to the INNER write instead: did any of ITS attempts end without
        /// proof of whether it applied? Every attempt of one inner write sends the same bytes, which is
        /// what makes "the resolve read found our bytes" a statement about an attempt of ours; an inner
        /// write that ended in `Conflict` saw the precondition move, which proves its ambiguous
        /// attempts dead. A credential answer never sets it: the store gives one before applying
        /// anything.
        bool any_ambiguous = false;
        Observation last_seen = NotObserved{};
        uint32_t reissues = 0;
        bool refresh_attempted = false;
    };

    /// Why a read-class request stopped without an answer. Every give-up below throws the same
    /// `NETWORK_ERROR`, so a caller that SWALLOWS the exception -- only the resolve read does -- cannot
    /// recover from it which bound refused, and reporting a lease refusal as a policy deadline is the
    /// confusion `GaveUp::Source` exists to prevent. `PolicyExhausted` names the `Retry` bound, whose
    /// own source is `Bound::lease_bound`.
    enum class ReadStop : uint8_t { FenceLost, NoBudgetLease, PolicyExhausted };

    /// What the resolve read saw, and why it stopped when it saw nothing. `stop` is set ONLY when a
    /// bound refused; a read that failed at the transport leaves it empty, and that is the one case
    /// `NotObserved` is still the whole story.
    struct Resolved
    {
        Observation seen;
        std::optional<ReadStop> stop;
    };

    /// One read-class request under the policy: admission, attempt, classification, jittered reissue.
    /// Returns whatever `once` returns, or throws -- the read surface reports failure by exception.
    template <typename Fn>
    auto readLoop(std::string_view verb, const String & subject, const Retry & policy,
                  const Retry::Bound & bound, Fn && once);

    std::optional<Object> readUnder(const String & key, const Retry & policy, const Retry::Bound & bound);
    std::optional<Meta>   headUnder(const String & key, const Retry & policy, const Retry::Bound & bound);
    ListPage               listUnder(const String & prefix, const String & cursor, size_t limit,
                                    const Retry & policy, const Retry::Bound & bound);
    Removal               removeUnder(const String & key, const String & expected_value,
                                      const Retry & policy, const Retry::Bound & bound);

    /// How a write settles a REFUSED PRECONDITION, which needs only to know what is at the key. An
    /// ambiguous attempt always reads the body, whichever this says, because only the bytes can prove
    /// the attempt landed.
    enum class ResolveWith : uint8_t { Body, Presence };

    /// The write engine: one call, any policy. `Committed` and `Conflict` are proven by an exact read
    /// or by the reissue's own 2xx before they are reported; an attempt whose transport error named a
    /// failed connection is reissued before its read. `Refused`, `Declined` and `GaveUp` report what
    /// the store or the bounds said.
    WriteResult writeLoop(const String & key, const String & bytes, const std::optional<Etag> & expected,
                          const Retry & policy, const Retry::Bound & bound, WriteState & state,
                          ResolveWith resolve_refusal_with);
    /// The resolve read: an exact read under the same policy and deadline, reporting what it saw and,
    /// when it saw nothing, which bound stopped it.
    Resolved observe(const String & key, const Retry & policy, const Retry::Bound & bound);
    /// The presence-only sibling, for the one loop that must not fetch a body.
    Resolved observePresence(const String & key, const Retry & policy, const Retry::Bound & bound);

    WriteResult postCommit(Etag inc, bool resolved_by_read, WriteState & state, const Retry::Bound & bound);
    WriteResult gaveUp(GaveUp::Why why, GaveUp::Source source, WriteState & state) const;
    /// The bound that refused the resolve read, reported as the outcome it actually is.
    WriteResult gaveUpForReadStop(ReadStop stop, WriteState & state, const Retry::Bound & bound) const;
    /// A resolve read that produced nothing. The fence is NOT resampled: a second sample reports a
    /// state the read never saw, which is how a lease refusal used to be reported as a policy deadline.
    WriteResult gaveUpAfterFailedObservation(std::optional<ReadStop> stop, WriteState & state,
                                             const Retry::Bound & bound) const;
    /// The shared shape behind every gated pause below: admission for `envelopes` attempt reservations
    /// plus `pause_ms`, the deadline check, the counter this pause records itself under, then the sleep
    /// -- called even with a zero `pause_ms` UNLESS `should_sleep` is false, which is reserved for the
    /// fuse's zero-pause reissue (a pace of "no pause", not "a zero-length one"). A value means the call
    /// ended during it; nullopt means the caller may send another attempt.
    std::optional<WriteResult> gatedPause(uint64_t pause_ms, uint32_t envelopes, WriteState & state,
                                         const Retry::Bound & bound, void (*record)(), bool should_sleep);
    /// Admission, then the jittered sleep. A value means the call ended during it; nullopt means the
    /// caller may send another attempt.
    std::optional<WriteResult> pauseAndReissue(WriteState & state, const Retry::Bound & bound);
    /// The sibling for a clean lost race: the same admission and the same reservation, a flat
    /// `Retry::conflictBackoff` sleep, and `state.reissues` untouched, so a transport fault that follows
    /// starts its own schedule at the beginning.
    std::optional<WriteResult> pauseForConflict(WriteState & state, const Retry::Bound & bound);
    /// The sibling for a failure text that named a failed connection. The same admission and the same
    /// reservation, a flat `kConnectHintPauseMs` sleep, and `state.reissues` untouched.
    std::optional<WriteResult> pauseFlat(WriteState & state, const Retry::Bound & bound);
    /// The sibling for a first-attempt fuse timeout: the same admission and the same reservation, NO
    /// sleep at all, and `state.reissues` untouched -- the fuse is a connection-quality answer about a
    /// fresh connection, not a store fault, so nothing here is paced against it.
    std::optional<WriteResult> reissueAtOnce(WriteState & state, const Retry::Bound & bound);

    /// `sleep_ms` plus `envelopes` attempt reservations, saturating.
    uint64_t reservedFor(uint64_t sleep_ms, uint32_t envelopes) const;
    /// Is there room to START something needing `needed_ms` before the bound? The guarantee is on the
    /// start side: nothing is begun that could not finish inside it.
    bool fits(uint64_t needed_ms, const Retry::Bound & bound) const;

    /// One failed read-class attempt, classified. A credential failure is refreshed HERE so the reissue
    /// signs with the new client, at most once per call -- `refresh_attempted` is the caller's, and a
    /// second credential failure under the same call is classified as if no refresh were available.
    /// `refreshed` is set true only when THIS call installed new credentials, mirroring the write
    /// loop's own local of the same name, so the caller can keep a credential reissue from also
    /// inflating a counter whose text it happens to match. TRUE means the failure must surface unchanged.
    bool refreshAndClassifyReadFault(const std::exception & e, bool & refresh_attempted, bool & refreshed);

    /// Each records its cause in `last_read_stop` before throwing, so the resolve read can report it.
    [[noreturn]] void giveUpReadFenceLost(std::string_view verb, const String & subject, std::string_view when);
    [[noreturn]] void giveUpReadNoBudget(std::string_view verb, const String & subject, std::string_view what);
    [[noreturn]] void giveUpReadDeadline(std::string_view verb, const String & subject,
                                         const Retry::Bound & bound, uint32_t attempts_made);

    CasRequests & owner;
    uint64_t admitted_generation;
    Liveness liveness;
    /// Written immediately before a read-class give-up throws, cleared and read only by the resolve
    /// read that swallows it. Every other caller lets the exception carry the verdict.
    std::optional<ReadStop> last_read_stop;
};

template <typename Fn>
auto CasOperation::readLoop(std::string_view verb, const String & subject, const Retry & policy,
                            const Retry::Bound & bound, Fn && once)
{
    bool refresh_attempted = false;
    /// Two counters, deliberately kept separate: `attempt_no` is the PHYSICAL attempt count handed to
    /// the transport (so a reissue is seen as attempt >= 2); `ordinary_reissues` is the
    /// exponential-backoff index. They advance together on an ordinary failure, but the first-attempt
    /// fuse below advances `attempt_no` alone (via `continue`, skipping the backoff pause) so a
    /// following ordinary failure's backoff still starts from `backoff(1)`, undisturbed.
    for (uint32_t attempt_no = 1, ordinary_reissues = 0;; ++attempt_no)
    {
        const uint64_t reservation = reservedFor(0, 1);
        switch (gate(reservation))
        {
            case Gate::FenceLost: giveUpReadFenceLost(verb, subject, "before the request");
            case Gate::NoBudget:  giveUpReadNoBudget(verb, subject, "for one more request");
            case Gate::Ok: break;
        }
        if (!fits(reservation, bound))
            giveUpReadDeadline(verb, subject, bound, attempt_no - 1);

        detail::recordAttempt();
        try
        {
            return owner.withTransportAccess(attempt_no, [&](auto & access) { return once(access); });
        }
        catch (const std::exception & e)
        {
            bool refreshed = false;
            if (refreshAndClassifyReadFault(e, refresh_attempted, refreshed))
                throw;
            /// Classified -- and counted -- before the single-attempt check below: `Retry::once` forbids
            /// the REISSUE, not the observation that this attempt hit the fuse, and the write path
            /// already counts at classification the same way -- except when `refreshed` is also true: a
            /// credential answer whose text happens to also match the fuse text is a credential reissue,
            /// not a fuse one, and must not inflate this count.
            const bool fuse = isFirstAttemptFuseTimeout(e, attempt_no);
            if (fuse && !refreshed)
                detail::recordFirstAttemptFuse();
            if (policy.single_attempt)
                throw;
            /// A first-attempt fuse is a connection-quality answer, not a store fault: re-check
            /// admission for the reissue alone and send it at once. `ordinary_reissues` stays put, so a
            /// following ordinary failure's backoff starts at `backoff(1)`, exactly as if this attempt
            /// had never happened.
            if (fuse)
            {
                const uint64_t needed = reservedFor(0, 1);
                switch (gate(needed))
                {
                    case Gate::FenceLost: giveUpReadFenceLost(verb, subject, "before the reissue");
                    case Gate::NoBudget:  giveUpReadNoBudget(verb, subject, "for the reissue");
                    case Gate::Ok: break;
                }
                if (!fits(needed, bound))
                    giveUpReadDeadline(verb, subject, bound, attempt_no);
                detail::recordReissue();
                continue;
            }
        }

        const uint64_t pause_ms = Retry::backoff(++ordinary_reissues);
        const uint64_t needed = reservedFor(pause_ms, 1);
        switch (gate(needed))
        {
            case Gate::FenceLost: giveUpReadFenceLost(verb, subject, "before the reissue");
            case Gate::NoBudget:  giveUpReadNoBudget(verb, subject, "for the reissue");
            case Gate::Ok: break;
        }
        if (!fits(needed, bound))
            giveUpReadDeadline(verb, subject, bound, attempt_no);
        detail::recordReissue();
        owner.sleep_ms(pause_ms);
    }
}

}
