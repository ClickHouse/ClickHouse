#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ABORTED;
}
}

namespace DB::Cas
{

/// Durable single-writer-slot state machine used by the per-server merged heartbeat
/// (`CasServerRoot.h`, `MountLeaseKeeper`): anchors a key that has EXACTLY ONE writer, renews it
/// asynchronously off the write path, and ends with a terminal op; any precondition failure during
/// renewal means a foreign touch — fail closed with an exception, never re-mint.
///
/// This base owns the common machinery (the `seq`/`last_token`/`dead` state, the renewal thread and
/// its loop, `renewOnce`/`startBackground`/`stopBackground`, and the start/terminal
/// bookkeeping). The noun ("watermark") and verb ("farewell") used in every fail-closed message are
/// passed to the base constructor. Subclasses differ ONLY in EXPLICIT policy hooks:
///   - `prepareRenew`     — runs OFF the state lock before each body encode, returning a per-call
///                          payload (the watermark reads `min_active` from a Pool callback here).
///                          Keeping it off `state_mutex` is load-bearing: the watermark callback
///                          reaches into the Pool's own lock.
///   - `encodeBody`       — builds the slot's exact JSON bytes for a given `seq` and payload;
///   - `claim`            — the slot-specific anchor sequence in `start` (the watermark's
///                          head→putIfAbsent/putOverwrite dance), returning the token we now hold;
///   - `terminate`        — the terminal op (the watermark's retiring putOverwrite), run under the
///                          state lock after `dead` is set, owning its own fail-closed throws and
///                          final bookkeeping.
///
/// Every observable op (the JSON body bytes, the anchor put sequence, the renew cadence, the
/// foreign-touch fail-close conditions and message wording, the stop semantics) is reproduced
/// exactly by the subclass hooks, with no conditionals in the base that would blur the
/// single-writer/fail-closed contract.
class SingleWriterSlot
{
public:
    /// `slot_name_` is the noun in every message ("watermark"); `terminal_verb_` is the
    /// verb used in the start/renew/terminal guards ("farewell").
    SingleWriterSlot(
        BackendPtr backend_, String key_, std::string_view slot_name_, std::string_view terminal_verb_,
        std::string_view logger_name_);

    /// Stops the background thread only (no terminal op). Destruction without a terminal op leaves
    /// the slot persisted, with its seq no longer advancing, so full GC observes frozen state.
    virtual ~SingleWriterSlot();

    /// seq++ via putOverwrite against the last token we wrote; LOGICAL_ERROR on a foreign touch
    /// (single-writer fail-closed contract).
    void renewOnce();

    /// Starts periodic renewal. The period controls only the wake-up cadence; a failed renewal stops
    /// the loop and is not silently re-armed by a later call.
    void startBackground(std::chrono::milliseconds period);

    /// Requests renewal-thread termination and joins it. Safe to call repeatedly, including after a
    /// renewal failure or from destruction; it does not perform the slot's terminal operation.
    void stopBackground();

protected:
    /// Per-call payload prepared OFF the state lock and handed to `encodeBody`. Subclasses needing
    /// dynamic values (the mount lease's `now_ms` + merged `min_active`) carry them through this opaque
    /// token. `value2` is a second scalar for slots that renew more than one dynamic field per beat (the
    /// merged heartbeat: `value` = now_ms, `value2` = min_active).
    struct RenewPayload
    {
        uint64_t value = 0;
        uint64_t value2 = 0;
    };

    using Token = ::DB::Cas::Token;

    /// === policy hooks (see class comment) ===
    virtual RenewPayload prepareRenew() const = 0;
    virtual String encodeBody(uint64_t seq, const RenewPayload & payload) const = 0;
    virtual Token claim(const String & body) = 0;

    /// === optional background-renewal observation hooks (default no-op) ===
    /// Called from `renewOnce` itself, right after a SUCCESSFUL write is recorded (under
    /// `state_mutex`, on EVERY caller of `renewOnce` — the background loop AND any direct caller,
    /// such as the startup-arm redo in `CasPool.cpp`'s `mountWritable`). The mount-lease keeper
    /// refreshes its confirmed-lease wall deadline here, so a direct `renewOnce` (which never goes
    /// through `onRenewSucceeded` below) still keeps that deadline current. The watermark keeper does
    /// not override it (no-op), so its behavior is unchanged.
    virtual void onRenewCommitted() {}
    /// Called from the background loop after a SUCCESSFUL `renewOnce` (off `state_mutex`, AFTER
    /// `onRenewCommitted` has already run); the mount-lease keeper fires the boot-domain
    /// write-fence callback (`on_renew_ok`) here — this hook is background-loop-only, unlike
    /// `onRenewCommitted` above. The watermark keeper does not override it (no-op), so its behavior
    /// is unchanged.
    virtual void onRenewSucceeded() {}
    /// Called from the background loop when `renewOnce` THREW (the loop is about to stop, off
    /// `state_mutex`); the mount-lease keeper latches the write fence to lost here. Default no-op.
    virtual void onRenewFailed() {}

    /// Called from the background loop when `renewOnce` threw and the
    /// throw was NOT a confirmed mismatch (`onRenewMismatch` -- an observed PUT outcome proving
    /// supersession; see the flag this checks, `last_renew_failure_was_confirmed_mismatch`, in the
    /// private section below). Returning `true` means "treat as terminal now" -- fence immediately, the
    /// legacy behavior and the correct default for a subclass with no lease-deadline concept.
    /// `MountLeaseKeeper` overrides this to ride out a TRANSIENT exception (a `putOverwrite` that threw
    /// before any outcome was observed -- a timeout, 5xx, or connection reset) while its last CONFIRMED
    /// lease has not yet reached its safety-margin boundary: the mount-lease protocol guarantees no
    /// other writer can claim the slot before that deadline, so continuing to retry (not fencing) is
    /// safe. Called OFF `state_mutex`, same as `onRenewFailed`.
    virtual bool shouldFenceOnTransientRenewFailure() { return true; }

    /// Called when the token-guarded renew PUT hits PreconditionFailed. The base contract stays
    /// fail-closed and LOUD; subclasses may re-read and throw a more precisely classified
    /// exception (the mount keeper distinguishes a GC fence of our own expired lease from a
    /// genuine foreign writer). MUST throw — a renew mismatch never continues.
    virtual void onRenewMismatch(const String & mismatched_key);

    /// Runs the slot's terminal op against the held `last_token`. Called under `state_mutex` with
    /// `dead` already set (so renewal can never race it). Owns its own fail-closed throws and final
    /// bookkeeping (the watermark bumps seq/last_token on its retiring putOverwrite).
    /// May throw — `dead` stays set regardless.
    virtual void terminate() = 0;

    /// Anchors the slot for seq=1 — durable when `doStart` returns. Subclasses expose this under their
    /// own public name (`start`). Computes the payload off the lock, then runs the policy `claim`.
    void doStart();

    /// Stops the background thread, takes the state lock, runs the dead/seq guards (e.g.
    /// "double farewell" / "discard before start"), sets `dead`, and delegates the op to `terminate`.
    /// Subclasses expose this under their own public name (`farewell`/`discard`).
    void doTerminate();

    /// Bookkeeping after a successful write of the given seq/token: records seq, the token we now
    /// hold, and the local-clock renew time. Must be called under `state_mutex`.
    void recordWrite(uint64_t new_seq, const Token & token);

    BackendPtr backend;
    String key;

    mutable std::mutex state_mutex;
    uint64_t seq = 0;            /// 0 = not started
    Token last_token;            /// the incarnation WE wrote — the only one we ever renew
    bool dead = false;           /// set by the terminal op

private:
    void backgroundLoop(std::chrono::milliseconds period);

    std::string_view slot_name;
    std::string_view terminal_verb;

    /// Set immediately before `renewOnce` invokes `onRenewMismatch` (which always
    /// throws) so `backgroundLoop`'s catch block can tell a CONFIRMED mismatch (the PUT completed and
    /// observed a foreign token -- proven supersession) apart from a TRANSIENT exception
    /// (`putOverwrite` itself threw before any outcome was observed, or a defensive `dead`/`seq==0`
    /// guard fired). Reset to `false` at the top of every `renewOnce` call. `renewOnce` and
    /// `backgroundLoop` run on the SAME background thread, sequentially, so no synchronization is
    /// needed for this flag.
    bool last_renew_failure_was_confirmed_mismatch = false;

    std::mutex background_mutex;
    std::condition_variable wakeup;
    bool stop_requested = false;
    ThreadFromGlobalPool thread;

    LoggerPtr log;
};

/// Validate a `server_root_id` — the explicit, configured identity of the content-addressed layout
/// subtree a server owns. It is a clean relative path: it composes into the
/// object-key tree (`gc/server-roots/<srid>/...`, `roots/<srid>/...`), so the same hygiene the layout
/// applies to a namespace applies here (mirrors `CasLayout.h::checkNamespace`):
///   - non-empty;
///   - no leading/trailing '/', no empty segment ("//");
///   - no '.' or '..' segment;
///   - total length <= 255;
///   - no segment equal to the reserved "_files" / "_manifests".
/// Throws `ErrorCodes::BAD_ARGUMENTS` on any violation. Fail closed — there is no sanitizing fallback.
inline void validateServerRootId(const String & id)
{
    if (id.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "server_root_id must be non-empty");

    if (id.size() > 255)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
            "server_root_id '{}' is too long ({} > 255 bytes)", id, id.size());

    size_t start = 0;
    while (true)
    {
        size_t end = id.find('/', start);
        const String segment = id.substr(start, end == String::npos ? String::npos : end - start);
        if (segment.empty())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                "server_root_id '{}' has an empty segment (leading/trailing or doubled '/')", id);
        if (segment == "." || segment == "..")
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                "server_root_id '{}' uses a relative segment ('.' or '..')", id);
        if (segment == "_files")
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                "server_root_id '{}' uses the reserved segment '_files'", id);
        if (segment == "_manifests")
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                "server_root_id '{}' uses the reserved segment '_manifests'", id);
        if (end == String::npos)
            break;
        start = end + 1;
    }
}

/// The per-server-root control objects (owner, epoch, and mount lease) and their text codecs are kept
/// in `Formats/CasServerRootFormats`. This header includes those definitions so the protocol logic
/// below can use `OwnerObject`, `ServerEpoch`, `MountLease`, and their `encode`/`decode` functions
/// without duplicating the wire-format implementation.

class Backend;
class Layout;

/// Mount-safety claim logic. These are the identity and epoch-allocation steps a server
/// runs at startup over its `server_root_id` subtree, BEFORE any ordinary data write. They fail
/// closed (`ErrorCodes::CORRUPTED_DATA`); there is no re-mint or silent-recreate fallback.

/// True iff the successfully decoded catalog names no current life owned by `server_root_id`, and
/// exact-component probes find neither `cas/manifests/<srid>/` nor `roots/<srid>/` work. Opaque
/// `cas/ns/` debris alone does not identify a logical owner.
bool serverRootSubtreeEmpty(
    Backend & b, const Layout & l, const String & srid, const RefCatalog & catalog_observation);

/// Supplied by the pool layer so the low-level server-root protocol always observes the mandatory
/// catalog. Every absent-control retry obtains a fresh, successfully decoded observation.
using ObserveRefCatalog = std::function<RefCatalog()>;

/// Read the owner anchor (`gc/server-roots/<srid>/owner`) WITHOUT claiming or validating identity —
/// a plain GET+decode. nullopt = anchor absent. Pool-member decommission uses this to read the
/// victim UUID before mounting writable; `claimOwnerOrThrow` below is the identity-claiming
/// counterpart used by normal opens and reuses this GET+decode path.
std::optional<UInt128> readOwnerUuid(Backend & b, const Layout & l, const String & server_root_id);

/// Claim (or validate) the sticky owner anchor that binds `srid` to a server UUID (identity).
///   - owner present, equal `our_uuid`, and not tombstoned → ok (return);
///   - owner present and tombstoned → throw `CORRUPTED_DATA` (explicitly retired — fail closed);
///   - owner present, different → throw `CORRUPTED_DATA` (foreign owner — fail closed);
///   - owner absent AND the subtree is provably empty → `putIfAbsent` the owner (claim);
///   - owner absent BUT the subtree is non-empty → throw `CORRUPTED_DATA` (identity lost over
///     existing data — never silently re-claim).
/// The owner object is never deleted and never reassigned to a different UUID.
void claimOwnerOrThrow(
    Backend & b, const Layout & l, const String & srid, UInt128 our_uuid,
    const ObserveRefCatalog & observe_catalog);

/// Which mint policy governs `allocateWriterEpoch`'s absent-epoch branch (see below).
enum class EpochMintPolicy : uint8_t
{
    NormalMount,            /// absent-epoch re-mint requires authoritative mount absence
    DecommissionRecovery,   /// absent-epoch re-mint requires a TERMINAL mount; mints a distinct epoch
};

/// Allocate the next durable-monotone `writer_epoch` by CAS-bumping the sticky `epoch` object
/// (`ServerEpoch{next_writer_epoch}`), returning the value the caller adopts as its writer_epoch.
///   - `epoch` absent AND the subtree is non-empty → throw `CORRUPTED_DATA` (missing epoch over
///     data is a reset hazard);
///   - `epoch` absent AND the subtree is empty → the absent-epoch branch is a LIFECYCLE decision,
///     so it uses `probeSentinelRaw`'s authoritative outcomes, never plain `get`-absence (which
///     flattens transport faults into "not found"), to decide whether the mount object is really
///     gone:
///       - `KeyAbsent` → authoritative absence, mint epoch 1 (fresh-root bootstrap);
///       - `Present`, `policy == NormalMount` → throw `CORRUPTED_DATA` (durable epoch state was
///         lost while a mount is live or recently live — refusing to re-mint epoch 1 there is how a
///         same-(uuid, epoch) twin is avoided);
///       - `Present`, `policy == DecommissionRecovery` → the surviving mount must be TERMINAL (not
///         live); a live member throws `ABORTED`, otherwise mint `surviving.writer_epoch + 1` —
///         distinct from the survivor's by construction (`now_ms` is required, nonzero, here);
///       - anything else (`ContainerAbsent`/`AccessDenied`/`Indeterminate`) → throw
///         `CORRUPTED_DATA` (absence was never proven; fail closed);
///   - otherwise read `next = current.next_writer_epoch`, `casPut` `{next + 1}` against the
///     observed token, retry on `Conflict` (bounded), and return `next`.
uint64_t allocateWriterEpoch(Backend & b, const Layout & l, const String & srid,
                             EpochMintPolicy policy, uint64_t now_ms,
                             const ObserveRefCatalog & observe_catalog);

/// Which certificate of death justified a same-uuid, different-epoch mount reclaim. `None` when no
/// reclaim of that kind happened (a fresh claim, a
/// same-epoch refresh, `LiveDoubleStart`, `ForeignOwner`, `FencedSelf`).
enum class MountPriorState
{
    None,
    Clean,             /// the predecessor's own graceful farewell (`min_active == UINT64_MAX`)
    Fenced,            /// the GC leader's own (already threshold-gated) fence-out (`gc_fenced`)
    UncleanObserved,   /// OUR observation watched the write-token hold stable for the full threshold
};

/// Startup decision for the mount lease (`gc/server-roots/<srid>/mount`), run AFTER the owner gate
/// (so `our_uuid` is the established owner). The lease is LIVENESS, not identity — the owner object
/// already settled who may write; the lease settles whether a live incarnation currently holds the
/// slot. Decision over `get(mountKey)`:
///   - absent → write our body via `putIfAbsent` → `Claimed`;
///   - same `server_uuid` AND same `writer_epoch` as (our_uuid, our_epoch) → it is OUR OWN claim
///     (a replay / the keeper adopting it):
///       - `gc_fenced` → terminal for THIS (uuid, epoch) — a fence costs an epoch, so refreshing it
///         in place would resurrect a fenced incarnation → `FencedSelf` (no write);
///       - otherwise → refresh (`putOverwrite` to bump seq + fresh `expires_at_ms`) → `Claimed`;
///   - same `server_uuid`, DIFFERENT `writer_epoch` → reclaimed ONLY on a certificate of death that
///     needs no fresh wall-clock trust (see
///     `claimMountAwaitingExpiry` below for how a plain "looks expired" reading is turned into one):
///       - `gc_fenced` (the GC leader already, itself, threshold-gated this incarnation dead; a fence
///         costs an epoch, so its keeper can never renew again) → reclaim, `prior = Fenced`;
///       - the clean marker (`min_active == UINT64_MAX`, the predecessor's own graceful farewell) →
///         reclaim, `prior = Clean`;
///       - `proven_dead_token` matches the CURRENTLY OBSERVED token (the caller itself watched this
///         exact token hold stable for the full observation threshold) → reclaim, `prior =
///         UncleanObserved`;
///       - none of the above → `LiveDoubleStart` (do NOT write). In particular `expires_at_ms <=
///         now_ms` ALONE is never sufficient — comparing a predecessor's stamp against OUR wall clock
///         is unsafe because a clock-skewed or merely late-observing
///         caller must never conclude death from a bare timestamp read);
///   - different `server_uuid` → `ForeignOwner` (do NOT write, regardless of expiry or prior state).
struct MountClaimResult
{
    /// Plain (unscoped) enum: callers compare with `MountClaimResult::Claimed` directly.
    enum Kind
    {
        Claimed,
        LiveDoubleStart,
        ForeignOwner,
        /// Same (uuid, epoch) as ours, but the body is `gc_fenced`: terminal for THIS epoch — a fence
        /// costs an epoch. The caller must re-open with a fresh `writer_epoch`; refreshing or adopting
        /// a fenced body in place is never correct.
        FencedSelf,
    };
    Kind kind = ForeignOwner;
    MountLease body;
    /// Which certificate of death justified a same-uuid, different-epoch `Claimed` reclaim (`None` for
    /// every other `Kind`, and for the absent-slot / same-epoch-refresh `Claimed` cases).
    MountPriorState prior = MountPriorState::None;
    /// The backend token of the body this result observed, for
    /// `LiveDoubleStart` only (a fresh `Claimed`/`FencedSelf`/`ForeignOwner` write/observe has no
    /// separate "prior body's token to remember" use). `claimMountAwaitingExpiry`'s observation loop
    /// used to re-GET the mount key itself just to recover this token that `claimMount` had already
    /// read one line earlier and thrown away -- one wasted GET per iteration. Empty for every other
    /// `Kind` (nothing to compare against).
    std::optional<Token> token;
};

/// Thrown when a mount operation observes that OUR OWN (uuid, epoch) slot was `gc_fenced` by the GC
/// after our lease expired — a RECOVERABLE state ("a fence costs an epoch"): the caller re-opens with
/// a fresh `writer_epoch`. A CAS-local typed exception rather than a new `ErrorCodes` number: a fork
/// carries these edits indefinitely and the numbered `ErrorCodes` list conflicts with upstream on
/// every rebase. Catch sites match BY TYPE (`catch (const MountFencedException &)`), never by code;
/// the base code is `ABORTED` so an uncaught one still surfaces as a clean startup abort.
class MountFencedException : public DB::Exception
{
public:
    explicit MountFencedException(const String & msg)
        : DB::Exception(msg, DB::ErrorCodes::ABORTED) {}
};

/// `proven_dead_token`: the write-token of a same-uuid, different-epoch lease that the CALLER already
/// proved dead by observation (see `claimMountAwaitingExpiry`) — matching it against the CURRENTLY
/// observed token is the ONLY way (besides `gc_fenced` / the clean marker) a same-uuid different-epoch
/// lease is ever reclaimed. Absent (`{}`, the default) for a bare claim attempt with no such proof.
MountClaimResult claimMount(
    Backend & b, const Layout & l, const String & srid, UInt128 our_uuid, uint64_t our_epoch,
    uint64_t now_ms, uint64_t ttl_ms, const std::optional<Token> & proven_dead_token = {},
    const CasEventSink & sink = {});

/// Format the operator-actionable startup error shown when the mount lease is held by a genuinely
/// live second server (the same `server_root_id` is mounted twice). Produced only AFTER this server
/// has already waited for the lease to lapse (see `claimMountAwaitingExpiry`) and it did not — so the
/// remediation is about a live twin, not about waiting.
String mountDoubleStartMessage(const String & srid, const MountLease & existing);

/// Observation-based mount claim for restart recovery.
/// Wraps `claimMount` in a loop:
///   - first attempt decided immediately for `Claimed` (fresh / refreshed / reclaimed via `Fenced` or
///     `Clean`), `ForeignOwner`, or `FencedSelf`;
///   - a `LiveDoubleStart` from OUR OWN uuid (a stale lease from a prior incarnation of this server,
///     OR a genuinely live twin — the two are indistinguishable from a bare read) is resolved by
///     WATCHING the lease's write-token on OUR OWN clock (`mono_ms_fn`), NEVER by comparing the
///     lease's stamped `expires_at_ms` against any clock: once the observed token has held stable for
///     the full rate-bound threshold (`ttl_ms + ttl_ms / 20 + poll_interval_ms` — the lease TTL, a 5%
///     clock-drift allowance, and one poll interval of discreteness. This rate bound ensures that a
///     holder which last renewed before the observation began can no longer be within its lease.
///     that token is handed to `claimMount` as `proven_dead_token`, which then reclaims token-guarded
///     (`prior = UncleanObserved`). If the token changes DURING the wait (the holder renewed, or a
///     genuine twin is alive) the observation RESTARTS from the new token; bounded to a handful of
///     restarts before giving up and returning the last `LiveDoubleStart` (a holder whose token keeps
///     changing across that many restarts is alive, not dead).
/// `now_ms_fn` is WALL clock, used only for stamping the body we (may) write / diagnostics — it never
/// participates in the reclaim decision. `mono_ms_fn` is the OBSERVATION clock: monotonic on this
/// process, never compared against any other node's clock, and the ONLY clock the threshold is
/// measured against. `sleep_ms_fn` paces the poll. All three, plus `on_wait_start`, are injected so
/// tests drive fake clocks with no real sleeping. `on_wait_start` (default no-op) fires once per
/// observation-window start (including restarts), with the currently-observed lease and the
/// threshold, for an operator-visible startup log.
/// All callers use this shared formula so a future adjustment cannot silently leave the startup
/// observation path and either GC heartbeat path with different thresholds. `cadence_ms` is the
/// caller's own poll or heartbeat interval; the additional interval accounts for observation
/// discreteness, while `ttl_ms / 20` allows for a five-percent clock-rate difference.
uint64_t mountObservationThresholdMs(uint64_t ttl_ms, uint64_t cadence_ms);

MountClaimResult claimMountAwaitingExpiry(
    Backend & b, const Layout & l, const String & srid, UInt128 our_uuid, uint64_t our_epoch,
    const std::function<uint64_t()> & now_ms_fn,
    const std::function<uint64_t()> & mono_ms_fn,
    uint64_t ttl_ms, uint64_t poll_interval_ms,
    const std::function<void(uint64_t)> & sleep_ms_fn,
    const std::function<void(const MountLease &, uint64_t)> & on_wait_start = {},
    const CasEventSink & sink = {});

/// One `server_root_id`'s cross-round token-stability observation,
/// owned by the GC leader instance (`Cas::Gc::mount_obs`) and threaded through consecutive
/// `computeHeartbeatFloor` calls — one GC round is one observation tick. Mirrors
/// `claimMountAwaitingExpiry`'s observation loop, but at heartbeat-gate granularity rather than a
/// tight poll loop.
struct MountTokenObservation
{
    Token token;
    uint64_t first_seen_mono_ms = 0;
};

/// Keyed by `server_root_id`. In-memory only: a fresh leader (after a steal, or a process restart)
/// starts with an empty map, which only delays fencing an already-dead mount by one extra round while
/// it (re)establishes the observation — safe (never fences early), never unsafe.
using MountObservationMap = std::map<String, MountTokenObservation>;

/// GC heartbeat gate (GC round protocol step 1). Run by the GC leader at the top of a round: LIST
/// `gc/server-roots/` (O(servers), single-digit counts), GET each mount body, and classify + fence out
/// dead mounts (liveness only — graduation itself paces on GC rounds, not on heartbeat acks).
/// Classification per body:
///   - `gc_fenced` already set → excluded (`already_fenced`); a fenced mount is terminal, no PUT;
///   - terminated (`min_active == UINT64_MAX`, the farewell sentinel stamped by
///     `MountLeaseKeeper::terminate`) → excluded (`terminated`). `expires_at_ms` alone cannot
///     distinguish a graceful farewell from an unclean stop, so the sentinel — not the timestamps — is the
///     terminated marker;
///   - otherwise, observation-based liveness (the same
///     principle `claimMountAwaitingExpiry` uses for a mount's OWN reopen, applied here to the GC's
///     fence-out): `obs` remembers, per srid, the write-token last seen and the leader's OWN
///     monotonic clock reading (`mono_now_ms`) at the moment it first saw that token. A body whose
///     CURRENT token differs from (or is absent from) `obs` is (re)started fresh — counted `live`,
///     never fenced this call, regardless of what its stamped `expires_at_ms` claims (a bare
///     wall-clock stamp is never trusted — see `claimMount`'s "certificate of death" doc). Only once
///     the SAME token has held for `>= stable_threshold_ms` OF THE LEADER'S OWN CLOCK does the body
///     become FENCE-eligible;
///   - FENCE-eligible → one token-guarded `putOverwrite` preserving the WHOLE body, setting
///     `gc_fenced = true` and `seq + 1`. On `Done` → excluded (`fenced_now`); on `PreconditionFailed`
///     (the holder renewed concurrently — a live token change) → re-GET and reclassify from the top
///     (bounded retries; the reclassify sees the new token and restarts the observation, counting it
///     `live` — conservative, never exclude a heartbeat without a landed fence-out).
///
/// `now_ms` is WALL clock, used only for the audit/diagnostic log line — it never participates in the
/// fence decision (mirrors `claimMountAwaitingExpiry`'s `now_ms_fn` vs `mono_ms_fn` split).
/// `mono_now_ms` is the OBSERVATION clock: the caller's OWN monotonic reading, never compared against
/// any other node's clock. `obs` is owned by the caller and threaded across consecutive calls (one GC
/// leader instance, `Cas::Gc::mount_obs`) — a fresh leader starts with an empty map (safe: delays
/// fencing one round, never fences early).
///
/// The fence-out is BOTH safety and liveness. Safety: a sleeper's later renewal permanently fails
/// (its `putOverwrite` now mismatches the fenced token → `tripMountLost`), so it can never re-arm
/// without a fresh `open`. Liveness: a dead server's stale mount slot must not linger forever.
/// Preserving the body keeps restart recovery intact: a same-uuid reopen reads the current body and
/// reclaims through the normal expired-our-uuid branch.
struct HeartbeatFloor
{
    size_t live = 0;
    size_t terminated = 0;
    size_t fenced_now = 0;
    size_t already_fenced = 0;
    /// The srids of every mount fenced-out THIS call (one GcFenceOut audit event each).
    std::vector<String> fenced_srids;
};

HeartbeatFloor computeHeartbeatFloor(Backend & b, const Layout & l, uint64_t now_ms,
                                     uint64_t mono_now_ms, uint64_t stable_threshold_ms,
                                     MountObservationMap & obs);

/// One `gc/server-roots/<srid>/mount` slot whose holder is not provably finished with the prefix.
struct NonTerminalMountSlot
{
    String server_root_id;
    /// What was read: the lease's identifying fields, or why the body could not be interpreted.
    String detail;
};

/// Read-only scan of every mount slot under the pool prefix, answering ONE question: is some writer
/// still entitled to this prefix? A slot counts as terminal on exactly the two clock-free certificates
/// the mount protocol already recognises (`computeHeartbeatFloor`'s own classification): `gc_fenced`
/// (the GC leader fenced that incarnation out, and a fence costs an epoch, so its keeper can never
/// renew again) and `min_active == UINT64_MAX` (the holder's own graceful farewell). Everything else is
/// reported, INCLUDING a body this build cannot decode -- an unreadable lease of some other format
/// generation is precisely the case that must block, not the one to wave through.
///
/// Deliberately no wall-clock judgement: `expires_at_ms` alone never proves death (comparing another
/// node's stamp against our clock is exactly what `claimMount` refuses to do), and this is not the
/// place to run an observation window either -- the answer to "someone may still be writing here" is
/// for the operator to stop that writer, not for us to wait it out.
///
/// The caller is pool RECREATION (`Pool::open`'s bootstrap over a prefix with no authoritative
/// `_pool_meta`): minting a fresh pool identity while a live writer still holds a slot would leave that
/// writer appending its old-format transactions into the new pool. Writes nothing.
std::vector<NonTerminalMountSlot> probeNonTerminalMountSlots(Backend & b, const Layout & l);

/// A read-only snapshot of one server's mount slot, for introspection (`system.cas_mounts`).
/// state: `live` (lease within TTL+skew), `expired` (lease ran out; the next GC round's heartbeat floor
/// will fence it), `terminated` (clean farewell: `min_active == UINT64_MAX`), `fenced` (`gc_fenced`),
/// `corrupt` (body failed to decode — surfaced as a row, never an exception).
struct MountInfo
{
    String srid;
    MountLease lease;
    String state;
};

/// Enumerate every mount slot under `gc/server-roots/`, decoded and classified — the read-only sibling
/// of `computeHeartbeatFloor`: ZERO writes (no fence-out), per-row fail-open. One LIST + one GET per slot.
std::vector<MountInfo> listMounts(Backend & backend, const Layout & layout, uint64_t now_ms, uint64_t skew_margin_ms);

/// Whether the mounted writer identified by `(server_root_id, writer_epoch)` (the two fields of a
/// `CatalogEntry::creator` / `CreatorFence`, `ref_catalog`'s spec INV-3 §3, that this predicate actually
/// needs) is PROVABLY unable to complete anything more — the gate
/// `CasRefCatalog::reconcileStaleCreator` requires before a stalled `Creating` entry may be stolen by a
/// NEW actor.
///
/// Takes the two scalars rather than a whole `CreatorFence` (review C4): that type lives on the
/// ref-catalog side (`Formats/CasRefCatalogFormat.h`), and this file is already widely included
/// through `CasPool.h` (mount/server-root plumbing reaches nearly every CAS translation unit), so
/// naming that type here would make the mount layer depend on the ref-catalog format instead of the
/// other way round, for a struct this function reads only two fields of. A caller holding a
/// `CreatorFence` passes `fence.server_root_id, fence.writer_epoch` directly.
///
/// `fence_generation` is deliberately NOT one of the two scalars this function takes, and not because
/// it is unavailable -- the catalog persists it (`cfg` in `CasRefCatalogFormat.cpp`) -- but because it
/// is not the property this predicate needs. It mirrors `CasMountRuntime::fence_generation`, an
/// in-process atomic that every mount bumps from its OWN zero on every open, so a DIFFERENT actor's
/// counter (or the SAME actor's after a restart) starts over at the same small values and cannot
/// answer "is the incarnation that minted this entry still alive" -- comparing it across actors
/// compares two unrelated counts that happen to share a name. This reads the durable, cross-process
/// proof instead: `server_root_id`'s CURRENT mount slot (`Layout::mountKey`), classified by the SAME
/// two clock-free certificates `probeNonTerminalMountSlots`/`computeHeartbeatFloor` already use for the
/// identical question at pool-prefix and GC-heartbeat granularity —
///   - `gc_fenced` (the GC leader already fenced this incarnation; a fence costs an epoch, so its
///     keeper can never renew again),
///   - the clean-farewell sentinel `min_active == UINT64_MAX`,
/// PLUS one more certificate available here that neither of those needs: a DIFFERENT `writer_epoch`
/// currently live at that slot proves `writer_epoch`'s specific incarnation is superseded regardless of
/// its OWN certificate — `allocateWriterEpoch`/`claimMount` are why an epoch, once superseded, is never
/// reclaimed by its former holder. The classification is an EXHAUSTIVE switch over these three
/// certificates, not a positive allowlist -- mirrors `CasPool.cpp`'s own exhaustive switch over
/// `MountPriorState` (the classification `claimMount`, in THIS file, only PRODUCES; the switch
/// consuming it lives in the caller) deliberately: a future certificate with no verdict assigned here
/// must fail the BUILD (a missing `-Wswitch` case), never silently read as terminal.
///
/// Deliberately conservative on the two cases that are NOT proof of death: an ABSENT mount slot
/// (`Backend::get` returning `nullopt` answers nothing about liveness — it is not proof either way)
/// and an UNDECODABLE body (an unreadable lease of some other format generation is precisely the case
/// that must block, not the one to wave through, mirroring `probeNonTerminalMountSlots`'s own stated
/// discipline for that case) both return `false` — refuse reconciliation rather than guess.
/// A merely `expired` lease (a wall-clock reading past `expires_at_ms`) is likewise NEVER
/// treated as a certificate, for the same reason `claimMount` itself refuses to trust one: comparing
/// another node's stamp against a clock is exactly the unsafe comparison the mount protocol exists to
/// avoid, and there is not even a caller-supplied clock offered here to make that comparison with.
///
/// WHAT THIS DOES NOT PROVE, stated rather than left implicit: `true` means the CURRENT mount-slot body
/// carries a certificate against `writer_epoch` specifically -- it is not a claim about the server
/// root's OTHER activity, about whether `server_root_id` will ever mount again, or about
/// anything beyond this one slot's current body at the instant of this GET. A caller that needs a
/// stronger, race-free guarantee (e.g. "and it will never come back") must build that from a WIDER
/// observation, the way `claimMountAwaitingExpiry`'s token-stability window does for its own decision --
/// this function performs no such window and answers from one point-in-time read alone. Answering
/// "unknown" (`false`, refuse) is the fail-closed choice on every path already listed above; there is
/// no path where this function answers `true` on evidence weaker than one of the three certificates.
bool isCreatorFenceTerminal(Backend & backend, const Layout & layout, const String & server_root_id,
                            uint64_t writer_epoch);

/// Per-server MERGED heartbeat: one `SingleWriterSlot` over the per-server-root mount object carries
/// the mount lease (liveness) AND the build-watermark floor (`min_active`). One renewal PUT stamps the
/// clock and the build-watermark floor together. Anchors the slot synchronously on `start`, renews it
/// async off the write path, and fails closed on any foreign touch (`renewOnce` throws on a
/// precondition miss). `graceful stop` folds the watermark farewell (`min_active = UINT64_MAX`) into
/// the terminal already-expired mount body.
///
/// ADOPT RULE (critical): the steady-state flow is `claimMount(...)` writes the live mount under
/// (our_uuid, our_epoch), THEN `keeper.start()`. So `start`'s `claim` hook must ADOPT a live mount
/// that is ALREADY ours — same `server_uuid` AND same `writer_epoch` — instead of self-tripping the
/// live-double-start guard. The discriminator is the (uuid, epoch) pair:
///   - same uuid + same epoch  → our own just-written claim (or a replay) → adopt: `putOverwrite`
///     against the observed token to refresh seq/expiry (no fail);
///   - same uuid + DIFFERENT live epoch → a newer incarnation superseded us → fail closed;
///   - foreign uuid → fail closed;
///   - absent → `putIfAbsent`; expired-our-uuid (any epoch) → `putOverwrite` reclaim.
/// After `start`, `renewOnce` (base) keeps the slot alive and already fails closed on a foreign touch.
class MountLeaseKeeper : public SingleWriterSlot
{
public:
    /// `min_active_fn_` is read OFF the state lock on each beat (via `prepareRenew`) and stamped into
    /// the mount body — the merged watermark floor. It reaches into the Pool's own lock, so it must
    /// never run under `state_mutex`.
    MountLeaseKeeper(
        BackendPtr backend_, const Layout & layout_, const String & srid_, UInt128 server_uuid_,
        uint64_t writer_epoch_, std::chrono::milliseconds ttl_, std::function<uint64_t()> now_ms_fn_,
        std::function<uint64_t()> min_active_fn_,
        CasEventSink event_sink_ = {},
        std::chrono::milliseconds lease_safety_margin_ = std::chrono::milliseconds(2000),
        /// boot-domain clock for the on_renew_ok anchor; empty = real CLOCK_BOOTTIME. Injectable for
        /// tests and wired by CasMountRuntime::installKeeper.
        std::function<uint64_t()> boot_ms_fn_ = {});

    /// Claims (adopts) the mount slot for (server_uuid, writer_epoch) with seq following the observed
    /// one — durable when `start` returns.
    void start() { doStart(); }

    /// Releases the mount: the terminal op. Stops the background thread first.
    void stop() { doTerminate(); }

    /// Join the renewal thread BEFORE this object's own `std::function` members (`on_renew_ok` /
    /// `on_lost`, which reach back into the Pool) are destroyed. The base `~SingleWriterSlot` also
    /// calls `stopBackground`, but it runs AFTER the derived members are gone — a renewal firing
    /// `on_lost` in that window would call a destroyed `std::function`. Stopping here closes that window.
    ~MountLeaseKeeper() override { stopBackground(); }

    /// Local write-fence coupling (set once by `Pool::open` before `startBackground`): the keeper is
    /// the ONLY thing that touches S3 for the lease, so the fence must reflect the live lease without a
    /// per-write S3 read. On each SUCCESSFUL background renew the keeper calls `on_renew_ok` (the Pool
    /// refreshes its monotonic fence deadline); when background renewal FAILS (a foreign/superseded
    /// touch makes `renewOnce` throw and the loop stop) the keeper calls `on_lost` (the Pool latches
    /// its fence to lost). Both default to no-op so a keeper used without a Pool is inert.
    /// `on_renew_ok` receives the ATTEMPT-START boot-domain instant of the renewal it acknowledges —
    /// the fence deadline must anchor there, never at response time (spec rev.4 Phase B).
    void setFenceCallbacks(std::function<void(uint64_t)> on_renew_ok_, std::function<void()> on_lost_)
    {
        on_renew_ok = std::move(on_renew_ok_);
        on_lost = std::move(on_lost_);
    }

protected:
    /// Reads the current wall-clock stamp and watermark floor without holding the base state lock;
    /// the callbacks can acquire the Pool lock, so reversing this order would deadlock renewal.
    RenewPayload prepareRenew() const override;

    /// Encodes one complete mount body, combining the sequence assigned by the base slot with the
    /// dynamic values returned by `prepareRenew`.
    String encodeBody(uint64_t seq_, const RenewPayload & payload) const override;

    /// Adopts or claims the mount object for this `(server_uuid, writer_epoch)`, returning the token
    /// from the durable write that the base slot must retain for its next token-guarded renewal.
    Token claim(const String & body) override;

    /// Stamps the terminal, already-expired mount body and folds the watermark farewell into it;
    /// the base state lock and `dead` flag prevent another renewal from racing this write.
    void terminate() override;

    /// Refreshes `confirmed_deadline_ms` after EVERY successful renewal (background OR a direct
    /// caller) — see `onRenewCommitted`'s base doc comment.
    void onRenewCommitted() override;

    /// Fires the boot-domain write-fence callback (`on_renew_ok`) after a confirmed BACKGROUND
    /// lease renewal. `confirmed_deadline_ms` itself is refreshed by `onRenewCommitted` above (which
    /// already ran, for this same successful renewal, before the background loop calls this).
    void onRenewSucceeded() override;

    /// Latches the Pool's local write fence when renewal can no longer prove that this incarnation
    /// owns the mount slot.
    void onRenewFailed() override;

    /// Re-reads a failed renewal and classifies it five ways -- fenced_by_gc, same_epoch_state_uncertain
    /// (this incarnation's own token was advanced past under our own (uuid, epoch): ambiguous, not
    /// proof of anything, and NOT fatal), superseded (a newer epoch), foreign_writer, and vanished
    /// (the backing object disappeared) -- every branch stays terminal and fail closed, and NONE
    /// constructs a `LOGICAL_ERROR`: they throw non-aborting codes (`ABORTED`/`FILE_DOESNT_EXIST`),
    /// because each is reachable by an ordinary network timeout or environmental condition rather than
    /// a programming bug -- and because this runs on the keeper's background thread, where an abort at
    /// exception construction takes the whole process with it.
    void onRenewMismatch(const String & mismatched_key) override;
    /// Fence immediately only once our last CONFIRMED lease (the last successful
    /// `claim`/renew) has reached its safety-margin boundary -- `confirmed_deadline_ms - now <=
    /// lease_safety_margin`. Until then the mount-lease protocol still guarantees exclusivity.
    bool shouldFenceOnTransientRenewFailure() override;

private:
    /// Refreshes `confirmed_deadline_ms` from `anchor_wall_ms` + `ttl` — anchor = the pre-I/O wall
    /// instant of the confirming attempt. Called on every point this keeper KNOWS it holds a live
    /// lease: both success paths of `claim` (mint and adopt), and every successful `renewOnce`
    /// (`onRenewCommitted`) — background-driven or a direct caller alike.
    void refreshConfirmedDeadline(uint64_t anchor_wall_ms);

    String srid;
    UInt128 server_uuid;
    uint64_t writer_epoch;
    std::chrono::milliseconds ttl;
    std::function<uint64_t()> now_ms_fn;
    std::function<uint64_t()> min_active_fn;
    std::function<void(uint64_t)> on_renew_ok;
    std::function<void()> on_lost;
    CasEventSink event_sink;
    std::chrono::milliseconds lease_safety_margin;
    /// boot-domain clock for the on_renew_ok anchor; empty = real CLOCK_BOOTTIME. Injectable for
    /// tests and wired by CasMountRuntime::installKeeper.
    std::function<uint64_t()> boot_ms_fn;
    /// BOOTTIME-ms deadline (same clock as `now_ms_fn`/`MountFence`, and for the SAME suspend-safety
    /// reason -- see `MountFence`'s doc comment in `CasMountRuntime.h`) of the last CONFIRMED lease.
    /// 0 = none yet (`claim` always sets this before `startBackground` can run, so
    /// `shouldFenceOnTransientRenewFailure` observing 0 is defensive, not an expected steady state).
    uint64_t confirmed_deadline_ms = 0;
    /// Whether this runtime has STOPPED BELIEVING IT OWNS THE MOUNT. Set at `onRenewFailed`, which is
    /// that one point and is deliberately broader than "a mismatch was classified": the background loop
    /// also reaches it when a TRANSIENT renewal failure persists past the confirmed lease's
    /// safety-margin boundary (`shouldFenceOnTransientRenewFailure`), where nothing was classified at
    /// all and the write fence latches anyway. Both are the same fact for the reader below, and the
    /// broader one is the SAFE one: what the release path needs to know is whether this runtime still
    /// claims the slot, not why it stopped.
    ///
    /// The release path reads it to tell two opposite situations apart — a deposed writer meeting its
    /// successor in the slot (the expected end of a failover) from a writer that still believed it owned
    /// the mount meeting a stranger there (single-writer exclusivity broken). Atomic because the
    /// keeper's background thread sets it and teardown reads it.
    std::atomic<bool> deposition_observed{false};
    /// Pre-I/O anchors of the CURRENT attempt, stashed by prepareRenew (which runs at the start of
    /// every doStart/renewOnce attempt, off the state lock) and consumed by the success hooks.
    /// `mutable` + no synchronization is safe: prepareRenew, claim, and the hooks all run on the
    /// single renewal driver thread (see renewOnce's single-driver invariant).
    mutable uint64_t last_attempt_wall_ms = 0;
    mutable uint64_t last_attempt_boot_ms = 0;
};

/// Mount-lease-scoped staging sweeper for objects left behind by S3-native staging.
///
/// A leaked S3 staging object happens two ways: (1) an exception between `promoteStaged` succeeding and
/// `cleanupPendingTempFiles` deleting the staging key (`ContentAddressedTransaction.cpp`), or (2) an
/// aborted/cancelled transaction whose pending blobs were staged but never promoted — by design,
/// `cleanupPendingTempFiles` deliberately leaves an S3 staging object in place on the abort path (never a
/// bare `fs::remove` on a remote key), so this sweeper is its ONLY reclaimer. Debris from either case is
/// bounded to `staging/<mount_id>/` — the ONE mount that could ever have written under that prefix, since
/// every staging key this mount ever mints comes from `ContentAddressedMetadataStorage::stagingKeyPrefix()`
/// (`physicalKey(pool_prefix + "/staging/" + server_root_id)`), keyed by THIS mount's own `server_root_id`.
///
/// LEASE-FENCE (fail-closed, never fail-open): `sweepOwnMountStaging` removes ONLY objects whose key
/// starts with the given `mount_staging_prefix` — pass your OWN mount's prefix, never another mount's.
/// The caller (`ContentAddressedMetadataStorage::startup()`) invokes this exactly once, at mount start,
/// with `stagingKeyPrefix() + "/"` — the SAME prefix construction the writer uses to mint staging keys, so
/// this sweep can never reach a different mount's `staging/<other_mount_id>/` subtree: no other writer
/// ever stages a key under THIS mount's own `server_root_id` prefix, and this function never lists or
/// touches anything outside the prefix it is given.
///
/// Best-effort and NEVER THROWS: one stubborn key (or a LIST failure) must never abort the sweep of the
/// rest, and must never fail the mount (mirrors `feedback_ca_gc_never_throw_on_404` — a throw here would
/// only wedge startup, not GC, but the same fail-open-on-error discipline applies to any best-effort
/// reclaim of debris).
///
/// GC excludes `staging/` entirely: GC blob discovery LISTs `Layout::blobsPrefix()`
/// (`<pool_prefix>/blobs/`) — a distinct top-level prefix from `staging/`, `cas/ns/`, and
/// `cas/manifests/` (see `CasLayout.h`) — so a `staging/` object is never listed, HEAD'd, or condemned by
/// GC's fold. This sweeper is the ONLY reclaimer of `staging/` debris.
void sweepOwnMountStaging(IObjectStorage & object_storage, const String & mount_staging_prefix) noexcept;

}
