#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Common/logger_useful.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/setThreadName.h>
#include <base/getFQDNOrHostName.h>
#include <fmt/format.h>
#include <magic_enum.hpp>

#include <algorithm>
#include <ctime>
#include <limits>
#include <set>
#include <string_view>
#include <unistd.h>

namespace ProfileEvents
{
    extern const Event CASMountLeaseLost;
    extern const Event CASMountReleaseSkippedForeignOccupant;
    extern const Event CASMountExclusivityViolation;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int CORRUPTED_DATA;
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
}
}

namespace DB::Cas
{

/// The owner, epoch, and mount-lease wire codecs are implemented in
/// `Formats/CasServerRootFormats`; this file contains the mount-safety protocol logic that uses
/// those codecs.

namespace
{
/// TRUE iff a `list(prefix, "", 1)` over `prefix` returns at least one key.
bool prefixHasAnyKey(Backend & b, const String & prefix)
{
    return !b.list(prefix, /*cursor*/ "", /*limit*/ 1).keys.empty();
}

uint64_t defaultBootMs()
{
    struct timespec ts{};
    clock_gettime(CLOCK_BOOTTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000 + static_cast<uint64_t>(ts.tv_nsec) / 1000000;
}

/// Forward declaration: defined below (same TU-unique anonymous namespace) — `allocateWriterEpoch`
/// names the current mount holder in its DecommissionRecovery live-refusal message.
String describeMountHolder(const MountLease & m);

std::optional<OwnerObject> readOwnerObject(Backend & b, const Layout & l, const String & server_root_id)
{
    const auto got = b.get(l.ownerKey(server_root_id));
    if (!got)
        return std::nullopt;
    return decodeOwner(got->bytes);
}

void throwIfOwnerRetired(const OwnerObject & owner, const String & srid)
{
    if (!owner.retired_at_ms)
        return;

    throw Exception(ErrorCodes::CORRUPTED_DATA,
        "CAS server-root '{}' was explicitly decommissioned by an operator (tombstoned at {} ms) "
        "and is refusing to silently resume — if you genuinely intend to bring this server-root "
        "back, manually clear the owner object's tombstone field and restart "
        "(same manual-recovery pattern as an owner anchor lost over existing data)",
        srid, *owner.retired_at_ms);
}
}

bool serverRootSubtreeEmpty(
    Backend & b, const Layout & l, const String & srid, const RefCatalog & catalog_observation)
{
    const String owned_prefix = srid + "/";
    for (const CatalogEntry & entry : catalog_observation.entries)
        if (entry.ns.string() == srid || entry.ns.string().starts_with(owned_prefix))
            return false;

    /// Manifests and loose roots retain logical path identity. Opaque namespace stream/state debris
    /// alone is not evidence that this server root owns live work.
    if (prefixHasAnyKey(b, l.casManifestsServerPrefix(srid)))
        return false;
    if (prefixHasAnyKey(b, l.serverRootDataPrefix(srid)))
        return false;
    return true;
}

std::optional<UInt128> readOwnerUuid(Backend & b, const Layout & l, const String & server_root_id)
{
    const std::optional<OwnerObject> owner = readOwnerObject(b, l, server_root_id);
    if (!owner)
        return std::nullopt;
    return owner->server_uuid;
}

void claimOwnerOrThrow(
    Backend & b, const Layout & l, const String & srid, UInt128 our_uuid,
    const ObserveRefCatalog & observe_catalog)
{
    if (!observe_catalog)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS server-root '{}': catalog observer is required", srid);
    const String key = l.ownerKey(srid);

    /// Owner present → it is identity: equal UUID is ok, a different UUID fails closed regardless
    /// of any lease/clock state.
    if (const std::optional<OwnerObject> owner = readOwnerObject(b, l, srid))
    {
        if (owner->server_uuid == our_uuid)
        {
            throwIfOwnerRetired(*owner, srid);
            return;
        }
        /// Mirror mountDoubleStartMessage's operator guidance: the by-far most common cause is a
        /// REGENERATED local ClickHouse uuid file (wiped /var/lib/clickhouse, a pod rescheduled
        /// without a persistent volume) while the pool kept the old identity — name it and the
        /// recovery options instead of a bare refusal.
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS server-root '{}' is owned by a different server (owner server_uuid={}, ours={}) — refusing to claim. "
            "This usually means THIS server's local uuid file was regenerated (e.g. /var/lib/clickhouse was wiped, "
            "or the container/pod was recreated without a persistent volume) while the pool kept the old identity. "
            "Recover by restoring the old local uuid file; or configure a fresh <server_root_id> for this disk; "
            "or — only after verifying that NO server uses this root — manually delete the owner object '{}' and restart.",
            srid, u128ToHex(owner->server_uuid), u128ToHex(our_uuid), key);
    }

    /// Owner absent. Claiming is allowed ONLY over a provably-empty subtree; an absent owner over
    /// existing data means the identity was lost and must never be silently re-claimed.
    if (!serverRootSubtreeEmpty(b, l, srid, observe_catalog()))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS server-root '{}' has no owner anchor but its data subtree is non-empty "
            "(identity lost over existing data) — refusing to re-claim",
            srid);

    const PutResult put = b.putIfAbsent(key, encodeOwner(OwnerObject{
        .server_uuid = our_uuid,
        .retired_at_ms = std::nullopt,
    }));
    if (put.outcome == PutOutcome::Done)
        return;

    /// The conditional create conflicted. Recompute the whole catalog + manifest + roots bundle;
    /// no stale emptiness result is carried across the conflict.
    if (!serverRootSubtreeEmpty(b, l, srid, observe_catalog()))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS server-root '{}' owner claim conflicted and newly visible owned work blocks recreation", srid);

    /// Race: another process claimed between our get and our putIfAbsent. Re-read and compare.
    const std::optional<OwnerObject> reread = readOwnerObject(b, l, srid);
    if (!reread)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS server-root '{}' owner anchor vanished during claim", srid);
    if (reread->server_uuid == our_uuid)
    {
        throwIfOwnerRetired(*reread, srid);
        return;
    }
    throw Exception(ErrorCodes::CORRUPTED_DATA,
        "CAS server-root '{}' was claimed by a different server during our claim (foreign owner) "
        "— refusing to proceed",
        srid);
}

uint64_t allocateWriterEpoch(
    Backend & b, const Layout & l, const String & srid, EpochMintPolicy policy, uint64_t now_ms,
    const ObserveRefCatalog & observe_catalog)
{
    if (!observe_catalog)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS server-root '{}': catalog observer is required", srid);
    const String key = l.epochKey(srid);

    static constexpr int max_attempts = 100;
    for (int attempt = 0; attempt < max_attempts; ++attempt)
    {
        const auto got = b.get(key);

        ServerEpoch current;
        std::optional<Token> expected;
        if (got)
        {
            current = decodeServerEpoch(got->bytes);
            expected = got->token;
        }
        else
        {
            /// A missing `epoch` over a non-empty subtree is a reset hazard (durable monotone
            /// counter cannot be reconstructed) — fail closed.
            if (!serverRootSubtreeEmpty(b, l, srid, observe_catalog()))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS server-root '{}' has no durable epoch object but its data subtree is "
                    "non-empty (writer_epoch reset hazard) — refusing to proceed",
                    srid);

            /// Same hazard through the CONTROL objects (spec rev.4 Phase C): an absent epoch while
            /// a mount object exists means epoch state was lost under a live/recent mount —
            /// re-minting epoch 1 there is how a same-(uuid, epoch) twin is born. This is a
            /// lifecycle decision, so it uses the authoritative probe, never get-absence.
            const SentinelProbeResult mount_probe = b.probeSentinelRaw(l.mountKey(srid));
            switch (mount_probe.outcome)
            {
                case ProbeOutcome::KeyAbsent:
                    break;   /// authoritative absence — fresh-root bootstrap proceeds below
                case ProbeOutcome::Present:
                {
                    if (policy == EpochMintPolicy::DecommissionRecovery)
                    {
                        chassert(now_ms != 0);   /// the decommission caller must pass its clock
                        const MountLease surviving = decodeMountLease(*mount_probe.body);
                        /// Deliberately weaker than claimMount's reclaim gate (this file, ~:370-380),
                        /// which never trusts a bare wall-clock comparison alone (only gc_fenced /
                        /// the clean-farewell min_active==UINT64_MAX marker / a caller-proven-dead
                        /// token justify a reclaim there, because clock skew can misjudge liveness).
                        /// This is still safe: (a) the mint below is DISTINCT from the survivor's
                        /// epoch by construction, so no same-(uuid, epoch) pair is ever representable
                        /// even if this liveness read is wrong; (b) claimMount right after this still
                        /// applies its own STRONG liveness gate and refuses a genuinely live member
                        /// regardless of what happens here. So a clock-skewed "terminal" misread can
                        /// only burn one epoch number on a doomed decommission attempt that aborts at
                        /// claimMount — it can never admit a claim over a live member.
                        const bool live = !surviving.gc_fenced && surviving.expires_at_ms > now_ms;
                        if (live)
                            throw Exception(ErrorCodes::ABORTED,
                                "CAS decommission '{}': epoch object missing but a LIVE mount lease "
                                "exists ({}) — refusing to re-mint an epoch under a live member "
                                "(stop the server or wait for its lease to lapse)",
                                srid, describeMountHolder(surviving));
                        /// Terminal mount: proceed, but mint an epoch DISTINCT from the survivor's
                        /// by construction — the same-pair state is unrepresentable on this path.
                        current.next_writer_epoch = std::max<uint64_t>(1, surviving.writer_epoch + 1);
                        break;
                    }
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "CAS server-root '{}' has no durable epoch object but a mount lease exists — "
                        "durable epoch state was lost while a mount is live or recently live; "
                        "refusing to re-mint epoch 1. If no server is live on this root, "
                        "decommission it or manually remove the stale mount object '{}'.",
                        srid, l.mountKey(srid));
                }
                case ProbeOutcome::ContainerAbsent:
                case ProbeOutcome::AccessDenied:
                case ProbeOutcome::Indeterminate:
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "CAS server-root '{}': cannot verify mount-lease absence before re-minting "
                        "the writer epoch (probe outcome: {}) — absence was never proven; failing closed",
                        srid, magic_enum::enum_name(mount_probe.outcome));
            }

            if (current.next_writer_epoch == 0)
                current.next_writer_epoch = 1;
        }

        const uint64_t next = current.next_writer_epoch;
        ServerEpoch new_state;
        new_state.next_writer_epoch = next + 1;

        const CasResult res = b.casPut(key, encodeServerEpoch(new_state), expected);
        if (res.outcome == CasOutcome::Committed)
            return next;
        if (!got)
        {
            /// The absent-epoch create conflicted. A winner may have installed an epoch while owned
            /// work became visible, so recompute the complete catalog + manifest + roots bundle
            /// before the next iteration is allowed to accept either a present or absent epoch.
            if (!serverRootSubtreeEmpty(b, l, srid, observe_catalog()))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS server-root '{}' writer_epoch allocation conflicted and newly visible owned "
                    "work blocks recreation",
                    srid);
        }
        /// Conflict: someone else allocated concurrently — retry against fresh state only after the
        /// absent-epoch safety bundle above has been recomputed when required.
    }

    throw Exception(ErrorCodes::CORRUPTED_DATA,
        "CAS server-root '{}' writer_epoch allocation did not converge after {} attempts",
        srid, max_attempts);
}

namespace
{
/// Build a fresh mount-lease body for (uuid, epoch) with the given seq, stamped from `now_ms`.
MountLease makeMountBody(UInt128 uuid, uint64_t epoch, uint64_t seq, uint64_t now_ms, uint64_t ttl_ms)
{
    return MountLease{
        .server_uuid = uuid,
        .writer_epoch = epoch,
        .hostname = getFQDNOrHostName(),
        .pid = static_cast<uint64_t>(::getpid()),
        .started_at_ms = now_ms,
        .seq = seq,
        .expires_at_ms = now_ms + ttl_ms,
    };
}

/// Mirrors `mountDoubleStartMessage`'s identity fields. The mount-audit sink is not yet installed
/// during `Pool::open`, so at first-open these refusal messages are the only holder-identity
/// carrier in err.log — name the toucher inline rather than just the key.
String describeMountHolder(const MountLease & m)
{
    return fmt::format("server_uuid={} hostname={} pid={} writer_epoch={} seq={} expires_at_ms={}",
        u128ToHex(m.server_uuid), m.hostname, m.pid, m.writer_epoch, m.seq, m.expires_at_ms);
}

/// The mount-slot "foreign writer" audit instrument: every mount-slot WRITE
/// (`MountClaim`/`MountRelease`) and every OBSERVED foreign/conflicting body (`MountConflict`)
/// becomes one `system.cas_log` row. `observed` is the CURRENT decoded body at the
/// point of decision — for a conflict it carries the identity that made us refuse (holder_uuid/
/// hostname/pid/epoch/seq/expires); null when no body was observed (e.g. a bare CAS race).
/// No-op when `sink` is unset, so a disabled log does no per-call work.
void emitMountEvent(const CasEventSink & sink, CasEventType type, const String & srid,
                    const String & branch, const MountLease * observed, const String & reason)
{
    if (!sink)
        return;
    CasEvent e;
    e.type = type;
    e.object_kind = CasEventObjectKind::None;
    e.outcome = branch;
    e.reason = reason;
    e.detail["server_root_id"] = srid;
    e.detail["branch"] = branch;
    if (observed)
    {
        e.detail["holder_uuid"] = u128ToHex(observed->server_uuid);
        e.detail["holder_hostname"] = observed->hostname;
        e.detail["holder_pid"] = std::to_string(observed->pid);
        e.detail["holder_epoch"] = std::to_string(observed->writer_epoch);
        e.detail["holder_seq"] = std::to_string(observed->seq);
        e.detail["holder_expires_at_ms"] = std::to_string(observed->expires_at_ms);
    }
    sink(std::move(e));
}
}

MountClaimResult claimMount(
    Backend & b, const Layout & l, const String & srid, UInt128 our_uuid, uint64_t our_epoch,
    uint64_t now_ms, uint64_t ttl_ms, const std::optional<Token> & proven_dead_token,
    const CasEventSink & sink)
{
    const String key = l.mountKey(srid);
    const auto got = b.get(key);

    /// Absent → fresh claim.
    if (!got)
    {
        const MountLease body = makeMountBody(our_uuid, our_epoch, /*seq=*/ 1, now_ms, ttl_ms);
        const PutResult put = b.putIfAbsent(key, encodeMountLease(body));
        if (put.outcome != PutOutcome::Done)
            /// Raced with a concurrent writer between get and putIfAbsent. Treat as a live double
            /// start — fail closed; never overwrite a slot that appeared under us. No re-read was
            /// done, so no conflicting identity is known to attach to an event.
            return {.kind = MountClaimResult::LiveDoubleStart, .body = body, .token = std::nullopt};
        emitMountEvent(sink, CasEventType::MountClaim, srid, "mint", nullptr, "fresh mount slot minted");
        return {.kind = MountClaimResult::Claimed, .body = body, .token = std::nullopt};
    }

    const MountLease existing = decodeMountLease(got->bytes);

    /// Foreign owner → fail closed regardless of expiry. (This runs after the owner gate, so a foreign
    /// mount should not normally exist, but the lease must never be taken across UUIDs.)
    if (existing.server_uuid != our_uuid)
    {
        emitMountEvent(sink, CasEventType::MountConflict, srid, "foreign_owner", &existing,
            "mount slot is held by a foreign server_uuid — refusing to take over across identities");
        return {.kind = MountClaimResult::ForeignOwner, .body = existing, .token = std::nullopt};
    }

    /// Same uuid + same epoch: it is OUR OWN claim — but a FENCED body is terminal for this
    /// (uuid, epoch): the GC dropped its ack from the floor when it fenced. Refreshing it in place
    /// would resurrect a fenced incarnation — a fence permanently consumes this `(server_uuid,
    /// writer_epoch)` pair, so the caller must re-open with a fresh `writer_epoch`.
    if (existing.writer_epoch == our_epoch)
    {
        if (existing.gc_fenced)
        {
            emitMountEvent(sink, CasEventType::MountConflict, srid, "fenced_by_gc", &existing,
                "own (uuid, epoch) mount slot is GC-fenced — terminal for this incarnation; "
                "recover with a fresh writer_epoch");
            return {.kind = MountClaimResult::FencedSelf, .body = existing, .token = std::nullopt};
        }
        const MountLease body = makeMountBody(our_uuid, our_epoch, existing.seq + 1, now_ms, ttl_ms);
        const PutResult put = b.putOverwrite(key, encodeMountLease(body), got->token);
        if (put.outcome != PutOutcome::Done)
            /// The mount changed under us between get and putOverwrite: `got->token` is now KNOWN
            /// STALE (that mismatch is exactly why the put failed), not merely unknown -- leaving
            /// `.token` unset (rather than handing back a token the caller would wrongly treat as
            /// current) is deliberate, matching the identical race below.
            return {.kind = MountClaimResult::LiveDoubleStart, .body = body, .token = std::nullopt};
        emitMountEvent(sink, CasEventType::MountClaim, srid, "refresh", &existing,
            "own claim replayed — refreshed seq + expiry");
        return {.kind = MountClaimResult::Claimed, .body = body, .token = std::nullopt};
    }

    /// Same uuid, DIFFERENT epoch: reclaim ONLY on a certificate of death that needs no fresh
    /// wall-clock trust — never by comparing `expires_at_ms` against `now_ms`:
    ///   - `gc_fenced` → the fence-out is terminal for that incarnation by construction (its keeper's
    ///     every renewal fails the token guard forever, so it can never write again) — there is no
    ///     liveness left to wait for. This is what makes self-remount (and a fast restart after a
    ///     fence-out) instant instead of an observation wait.
    ///   - the clean marker (`min_active == UINT64_MAX`) → the predecessor's OWN graceful farewell
    ///     (`MountLeaseKeeper::terminate`) — no observation needed either.
    ///   - `proven_dead_token` matches the token we just read → the CALLER (`claimMountAwaitingExpiry`)
    ///     already watched this exact token hold stable for the full observation threshold on its own
    ///     clock; re-deriving that here from a bare wall-clock comparison would be exactly the
    ///     cross-node trust would make a clock-skewed or delayed observer unsafe.
    /// Anything else → `LiveDoubleStart` (do NOT write): a same-uuid, different-epoch, not fenced, not
    /// clean-marked, not (yet) proven-dead lease may simply be a live twin, and `expires_at_ms` alone
    /// can never distinguish that from a dead predecessor across two different clocks.
    const bool clean_marker = existing.min_active == std::numeric_limits<uint64_t>::max();
    const bool proven_dead = proven_dead_token && *proven_dead_token == got->token;
    if (existing.gc_fenced || clean_marker || proven_dead)
    {
        const MountLease body = makeMountBody(our_uuid, our_epoch, existing.seq + 1, now_ms, ttl_ms);
        const PutResult put = b.putOverwrite(key, encodeMountLease(body), got->token);
        if (put.outcome != PutOutcome::Done)
            /// The mount changed under us between get and putOverwrite — someone else is racing the
            /// reclaim. Fail closed. `got->token` is now KNOWN STALE (that mismatch is exactly why the
            /// put failed) -- leaving `.token` unset is deliberate, not an oversight.
            return {.kind = MountClaimResult::LiveDoubleStart, .body = body, .token = std::nullopt};
        const MountPriorState prior = existing.gc_fenced ? MountPriorState::Fenced
                                     : clean_marker       ? MountPriorState::Clean
                                                           : MountPriorState::UncleanObserved;
        emitMountEvent(sink, CasEventType::MountClaim, srid, "reclaim", &existing,
            existing.gc_fenced ? "same server_uuid, different writer_epoch, GC-fenced — reclaimed"
            : clean_marker     ? "same server_uuid, different writer_epoch, clean farewell — reclaimed"
                               : "same server_uuid, different writer_epoch, observed dead by "
                                 "token-stability — reclaimed");
        return {.kind = MountClaimResult::Claimed, .body = body, .prior = prior, .token = std::nullopt};
    }

    emitMountEvent(sink, CasEventType::MountConflict, srid, "live_double_start", &existing,
        "same server_uuid, different writer_epoch, not fenced/clean/proven-dead — no wall-clock trust; "
        "the caller must run the token-stability observation wait before reclaiming");
    /// No write was attempted on this path -- `got->token` is exactly the CURRENT body's
    /// token (what we just read is what's still there), so it is safe to hand back for the caller's
    /// observation loop to compare across polls without a redundant re-GET.
    return {.kind = MountClaimResult::LiveDoubleStart, .body = existing, .token = got->token};
}

String mountDoubleStartMessage(const String & srid, const MountLease & existing)
{
    return fmt::format(
        "Content-addressed disk cannot start: server_root_id '{}' is actively mounted by another LIVE server.\n"
        "  Existing mount: server_uuid={} hostname={} pid={} last_seq={} expires_at_ms={}\n"
        "This server already waited for the mount lease to lapse, but it kept being renewed — a second\n"
        "server is holding the same CAS namespace. This prevents two ClickHouse servers from writing it.\n"
        " - If the other server is running intentionally, configure a unique <server_root_id> for this disk.\n"
        " - If the other server is a stale/zombie process, stop it; this server will then reclaim the mount on restart.\n"
        " - CLOCK SKEW CAVEAT: liveness is judged by comparing the lease's wall-clock expires_at_ms against\n"
        "   THIS server's clock, so a large clock skew between the two servers can misjudge it (a healthy holder\n"
        "   may look mounted here, or a dead one may look live). Verify both servers' clocks are in sync (NTP).\n"
        " - If the local ClickHouse uuid file was regenerated, restore the old uuid file, or remove the stale\n"
        "   owner object gc/server-roots/{}/owner only after verifying no server uses this root.\n"
        " - As a LAST RESORT, after verifying that NO server is writing this root, manually delete the mount\n"
        "   object gc/server-roots/{}/mount and restart; this server will then re-claim it.",
        srid, u128ToHex(existing.server_uuid), existing.hostname, existing.pid,
        existing.seq, existing.expires_at_ms, srid, srid);
}

namespace
{
/// Bounded number of observation restarts before giving up on a same-uuid slot whose write-token keeps
/// changing: each restart means the token changed DURING our observation window — i.e. something is
/// actively renewing it. A genuinely dead predecessor's token never changes again after its last
/// renewal, so it is observed stable well within one window; only a truly LIVE writer (a real second
/// incarnation, or the predecessor's own background renewer racing our first few polls) keeps resetting
/// the clock. Bounding this converts "wait forever for a live twin" into the same bounded-then-report
/// shape the old wall-clock wait had, without ever trusting a wall-clock deadline to get there.
constexpr size_t kMaxObservationRestarts = 3;
}

uint64_t mountObservationThresholdMs(uint64_t ttl_ms, uint64_t cadence_ms)
{
    return ttl_ms + ttl_ms / 20 + cadence_ms;
}

MountClaimResult claimMountAwaitingExpiry(
    Backend & b, const Layout & l, const String & srid, UInt128 our_uuid, uint64_t our_epoch,
    const std::function<uint64_t()> & now_ms_fn,
    const std::function<uint64_t()> & mono_ms_fn,
    uint64_t ttl_ms, uint64_t poll_interval_ms,
    const std::function<void(uint64_t)> & sleep_ms_fn,
    const std::function<void(const MountLease &, uint64_t)> & on_wait_start,
    const CasEventSink & sink)
{
    /// A zero poll interval would spin; a single-ms floor keeps the loop a real (bounded) wait.
    const uint64_t poll = poll_interval_ms == 0 ? 1 : poll_interval_ms;

    /// Rate-bound observation threshold: the full lease TTL, plus a 5% allowance for clock-rate
    /// mismatch between the holder's and our own local clock, plus one poll interval for observation
    /// discreteness. It is measured only with OUR OWN clock (`mono_ms_fn`); no cross-node wall-clock
    /// comparison participates in this loop. The shared helper keeps the startup and GC thresholds
    /// identical.
    const uint64_t threshold_ms = mountObservationThresholdMs(ttl_ms, poll);

    std::optional<Token> observed;
    uint64_t observed_since = 0;
    size_t restarts = 0;

    while (true)
    {
        const bool threshold_met = observed && mono_ms_fn() - observed_since >= threshold_ms;
        MountClaimResult r = claimMount(b, l, srid, our_uuid, our_epoch, now_ms_fn(), ttl_ms,
            threshold_met ? observed : std::nullopt, sink);
        if (r.kind != MountClaimResult::LiveDoubleStart)
            return r;

        /// `claimMount` already read the current body. Reuse `r.token` whenever `claimMount`
        /// set it (the common case: no write was attempted, so what it read is still current) instead of
        /// re-GETting the SAME key here. The rare stale-race branches deliberately leave `.token` unset
        /// (see their own comments), so this still falls back to a fresh read exactly there.
        std::optional<Token> current_token = r.token;
        if (!current_token)
        {
            const auto got = b.get(l.mountKey(srid));
            if (!got)
            {
                /// The slot vanished between claimMount's own GET and ours — normally self-resolving
                /// within one more `claimMount` call (which re-mints fresh on an absent slot), but under
                /// slot churn (something else concurrently removing/re-minting it) that resolution could
                /// keep losing the same race. Pace this like every other iteration and
                /// count it toward the SAME bounded restart budget the token-churn case below uses,
                /// instead of spinning `get`/`claimMount`/`put` at backend RTT with no sleep and no bound
                /// — a persistently vanishing slot is exactly as "alive and contended" as a persistently
                /// renewing token.
                if (++restarts > kMaxObservationRestarts)
                    return r;
                sleep_ms_fn(poll);
                continue;
            }
            current_token = got->token;
        }

        if (!observed || *observed != *current_token)
        {
            if (observed && ++restarts > kMaxObservationRestarts)
                /// The token kept changing across bounded restarts — the holder is genuinely alive
                /// (actively renewing), not a dead predecessor. Report it rather than waiting forever.
                return r;
            observed = *current_token;
            observed_since = mono_ms_fn();
            if (on_wait_start)
                on_wait_start(r.body, threshold_ms);
            LOG_INFO(getLogger("CasMountLease"),
                "Attempting to mount content-addressed server root {} after node change or hard "
                "restart; waiting ~{} ms (token-stability observation) to confirm the previous "
                "incarnation's operations are all finalized", srid, threshold_ms);
        }

        sleep_ms_fn(poll);
    }
}

HeartbeatFloor computeHeartbeatFloor(Backend & b, const Layout & l, uint64_t now_ms,
                                     uint64_t mono_now_ms, uint64_t stable_threshold_ms,
                                     MountObservationMap & obs)
{
    HeartbeatFloor floor;

    /// `obs` is keyed by every srid this leader has EVER observed, but a
    /// srid removed from the LIST entirely (its `/mount` key gone -- e.g. `SYSTEM CAS
    /// DROP POOL MEMBER`) is never visited by the loop below again, so its entry would otherwise linger
    /// forever (~150-250 B/srid, worse on a long-lived leader across many decommissions). Track every
    /// srid actually seen THIS pass and prune anything else out of `obs` at the end -- disjoint from the
    /// mid-loop `obs.erase(srid)` calls below (those fire for a srid seen but now terminal/fenced/gone
    /// this pass; this is for a srid not seen AT ALL).
    std::set<String> seen_srids;

    const String prefix = l.serverRootsPrefix();
    String cursor;
    while (true)
    {
        const ListPage page = b.list(prefix, cursor, /*limit*/ 1000);
        for (const auto & listed : page.keys)
        {
            /// `/owner` and `/epoch` objects share the subtree — only mount bodies gate the floor.
            static constexpr std::string_view mount_suffix = "/mount";
            if (!listed.key.ends_with(mount_suffix))
                continue;

            const String & key = listed.key;

            /// The srid is the path segment between `serverRootsPrefix()` and the `/mount` suffix
            /// (`<prefix>/gc/server-roots/<srid>/mount`). Used both for observability (fenced) and as
            /// the key into `obs`.
            const String srid = key.substr(prefix.size(),
                key.size() - prefix.size() - mount_suffix.size());
            seen_srids.insert(srid);

            /// Fence-out on PreconditionFailed re-GETs and reclassifies from the top; bound the retries
            /// so a pathologically contended holder cannot spin forever. On exhaustion the entry is
            /// counted as live (conservative — never excluded without a landed fence-out).
            constexpr int max_reclassify = 4;
            for (int attempt = 0; ; ++attempt)
            {
                const auto got = b.get(key);
                if (!got)
                {
                    obs.erase(srid);
                    break;   /// Raced away (deleted) — nothing to classify.
                }

                const MountLease m = decodeMountLease(got->bytes);

                if (m.gc_fenced)
                {
                    ++floor.already_fenced;
                    obs.erase(srid);   /// terminal — no further observation needed
                    break;
                }
                if (m.min_active == std::numeric_limits<uint64_t>::max())
                {
                    ++floor.terminated;
                    obs.erase(srid);   /// terminal — no further observation needed
                    break;
                }

                /// Observation-based liveness: stable ONLY if the
                /// SAME token was already being watched and has now held for the full threshold on our
                /// OWN monotonic clock. Anything else — no prior observation, or a changed token (a
                /// live renewal, including one raced against our own fence-out attempt below) —
                /// (re)starts the observation window and counts as `live` this call.
                const auto it = obs.find(srid);
                const bool stable = it != obs.end() && it->second.token == got->token
                    && mono_now_ms - it->second.first_seen_mono_ms >= stable_threshold_ms;

                if (!stable)
                {
                    if (it == obs.end() || it->second.token != got->token)
                        obs[srid] = MountTokenObservation{got->token, mono_now_ms};
                    ++floor.live;
                    break;
                }

                const bool exhausted = attempt >= max_reclassify;
                if (exhausted)
                {
                    ++floor.live;   /// conservative — never exclude without a landed fence-out
                    break;
                }

                /// Stable past the threshold, not yet fenced → token-guarded fence-out preserving the
                /// whole body (gc_fenced = true, seq + 1).
                MountLease fenced = m;
                fenced.gc_fenced = true;
                fenced.seq = m.seq + 1;
                const PutResult res = b.putOverwrite(key, encodeMountLease(fenced), got->token);
                if (res.outcome == PutOutcome::Done)
                {
                    ++floor.fenced_now;
                    floor.fenced_srids.push_back(srid);
                    obs.erase(srid);
                    LOG_INFO(getLogger("CasHeartbeatFloor"),
                        "CAS GC fenced out mount lease for content-addressed server root {} at "
                        "wall-clock ms {}: its write token held unchanged for >= {} ms on the GC "
                        "leader's own monotonic clock (token-stability observation)",
                        srid, now_ms, stable_threshold_ms);
                    break;
                }
                /// PreconditionFailed: the holder renewed between our GET and PUT — re-GET and
                /// reclassify (the observation check above will see the new token and restart it).
            }
        }

        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }

    /// Prune every `obs` entry for a srid this pass's LIST never saw at all.
    for (auto it = obs.begin(); it != obs.end(); )
        it = seen_srids.contains(it->first) ? std::next(it) : obs.erase(it);

    return floor;
}

std::vector<NonTerminalMountSlot> probeNonTerminalMountSlots(Backend & b, const Layout & l)
{
    std::vector<NonTerminalMountSlot> slots;

    /// Same enumeration as `computeHeartbeatFloor`'s gate -- LIST the server-roots subtree, keep the
    /// `/mount` bodies -- but read-only and without any observation state: this answers "is anyone
    /// still entitled to write here", not "may I fence them out".
    const String prefix = l.serverRootsPrefix();
    String cursor;
    while (true)
    {
        const ListPage page = b.list(prefix, cursor, /*limit*/ 1000);
        for (const auto & listed : page.keys)
        {
            static constexpr std::string_view mount_suffix = "/mount";
            if (!listed.key.ends_with(mount_suffix))
                continue;   /// `/owner` and `/epoch` share the subtree; only the lease says "live".

            const String srid = listed.key.substr(prefix.size(),
                listed.key.size() - prefix.size() - mount_suffix.size());

            const auto got = b.get(listed.key);
            if (!got)
                continue;   /// raced away between LIST and GET -- there is no slot to be held.

            MountLease m;
            try
            {
                m = decodeMountLease(got->bytes);
            }
            catch (...)
            {
                /// An undecodable lease is the WORST case for a recreation, not an ignorable one: it is
                /// what a slot written by a format this build does not understand looks like, and the
                /// holder of that slot is exactly the writer we must not run over.
                slots.push_back(NonTerminalMountSlot{srid, fmt::format(
                    "mount lease could not be decoded by this build ({})",
                    getCurrentExceptionMessage(/*with_stacktrace=*/false))});
                continue;
            }

            if (m.gc_fenced || m.min_active == std::numeric_limits<uint64_t>::max())
                continue;   /// terminal: fenced out by GC, or the holder's own graceful farewell.

            slots.push_back(NonTerminalMountSlot{srid, fmt::format(
                "held by server uuid {} (writer_epoch {}, host '{}', pid {}, lease seq {}, stamped "
                "expiry {} ms) with neither a graceful farewell nor a GC fence-out",
                u128ToHex(m.server_uuid), m.writer_epoch, m.hostname, m.pid, m.seq, m.expires_at_ms)});
        }

        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }

    return slots;
}

std::vector<MountInfo> listMounts(Backend & backend, const Layout & layout, uint64_t now_ms, uint64_t skew_margin_ms)
{
    std::vector<MountInfo> out;
    const String prefix = layout.serverRootsPrefix();
    String cursor;
    while (true)
    {
        const ListPage page = backend.list(prefix, cursor, 1000);
        for (const auto & k : page.keys)
        {
            static constexpr std::string_view suffix = "/mount";
            if (!k.key.ends_with(suffix))
                continue;
            const auto got = backend.get(k.key);
            if (!got)
                continue;   /// raced a delete — read-only view, skip the row
            MountInfo info;
            /// The srid is the path segment between `serverRootsPrefix()` and the `/mount` suffix —
            /// may itself contain `/` (e.g. `shard-01/replica-a`), so slice by prefix length rather
            /// than `rfind('/')`, matching `computeHeartbeatFloor`'s extraction.
            info.srid = k.key.substr(prefix.size(), k.key.size() - prefix.size() - suffix.size());
            try
            {
                info.lease = decodeMountLease(got->bytes);
            }
            catch (...)
            {
                info.state = "corrupt";
                out.push_back(std::move(info));
                continue;
            }
            if (info.lease.gc_fenced)
                info.state = "fenced";
            else if (info.lease.min_active == std::numeric_limits<uint64_t>::max())
                info.state = "terminated";
            else if (now_ms <= info.lease.expires_at_ms + skew_margin_ms)
                info.state = "live";
            else
                info.state = "expired";
            out.push_back(std::move(info));
        }
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }
    return out;
}

namespace
{

/// The three clock-free certificates `isCreatorFenceTerminal` recognises, plus `None` for a live body
/// that carries none of them -- see the function's header doc for what each one proves and why
/// `fence_generation` is not among them.
enum class FenceCertificate : uint8_t
{
    None,
    GcFenced,
    CleanFarewell,
    SupersededEpoch,
};

FenceCertificate classifyFenceCertificate(const MountLease & lease, uint64_t fence_writer_epoch)
{
    if (lease.gc_fenced)
        return FenceCertificate::GcFenced;
    if (lease.min_active == std::numeric_limits<uint64_t>::max())
        return FenceCertificate::CleanFarewell;
    if (lease.writer_epoch != fence_writer_epoch)
        return FenceCertificate::SupersededEpoch;
    return FenceCertificate::None;
}

}

bool isCreatorFenceTerminal(Backend & backend, const Layout & layout, const String & server_root_id,
                            uint64_t writer_epoch)
{
    const auto got = backend.get(layout.mountKey(server_root_id));
    if (!got)
        return false;   /// absence proves nothing about liveness -- see the header doc

    MountLease lease;
    try
    {
        lease = decodeMountLease(got->bytes);
    }
    catch (...)
    {
        return false;   /// undecodable -- fail closed, never wave through
    }

    /// EXHAUSTIVE switch, not a positive allowlist -- mirrors `CasPool.cpp`'s own exhaustive switch over
    /// `MountPriorState` (`claimMount`, in this file, only PRODUCES that classification; the switch
    /// consuming it lives in the caller) deliberately: a future `FenceCertificate` enumerator with no
    /// verdict assigned here must fail the BUILD (a missing `-Wswitch` case), never silently read as
    /// terminal (which would let a reconciler steal a namespace out from under a writer that might
    /// still be alive) or as live (which would block a reconciliation the certificate already proves
    /// is safe).
    /// The initializer is dead: the exhaustive switch below assigns every enumerator, and a future
    /// enumerator left unassigned fails the build via `-Wswitch`, not this value.
    bool terminal = false;
    switch (classifyFenceCertificate(lease, writer_epoch))
    {
        case FenceCertificate::None:
            terminal = false;
            break;
        case FenceCertificate::GcFenced:
        case FenceCertificate::CleanFarewell:
        case FenceCertificate::SupersededEpoch:
            terminal = true;
            break;
    }
    return terminal;
}

MountLeaseKeeper::MountLeaseKeeper(
    BackendPtr backend_, const Layout & layout_, const String & srid_, UInt128 server_uuid_,
    uint64_t writer_epoch_, std::chrono::milliseconds ttl_, std::function<uint64_t()> now_ms_fn_,
    std::function<uint64_t()> min_active_fn_,
    CasEventSink event_sink_,
    std::chrono::milliseconds lease_safety_margin_,
    std::function<uint64_t()> boot_ms_fn_)
    : SingleWriterSlot(std::move(backend_), layout_.mountKey(srid_), "mount-lease", "release", "CasMountLeaseKeeper")
    , srid(srid_)
    , server_uuid(server_uuid_)
    , writer_epoch(writer_epoch_)
    , ttl(ttl_)
    , now_ms_fn(std::move(now_ms_fn_))
    , min_active_fn(std::move(min_active_fn_))
    , event_sink(std::move(event_sink_))
    , lease_safety_margin(lease_safety_margin_)
    , boot_ms_fn(boot_ms_fn_ ? std::move(boot_ms_fn_) : defaultBootMs)
{
}

void MountLeaseKeeper::refreshConfirmedDeadline(uint64_t anchor_wall_ms)
{
    confirmed_deadline_ms = anchor_wall_ms + static_cast<uint64_t>(ttl.count());
}

bool MountLeaseKeeper::shouldFenceOnTransientRenewFailure()
{
    /// Defensive: should never observe 0 here (see the member's doc comment) -- fail closed if it ever did.
    if (confirmed_deadline_ms == 0)
        return true;
    const uint64_t now = now_ms_fn();
    const uint64_t margin = static_cast<uint64_t>(lease_safety_margin.count());
    return now + margin >= confirmed_deadline_ms;
}

SingleWriterSlot::RenewPayload MountLeaseKeeper::prepareRenew() const
{
    /// Carry the two dynamic fields (both read OFF the state lock — the merged floor callback reaches
    /// into the Pool's own lock): `value` = wall-clock `now_ms` (so `encodeBody` stamps a fresh
    /// `expires_at_ms = now_ms + ttl`), `value2` = `min_active` (the build-watermark floor).
    /// Pre-I/O anchors (spec rev.4 Phase B): both fence deadlines anchor at this instant — the
    /// wall stamp doubles as the payload's now_ms, so anchor <= the durable stamp trivially.
    last_attempt_wall_ms = now_ms_fn();
    last_attempt_boot_ms = boot_ms_fn();
    return {.value = last_attempt_wall_ms, .value2 = min_active_fn()};
}

String MountLeaseKeeper::encodeBody(uint64_t seq_, const RenewPayload & payload) const
{
    const uint64_t now_ms = payload.value;
    const uint64_t ttl_ms = static_cast<uint64_t>(ttl.count());
    return encodeMountLease(MountLease{
        .server_uuid = server_uuid,
        .writer_epoch = writer_epoch,
        .hostname = getFQDNOrHostName(),
        .pid = static_cast<uint64_t>(::getpid()),
        .started_at_ms = now_ms,
        .seq = seq_,
        .expires_at_ms = now_ms + ttl_ms,
        .min_active = payload.value2,
    });
}

SingleWriterSlot::Token MountLeaseKeeper::claim(const String & body)
{
    /// ADOPT-aware claim. The normal flow is `claimMount` wrote the live mount under
    /// (server_uuid, writer_epoch); `start` then adopts that very slot. We must NOT self-trip the
    /// live-double-start guard on our own (uuid, epoch).
    const HeadResult head = backend->head(key);
    if (!head.exists)
    {
        /// Absent → put it ourselves (a fresh start that ran without a prior claimMount, or a slot
        /// that lapsed and was swept). putIfAbsent fails closed if it appears under us; that race has
        /// no re-read (no observed body), so there is nothing to attach to a conflict event.
        const PutResult res = backend->putIfAbsent(key, body);
        if (res.outcome != PutOutcome::Done)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS mount-lease: key '{}' appeared between head and putIfAbsent — concurrent writer on our mount slot", key);
        emitMountEvent(event_sink, CasEventType::MountClaim, srid, "mint", nullptr,
            "mount slot absent — keeper minted it directly (no prior claimMount)");
        refreshConfirmedDeadline(last_attempt_wall_ms);
        return res.token;
    }

    /// Read the observed slot to decide adopt vs fail-closed by the (uuid, epoch) discriminator.
    const auto got = backend->get(key);
    if (!got)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CAS mount-lease: key '{}' vanished between head and get while claiming", key);
    const MountLease observed = decodeMountLease(got->bytes);

    /// Foreign uuid → fail closed (no cross-UUID takeover, ever). The audit payload: the CURRENT decoded
    /// body's identity is exactly WHO touched the slot.
    if (observed.server_uuid != server_uuid)
    {
        emitMountEvent(event_sink, CasEventType::MountConflict, srid, "adopt", &observed,
            "mount slot is held by a foreign server — failing closed, never taking over");
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CAS mount-lease: key '{}' is held by a foreign server ({}) — failing closed, never taking over",
            key, describeMountHolder(observed));
    }

    /// Same uuid but a DIFFERENT epoch → a newer incarnation superseded us (or a concurrent
    /// double-start). Fail closed.
    if (observed.writer_epoch != writer_epoch)
    {
        emitMountEvent(event_sink, CasEventType::MountConflict, srid, "adopt", &observed,
            fmt::format("mount slot is held by a different writer_epoch ({} != ours {}) — superseded, failing closed",
                observed.writer_epoch, writer_epoch));
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CAS mount-lease: key '{}' is held by a different writer_epoch ({} != ours {}) — superseded, failing closed ({})",
            key, observed.writer_epoch, writer_epoch, describeMountHolder(observed));
    }

    /// Same (uuid, epoch) but FENCED: the GC fenced our fresh lease before we adopted it (the
    /// lease expired mid-open — e.g. a slow first beat). Terminal for THIS epoch; the open path
    /// recovers by allocating a fresh `writer_epoch` and re-claiming.
    if (observed.gc_fenced)
    {
        emitMountEvent(event_sink, CasEventType::MountConflict, srid, "fenced_by_gc", &observed,
            "own mount slot fenced by GC after lease expiry — recoverable with a fresh writer_epoch");
        throw MountFencedException(fmt::format(
            "CAS mount-lease: key '{}' was fenced by GC after lease expiry ({}) — "
            "recoverable: re-open with a fresh writer_epoch", key, describeMountHolder(observed)));
    }

    /// Same uuid AND same epoch → it is OUR OWN claim → ADOPT: overwrite against the observed token
    /// to refresh seq/expiry. (`body` is encoded for seq=1 by the base `doStart`; that is fine —
    /// renewals advance from there.)
    const PutResult res = backend->putOverwrite(key, body, got->token);
    if (res.outcome != PutOutcome::Done)
    {
        /// The slot moved between our GET and PUT. Diagnose by the CURRENT body, not the token
        /// The current body is the useful diagnostic: a GC fence is the only same-(uuid, epoch)-preserving
        /// touch that can normally occur during adoption.
        const auto reread = backend->get(key);
        if (reread)
        {
            const MountLease current = decodeMountLease(reread->bytes);
            if (current.server_uuid == server_uuid && current.gc_fenced)
            {
                emitMountEvent(event_sink, CasEventType::MountConflict, srid, "fenced_by_gc", &current,
                    "GC fenced our mount between the adopt's read and write — recoverable with a "
                    "fresh writer_epoch");
                throw MountFencedException(fmt::format(
                    "CAS mount-lease: key '{}' was fenced by GC inside the adopt window ({}) — "
                    "recoverable: re-open with a fresh writer_epoch", key, describeMountHolder(current)));
            }
            emitMountEvent(event_sink, CasEventType::MountConflict, srid, "adopt", &current,
                "mount slot was touched while adopting our own mount slot — failing closed");
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS mount-lease: key '{}' was touched while adopting our own mount slot ({}) — failing closed",
                key, describeMountHolder(current));
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CAS mount-lease: key '{}' vanished while adopting our own mount slot — failing closed", key);
    }
    emitMountEvent(event_sink, CasEventType::MountClaim, srid, "adopt", &observed,
        "adopted our own already-live (uuid, epoch) mount slot");
    refreshConfirmedDeadline(last_attempt_wall_ms);
    return res.token;
}

void MountLeaseKeeper::onRenewCommitted()
{
    /// Anchor at the attempt start (stashed by prepareRenew), never at this ack instant — a slow
    /// ack must not extend either fence past what the durable body it acknowledges authorizes
    /// (spec rev.4 Phase B). Runs for EVERY successful `renewOnce` — background-driven or a direct
    /// caller (e.g. the startup-arm redo in `CasPool.cpp`'s `mountWritable`) — so the wall deadline
    /// never goes stale just because a renewal happened to be invoked outside the background loop.
    refreshConfirmedDeadline(last_attempt_wall_ms);
}

void MountLeaseKeeper::onRenewSucceeded()
{
    /// `confirmed_deadline_ms` was already refreshed by `onRenewCommitted` above, which `renewOnce`
    /// (base) calls right after recording the successful write — before the background loop reaches
    /// this hook. This hook fires only the boot-domain write-fence callback.
    if (on_renew_ok)
        on_renew_ok(last_attempt_boot_ms);
}

void MountLeaseKeeper::onRenewFailed()
{
    /// This is THE point at which this runtime stops believing it owns the mount, so it is where the
    /// release path's arm-A/arm-B split is decided (see `terminate`). Set BEFORE the fence callback, so
    /// a teardown racing this observation reads the conservative value.
    deposition_observed.store(true, std::memory_order_release);
    /// Background renewal failed: `renewOnce` threw and the loop is stopping. Latch the local write
    /// fence to lost so no further mutation proceeds — fail closed. The mismatch itself was already
    /// classified and emitted by `onRenewMismatch` (fenced_by_gc / same_epoch_state_uncertain /
    /// superseded / foreign_writer / vanished) just before this throw propagated here — this event is
    /// only the fence-latch timeline marker.
    emitMountEvent(event_sink, CasEventType::MountConflict, srid, "renew_failed", nullptr,
        "renew mismatch — see the preceding classified mount_conflict event; write fence latched to lost");
    if (on_lost)
        on_lost();
}

void MountLeaseKeeper::onRenewMismatch(const String & mismatched_key)
{
    /// The base contract's PreconditionFailed just means "our token didn't match" — re-read the
    /// CURRENT body and classify. All four body-present cases and the absent case are covered
    /// below, each fail-closed and NONE constructing a `LOGICAL_ERROR`, which aborts debug/ASan
    /// builds at exception construction — on this KEEPER THREAD, taking the whole process with it
    /// (STID 3982-3b48; parts 1a/1b covered vanished/absent-at-release, this covers the rest).
    const auto got = backend->get(mismatched_key);
    if (got)
    {
        const MountLease current = decodeMountLease(got->bytes);

        if (current.server_uuid == server_uuid && current.gc_fenced)
        {
            emitMountEvent(event_sink, CasEventType::MountConflict, srid, "fenced_by_gc", &current,
                "own mount slot fenced by GC after lease expiry (late renewal) — recoverable with a "
                "fresh writer_epoch");
            throw MountFencedException(fmt::format(
                "CAS mount-lease: key '{}' was fenced by GC after lease expiry (late renewal) ({}) — "
                "recoverable: re-open with a fresh writer_epoch", mismatched_key, describeMountHolder(current)));
        }

        if (current.server_uuid == server_uuid && current.writer_epoch == writer_epoch && !current.gc_fenced)
        {
            /// The slot advanced past our held token under our OWN (uuid, epoch), unfenced. This is
            /// state UNCERTAINTY, not proof of anything (spec rev.4): the common cause is our own
            /// earlier renewal PUT that landed while its ack was lost to a client-side timeout; the
            /// pathological one is a same-pair twin after durable epoch-state loss (narrowed by the
            /// allocateWriterEpoch re-mint guard). Both recover identically and fail closed: stop
            /// renewing, latch the write fence, self-remount under a fresh writer_epoch. Never a
            /// LOGICAL_ERROR — this shape is reachable by an ordinary network timeout.
            ProfileEvents::increment(ProfileEvents::CASMountLeaseLost);
            emitMountEvent(event_sink, CasEventType::MountConflict, srid, "same_epoch_state_uncertain", &current,
                "own mount slot advanced past our held token under our own (uuid, epoch) — state "
                "uncertain (ambiguous prior renewal or epoch-state loss); fencing and self-remounting");
            throw Exception(ErrorCodes::ABORTED,
                "CAS mount-lease: key '{}' advanced past our held token under our own (uuid, epoch) — "
                "state uncertain; fencing and recovering via self-remount (observed {} vs our seq={})",
                mismatched_key, describeMountHolder(current), seq);
        }

        if (current.server_uuid == server_uuid && current.writer_epoch != writer_epoch)
        {
            ProfileEvents::increment(ProfileEvents::CASMountLeaseLost);
            emitMountEvent(event_sink, CasEventType::MountConflict, srid, "superseded", &current,
                "own mount slot is held by a different writer_epoch — superseded by a newer incarnation");
            /// A normal fencing outcome (the model's localLost), not a programming assertion:
            /// a suspended predecessor legitimately resumes into this after a successor reclaimed.
            throw Exception(ErrorCodes::ABORTED,
                "CAS mount-lease: key '{}' was superseded by a newer incarnation ({}) — fencing "
                "(this incarnation is deposed; recovery is a fresh-epoch self-remount)",
                mismatched_key, describeMountHolder(current));
        }

        /// `current.server_uuid != server_uuid` — a DIFFERENT server holds the slot we thought was ours.
        /// The owner anchor refuses foreign claims at open and decommission impersonates the victim uuid
        /// rather than manufacturing a foreign one, so this is not something a healthy protocol run
        /// produces. It is still ENVIRONMENT-REACHABLE, and this comment used to claim otherwise: clear
        /// the pool prefix and recreate under a different server id — an operator `rm -rf`, or a
        /// recreation over a reused prefix — and the surviving writer's very next renewal lands exactly
        /// here. `CasRefContiguousAlloc.SurvivingWriterIsFencedByTheRecreatedPoolsMount` drives it
        /// deliberately, which is the plainest possible refutation of "unreachable".
        ///
        /// So it must not be a `LOGICAL_ERROR`. That class aborts debug/ASan builds at CONSTRUCTION, and
        /// this runs on the keeper's background thread, so a condition the environment can create took
        /// the whole process down. `ABORTED` instead — the same class the two sibling fencing arms above
        /// use, and one the storage layer already treats as a retry-safe mount-lost signal. The OUTCOME
        /// is unchanged and is the whole point: renewal stops, `onRenewFailed` latches the write fence,
        /// and this incarnation never takes over the foreign holder's slot.
        ///
        /// NOT yet done at the RELEASE path (`terminate` below): its foreign-incarnation arm has the
        /// same defect and is reached by the same test at teardown, but three `EXPECT_DEATH` tests
        /// (`CasGcRound.OrphanManifestCursorSweepDeletesAndPersistsCursor`,
        /// `CasMountStartup.StaleSelfMountReclaimedAfterWait`,
        /// `CasPoolRemount.ForeignOwnerIsNeverTakenOver`) deliberately pin that abort, so changing it is
        /// a ruled decision rather than a local fix.
        ProfileEvents::increment(ProfileEvents::CASMountLeaseLost);
        emitMountEvent(event_sink, CasEventType::MountConflict, srid, "foreign_writer", &current,
            "mount slot is held by a foreign server — failing closed, never taking over");
        throw Exception(ErrorCodes::ABORTED,
            "CAS mount-lease: key '{}' is held by a foreign server ({}) — failing closed, never taking over",
            mismatched_key, describeMountHolder(current));
    }

    /// The mount slot object VANISHED (backing store deleted under a live mount -- e.g. an
    /// operator or test rm -rf'd the pool dir). This is an ENVIRONMENTAL condition, not a logic
    /// error: there is no foreign writer to fail closed against. Stop renewing (fail-closed: the
    /// write fence latches to lost, we never re-mint) WITHOUT aborting the server --
    /// LOGICAL_ERROR here aborts debug/ASan builds at exception construction.
    ProfileEvents::increment(ProfileEvents::CASMountLeaseLost);
    emitMountEvent(event_sink, CasEventType::MountConflict, srid, "vanished", nullptr,
        "mount slot object vanished (backing store deleted under a live mount) — stopping renewal, fail-closed");
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
        "CAS mount-lease: key '{}' vanished (backing store deleted under a live mount) — "
        "stopping renewal, fail-closed (never re-minting)", mismatched_key);

    /// NOTE: the pre-rev.4 trailing `SingleWriterSlot::onRenewMismatch(mismatched_key)` call is
    /// GONE — the five cases above are exhaustive for this keeper (body-present × {fenced,
    /// same-pair-unfenced, superseded, foreign} + absent), so the base class's generic
    /// LOGICAL_ERROR is unreachable here. The base implementation stays for other slot subclasses.
}

void MountLeaseKeeper::terminate()
{
    /// Terminal op: retire the lease by stamping it already-expired (expires_at_ms = started_at_ms),
    /// seq+1, against the token we hold. This makes a same-uuid reopen immediately reclaimable. The
    /// merged watermark farewell folds in HERE: `min_active = UINT64_MAX` is the retired sentinel the
    /// GC floor treats as "every build_seq of this server is retired" — one release retires both the
    /// mount lease and the build watermark.
    const uint64_t now_ms = now_ms_fn();
    const String body = encodeMountLease(MountLease{
        .server_uuid = server_uuid,
        .writer_epoch = writer_epoch,
        .hostname = getFQDNOrHostName(),
        .pid = static_cast<uint64_t>(::getpid()),
        .started_at_ms = now_ms,
        .seq = seq + 1,
        .expires_at_ms = now_ms,
        .min_active = std::numeric_limits<uint64_t>::max(),
    });
    const PutResult res = backend->putOverwrite(key, body, last_token);
    if (res.outcome != PutOutcome::Done)
    {
        /// A foreign incarnation on OUR release path has one clean cause: GC fenced this mount out
        /// after its lease expired (the `gc_fenced` stamp). The slot is already released-by-fence and
        /// there is nothing left to retire.
        if (const auto got = backend->get(key))
        {
            const MountLease current = decodeMountLease(got->bytes);
            if (current.gc_fenced)
            {
                LOG_INFO(getLogger("CasMountLeaseKeeper"),
                    "CAS mount-lease: '{}' was fenced out by GC (expired lease); release is a no-op", key);
                return;
            }

            /// Everything else used to be one arm raising `LOGICAL_ERROR` — "the world is broken". It
            /// is TWO situations, they mean opposite things, and neither may abort: this runs from
            /// `~Pool` via `finishTeardown`, whose `catch` a `LOGICAL_ERROR` defeats by aborting at
            /// CONSTRUCTION, so an ordinary failover took the process down.
            ///
            /// ARM A — this runtime has already stopped believing it owns the mount (renewal failed
            /// and the write fence latched). A foreign occupant is then the EXPECTED end state of
            /// failover: our successor owns the slot, and the farewell we were about to write would
            /// stamp OUR identity over THEIRS. Skip it, leave the slot byte-for-byte untouched, and let
            /// teardown finish quietly. Reached whenever a deposed writer shuts down.
            ///
            /// Nothing here is ABORT-CAPABLE, which is the property that matters on a destructor path —
            /// not "nothing throws". `finishTeardown` wraps this call in a `catch` and logs, so a throw
            /// is contained; what a `LOGICAL_ERROR` did instead was abort at CONSTRUCTION, before that
            /// catch could ever run. This arm happens not to throw at all, but it is the exception CLASS
            /// discipline, not the absence of a `throw`, that keeps teardown alive.
            if (deposition_observed.load(std::memory_order_acquire))
            {
                ProfileEvents::increment(ProfileEvents::CASMountReleaseSkippedForeignOccupant);
                emitMountEvent(event_sink, CasEventType::MountRelease, srid, "deposed_foreign_occupant", &current,
                    "mount slot is held by our successor and this incarnation was already deposed — "
                    "skipping the farewell, slot left untouched");
                LOG_WARNING(getLogger("CasMountLeaseKeeper"),
                    "CAS mount-lease: '{}' is held by {} and this incarnation was already deposed — skipping "
                    "the farewell rather than stamping our identity over the successor's; release is a no-op",
                    key, describeMountHolder(current));
                return;
            }

            /// ARM B — we never observed a deposition, so this runtime still believed it owned the mount
            /// and a DIFFERENT one is in the slot. That is the single-writer guarantee broken, and it
            /// stays maximally loud: named identities on both sides, its own counter, the write fence
            /// latched so the runtime stops trusting itself, and NO write (the occupant is left exactly
            /// as found — we do not retire someone else's lease).
            ///
            /// Loud, but still not abort-capable. Logical errors are exceptions here, not crashes, and
            /// this verdict rests on a READ of the slot: a stale or adversarial backend can fabricate it
            /// from the environment, which is precisely the input class that must never be able to kill
            /// the server. There is deliberately no `chassert` either — it would abort exactly the
            /// debug/ASan runs of the tests that now have to prove teardown SURVIVES this.
            ProfileEvents::increment(ProfileEvents::CASMountExclusivityViolation);
            emitMountEvent(event_sink, CasEventType::MountConflict, srid, "exclusivity_violation", &current,
                "mount slot is held by a foreign incarnation although this runtime never observed a "
                "deposition — single-writer exclusivity is broken; refusing the release and fencing");
            LOG_ERROR(getLogger("CasMountLeaseKeeper"),
                "CAS mount-lease: release of key '{}' found a FOREIGN incarnation ({}) while this runtime "
                "(server_uuid={} writer_epoch={} seq={}) still believed it owned the mount — single-writer "
                "exclusivity is broken. Refusing to retire another incarnation's lease; the slot is left "
                "untouched and this runtime's write fence is latched.",
                key, describeMountHolder(current), u128ToHex(server_uuid), writer_epoch, seq);
            if (on_lost)
                on_lost();
            throw Exception(ErrorCodes::ABORTED,
                "CAS mount-lease: release of key '{}' found a foreign incarnation ({}) while this runtime "
                "still believed it owned the mount — single-writer exclusivity is broken; the slot is left "
                "untouched and this runtime is fenced", key, describeMountHolder(current));
        }
        /// The lease object is ABSENT: the backing store was deleted under us (rm -rf of the pool
        /// dir -- the same environmental condition the renewal path classifies as "vanished").
        /// The desired end state of a release is "no live lease object", which is already true, so
        /// this is a clean no-op release, never a LOGICAL_ERROR (which aborts debug/ASan builds).
        ProfileEvents::increment(ProfileEvents::CASMountLeaseLost);
        emitMountEvent(event_sink, CasEventType::MountRelease, srid, "vanished", nullptr,
            "mount slot object already gone at release (backing store deleted) — no-op release");
        LOG_INFO(getLogger("CasMountLeaseKeeper"),
            "CAS mount-lease: '{}' is already gone at release (backing store deleted); release is a no-op", key);
        return;
    }
    emitMountEvent(event_sink, CasEventType::MountRelease, srid, "farewell", nullptr,
        "graceful release — lease stamped already-expired, watermark farewell folded in");
    recordWrite(seq + 1, res.token);
}

SingleWriterSlot::SingleWriterSlot(
    BackendPtr backend_, String key_, std::string_view slot_name_, std::string_view terminal_verb_,
    std::string_view logger_name_)
    : backend(std::move(backend_))
    , key(std::move(key_))
    , slot_name(slot_name_)
    , terminal_verb(terminal_verb_)
    , log(getLogger(String(logger_name_)))
{
}

SingleWriterSlot::~SingleWriterSlot()
{
    /// Stop the renewal thread only — deliberately NO terminal op. Destruction without a terminal op
    /// leaves the slot object persisted with a frozen seq, which full GC observes as stale state.
    stopBackground();
}

void SingleWriterSlot::recordWrite(uint64_t new_seq, const Token & token)
{
    seq = new_seq;
    last_token = token;
}

void SingleWriterSlot::doStart()
{
    /// Compute the per-call payload BEFORE taking state_mutex: a subclass callback (the watermark's
    /// min_active hook) may reach into the Pool's own lock, so we never hold state_mutex across it.
    const RenewPayload payload = prepareRenew();

    std::lock_guard lock(state_mutex);
    if (dead)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS {}: start after {} on key '{}'", slot_name, terminal_verb, key);
    if (seq != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS {}: already started on key '{}'", slot_name, key);

    const String body = encodeBody(/*seq=*/1, payload);
    const Token token = claim(body);
    recordWrite(/*new_seq=*/1, token);
}

void SingleWriterSlot::renewOnce()
{
    /// Compute the per-call payload BEFORE taking state_mutex (see doStart): never hold state_mutex
    /// across the subclass callback.
    const RenewPayload payload = prepareRenew();

    /// INVARIANT: holding `state_mutex` across the PUT below is safe ONLY because (a) doTerminate
    /// joins the renewal thread before taking this mutex and (b) renewOnce has a single driver.
    /// Do NOT add new `state_mutex`-guarded accessors without revisiting this (they would stall for
    /// a full network round trip); the prepareRenew pattern above shows the lock-free alternative.
    std::lock_guard lock(state_mutex);
    /// Reset BEFORE the guards below: a `dead`/`seq==0` throw (a programming-bug guard, not a backend
    /// outcome) must not be misread as a CONFIRMED mismatch by `backgroundLoop` -- it falls into the
    /// TRANSIENT bucket by leaving this false, exactly like a `putOverwrite` exception below.
    last_renew_failure_was_confirmed_mismatch = false;
    if (dead)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS {}: renew after {} on key '{}'", slot_name, terminal_verb, key);
    if (seq == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS {}: renew before start on key '{}'", slot_name, key);

    const String body = encodeBody(seq + 1, payload);
    const PutResult res = backend->putOverwrite(key, body, last_token);
    if (res.outcome != PutOutcome::Done)
    {
        /// The PUT completed and observed a foreign token -- a CONFIRMED mismatch (proven
        /// supersession), not a transient failure. Mark it BEFORE calling the hook, which always throws.
        last_renew_failure_was_confirmed_mismatch = true;
        onRenewMismatch(key);
    }

    recordWrite(seq + 1, res.token);
    /// Reached only on success (the branch above always throws on a mismatch). Notify the subclass
    /// EVERY successful renewal is committed — background-driven (`backgroundLoop`'s own call) or a
    /// direct caller (a redo site invoking `renewOnce` outright) alike; the mount-lease keeper
    /// refreshes its confirmed-lease wall deadline here regardless of who called us.
    onRenewCommitted();
}

void SingleWriterSlot::onRenewMismatch(const String & mismatched_key)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "CAS {}: key '{}' was touched by a foreign writer — failing closed, never re-minting", slot_name, mismatched_key);
}

void SingleWriterSlot::doTerminate()
{
    /// Join the renewal thread before taking the state lock, so no renewal races the terminal op.
    stopBackground();

    std::lock_guard lock(state_mutex);
    if (seq == 0)
        /// Never started (e.g. `Pool::open` failed before/inside `doStart`) — nothing was claimed,
        /// so there is nothing to release. A never-started slot is inert: BOTH/ALL terminate calls on
        /// it are quiet no-ops, and — unlike the genuinely-started path below — we do NOT set `dead`,
        /// so a second no-op call takes this same early-return rather than tripping the "double
        /// terminate" throw below. Throwing here only turned an already-failing teardown into extra
        /// `LOGICAL_ERROR` noise during teardown.
        return;
    if (dead)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS {}: double {} on key '{}'", slot_name, terminal_verb, key);

    /// Dead regardless of what terminate does below: we attempted the terminal op, the keeper must
    /// never renew this key again.
    dead = true;
    terminate();
}

void SingleWriterSlot::startBackground(std::chrono::milliseconds period)
{
    /// After a thread-side renewal failure the loop returns (see backgroundLoop) but the thread
    /// handle stays joinable, so a subsequent startBackground throws "already running" until
    /// stopBackground is called. Intentional fail-closed: we never silently re-arm renewal after it
    /// has failed.
    std::lock_guard lock(background_mutex);
    if (thread.joinable())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS {}: background renewal is already running for key '{}'", slot_name, key);
    stop_requested = false;
    thread = ThreadFromGlobalPool([this, period] { backgroundLoop(period); });
}

void SingleWriterSlot::stopBackground()
{
    ThreadFromGlobalPool to_join;
    {
        std::lock_guard lock(background_mutex);
        if (!thread.joinable())
            return;
        stop_requested = true;
        wakeup.notify_all();
        to_join = std::move(thread);
    }
    to_join.join();
}

void SingleWriterSlot::backgroundLoop(std::chrono::milliseconds period)
{
    setThreadName(ThreadName::CAS_LEASE_KEEPER);
    /// A CONFIRMED mismatch, or a TRANSIENT failure once `shouldFenceOnTransientRenewFailure` says the
    /// lease deadline has neared, stops the loop for good: the slot's seq stops
    /// advancing and GC observes the frozen seq. No retry, no re-mint. A TRANSIENT failure while the
    /// deadline is still safely away keeps the loop alive -- the mount-lease protocol guarantees no
    /// other writer can claim the slot before that deadline, so retrying is safe.
    std::unique_lock lock(background_mutex);
    while (!stop_requested)
    {
        if (wakeup.wait_for(lock, period, [this] { return stop_requested; }))
            break;

        lock.unlock();
        try
        {
            renewOnce();
        }
        catch (...)
        {
            /// `renewOnce` and this loop run on the SAME background thread, sequentially -- no
            /// synchronization needed to read the flag it just set.
            const bool confirmed = last_renew_failure_was_confirmed_mismatch;
            if (!confirmed && !shouldFenceOnTransientRenewFailure())
            {
                tryLogCurrentException(log, fmt::format(
                    "CAS {}: background renewal failed transiently, retrying while the lease is still valid",
                    slot_name));
                lock.lock();
                continue;
            }

            tryLogCurrentException(
                log, fmt::format("CAS {}: background renewal failed, the {} stops advancing", slot_name, slot_name));
            /// Notify the subclass that renewal failed and the loop is stopping (off `state_mutex`).
            /// The mount-lease keeper latches its local write fence to lost here. Never let the hook's
            /// own throw escape the loop — we are already stopping.
            try
            {
                onRenewFailed();
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
                /// The renewal loop is already stopping; a hook exception must not escape it.
            }
            return;
        }
        /// Successful renewal: notify the subclass (off `state_mutex`) before sleeping again. The
        /// wall deadline was already refreshed inside `renewOnce` (`onRenewCommitted`); the
        /// mount-lease keeper fires the boot-domain write-fence callback here.
        try
        {
            onRenewSucceeded();
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// A notification hook cannot be allowed to stop the already-renewed lease loop.
        }
        lock.lock();
    }
}

void sweepOwnMountStaging(IObjectStorage & object_storage, const String & mount_staging_prefix) noexcept
{
    try
    {
        /// max_keys=0 asks `listObjects` for the FULL listing under the prefix (it paginates until
        /// exhausted rather than capping at some default page size) — see `IObjectStorage::listObjects`.
        /// A mount's own staging debris is bounded (one mount's in-flight + leaked uploads), so a single
        /// unbounded LIST at startup is acceptable; unlike GC's per-round budgets, this runs once per
        /// mount, not on a recurring schedule.
        RelativePathsWithMetadata children;
        object_storage.listObjects(mount_staging_prefix, children, /*max_keys=*/0);

        size_t removed = 0;
        for (const auto & child : children)
        {
            try
            {
                object_storage.removeObjectIfExists(StoredObject(child->relative_path));
                ++removed;
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
                /// Best-effort: one stubborn key must not abort the sweep of the rest — it is retried
                /// by a later mount's sweep.
            }
        }

        if (removed)
            LOG_INFO(getLogger("CasStagingSweeper"),
                "Reclaimed {} leaked S3 staging object(s) under '{}' at mount start",
                removed, mount_staging_prefix);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Best-effort: a LIST failure (a transient backend hiccup) at mount time must never fail the
        /// mount — any leaked staging objects are bounded debris, reclaimed by a later mount's sweep.
    }
}

}
