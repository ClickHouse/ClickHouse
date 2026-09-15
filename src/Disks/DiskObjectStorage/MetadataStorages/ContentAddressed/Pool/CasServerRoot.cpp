#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Common/logger_useful.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/setThreadName.h>
#include <Core/UUID.h>
#include <base/getFQDNOrHostName.h>
#include <fmt/format.h>
#include <magic_enum.hpp>

#include <algorithm>
#include <array>
#include <ctime>
#include <exception>
#include <limits>
#include <set>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <unistd.h>

namespace ProfileEvents
{
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

void reportMountRenewCompletion(const MountRenewResult & result) noexcept;
void configureMountRenewObservability(
    const String * server_root_id, const CasEventSink * event_sink, bool deferred) noexcept;
void deliverDeferredMountRenewObservability(uint64_t remount_attempt_no) noexcept;

/// The owner, epoch, and mount-lease wire codecs are implemented in
/// `Formats/CasServerRootFormats`; this file contains the mount-safety protocol logic that uses
/// those codecs.

namespace
{
/// TRUE iff a one-key listing of `prefix` returns anything.
bool prefixHasAnyKey(CasOperation & op, const String & prefix)
{
    return !op.list(prefix, /*cursor*/ "", /*limit*/ 1, Retry::standard()).keys.empty();
}

/// The write's own verdict on whether somebody else holds the key: for a refused precondition, what
/// the write's resolve read saw there; nothing when this write landed. Every other ending failed to
/// reach the store and must surface as itself -- reading it as a rival writer is how a transport
/// outage becomes a "double start" report.
std::optional<Observation> conflictOrThrow(WriteResult && result, const String & what)
{
    if (Conflict * conflict = std::get_if<Conflict>(&result))
        return std::move(conflict->seen);
    orThrow(std::move(result), what);
    return std::nullopt;
}

/// A raced claim, reported as the OCCUPANT the write's own resolve read observed rather than as the
/// lease this server proposed and failed to install -- that body is what the caller renders into the
/// fail-closed operator message, and naming ourselves there points an operator at the wrong process.
/// The observed incarnation names the body returned beside it, so the caller's observation loop
/// compares like with like. A conflict the resolve read could not settle to a body saw NOBODY, and
/// reports nobody: the caller's own re-read is what identifies the holder there.
MountClaimResult racedDoubleStart(const Observation & seen)
{
    if (const Object * occupant = std::get_if<Object>(&seen))
        return {.kind = MountClaimResult::LiveDoubleStart,
                .body = decodeMountLease(occupant->bytes),
                .etag = occupant->etag};
    return {.kind = MountClaimResult::LiveDoubleStart, .body = std::nullopt, .etag = std::nullopt};
}

uint64_t defaultBootMs()
{
    struct timespec ts{};
    clock_gettime(CLOCK_BOOTTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000 + static_cast<uint64_t>(ts.tv_nsec) / 1000000;
}

/// Why a renewal ended without a retained lease, in the vocabulary the audit event reports. Each
/// value is assigned from exactly one arm of the write's verdict, so the event never re-derives a
/// reason from state the request engine does not carry.
enum class MountRenewTerminalClassification : uint8_t
{
    Unclassified,
    DeterministicFailure,
    Conflict,
    Vanished,
    Cancelled,
    FenceOrLifecycleLost,
    ExternalLeaseDeadline,
    RequestDeadline,
    Unresolved,
};

/// One logical renewal's audit snapshot. Fixed-size and trivially copyable so a reentrant event sink
/// gets a distinct stack slot instead of aliasing the call that is still running.
struct MountRenewObservabilityContext
{
    bool active = false;
    bool completed = false;
    bool deferred = false;
    const String * server_root_id = nullptr;
    const CasEventSink * event_sink = nullptr;
    uint64_t writer_epoch = 0;
    uint64_t seq = 0;
    UInt128 write_attempt_id{};
    uint64_t observability_start_boot_ms = 0;
    uint64_t confirmed_deadline_boot_ms = 0;
    uint64_t initial_confirmed_budget_ms = 0;
    MountRenewOutcome outcome = MountRenewOutcome::NotAttempted;
    MountRenewTerminalClassification terminal_classification = MountRenewTerminalClassification::Unclassified;
    uint32_t attempts_sent = 0;
    bool resolved_by_read = false;
};

static_assert(std::is_trivially_copyable_v<MountRenewObservabilityContext>);

struct MountRenewObservabilityConfiguration
{
    bool configured = false;
    bool deferred = false;
    const String * server_root_id = nullptr;
    const CasEventSink * event_sink = nullptr;
};

/// Event sinks may synchronously renew another Pool on the same thread. A fixed stack keeps every
/// registered outer per-call snapshot stable without allocation, including while a parked redo holds
/// `remount_mutex`. Overflow suppresses rich event/log delivery for the nested call rather than
/// aliasing an outer call or changing protocol behavior; physical attempt truth is independently
/// retained by the stack-local observer in `MountLeaseRenewer::renew`.
struct MountRenewObservabilityStack
{
    static constexpr size_t capacity = 8;
    std::array<MountRenewObservabilityContext, capacity> contexts;
    size_t depth = 0;
    size_t suppressed_depth = 0;
    MountRenewObservabilityConfiguration pending;
};

thread_local MountRenewObservabilityStack mount_renew_observability;

MountRenewObservabilityContext * currentMountRenewObservability() noexcept
{
    if (mount_renew_observability.suppressed_depth != 0 || mount_renew_observability.depth == 0)
        return nullptr;
    return &mount_renew_observability.contexts[mount_renew_observability.depth - 1];
}

void markMountRenewTermination(MountRenewTerminalClassification classification) noexcept
{
    if (MountRenewObservabilityContext * context = currentMountRenewObservability())
        context->terminal_classification = classification;
}

enum class MountRenewObservabilityRegistration : uint8_t
{
    Stack,
    Suppressed,
    Ignored,
};

MountRenewObservabilityRegistration beginMountRenewObservabilityCall() noexcept
{
    const MountRenewObservabilityConfiguration configured = std::exchange(
        mount_renew_observability.pending, MountRenewObservabilityConfiguration{});
    if (!configured.configured)
        return MountRenewObservabilityRegistration::Ignored;
    if (mount_renew_observability.depth == MountRenewObservabilityStack::capacity)
    {
        ++mount_renew_observability.suppressed_depth;
        return MountRenewObservabilityRegistration::Suppressed;
    }

    mount_renew_observability.contexts[mount_renew_observability.depth++] = MountRenewObservabilityContext{
        .deferred = configured.deferred,
        .server_root_id = configured.server_root_id,
        .event_sink = configured.event_sink,
    };
    return MountRenewObservabilityRegistration::Stack;
}

void abandonMountRenewObservabilityCall() noexcept
{
    if (mount_renew_observability.suppressed_depth != 0)
    {
        --mount_renew_observability.suppressed_depth;
        return;
    }
    if (mount_renew_observability.depth != 0)
        --mount_renew_observability.depth;
}

class MountRenewObservabilityCallGuard
{
public:
    explicit MountRenewObservabilityCallGuard(MountRenewObservabilityRegistration registration_)
        : registration(registration_)
        , uncaught_on_entry(std::uncaught_exceptions())
    {
    }

    ~MountRenewObservabilityCallGuard()
    {
        if (registration == MountRenewObservabilityRegistration::Ignored)
            return;
        if (std::uncaught_exceptions() > uncaught_on_entry)
            abandonMountRenewObservabilityCall();
    }

private:
    MountRenewObservabilityRegistration registration;
    int uncaught_on_entry;
};

void initializeMountRenewObservability(
    const String & server_root_id,
    uint64_t writer_epoch,
    uint64_t seq,
    UInt128 write_attempt_id,
    uint64_t attempt_start_boot_ms,
    uint64_t confirmed_deadline_boot_ms,
    const CasEventSink & event_sink) noexcept
{
    MountRenewObservabilityContext * context = currentMountRenewObservability();
    if (!context)
        return;
    const bool deferred = context->deferred;
    const String * configured_server_root_id = context->server_root_id;
    const CasEventSink * configured_event_sink = context->event_sink;
    *context = MountRenewObservabilityContext{
        .active = true,
        .completed = false,
        .deferred = deferred,
        .server_root_id = configured_server_root_id ? configured_server_root_id : &server_root_id,
        .event_sink = configured_event_sink ? configured_event_sink : &event_sink,
        .writer_epoch = writer_epoch,
        .seq = seq,
        .write_attempt_id = write_attempt_id,
        .observability_start_boot_ms = defaultBootMs(),
        .confirmed_deadline_boot_ms = confirmed_deadline_boot_ms,
        .initial_confirmed_budget_ms = confirmed_deadline_boot_ms > attempt_start_boot_ms
            ? confirmed_deadline_boot_ms - attempt_start_boot_ms
            : 0,
    };
}

uint64_t elapsedSince(uint64_t start_boot_ms, uint64_t now_boot_ms)
{
    return now_boot_ms >= start_boot_ms ? now_boot_ms - start_boot_ms : 0;
}

uint64_t remainingConfirmedBudget(const MountRenewObservabilityContext & context, uint64_t now_boot_ms)
{
    const uint64_t elapsed_ms = elapsedSince(context.observability_start_boot_ms, now_boot_ms);
    return context.initial_confirmed_budget_ms > elapsed_ms
        ? context.initial_confirmed_budget_ms - elapsed_ms
        : 0;
}

void emitMountRenewEvent(
    const MountRenewObservabilityContext & context,
    const String & write_attempt_id,
    std::string_view outcome,
    uint32_t attempts_sent,
    uint64_t now_boot_ms,
    std::string_view classification,
    uint64_t remount_attempt_no) noexcept
{
    if (!context.event_sink || !*context.event_sink || !context.server_root_id)
        return;
    try
    {
        CasEvent event;
        event.type = CasEventType::WatermarkRenew;
        event.outcome = String{outcome};
        event.reason = outcome == "recovered"
            ? "CAS mount renewal recovered before its confirmed lease-safety deadline"
            : "CAS mount renewal ended without retained authority and fenced the mount";
        event.detail = {
            {"server_root_id", *context.server_root_id},
            {"writer_epoch", std::to_string(context.writer_epoch)},
            {"seq", std::to_string(context.seq)},
            {"write_attempt_id", write_attempt_id},
            {"attempts_sent", std::to_string(attempts_sent)},
            {"elapsed_ms", std::to_string(elapsedSince(context.observability_start_boot_ms, now_boot_ms))},
            {"remaining_confirmed_budget_ms", std::to_string(remainingConfirmedBudget(context, now_boot_ms))},
            {"classification", String{classification}},
        };
        if (remount_attempt_no != 0)
            event.detail["remount_attempt_no"] = std::to_string(remount_attempt_no);
        (*context.event_sink)(std::move(event));
    }
    catch (...)
    {
        /// Event construction and sink failures are diagnostic-only.
    }
}

constexpr std::string_view terminalClassificationName(MountRenewTerminalClassification classification)
{
    switch (classification)
    {
        case MountRenewTerminalClassification::DeterministicFailure: return "deterministic_failure";
        case MountRenewTerminalClassification::Conflict: return "conflict";
        case MountRenewTerminalClassification::Vanished: return "vanished";
        case MountRenewTerminalClassification::Cancelled: return "cancelled";
        case MountRenewTerminalClassification::FenceOrLifecycleLost: return "fence_or_lifecycle_lost";
        case MountRenewTerminalClassification::ExternalLeaseDeadline: return "external_lease_deadline";
        case MountRenewTerminalClassification::RequestDeadline: return "request_deadline";
        case MountRenewTerminalClassification::Unresolved: return "unresolved";
        case MountRenewTerminalClassification::Unclassified: return "terminal_unclassified";
    }
    return "terminal_unclassified";
}

void deliverMountRenewObservability(
    const MountRenewObservabilityContext & context, uint64_t remount_attempt_no) noexcept
{
    if (!context.active || !context.completed || !context.server_root_id)
        return;

    try
    {
        const uint64_t now_boot_ms = defaultBootMs();
        const String write_attempt_id = u128ToHex(context.write_attempt_id).substr(0, 12);

        for (uint32_t attempt_no = 2; attempt_no <= context.attempts_sent; ++attempt_no)
        {
            try
            {
                LOG_DEBUG(
                    getLogger("CasMountLeaseRenewer"),
                    "CAS mount renewal '{}' physical retry attempt {} (writer_epoch={}, seq={})",
                    *context.server_root_id,
                    attempt_no,
                    context.writer_epoch,
                    context.seq);
            }
            catch (...)
            {
            }
        }

        const bool recovered = context.outcome == MountRenewOutcome::Committed
            && (context.attempts_sent > 1 || context.resolved_by_read);
        if (recovered)
        {
            const std::string_view classification = context.resolved_by_read
                ? "committed_by_read"
                : "committed_after_retry";
            emitMountRenewEvent(
                context,
                write_attempt_id,
                "recovered",
                context.attempts_sent,
                now_boot_ms,
                classification,
                remount_attempt_no);
            try
            {
                LOG_INFO(
                    getLogger("CasMountLeaseRenewer"),
                    "CAS mount renewal '{}' recovered after {} physical attempts in {} ms "
                    "(classification={}, confirmed_deadline_boot_ms={})",
                    *context.server_root_id,
                    context.attempts_sent,
                    elapsedSince(context.observability_start_boot_ms, now_boot_ms),
                    classification,
                    context.confirmed_deadline_boot_ms);
            }
            catch (...)
            {
            }
        }
        else if (context.outcome == MountRenewOutcome::Terminal)
        {
            const std::string_view classification = terminalClassificationName(context.terminal_classification);
            emitMountRenewEvent(
                context,
                write_attempt_id,
                "failed",
                context.attempts_sent,
                now_boot_ms,
                classification,
                remount_attempt_no);
            try
            {
                LOG_WARNING(
                    getLogger("CasMountLeaseRenewer"),
                    "CAS mount renewal '{}' fenced after {} physical attempts in {} ms "
                    "(classification={}, confirmed_deadline_boot_ms={})",
                    *context.server_root_id,
                    context.attempts_sent,
                    elapsedSince(context.observability_start_boot_ms, now_boot_ms),
                    classification,
                    context.confirmed_deadline_boot_ms);
            }
            catch (...)
            {
            }
        }
    }
    catch (...)
    {
        /// Formatting, logger, and event-sink failures are diagnostic-only.
    }
}

/// Forward declaration: defined below (same TU-unique anonymous namespace) — `allocateWriterEpoch`
/// names the current mount holder in its DecommissionRecovery live-refusal message.
String describeMountHolder(const MountLease & m);

std::optional<OwnerObject> readOwnerObject(CasOperation & op, const Layout & l, const String & server_root_id)
{
    const auto got = op.read(l.ownerKey(server_root_id), Retry::standard());
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

void configureMountRenewObservability(
    const String * server_root_id, const CasEventSink * event_sink, bool deferred) noexcept
{
    mount_renew_observability.pending = MountRenewObservabilityConfiguration{
        .configured = true,
        .deferred = deferred,
        .server_root_id = server_root_id,
        .event_sink = event_sink,
    };
}

void reportMountRenewCompletion(const MountRenewResult & result) noexcept
{
    if (mount_renew_observability.suppressed_depth != 0)
    {
        --mount_renew_observability.suppressed_depth;
        return;
    }
    MountRenewObservabilityContext * context = currentMountRenewObservability();
    if (!context || !context->active)
        return;
    context->completed = true;
    context->outcome = result.outcome;
    context->attempts_sent = std::max(context->attempts_sent, result.attempts_sent);
    context->resolved_by_read = result.resolved_by_read;
    if (context->deferred)
        return;

    /// Pop before invoking any callback. A reentrant sink gets a distinct stack slot and cannot alter
    /// the completed outer snapshot.
    const MountRenewObservabilityContext completed = *context;
    --mount_renew_observability.depth;
    deliverMountRenewObservability(completed, /*remount_attempt_no=*/0);
}

void deliverDeferredMountRenewObservability(uint64_t remount_attempt_no) noexcept
{
    MountRenewObservabilityContext * context = currentMountRenewObservability();
    if (!context || !context->deferred)
        return;
    const MountRenewObservabilityContext completed = *context;
    --mount_renew_observability.depth;
    deliverMountRenewObservability(completed, remount_attempt_no);
}

bool serverRootSubtreeEmpty(
    CasOperation & op, const Layout & l, const String & srid, const RefCatalog & catalog_observation)
{
    const String owned_prefix = srid + "/";
    for (const CatalogEntry & entry : catalog_observation.entries)
        if (entry.ns.string() == srid || entry.ns.string().starts_with(owned_prefix))
            return false;

    /// Manifests and loose roots retain logical path identity. Opaque namespace stream/state debris
    /// alone is not evidence that this server root owns live work.
    if (prefixHasAnyKey(op, l.casManifestsServerPrefix(srid)))
        return false;
    if (prefixHasAnyKey(op, l.serverRootDataPrefix(srid)))
        return false;
    return true;
}

std::optional<UInt128> readOwnerUuid(CasOperation & op, const Layout & l, const String & server_root_id)
{
    const std::optional<OwnerObject> owner = readOwnerObject(op, l, server_root_id);
    if (!owner)
        return std::nullopt;
    return owner->server_uuid;
}

void claimOwnerOrThrow(
    CasOperation & op, const Layout & l, const String & srid, UInt128 our_uuid,
    const ObserveRefCatalog & observe_catalog)
{
    if (!observe_catalog)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS server-root '{}': catalog observer is required", srid);
    const String key = l.ownerKey(srid);

    /// Owner present → it is identity: equal UUID is ok, a different UUID fails closed regardless
    /// of any lease/clock state.
    if (const std::optional<OwnerObject> owner = readOwnerObject(op, l, srid))
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
            "Recover by restoring the old local uuid file; or configure a fresh <cas_server_root_id> for this disk; "
            "or — only after verifying that NO server uses this root — manually delete the owner object '{}' and restart.",
            srid, u128ToHex(owner->server_uuid), u128ToHex(our_uuid), key);
    }

    /// Owner absent. Claiming is allowed ONLY over a provably-empty subtree; an absent owner over
    /// existing data means the identity was lost and must never be silently re-claimed.
    if (!serverRootSubtreeEmpty(op, l, srid, observe_catalog()))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS server-root '{}' has no owner anchor but its data subtree is non-empty "
            "(identity lost over existing data) — refusing to re-claim",
            srid);

    const std::optional<Observation> occupant = conflictOrThrow(
        op.create(key, encodeOwner(OwnerObject{.server_uuid = our_uuid, .retired_at_ms = std::nullopt}),
                  Retry::standard()),
        fmt::format("CAS server-root '{}' owner claim", srid));
    if (!occupant)
        return;

    /// The conditional create conflicted. Recompute the whole catalog + manifest + roots bundle;
    /// no stale emptiness result is carried across the conflict.
    if (!serverRootSubtreeEmpty(op, l, srid, observe_catalog()))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS server-root '{}' owner claim conflicted and newly visible owned work blocks recreation", srid);

    /// Race: another process claimed between our read and our create. The write's own resolve read
    /// already observed who took the key, and reading again would answer a later question than the one
    /// the conflict asked. Only an observation that settled nothing still owes a read.
    std::optional<OwnerObject> reread;
    if (const Object * observed = std::get_if<Object>(&*occupant))
        reread = decodeOwner(observed->bytes);
    else if (!std::holds_alternative<ProvenAbsent>(*occupant))
        reread = readOwnerObject(op, l, srid);
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
    CasOperation & op, const Layout & l, const String & srid, EpochMintPolicy policy, uint64_t now_ms,
    const ObserveRefCatalog & observe_catalog)
{
    if (!observe_catalog)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS server-root '{}': catalog observer is required", srid);
    const String key = l.epochKey(srid);

    uint64_t allocated = 0;
    /// Set when the PREVIOUS decision wrote against an absent epoch. Its conflict means a winner may
    /// have installed an epoch while owned work became visible, so the emptiness bundle that
    /// authorized that attempt is recomputed before this decision accepts any epoch state at all.
    bool previous_decision_saw_no_epoch = false;

    WriteResult result = op.readModifyWrite(key,
        [&](const std::optional<Object> & observed) -> std::optional<String>
        {
            if (previous_decision_saw_no_epoch && !serverRootSubtreeEmpty(op, l, srid, observe_catalog()))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS server-root '{}' writer_epoch allocation conflicted and newly visible owned "
                    "work blocks recreation",
                    srid);
            previous_decision_saw_no_epoch = !observed;

            ServerEpoch current;
            if (observed)
            {
                current = decodeServerEpoch(observed->bytes);
            }
            else
            {
                /// A missing `epoch` over a non-empty subtree is a reset hazard (durable monotone
                /// counter cannot be reconstructed) — fail closed.
                if (!serverRootSubtreeEmpty(op, l, srid, observe_catalog()))
                    throw Exception(ErrorCodes::CORRUPTED_DATA,
                        "CAS server-root '{}' has no durable epoch object but its data subtree is "
                        "non-empty (writer_epoch reset hazard) — refusing to proceed",
                        srid);

                /// Same hazard through the CONTROL objects: an absent epoch while a mount object
                /// exists means epoch state was lost under a live/recent mount — re-minting epoch 1
                /// there is how a same-(uuid, epoch) twin is born. This is a lifecycle decision, so it
                /// uses the authoritative probe, never a read's absence (which flattens transport
                /// faults into "not found").
                const SentinelProbeResult mount_probe = op.probeSentinel(l.mountKey(srid), Retry::standard());
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
                            /// Deliberately weaker than claimMount's reclaim gate, which never trusts a
                            /// bare wall-clock comparison alone (only gc_fenced / the clean-farewell
                            /// min_active_build_sequence==UINT64_MAX marker / a caller-proven-dead
                            /// incarnation justify a reclaim there, because clock skew can misjudge
                            /// liveness). This is still safe: (a) the mint below is DISTINCT from the
                            /// survivor's epoch by construction, so no same-(uuid, epoch) pair is ever
                            /// representable even if this liveness read is wrong; (b) claimMount right
                            /// after this still applies its own STRONG liveness gate and refuses a
                            /// genuinely live member regardless of what happens here. So a clock-skewed
                            /// "terminal" misread can only burn one epoch number on a doomed
                            /// decommission attempt that aborts at claimMount — it can never admit a
                            /// claim over a live member.
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

            allocated = current.next_writer_epoch;
            return encodeServerEpoch(ServerEpoch{.next_writer_epoch = allocated + 1});
        },
        Retry::standard());

    /// Non-convergence used to be `CORRUPTED_DATA`, on the reasoning that a hundred lost conditional
    /// writes really is evidence of something wrong. The bound is a wall-clock deadline now, and ninety
    /// seconds of a throttled store is not evidence of anything, so `orThrow`'s retry-later class is
    /// the honest verdict. Both fail closed at `Pool::open`.
    orThrow(std::move(result), fmt::format("CAS server-root '{}' writer_epoch allocation", srid));
    return allocated;
}

namespace
{
/// Every holder-originated mount body gets a random durable identity. UUIDv4 cannot be zero, but
/// keep the postcondition explicit because zero is reserved as an uninitialized in-memory value.
UInt128 newMountWriteAttemptId()
{
    const UInt128 id = UUIDHelpers::generateV4().toUnderType();
    chassert(id != UInt128{});
    return id;
}

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
        .write_attempt_id = newMountWriteAttemptId(),
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
/// No-op when `sink` is unset, so a disabled log does no per-call work. This is a diagnostic-only,
/// non-interfering boundary: allocation while constructing the event and every sink failure are
/// contained so they cannot replace the protocol decision made at the call site. Branch and reason
/// are views specifically so literal arguments cannot allocate before entering this boundary.
void emitMountEvent(const CasEventSink & sink, CasEventType type, const String & srid,
                    std::string_view branch, const MountLease * observed, std::string_view reason) noexcept
{
    try
    {
        if (!sink)
            return;
        CasEvent e;
        e.type = type;
        e.object_kind = CasEventObjectKind::None;
        e.outcome = String{branch};
        e.reason = String{reason};
        e.detail["server_root_id"] = srid;
        e.detail["branch"] = String{branch};
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
    catch (...)
    {
        /// Mount audit delivery is optional. Do not log from this containment path: a logger may
        /// allocate or recurse through the same diagnostic machinery at a protocol-critical site.
    }
}
}

MountClaimResult claimMount(
    CasOperation & op, const Layout & l, const String & srid, UInt128 our_uuid, uint64_t our_epoch,
    uint64_t now_ms, uint64_t ttl_ms, const std::optional<Etag> & proven_dead_incarnation,
    const CasEventSink & sink, const std::optional<Etag> & unsafe_reclaim_authorization)
{
    const String key = l.mountKey(srid);
    const auto got = op.read(key, Retry::standard());

    /// Absent → fresh claim.
    if (!got)
    {
        const MountLease body = makeMountBody(our_uuid, our_epoch, /*seq=*/ 1, now_ms, ttl_ms);
        if (const std::optional<Observation> raced
                = conflictOrThrow(op.create(key, encodeMountLease(body), Retry::standard()),
                                  fmt::format("CAS mount slot claim of '{}'", key)))
            /// Raced with a concurrent writer between the read and the create. Treat as a live double
            /// start — fail closed; never overwrite a slot that appeared under us.
            return racedDoubleStart(*raced);
        emitMountEvent(sink, CasEventType::MountClaim, srid, "mint", nullptr, "fresh mount slot minted");
        return {.kind = MountClaimResult::Claimed, .body = body, .etag = std::nullopt};
    }

    const MountLease existing = decodeMountLease(got->bytes);

    /// Foreign owner → fail closed regardless of expiry. (This runs after the owner gate, so a foreign
    /// mount should not normally exist, but the lease must never be taken across UUIDs.)
    if (existing.server_uuid != our_uuid)
    {
        emitMountEvent(sink, CasEventType::MountConflict, srid, "foreign_owner", &existing,
            "mount slot is held by a foreign server_uuid — refusing to take over across identities");
        return {.kind = MountClaimResult::ForeignOwner, .body = existing, .etag = std::nullopt};
    }

    /// Same uuid + same epoch: it is OUR OWN claim — but a FENCED body is terminal for this
    /// (uuid, epoch): the GC dropped its ack from the floor when it fenced. Refreshing it in place
    /// would reactivate a fenced incarnation — a fence permanently consumes this `(server_uuid,
    /// writer_epoch)` pair, so the caller must re-open with a fresh `writer_epoch`.
    if (existing.writer_epoch == our_epoch)
    {
        if (existing.gc_fenced)
        {
            emitMountEvent(sink, CasEventType::MountConflict, srid, "fenced_by_gc", &existing,
                "own (uuid, epoch) mount slot is GC-fenced — terminal for this incarnation; "
                "recover with a fresh writer_epoch");
            return {.kind = MountClaimResult::FencedSelf, .body = existing, .etag = std::nullopt};
        }
        const MountLease body = makeMountBody(our_uuid, our_epoch, existing.seq + 1, now_ms, ttl_ms);
        if (const std::optional<Observation> raced
                = conflictOrThrow(op.replace(key, encodeMountLease(body), got->etag, Retry::standard()),
                                  fmt::format("CAS mount slot refresh of '{}'", key)))
            /// The mount changed under us between the read and the write, so `got->etag` is KNOWN
            /// STALE -- that mismatch is exactly why the write was refused. What the write's resolve
            /// read observed is current, and it is that pair that is reported.
            return racedDoubleStart(*raced);
        emitMountEvent(sink, CasEventType::MountClaim, srid, "refresh", &existing,
            "own claim replayed — refreshed seq + expiry");
        return {.kind = MountClaimResult::Claimed, .body = body, .etag = std::nullopt};
    }

    /// Same uuid, DIFFERENT epoch: reclaim ONLY on a certificate of death that needs no fresh
    /// wall-clock trust — never by comparing `expires_at_ms` against `now_ms`:
    ///   - `gc_fenced` → the fence-out is terminal for that incarnation by construction (its renewer's
    ///     every renewal fails the token guard forever, so it can never write again) — there is no
    ///     liveness left to wait for. This is what makes self-remount (and a fast restart after a
    ///     fence-out) instant instead of an observation wait.
    ///   - the clean marker (`min_active_build_sequence == UINT64_MAX`) → the predecessor's OWN graceful farewell
    ///     (`MountLeaseRenewer::terminate`) — no observation needed either.
    ///   - `proven_dead_incarnation` matches the one we just read → the CALLER
    ///     (`claimMountAwaitingExpiry`) already watched that exact incarnation hold stable for the full
    ///     observation threshold on its own clock; re-deriving that here from a bare wall-clock
    ///     comparison is exactly the cross-node trust that makes a clock-skewed or delayed observer
    ///     unsafe.
    ///   - `unsafe_reclaim_authorization` matches the one we just read → the operator's
    ///     `cas_unsafe_remount_no_delay` setting explicitly authorized this reclaim with NO
    ///     observation at all; the caller read this exact token and accepted the availability risk.
    /// Anything else → `LiveDoubleStart` (do NOT write): a same-uuid, different-epoch, not fenced, not
    /// clean-marked, not (yet) proven-dead, not unsafe-authorized lease may simply be a live twin, and
    /// `expires_at_ms` alone can never distinguish that from a dead predecessor across two different
    /// clocks.
    const bool clean_marker = existing.min_active_build_sequence == std::numeric_limits<uint64_t>::max();
    const bool proven_dead = proven_dead_incarnation && *proven_dead_incarnation == got->etag;
    const bool unsafe_authorized = unsafe_reclaim_authorization && *unsafe_reclaim_authorization == got->etag;
    if (existing.gc_fenced || clean_marker || proven_dead || unsafe_authorized)
    {
        const MountLease body = makeMountBody(our_uuid, our_epoch, existing.seq + 1, now_ms, ttl_ms);
        if (const std::optional<Observation> raced
                = conflictOrThrow(op.replace(key, encodeMountLease(body), got->etag, Retry::standard()),
                                  fmt::format("CAS mount slot reclaim of '{}'", key)))
            /// The mount changed under us between the read and the write — someone else is racing the
            /// reclaim. Fail closed, and report what the write's resolve read observed rather than
            /// `got->etag`, which that mismatch just proved stale.
            return racedDoubleStart(*raced);
        const MountPriorState prior = existing.gc_fenced ? MountPriorState::Fenced
                                     : clean_marker       ? MountPriorState::Clean
                                     : proven_dead        ? MountPriorState::UncleanObserved
                                                           : MountPriorState::UncleanUnsafe;
        emitMountEvent(sink, CasEventType::MountClaim, srid, "reclaim", &existing,
            existing.gc_fenced ? "same server_uuid, different writer_epoch, GC-fenced — reclaimed"
            : clean_marker     ? "same server_uuid, different writer_epoch, clean farewell — reclaimed"
            : proven_dead      ? "same server_uuid, different writer_epoch, observed dead by "
                                 "token-stability observation — reclaimed"
                               : "same server_uuid, different writer_epoch, reclaimed at once under "
                                 "cas_unsafe_remount_no_delay — the operator accepted that a live "
                                 "predecessor with this uuid may still be writing");
        return {.kind = MountClaimResult::Claimed, .body = body, .prior = prior, .etag = std::nullopt};
    }

    emitMountEvent(sink, CasEventType::MountConflict, srid, "live_double_start", &existing,
        "same server_uuid, different writer_epoch, not fenced/clean/proven-dead — no wall-clock trust; "
        "the caller must run the token-stability observation wait before reclaiming");
    /// No write was attempted on this path -- `got->etag` is exactly the CURRENT body's
    /// etag (what we just read is what's still there), so it is safe to hand back for the
    /// caller's observation loop to compare across polls without a redundant re-read.
    return {.kind = MountClaimResult::LiveDoubleStart, .body = existing, .etag = got->etag};
}

String mountDoubleStartMessage(const String & srid, const std::optional<MountLease> & existing)
{
    const String identity = existing
        ? fmt::format("server_uuid={} hostname={} pid={} last_seq={} expires_at_ms={}",
                      u128ToHex(existing->server_uuid), existing->hostname, existing->pid,
                      existing->seq, existing->expires_at_ms)
        : String("could not be observed -- the conditional write that lost this slot saw nothing at "
                 "the key, so the holder's identity is unknown to this server");
    return fmt::format(
        "Content-addressed disk cannot start: server_root_id '{}' is actively mounted by another LIVE server.\n"
        "  Existing mount: {}\n"
        "This server already waited for the mount lease to lapse, but it kept being renewed — a second\n"
        "server is holding the same CAS namespace. This prevents two ClickHouse servers from writing it.\n"
        " - If the other server is running intentionally, configure a unique <cas_server_root_id> for this disk.\n"
        " - If the other server is a stale/zombie process, stop it; this server will then reclaim the mount on restart.\n"
        " - LIVENESS: this wait judges the holder alive by its write token holding stable on THIS server's\n"
        "   own clock for the full observation threshold; the stamped expires_at_ms above never enters that\n"
        "   judgment on its own -- it is a writer-stamped diagnostic (also shown in system.cas_mounts), not\n"
        "   an authorization. Every server sharing this pool must run the SAME cas_mount_lease_ttl_ms and\n"
        "   cas_mount_renew_period_ms: a server configured with a shorter threshold than its peers can fence\n"
        "   out a healthy one.\n"
        " - If the local ClickHouse uuid file was regenerated, restore the old uuid file, or remove the stale\n"
        "   owner object gc/server-roots/{}/owner only after verifying no server uses this root.\n"
        " - As a LAST RESORT, after verifying that NO server is writing this root, manually delete the mount\n"
        "   object gc/server-roots/{}/mount and restart; this server will then re-claim it.\n"
        " - For a test stand or a deployment that guarantees one process per server_uuid,\n"
        "   cas_unsafe_remount_no_delay reclaims a slot carrying this server's own uuid at once instead of\n"
        "   waiting -- but a live predecessor sharing this uuid may still be writing, so enable it only\n"
        "   under that guarantee.",
        srid, identity, srid, srid);
}

uint64_t mountObservationThresholdMs(uint64_t ttl_ms, uint64_t cadence_ms)
{
    return ttl_ms + ttl_ms / 20 + cadence_ms;
}

MountClaimResult claimMountAwaitingExpiry(
    CasOperation & op, const Layout & l, const String & srid, UInt128 our_uuid, uint64_t our_epoch,
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
    /// comparison participates in this loop. `poll` here is half the renewal period (the caller's own
    /// poll cadence), so this threshold is close to, but not identical to, GC's heartbeat fence-out
    /// threshold, which passes the full renewal period into the same shared helper.
    const uint64_t threshold_ms = mountObservationThresholdMs(ttl_ms, poll);

    std::optional<Etag> observed;
    uint64_t observed_since = 0;
    size_t restarts = 0;

    while (true)
    {
        const bool threshold_met = observed && mono_ms_fn() - observed_since >= threshold_ms;
        MountClaimResult r = claimMount(op, l, srid, our_uuid, our_epoch, now_ms_fn(), ttl_ms,
            threshold_met ? observed : std::nullopt, sink, /*unsafe_reclaim_authorization=*/{});
        if (r.kind != MountClaimResult::LiveDoubleStart)
            return r;

        /// `claimMount` already read the current body, and a raced write reports whatever its own
        /// resolve read observed. Reuse `r.etag` whenever it is set instead of re-reading the SAME key
        /// here; only a raced write whose conflict observed nothing at all leaves it unset, and that is
        /// exactly where this reads -- for the body as well as the incarnation, since a result with no
        /// observation has no holder to report either.
        std::optional<Etag> current_etag = r.etag;
        if (!current_etag)
        {
            const auto got = op.read(l.mountKey(srid), Retry::standard());
            if (!got)
            {
                /// The slot vanished between claimMount's own GET and ours — normally self-resolving
                /// within one more `claimMount` call (which re-mints fresh on an absent slot), but under
                /// slot churn (something else concurrently removing/re-minting it) that resolution could
                /// keep losing the same race. Pace this like every other iteration and
                /// count it toward the SAME bounded restart budget the incarnation-churn case below
                /// uses, instead of spinning read/claim/write at backend RTT with no sleep and no bound
                /// — a persistently vanishing slot is exactly as "alive and contended" as a persistently
                /// renewing holder.
                if (++restarts > kMaxObservationRestarts)
                    return r;
                sleep_ms_fn(poll);
                continue;
            }
            current_etag = got->etag;
            r.body = decodeMountLease(got->bytes);
        }

        if (!observed || *observed != *current_etag)
        {
            if (observed && ++restarts > kMaxObservationRestarts)
                /// The incarnation kept changing across bounded restarts — the holder is genuinely alive
                /// (actively renewing), not a dead predecessor. Report it rather than waiting forever.
                return r;
            observed = *current_etag;
            observed_since = mono_ms_fn();
            if (on_wait_start && r.body)
                on_wait_start(*r.body, threshold_ms);
            LOG_INFO(getLogger("CasMountLease"),
                "Attempting to mount content-addressed server root {} after node change or hard "
                "restart; waiting ~{} ms (token-stability observation) to confirm the previous "
                "incarnation's operations are all finalized", srid, threshold_ms);
        }

        sleep_ms_fn(poll);
    }
}

HeartbeatFloor computeHeartbeatFloor(CasOperation & op, const Layout & l, uint64_t now_ms,
                                     uint64_t mono_now_ms, uint64_t stable_threshold_ms,
                                     MountObservationMap & obs)
{
    HeartbeatFloor floor;

    /// `obs` is keyed by every srid this leader has EVER observed, but a
    /// srid removed from the LIST entirely (its `/mount` key gone -- e.g. `SYSTEM CAS
    /// DROP POOL MEMBER`) is never visited by the walk below again, so its entry would otherwise linger
    /// forever (~150-250 B/srid, worse on a long-lived leader across many decommissions). Track every
    /// srid actually seen THIS pass and prune anything else out of `obs` at the end -- disjoint from the
    /// mid-walk `obs.erase(srid)` calls below (those fire for a srid seen but now terminal/fenced/gone
    /// this pass; this is for a srid not seen AT ALL).
    std::set<String> seen_srids;

    const String prefix = l.serverRootsPrefix();
    op.forEachListedKey(prefix, [&](const ListedKey & listed)
    {
        /// `/owner` and `/epoch` objects share the subtree — only mount bodies gate the floor.
        static constexpr std::string_view mount_suffix = "/mount";
        if (!listed.key.ends_with(mount_suffix))
            return true;

        const String & key = listed.key;

        /// The srid is the path segment between `serverRootsPrefix()` and the `/mount` suffix
        /// (`<prefix>/gc/server-roots/<srid>/mount`). Used both for observability (fenced) and as
        /// the key into `obs`.
        const String srid = key.substr(prefix.size(), key.size() - prefix.size() - mount_suffix.size());
        seen_srids.insert(srid);

        /// One decision per re-read: a refused fence-out re-enters this lambda with the body the
        /// holder's own renewal installed, and the observation check below then sees the new
        /// incarnation and restarts the window -- which counts the slot `live` and declines the write.
        /// That is why no arm that counts a slot ever also asks for a fence-out body.
        WriteResult fenced_out = op.readModifyWrite(key,
            [&](const std::optional<Object> & observed) -> std::optional<String>
            {
                if (!observed)
                {
                    obs.erase(srid);
                    return std::nullopt;   /// raced away (deleted) — nothing to classify
                }

                const MountLease m = decodeMountLease(observed->bytes);

                if (m.gc_fenced)
                {
                    ++floor.already_fenced;
                    obs.erase(srid);   /// terminal — no further observation needed
                    return std::nullopt;
                }
                if (m.min_active_build_sequence == std::numeric_limits<uint64_t>::max())
                {
                    ++floor.terminated;
                    obs.erase(srid);   /// terminal — no further observation needed
                    return std::nullopt;
                }

                /// Observation-based liveness: stable ONLY if the SAME incarnation was already being
                /// watched and has now held for the full threshold on our OWN monotonic clock. Anything
                /// else — no prior observation, or a changed incarnation (a live renewal, including one
                /// raced against our own fence-out attempt) — (re)starts the observation window and
                /// counts as `live` this call.
                const auto it = obs.find(srid);
                const bool stable = it != obs.end() && it->second.etag == observed->etag
                    && mono_now_ms - it->second.first_seen_mono_ms >= stable_threshold_ms;

                if (!stable)
                {
                    if (it == obs.end() || it->second.etag != observed->etag)
                        obs.insert_or_assign(srid, MountIncarnationObservation{observed->etag, mono_now_ms});
                    ++floor.live;
                    return std::nullopt;
                }

                /// Stable past the threshold, not yet fenced → fence-out preserving the whole body
                /// (gc_fenced = true, seq + 1) against the incarnation this decision observed.
                MountLease fenced = m;
                fenced.gc_fenced = true;
                fenced.seq = m.seq + 1;
                return encodeMountLease(fenced);
            },
            Retry::standard());

        if (std::holds_alternative<Committed>(fenced_out))
        {
            ++floor.fenced_now;
            floor.fenced_srids.push_back(srid);
            obs.erase(srid);
            LOG_INFO(getLogger("CasHeartbeatFloor"),
                "CAS GC fenced out mount lease for content-addressed server root {} at "
                "wall-clock ms {}: its write incarnation held unchanged for >= {} ms on the GC "
                "leader's own monotonic clock (token-stability observation)",
                srid, now_ms, stable_threshold_ms);
            return true;
        }
        /// Declined: the decision above already classified and counted this slot, and asked for no
        /// write. Every remaining verdict means the store was not reached, which is not a
        /// classification -- surface it rather than record a floor built on an unread slot.
        if (!std::holds_alternative<Declined>(fenced_out))
            orThrow(std::move(fenced_out), fmt::format("CAS mount fence-out of '{}'", key));
        return true;
    }, Retry::standard());

    /// Prune every `obs` entry for a srid this pass's walk never saw at all.
    for (auto it = obs.begin(); it != obs.end(); )
        it = seen_srids.contains(it->first) ? std::next(it) : obs.erase(it);

    return floor;
}

std::vector<NonTerminalMountSlot> probeNonTerminalMountSlots(CasOperation & op, const Layout & l)
{
    std::vector<NonTerminalMountSlot> slots;

    /// Same enumeration as `computeHeartbeatFloor`'s gate -- walk the server-roots subtree, keep the
    /// `/mount` bodies -- but read-only and without any observation state: this answers "is anyone
    /// still entitled to write here", not "may I fence them out".
    const String prefix = l.serverRootsPrefix();
    op.forEachListedKey(prefix, [&](const ListedKey & listed)
    {
        static constexpr std::string_view mount_suffix = "/mount";
        if (!listed.key.ends_with(mount_suffix))
            return true;   /// `/owner` and `/epoch` share the subtree; only the lease says "live".

        const String srid = listed.key.substr(prefix.size(),
            listed.key.size() - prefix.size() - mount_suffix.size());

        const auto got = op.read(listed.key, Retry::standard());
        if (!got)
            return true;   /// raced away between the listing and the read -- there is no slot to be held.

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
            return true;
        }

        if (m.gc_fenced || m.min_active_build_sequence == std::numeric_limits<uint64_t>::max())
            return true;   /// terminal: fenced out by GC, or the holder's own graceful farewell.

        slots.push_back(NonTerminalMountSlot{srid, fmt::format(
            "held by server uuid {} (writer_epoch {}, host '{}', pid {}, lease seq {}, stamped "
            "expiry {} ms) with neither a graceful farewell nor a GC fence-out",
            u128ToHex(m.server_uuid), m.writer_epoch, m.hostname, m.pid, m.seq, m.expires_at_ms)});
        return true;
    }, Retry::standard());

    return slots;
}

std::vector<MountInfo> listMounts(CasOperation & op, const Layout & layout, uint64_t now_ms, uint64_t skew_margin_ms)
{
    std::vector<MountInfo> out;
    const String prefix = layout.serverRootsPrefix();
    op.forEachListedKey(prefix, [&](const ListedKey & listed)
    {
        static constexpr std::string_view suffix = "/mount";
        if (!listed.key.ends_with(suffix))
            return true;
        const auto got = op.read(listed.key, Retry::standard());
        if (!got)
            return true;   /// raced a delete — read-only view, skip the row
        MountInfo info;
        /// The srid is the path segment between `serverRootsPrefix()` and the `/mount` suffix —
        /// may itself contain `/` (e.g. `shard-01/replica-a`), so slice by prefix length rather
        /// than `rfind('/')`, matching `computeHeartbeatFloor`'s extraction.
        info.srid = listed.key.substr(prefix.size(), listed.key.size() - prefix.size() - suffix.size());
        try
        {
            info.lease = decodeMountLease(got->bytes);
        }
        catch (...)
        {
            info.state = "corrupt";
            out.push_back(std::move(info));
            return true;
        }
        if (info.lease.gc_fenced)
            info.state = "fenced";
        else if (info.lease.min_active_build_sequence == std::numeric_limits<uint64_t>::max())
            info.state = "terminated";
        else if (now_ms <= info.lease.expires_at_ms + skew_margin_ms)
            info.state = "live";
        else
            info.state = "expired";
        out.push_back(std::move(info));
        return true;
    }, Retry::standard());
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
    if (lease.min_active_build_sequence == std::numeric_limits<uint64_t>::max())
        return FenceCertificate::CleanFarewell;
    if (lease.writer_epoch != fence_writer_epoch)
        return FenceCertificate::SupersededEpoch;
    return FenceCertificate::None;
}

}

bool isCreatorFenceTerminal(CasOperation & op, const Layout & layout, const String & server_root_id,
                            uint64_t writer_epoch, const Retry & policy)
{
    const auto got = op.read(layout.mountKey(server_root_id), policy);
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

/// The farewell's FLOOR, not its whole budget: a departing mount is holding shutdown open, so the
/// window still wants to be short, but it can never be shorter than what the farewell's own write
/// needs to send even one attempt. `terminate` below takes the larger of this and that requirement.
/// A window below the requirement is strictly worse than a slightly longer shutdown: the write is
/// refused before it tries the wire, the slot is left holding the departing incarnation, and the next
/// start pays a full incarnation-stability observation (up to the mount lease TTL) instead of
/// reclaiming instantly off a clean farewell.
constexpr uint64_t kFarewellBudgetMs = 10'000;

/// Slack added on top of the write's bare two-envelope reservation (see `terminate`). `fits` admits a
/// write whose reservation exactly equals the remaining window, but only at the instant it is checked;
/// with zero slack the farewell would be admitted only to immediately re-fail its own deadline check
/// once the clock advances by even one millisecond. This mirrors `lease_safety_margin_ms`'s default
/// (`CasRequestBudget.h`) -- the same order of magnitude already trusted elsewhere on this path for
/// "admission-time arithmetic needs room to actually run, not just to pass at t=0".
constexpr uint64_t kFarewellSlackMs = 2'000;

MountLeaseRenewer::MountLeaseRenewer(
    CasRequests & mount_requests_, CasRequests & open_requests_, const Layout & layout_,
    const String & srid_, UInt128 server_uuid_,
    uint64_t writer_epoch_, std::chrono::milliseconds ttl_, std::function<uint64_t()> now_ms_fn_,
    std::function<uint64_t()> min_active_build_sequence_fn_,
    CasEventSink event_sink_,
    std::chrono::milliseconds lease_safety_margin_,
    std::function<uint64_t()> boot_ms_fn_)
    : mount_requests(mount_requests_)
    , open_requests(open_requests_)
    , key(layout_.mountKey(srid_))
    , srid(srid_)
    , server_uuid(server_uuid_)
    , writer_epoch(writer_epoch_)
    , ttl(ttl_)
    , now_ms_fn(std::move(now_ms_fn_))
    , min_active_build_sequence_fn(std::move(min_active_build_sequence_fn_))
    , event_sink(std::move(event_sink_))
    , lease_safety_margin(lease_safety_margin_)
    , boot_ms_fn(boot_ms_fn_ ? std::move(boot_ms_fn_) : defaultBootMs)
{
}

String MountLeaseRenewer::encodeBody(
    uint64_t seq_, uint64_t wall_ms, uint64_t min_active_build_sequence, UInt128 write_attempt_id) const
{
    const uint64_t ttl_ms = static_cast<uint64_t>(ttl.count());
    const uint64_t expires_at_ms = wall_ms > std::numeric_limits<uint64_t>::max() - ttl_ms
        ? std::numeric_limits<uint64_t>::max()
        : wall_ms + ttl_ms;
    return encodeMountLease(MountLease{
        .server_uuid = server_uuid,
        .writer_epoch = writer_epoch,
        .hostname = getFQDNOrHostName(),
        .pid = static_cast<uint64_t>(::getpid()),
        .started_at_ms = wall_ms,
        .seq = seq_,
        .expires_at_ms = expires_at_ms,
        .min_active_build_sequence = min_active_build_sequence,
        .write_attempt_id = write_attempt_id,
    });
}

const Etag & MountLeaseRenewer::precondition() const
{
    if (!last_etag)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "CAS mount-lease: key '{}' has no incarnation to name as a write precondition", key);
    return *last_etag;
}

Etag MountLeaseRenewer::claim(CasOperation & op, const String & body)
{
    /// One read decides the branch AND supplies the precondition, so both the mint and the adoption
    /// are two requests: a separate presence probe would only re-ask what these bytes already answer.
    const std::optional<Object> got = op.read(key, Retry::standard());
    if (!got)
    {
        WriteResult minted = op.create(key, body, Retry::standard());
        if (std::holds_alternative<Conflict>(minted))
            throw Exception(
                ErrorCodes::ABORTED,
                "CAS mount-lease: key '{}' appeared between the read and the create", key);
        const std::optional<Etag> etag
            = orThrow(std::move(minted), fmt::format("CAS mount-lease mint of key '{}'", key));
        emitMountEvent(
            event_sink, CasEventType::MountClaim, srid, "mint", nullptr,
            "mount slot absent -- renewer minted it directly");
        return *etag;
    }

    const MountLease observed = decodeMountLease(got->bytes);
    if (observed.server_uuid != server_uuid)
    {
        emitMountEvent(
            event_sink, CasEventType::MountConflict, srid, "adopt", &observed,
            "mount slot is held by a foreign server -- failing closed");
        throw Exception(
            ErrorCodes::ABORTED,
            "CAS mount-lease: key '{}' is held by a foreign server ({}) -- failing closed",
            key, describeMountHolder(observed));
    }
    if (observed.writer_epoch != writer_epoch)
    {
        emitMountEvent(
            event_sink, CasEventType::MountConflict, srid, "adopt", &observed,
            "mount slot is held by a different writer_epoch -- failing closed");
        throw Exception(
            ErrorCodes::ABORTED,
            "CAS mount-lease: key '{}' is held by a different writer_epoch {} rather than ours {} -- failing closed",
            key, observed.writer_epoch, writer_epoch);
    }
    if (observed.gc_fenced)
    {
        emitMountEvent(
            event_sink, CasEventType::MountConflict, srid, "fenced_by_gc", &observed,
            "own mount slot was fenced by GC before renewer adoption");
        throw MountFencedException(fmt::format(
            "CAS mount-lease: key '{}' was fenced by GC before renewer adoption ({})",
            key, describeMountHolder(observed)));
    }

    WriteResult adopted = op.replace(key, body, got->etag, Retry::standard());
    if (const Conflict * conflict = std::get_if<Conflict>(&adopted))
    {
        /// The write's own resolve read is the re-read: it observed what took the key from us.
        if (const Object * occupant = std::get_if<Object>(&conflict->seen))
        {
            const MountLease lease = decodeMountLease(occupant->bytes);
            if (lease.server_uuid == server_uuid && lease.gc_fenced)
                throw MountFencedException(fmt::format(
                    "CAS mount-lease: key '{}' was fenced by GC inside the adoption window ({})",
                    key, describeMountHolder(lease)));
            throw Exception(
                ErrorCodes::ABORTED,
                "CAS mount-lease: key '{}' changed while adopting our own mount slot ({})",
                key, describeMountHolder(lease));
        }
        if (std::holds_alternative<ProvenAbsent>(conflict->seen))
            throw Exception(
                ErrorCodes::ABORTED,
                "CAS mount-lease: key '{}' vanished while adopting our own mount slot", key);
    }
    const std::optional<Etag> etag
        = orThrow(std::move(adopted), fmt::format("CAS mount-lease adoption of key '{}'", key));

    emitMountEvent(
        event_sink, CasEventType::MountClaim, srid, "adopt", &observed,
        "adopted our own already-live mount slot");
    return *etag;
}

uint64_t MountLeaseRenewer::start(Liveness liveness)
{
    if (renewer_state != MountLeaseRenewerState::New)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount-lease: start is allowed only in New state for key '{}'", key);

    const uint64_t wall_ms = now_ms_fn();
    const uint64_t attempt_start_boot_ms = boot_ms_fn();
    const String body = encodeBody(/*seq_=*/1, wall_ms, min_active_build_sequence_fn(), newMountWriteAttemptId());
    /// Off the mount fence: a self-remount claims with the fence already latched lost, and a claim
    /// admitted under it would be refused on every request. What makes the claim safe is that every
    /// write below is conditional.
    CasOperation op = open_requests.admit(std::move(liveness));
    const Etag etag = claim(op, body);

    seq = 1;
    last_etag = etag;
    last_committed_attempt_start_boot_ms = attempt_start_boot_ms;
    const uint64_t ttl_ms = static_cast<uint64_t>(ttl.count());
    confirmed_deadline_boot_ms = attempt_start_boot_ms > std::numeric_limits<uint64_t>::max() - ttl_ms
        ? std::numeric_limits<uint64_t>::max()
        : attempt_start_boot_ms + ttl_ms;
    renewer_state = MountLeaseRenewerState::Active;
    return attempt_start_boot_ms;
}

[[noreturn]] void MountLeaseRenewer::throwRenewConflict(const Observation & seen) const
{
    if (const Object * occupant = std::get_if<Object>(&seen))
    {
        markMountRenewTermination(MountRenewTerminalClassification::Conflict);
        const MountLease current = decodeMountLease(occupant->bytes);
        if (current.server_uuid == server_uuid && current.gc_fenced)
        {
            emitMountEvent(
                event_sink, CasEventType::MountConflict, srid, "fenced_by_gc", &current,
                "own mount slot was fenced by GC after lease expiry");
            throw MountFencedException(fmt::format(
                "CAS mount-lease: key '{}' was fenced by GC after lease expiry ({})",
                key, describeMountHolder(current)));
        }
        if (current.server_uuid == server_uuid && current.writer_epoch == writer_epoch)
        {
            emitMountEvent(
                event_sink, CasEventType::MountConflict, srid, "same_epoch_state_uncertain", &current,
                "own mount slot advanced past our incarnation -- state uncertain");
            throw Exception(
                ErrorCodes::ABORTED,
                "CAS mount-lease: key '{}' advanced under our own (uuid, epoch); state uncertain ({} vs our seq={})",
                key, describeMountHolder(current), seq);
        }
        if (current.server_uuid == server_uuid)
        {
            emitMountEvent(
                event_sink, CasEventType::MountConflict, srid, "superseded", &current,
                "mount slot is held by a newer writer epoch");
            throw Exception(
                ErrorCodes::ABORTED,
                "CAS mount-lease: key '{}' was superseded by a newer incarnation ({})",
                key, describeMountHolder(current));
        }

        /// This decoded authoritative observation is the exact point at which this incarnation learns
        /// that a foreign successor owns the slot. Terminal teardown intentionally performs no release
        /// I/O, so account the skipped farewell here, once, before the renewer enters its terminal state.
        /// The renewal may be parked under `remount_mutex`; keep the increment trace-free.
        ProfileEvents::incrementNoTrace(ProfileEvents::CASMountReleaseSkippedForeignOccupant);
        emitMountEvent(
            event_sink, CasEventType::MountConflict, srid, "foreign_writer", &current,
            "mount slot is held by a foreign server -- failing closed");
        throw Exception(
            ErrorCodes::ABORTED,
            "CAS mount-lease: key '{}' is held by a foreign server ({}) -- failing closed",
            key, describeMountHolder(current));
    }

    if (std::holds_alternative<ProvenAbsent>(seen))
    {
        markMountRenewTermination(MountRenewTerminalClassification::Vanished);
        emitMountEvent(
            event_sink, CasEventType::MountConflict, srid, "vanished", nullptr,
            "mount slot vanished while renewing -- failing closed");
        throw Exception(
            ErrorCodes::FILE_DOESNT_EXIST,
            "CAS mount-lease: key '{}' vanished while renewing -- failing closed", key);
    }

    /// The precondition was refused but nothing identifiable was read back: neither the successor nor
    /// an absence is established, so the only honest verdict is that this renewal settled nothing.
    markMountRenewTermination(MountRenewTerminalClassification::Unresolved);
    throwCasWriteRetryLater(fmt::format(
        "CAS mount-lease: key '{}' refused our precondition and the resolving read established neither "
        "an occupant nor an absence", key));
}

MountRenewResult MountLeaseRenewer::terminalResult(MountRenewResult result)
{
    if (!result.failure)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount-lease: terminal renewal requires a failure");
    try
    {
        std::rethrow_exception(result.failure);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::LOGICAL_ERROR)
            throw;
    }
    catch (...)
    {
    }
    if (renewer_state != MountLeaseRenewerState::Active)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "CAS mount-lease: terminal renewal outside Active state (observed state {})",
            static_cast<uint32_t>(renewer_state));
    renewer_state = MountLeaseRenewerState::RenewalTerminal;
    result.outcome = MountRenewOutcome::Terminal;
    return result;
}

MountRenewResult MountLeaseRenewer::renew(const MountRenewOperationEnvironment & environment)
{
    return renewOn(mount_requests, environment);
}

MountRenewResult MountLeaseRenewer::renewForRemount(const MountRenewOperationEnvironment & environment)
{
    return renewOn(open_requests, environment);
}

MountRenewResult MountLeaseRenewer::renewOn(
    CasRequests & plane, const MountRenewOperationEnvironment & environment)
{
    const MountRenewObservabilityRegistration observability_registration = beginMountRenewObservabilityCall();
    const MountRenewObservabilityCallGuard observability_guard(observability_registration);

    if (renewer_state != MountLeaseRenewerState::Active)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "CAS mount-lease: renew is allowed only in Active state for key '{}' (observed state {})",
            key,
            static_cast<uint32_t>(renewer_state));

    const auto boot_clock = environment.boot_ms ? environment.boot_ms : boot_ms_fn;
    /// Sampled BEFORE the write. A refused admission is reported as "never attempted" only when this
    /// node had already been asked to stop, and reading the flag afterwards could not tell that apart
    /// from a flag the refusal itself set.
    const bool cancelled = environment.cancelled && environment.cancelled();

    const uint64_t wall_ms = now_ms_fn();
    const uint64_t attempt_start_boot_ms = boot_clock();
    const uint64_t next_seq = seq + 1;
    const UInt128 write_attempt_id = newMountWriteAttemptId();
    const String body = encodeBody(next_seq, wall_ms, min_active_build_sequence_fn(), write_attempt_id);

    if (observability_registration != MountRenewObservabilityRegistration::Ignored)
    {
        initializeMountRenewObservability(
            srid,
            writer_epoch,
            next_seq,
            write_attempt_id,
            attempt_start_boot_ms,
            confirmed_deadline_boot_ms,
            event_sink);
    }

    MountRenewResult result;
    result.attempt_start_boot_ms = attempt_start_boot_ms;

    CasOperation op = plane.admit(environment.live);
    std::optional<WriteResult> written;
    try
    {
        written = op.replace(key, body, precondition(),
            Retry::untilLeaseSafe(confirmed_deadline_boot_ms, static_cast<uint64_t>(lease_safety_margin.count())));
    }
    catch (...)
    {
        /// The engine surfaces a deterministic local failure unchanged rather than reissuing it.
        markMountRenewTermination(MountRenewTerminalClassification::DeterministicFailure);
        result.failure = std::current_exception();
        return terminalResult(std::move(result));
    }

    if (Committed * committed = std::get_if<Committed>(&*written))
    {
        seq = next_seq;
        last_etag = std::move(committed->etag);
        last_committed_attempt_start_boot_ms = attempt_start_boot_ms;
        const uint64_t ttl_ms = static_cast<uint64_t>(ttl.count());
        confirmed_deadline_boot_ms = attempt_start_boot_ms > std::numeric_limits<uint64_t>::max() - ttl_ms
            ? std::numeric_limits<uint64_t>::max()
            : attempt_start_boot_ms + ttl_ms;
        result.outcome = MountRenewOutcome::Committed;
        result.attempts_sent = committed->attempts_sent;
        result.resolved_by_read = committed->resolved_by_read;
        result.sent_any = committed->attempts_sent != 0;
        return result;
    }

    if (const Conflict * conflict = std::get_if<Conflict>(&*written))
    {
        result.sent_any = true;
        result.attempts_sent = conflict->attempts_sent;
        try
        {
            throwRenewConflict(conflict->seen);
        }
        catch (...)
        {
            result.failure = std::current_exception();
        }
        return terminalResult(std::move(result));
    }

    if (const Refused * refused = std::get_if<Refused>(&*written))
    {
        result.sent_any = true;
        result.attempts_sent = refused->attempts_sent;
        markMountRenewTermination(MountRenewTerminalClassification::DeterministicFailure);
        result.failure = std::make_exception_ptr(Exception(
            refused->store_error,
            "CAS mount-lease: the store refused the renewal of key '{}': {}", key, refused->message));
        return terminalResult(std::move(result));
    }

    if (const GaveUp * gave_up = std::get_if<GaveUp>(&*written))
    {
        result.sent_any = gave_up->sent_any;
        result.attempts_sent = gave_up->attempts_sent;
        if (gave_up->why == GaveUp::Why::Deadline)
            result.deadline_source = gave_up->deadline_source;

        /// Nothing was sent and the node was already stopping: the lease is exactly as it was, so this
        /// is a renewal that never ran, not one that lost its authority.
        if (gave_up->why == GaveUp::Why::FenceLost && !gave_up->sent_any && cancelled)
        {
            markMountRenewTermination(MountRenewTerminalClassification::Cancelled);
            result.outcome = MountRenewOutcome::NotAttempted;
            return result;
        }

        MountRenewTerminalClassification classification = MountRenewTerminalClassification::Unresolved;
        switch (gave_up->why)
        {
            case GaveUp::Why::FenceLost:
                classification = cancelled
                    ? MountRenewTerminalClassification::Cancelled
                    : MountRenewTerminalClassification::FenceOrLifecycleLost;
                break;
            case GaveUp::Why::Deadline:
                classification = gave_up->deadline_source == GaveUp::Source::Lease
                    ? MountRenewTerminalClassification::ExternalLeaseDeadline
                    : MountRenewTerminalClassification::RequestDeadline;
                break;
            case GaveUp::Why::Unresolved:
                classification = MountRenewTerminalClassification::Unresolved;
                break;
        }
        markMountRenewTermination(classification);
        result.failure = makeCasWriteRetryLaterExceptionPtr(fmt::format(
            "CAS mount-lease renewal for key '{}' did not retain the lease ({}, {} attempt sent, last "
            "observation: {})",
            key,
            terminalClassificationName(classification),
            gave_up->sent_any ? "at least one" : "no",
            detail::renderObservation(gave_up->last_seen)));
        return terminalResult(std::move(result));
    }

    /// The remaining alternative is `Declined`, which only a decide returning nothing produces; a
    /// renewal always has bytes to write.
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "CAS mount-lease: the renewal of key '{}' was declined, which a replace cannot report", key);
}

void MountLeaseRenewer::terminate(CasOperation & op)
{
    const uint64_t wall_ms = now_ms_fn();
    const String body = encodeMountLease(MountLease{
        .server_uuid = server_uuid,
        .writer_epoch = writer_epoch,
        .hostname = getFQDNOrHostName(),
        .pid = static_cast<uint64_t>(::getpid()),
        .started_at_ms = wall_ms,
        .seq = seq + 1,
        .expires_at_ms = wall_ms,
        .min_active_build_sequence = std::numeric_limits<uint64_t>::max(),
        .write_attempt_id = newMountWriteAttemptId(),
    });
    /// The farewell is admitted on `open_requests` (see `release`, which calls this via `open_requests.admit()`),
    /// so its own reservation -- attempt plus the read that settles it, `reservedFor(0, 2)` in
    /// `CasOperation::writeLoop` -- is exactly `2 * open_requests.attemptReservationMs()`. A window
    /// below that value refuses the write before its first attempt, deterministically, on every call:
    /// `kFarewellBudgetMs` alone predates the attempt-envelope reservation and can no longer be trusted
    /// to admit it. Saturating, like every other deadline computation on this path (see the
    /// `expires_at_ms`/`confirmed_deadline_boot_ms` arithmetic above): an operator-configured envelope
    /// is not bounds-checked against this doubling, and wrapping past `UINT64_MAX` would turn a too-long
    /// window into a too-SHORT one -- the exact failure mode this fix exists to remove.
    const uint64_t reservation_ms = open_requests.attemptReservationMs();
    const uint64_t doubled_reservation_ms = reservation_ms > std::numeric_limits<uint64_t>::max() / 2
        ? std::numeric_limits<uint64_t>::max()
        : reservation_ms * 2;
    const uint64_t two_envelope_reservation_plus_slack_ms = doubled_reservation_ms > std::numeric_limits<uint64_t>::max() - kFarewellSlackMs
        ? std::numeric_limits<uint64_t>::max()
        : doubled_reservation_ms + kFarewellSlackMs;
    const uint64_t farewell_window_ms = std::max<uint64_t>(kFarewellBudgetMs, two_envelope_reservation_plus_slack_ms);
    /// The derived window alone is not enough: mount-control activity must also never run past the
    /// point this node's own fence may already be gone (the same rule `renew` enforces via
    /// `Retry::untilLeaseSafe` above). The precondition on this write already stops it from clobbering
    /// a successor if it DOES land late, but a shutdown holding the process open to retry a write past
    /// its own lease-safe deadline serves no one -- the successor's own reclaim does not wait for it.
    /// `confirmed_deadline_boot_ms` is set at `start()` and kept current by every successful `renew`,
    /// so it is valid here whenever `terminate` runs (only reachable from `release`, which requires
    /// `Active`, which `start` alone establishes).
    WriteResult written = op.replace(key, body, precondition(),
        Retry::untilLeaseSafe(confirmed_deadline_boot_ms, static_cast<uint64_t>(lease_safety_margin.count()), farewell_window_ms));

    if (Committed * committed = std::get_if<Committed>(&written))
    {
        seq += 1;
        last_etag = std::move(committed->etag);
        emitMountEvent(
            event_sink, CasEventType::MountRelease, srid, "farewell", nullptr,
            "graceful release -- lease stamped already-expired and watermark retired");
        return;
    }

    if (const Conflict * conflict = std::get_if<Conflict>(&written))
    {
        /// The write's own resolve read is the re-read this branch used to issue for itself.
        if (const Object * occupant = std::get_if<Object>(&conflict->seen))
        {
            const MountLease current = decodeMountLease(occupant->bytes);
            if (current.gc_fenced)
                return;
            ProfileEvents::increment(ProfileEvents::CASMountExclusivityViolation);
            throw Exception(
                ErrorCodes::ABORTED,
                "CAS mount-lease: release of key '{}' found a foreign incarnation ({}) and left it untouched",
                key, describeMountHolder(current));
        }
        if (std::holds_alternative<ProvenAbsent>(conflict->seen))
            return;   /// the slot is already gone; there is nothing left to hand back
    }

    orThrow(std::move(written), fmt::format("CAS mount-lease release of key '{}'", key));
}

void MountLeaseRenewer::release()
{
    if (renewer_state != MountLeaseRenewerState::Active)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount-lease: release is allowed only in Active state for key '{}'", key);
    renewer_state = MountLeaseRenewerState::Released;
    /// Off the mount fence, for the same reason the claim is: a departing mount whose lease has already
    /// run down still has to hand the slot back, and refusing the write there would leave the slot
    /// looking live until GC fences it out.
    CasOperation op = open_requests.admit();
    terminate(op);
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
