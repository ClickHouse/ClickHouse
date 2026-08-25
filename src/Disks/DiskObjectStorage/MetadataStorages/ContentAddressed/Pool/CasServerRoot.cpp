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
    extern const int NETWORK_ERROR;
}
}

namespace DB::Cas
{

void reportMountRenewProgress(const CasOverwriteProgress & progress) noexcept;
void reportMountRenewCompletion(const MountRenewResult & result) noexcept;
void configureMountRenewObservability(
    const String * server_root_id, const CasEventSink * event_sink, bool deferred) noexcept;
void deliverDeferredMountRenewObservability(uint64_t remount_attempt_no) noexcept;

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

enum class MountRenewTerminalClassification : uint8_t
{
    FromDiagnostics,
    DeterministicFailure,
    Conflict,
    Vanished,
};

/// Retry admission (`RetryStarted`/`PutStarted`) touches only this fixed-size state. First ambiguity
/// may deliver its one bounded warning/event before the controller's following pre-resolve gate; it
/// never runs after a pre-request gate.
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
    CasOverwriteDeadlineSource deadline_source = CasOverwriteDeadlineSource::RequestBudget;
    CasOverwriteStopCause stop_cause = CasOverwriteStopCause::Continue;
    CasUnresolvedReason unresolved_reason = CasUnresolvedReason::NotUnresolved;
    MountRenewOutcome outcome = MountRenewOutcome::NotAttempted;
    MountRenewTerminalClassification terminal_classification = MountRenewTerminalClassification::FromDiagnostics;
    uint32_t attempts_sent = 0;
    uint32_t ambiguity_attempt_no = 0;
    bool resolved_by_get = false;
    bool retrying_delivered = false;
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
/// retained by the stack-local observer in `MountLeaseKeeper::renew`.
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
    CasOverwriteDeadlineSource deadline_source,
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
        .deadline_source = deadline_source,
    };
}

constexpr std::string_view unresolvedReasonName(CasUnresolvedReason reason)
{
    switch (reason)
    {
        case CasUnresolvedReason::NotUnresolved: return "not_unresolved";
        case CasUnresolvedReason::NoAttemptSent: return "no_attempt_sent";
        case CasUnresolvedReason::FenceLostMidWay: return "fence_lost_mid_way";
        case CasUnresolvedReason::DeadlineMidWay: return "deadline_mid_way";
        case CasUnresolvedReason::FenceLostPostWrite: return "fence_lost_post_write";
        case CasUnresolvedReason::AttemptsExhausted: return "attempts_exhausted";
        case CasUnresolvedReason::DefiniteFailureAfterAmbiguity: return "definite_failure_after_ambiguity";
    }
    return "unknown";
}

constexpr std::string_view deadlineSourceName(CasOverwriteDeadlineSource source)
{
    switch (source)
    {
        case CasOverwriteDeadlineSource::RequestBudget: return "request_budget";
        case CasOverwriteDeadlineSource::ExternalLeaseSafety: return "external_lease_safety";
    }
    return "unknown";
}

constexpr std::string_view stopCauseName(CasOverwriteStopCause cause)
{
    switch (cause)
    {
        case CasOverwriteStopCause::Continue: return "continue";
        case CasOverwriteStopCause::Cancelled: return "cancelled";
        case CasOverwriteStopCause::FenceOrLifecycleLost: return "fence_or_lifecycle_lost";
    }
    return "unknown";
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
    CasUnresolvedReason unresolved_reason,
    CasOverwriteDeadlineSource deadline_source,
    CasOverwriteStopCause stop_cause,
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
        event.reason = outcome == "retrying"
            ? "CAS mount renewal entered bounded retry after an ambiguous physical attempt"
            : (outcome == "recovered"
                ? "CAS mount renewal recovered before its confirmed lease-safety deadline"
                : "CAS mount renewal ended without retained authority and fenced the mount");
        event.detail = {
            {"server_root_id", *context.server_root_id},
            {"writer_epoch", std::to_string(context.writer_epoch)},
            {"seq", std::to_string(context.seq)},
            {"write_attempt_id", write_attempt_id},
            {"attempts_sent", std::to_string(attempts_sent)},
            {"elapsed_ms", std::to_string(elapsedSince(context.observability_start_boot_ms, now_boot_ms))},
            {"remaining_confirmed_budget_ms", std::to_string(remainingConfirmedBudget(context, now_boot_ms))},
            {"unresolved_reason", String{unresolvedReasonName(unresolved_reason)}},
            {"deadline_source", String{deadlineSourceName(deadline_source)}},
            {"stop_cause", String{stopCauseName(stop_cause)}},
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

void deliverMountRenewRetrying(
    const MountRenewObservabilityContext & context,
    const String & write_attempt_id,
    uint64_t now_boot_ms,
    uint64_t remount_attempt_no) noexcept
{
    /// Publish the structured event before the text logger. Either callback may consume recovery
    /// budget, but this transition is followed by the controller's pre-resolve gate, so it cannot
    /// start backend I/O after that budget has expired.
    emitMountRenewEvent(
        context,
        write_attempt_id,
        "retrying",
        context.ambiguity_attempt_no,
        now_boot_ms,
        CasUnresolvedReason::NotUnresolved,
        context.deadline_source,
        CasOverwriteStopCause::Continue,
        "ambiguous",
        remount_attempt_no);
    try
    {
        LOG_WARNING(
            getLogger("CasMountLeaseKeeper"),
            "CAS mount renewal '{}' entered retry after physical attempt {} (writer_epoch={}, seq={}, "
            "remaining_confirmed_budget_ms={})",
            *context.server_root_id,
            context.ambiguity_attempt_no,
            context.writer_epoch,
            context.seq,
            remainingConfirmedBudget(context, now_boot_ms));
    }
    catch (...)
    {
    }
}

constexpr std::string_view terminalClassificationName(const MountRenewObservabilityContext & context)
{
    switch (context.terminal_classification)
    {
        case MountRenewTerminalClassification::DeterministicFailure: return "deterministic_failure";
        case MountRenewTerminalClassification::Conflict: return "conflict";
        case MountRenewTerminalClassification::Vanished: return "vanished";
        case MountRenewTerminalClassification::FromDiagnostics: break;
    }

    switch (context.unresolved_reason)
    {
        case CasUnresolvedReason::AttemptsExhausted: return "attempts_exhausted";
        case CasUnresolvedReason::DefiniteFailureAfterAmbiguity: return "definite_failure_after_ambiguity";
        case CasUnresolvedReason::FenceLostMidWay:
        case CasUnresolvedReason::FenceLostPostWrite:
            return context.stop_cause == CasOverwriteStopCause::Cancelled
                ? "cancelled"
                : "fence_or_lifecycle_lost";
        case CasUnresolvedReason::NoAttemptSent:
        case CasUnresolvedReason::DeadlineMidWay:
            if (context.stop_cause == CasOverwriteStopCause::Cancelled)
                return "cancelled";
            if (context.stop_cause == CasOverwriteStopCause::FenceOrLifecycleLost)
                return "fence_or_lifecycle_lost";
            return context.deadline_source == CasOverwriteDeadlineSource::ExternalLeaseSafety
                ? "external_lease_deadline"
                : "request_deadline";
        case CasUnresolvedReason::NotUnresolved: return "terminal_unclassified";
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

        if (context.ambiguity_attempt_no != 0 && !context.retrying_delivered)
            deliverMountRenewRetrying(context, write_attempt_id, now_boot_ms, remount_attempt_no);

        for (uint32_t attempt_no = 2; attempt_no <= context.attempts_sent; ++attempt_no)
        {
            try
            {
                LOG_DEBUG(
                    getLogger("CasMountLeaseKeeper"),
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
            && (context.attempts_sent > 1 || context.resolved_by_get);
        if (recovered)
        {
            const std::string_view classification = context.resolved_by_get
                ? "committed_by_get"
                : "committed_after_retry";
            emitMountRenewEvent(
                context,
                write_attempt_id,
                "recovered",
                context.attempts_sent,
                now_boot_ms,
                context.unresolved_reason,
                context.deadline_source,
                context.stop_cause,
                classification,
                remount_attempt_no);
            try
            {
                LOG_INFO(
                    getLogger("CasMountLeaseKeeper"),
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
            const std::string_view classification = terminalClassificationName(context);
            emitMountRenewEvent(
                context,
                write_attempt_id,
                "failed",
                context.attempts_sent,
                now_boot_ms,
                context.unresolved_reason,
                context.deadline_source,
                context.stop_cause,
                classification,
                remount_attempt_no);
            try
            {
                LOG_WARNING(
                    getLogger("CasMountLeaseKeeper"),
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

void reportMountRenewProgress(const CasOverwriteProgress & progress) noexcept
{
    MountRenewObservabilityContext * context = currentMountRenewObservability();
    if (!context || !context->active)
        return;

    switch (progress.kind)
    {
        case CasOverwriteProgressKind::PutStarted:
            context->attempts_sent = std::max(context->attempts_sent, progress.attempt_no);
            break;
        case CasOverwriteProgressKind::BecameAmbiguous:
            if (context->ambiguity_attempt_no == 0)
            {
                context->ambiguity_attempt_no = progress.attempt_no;
                if (!context->deferred)
                {
                    /// Mark first, because either diagnostic callback may synchronously renew another
                    /// Pool. The fixed observation stack keeps this outer snapshot stable.
                    context->retrying_delivered = true;
                    try
                    {
                        const uint64_t now_boot_ms = defaultBootMs();
                        const String write_attempt_id = u128ToHex(context->write_attempt_id).substr(0, 12);
                        deliverMountRenewRetrying(
                            *context, write_attempt_id, now_boot_ms, /*remount_attempt_no=*/0);
                    }
                    catch (...)
                    {
                        /// First-ambiguity observability is diagnostic-only. The controller now runs
                        /// its pre-resolve gate before starting any additional backend I/O.
                    }
                }
            }
            break;
        case CasOverwriteProgressKind::RetryStarted:
        case CasOverwriteProgressKind::ResolveStarted:
        case CasOverwriteProgressKind::ResolvedByGet:
            break;
    }
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
    context->attempts_sent = std::max(context->attempts_sent, result.diagnostics.attempts_sent);
    context->resolved_by_get = result.diagnostics.resolved_by_get;
    context->unresolved_reason = result.diagnostics.unresolved_reason;
    context->deadline_source = result.diagnostics.deadline_source;
    context->stop_cause = result.diagnostics.stop_cause;
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
    /// would reactivate a fenced incarnation — a fence permanently consumes this `(server_uuid,
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
    : backend(std::move(backend_))
    , key(layout_.mountKey(srid_))
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

String MountLeaseKeeper::encodeBody(
    uint64_t seq_, uint64_t wall_ms, uint64_t min_active, UInt128 write_attempt_id) const
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
        .min_active = min_active,
        .write_attempt_id = write_attempt_id,
    });
}

Token MountLeaseKeeper::claim(const String & body)
{
    const HeadResult head = backend->head(key);
    if (!head.exists)
    {
        const PutResult result = backend->putIfAbsent(key, body);
        if (result.outcome != PutOutcome::Done)
            throw Exception(
                ErrorCodes::ABORTED,
                "CAS mount-lease: key '{}' appeared between head and putIfAbsent", key);
        emitMountEvent(
            event_sink, CasEventType::MountClaim, srid, "mint", nullptr,
            "mount slot absent -- keeper minted it directly");
        return result.token;
    }

    const auto got = backend->get(key);
    if (!got)
        throw Exception(
            ErrorCodes::ABORTED,
            "CAS mount-lease: key '{}' vanished between head and get while claiming", key);

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
            "own mount slot was fenced by GC before keeper adoption");
        throw MountFencedException(fmt::format(
            "CAS mount-lease: key '{}' was fenced by GC before keeper adoption ({})",
            key, describeMountHolder(observed)));
    }

    const PutResult result = backend->putOverwrite(key, body, got->token);
    if (result.outcome != PutOutcome::Done)
    {
        const auto current = backend->get(key);
        if (current)
        {
            const MountLease lease = decodeMountLease(current->bytes);
            if (lease.server_uuid == server_uuid && lease.gc_fenced)
                throw MountFencedException(fmt::format(
                    "CAS mount-lease: key '{}' was fenced by GC inside the adoption window ({})",
                    key, describeMountHolder(lease)));
            throw Exception(
                ErrorCodes::ABORTED,
                "CAS mount-lease: key '{}' changed while adopting our own mount slot ({})",
                key, describeMountHolder(lease));
        }
        throw Exception(
            ErrorCodes::ABORTED,
            "CAS mount-lease: key '{}' vanished while adopting our own mount slot", key);
    }

    emitMountEvent(
        event_sink, CasEventType::MountClaim, srid, "adopt", &observed,
        "adopted our own already-live mount slot");
    return result.token;
}

uint64_t MountLeaseKeeper::start()
{
    if (keeper_state != MountLeaseKeeperState::New)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount-lease: start is allowed only in New state for key '{}'", key);

    const uint64_t wall_ms = now_ms_fn();
    const uint64_t attempt_start_boot_ms = boot_ms_fn();
    const String body = encodeBody(/*seq_=*/1, wall_ms, min_active_fn(), newMountWriteAttemptId());
    const Token token = claim(body);

    seq = 1;
    last_token = token;
    last_committed_attempt_start_boot_ms = attempt_start_boot_ms;
    const uint64_t ttl_ms = static_cast<uint64_t>(ttl.count());
    confirmed_deadline_boot_ms = attempt_start_boot_ms > std::numeric_limits<uint64_t>::max() - ttl_ms
        ? std::numeric_limits<uint64_t>::max()
        : attempt_start_boot_ms + ttl_ms;
    keeper_state = MountLeaseKeeperState::Active;
    return attempt_start_boot_ms;
}

[[noreturn]] void MountLeaseKeeper::throwRenewConflict(const CasOverwriteDiagnostics & diagnostics) const
{
    if (!diagnostics.resolve_observation_completed)
        throw Exception(
            ErrorCodes::NETWORK_ERROR,
            "CAS mount-lease: key '{}' conflicted but the controller has no authoritative resolve observation",
            key);
    if (!diagnostics.observed_bytes)
    {
        emitMountEvent(
            event_sink, CasEventType::MountConflict, srid, "vanished", nullptr,
            "mount slot vanished while renewing -- failing closed");
        throw Exception(
            ErrorCodes::FILE_DOESNT_EXIST,
            "CAS mount-lease: key '{}' vanished while renewing -- failing closed", key);
    }

    const MountLease current = decodeMountLease(*diagnostics.observed_bytes);
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
            "own mount slot advanced past our token -- state uncertain");
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
    /// I/O, so account the skipped farewell here, once, before the keeper enters its terminal state.
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

MountRenewResult MountLeaseKeeper::terminalResult(
    uint64_t attempt_start_boot_ms,
    CasOverwriteDiagnostics diagnostics,
    std::exception_ptr failure)
{
    if (!failure)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount-lease: terminal renewal requires a failure");
    try
    {
        std::rethrow_exception(failure);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::LOGICAL_ERROR)
            throw;
    }
    catch (...)
    {
    }
    if (keeper_state != MountLeaseKeeperState::Active)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount-lease: terminal renewal outside Active state");
    keeper_state = MountLeaseKeeperState::RenewalTerminal;
    return MountRenewResult{
        .outcome = MountRenewOutcome::Terminal,
        .attempt_start_boot_ms = attempt_start_boot_ms,
        .diagnostics = diagnostics,
        .failure = std::move(failure),
    };
}

MountRenewResult MountLeaseKeeper::renew(
    const CasRequestBudget & budget,
    const MountRenewOperationEnvironment & environment)
{
    const MountRenewObservabilityRegistration observability_registration = beginMountRenewObservabilityCall();
    const MountRenewObservabilityCallGuard observability_guard(observability_registration);

    if (keeper_state != MountLeaseKeeperState::Active)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount-lease: renew is allowed only in Active state for key '{}'", key);

    const auto boot_clock = environment.boot_ms ? environment.boot_ms : boot_ms_fn;
    const auto stop_cause = environment.stop_cause
        ? environment.stop_cause
        : [] { return CasOverwriteStopCause::Continue; };
    const auto wait_before_retry = environment.wait_before_retry
        ? environment.wait_before_retry
        : [](uint64_t) { return true; };
    const auto downstream_observe = environment.observe
        ? environment.observe
        : [](const CasOverwriteProgress &) {};
    uint32_t physical_attempts_sent = 0;
    const auto observe = [&physical_attempts_sent, &downstream_observe](const CasOverwriteProgress & progress)
    {
        /// This call-stack-owned value is protocol diagnostic truth even when rich observability is
        /// intentionally suppressed after deeply reentrant sinks exhaust its bounded TLS slots.
        if (progress.kind == CasOverwriteProgressKind::PutStarted)
            physical_attempts_sent = std::max(physical_attempts_sent, progress.attempt_no);
        downstream_observe(progress);
    };

    const uint64_t wall_ms = now_ms_fn();
    const uint64_t attempt_start_boot_ms = boot_clock();
    const uint64_t next_seq = seq + 1;
    const UInt128 write_attempt_id = newMountWriteAttemptId();
    const String body = encodeBody(next_seq, wall_ms, min_active_fn(), write_attempt_id);
    const Token expected = last_token;

    const uint64_t safety_ms = static_cast<uint64_t>(lease_safety_margin.count());
    const uint64_t lease_retry_deadline = confirmed_deadline_boot_ms > safety_ms
        ? confirmed_deadline_boot_ms - safety_ms
        : 0;
    const uint64_t request_deadline = attempt_start_boot_ms > std::numeric_limits<uint64_t>::max() - budget.operation_deadline_ms
        ? std::numeric_limits<uint64_t>::max()
        : attempt_start_boot_ms + budget.operation_deadline_ms;
    const uint64_t absolute_deadline = std::min(lease_retry_deadline, request_deadline);
    const CasOverwriteDeadlineSource deadline_source = lease_retry_deadline <= request_deadline
        ? CasOverwriteDeadlineSource::ExternalLeaseSafety
        : CasOverwriteDeadlineSource::RequestBudget;

    if (observability_registration != MountRenewObservabilityRegistration::Ignored)
    {
        initializeMountRenewObservability(
            srid,
            writer_epoch,
            next_seq,
            write_attempt_id,
            attempt_start_boot_ms,
            confirmed_deadline_boot_ms,
            deadline_source,
            event_sink);
    }

    CasRequestController controller(backend, budget, boot_clock);
    const CasOverwriteOperationContext context{
        .absolute_deadline_ms = absolute_deadline,
        .deadline_source = deadline_source,
        .stop_cause = stop_cause,
        .wait_before_retry = wait_before_retry,
        .observe = observe,
    };

    CasOverwriteResult controlled;
    controlled.diagnostics.deadline_source = deadline_source;
    try
    {
        controlled = controller.putOverwriteControlled(key, body, expected, context);
    }
    catch (...)
    {
        /// The controller may propagate a deterministic/non-retryable exception after `PutStarted`.
        /// Preserve the physical observer's already-published truth instead of returning the default
        /// zero-attempt diagnostics from the result object that was never assigned. This local is not
        /// coupled to the bounded rich-event stack and therefore remains truthful at arbitrary nesting.
        controlled.diagnostics.attempts_sent = std::max(
            controlled.diagnostics.attempts_sent, physical_attempts_sent);
        controlled.diagnostics.deadline_source = deadline_source;
        if (MountRenewObservabilityContext * observation = currentMountRenewObservability())
            observation->terminal_classification = MountRenewTerminalClassification::DeterministicFailure;
        return terminalResult(attempt_start_boot_ms, controlled.diagnostics, std::current_exception());
    }

    if (controlled.outcome == CasOverwriteOutcome::Committed)
    {
        seq = next_seq;
        last_token = controlled.token;
        last_committed_attempt_start_boot_ms = attempt_start_boot_ms;
        const uint64_t ttl_ms = static_cast<uint64_t>(ttl.count());
        confirmed_deadline_boot_ms = attempt_start_boot_ms > std::numeric_limits<uint64_t>::max() - ttl_ms
            ? std::numeric_limits<uint64_t>::max()
            : attempt_start_boot_ms + ttl_ms;
        return MountRenewResult{
            .outcome = MountRenewOutcome::Committed,
            .attempt_start_boot_ms = attempt_start_boot_ms,
            .diagnostics = controlled.diagnostics,
            .failure = nullptr,
        };
    }

    if (controlled.outcome == CasOverwriteOutcome::Conflict)
    {
        if (MountRenewObservabilityContext * observation = currentMountRenewObservability())
            observation->terminal_classification = MountRenewTerminalClassification::Conflict;
        try
        {
            throwRenewConflict(controlled.diagnostics);
        }
        catch (...)
        {
            return terminalResult(attempt_start_boot_ms, controlled.diagnostics, std::current_exception());
        }
    }

    if (controlled.diagnostics.attempts_sent == 0
        && controlled.diagnostics.stop_cause == CasOverwriteStopCause::Cancelled)
    {
        return MountRenewResult{
            .outcome = MountRenewOutcome::NotAttempted,
            .attempt_start_boot_ms = attempt_start_boot_ms,
            .diagnostics = controlled.diagnostics,
            .failure = nullptr,
        };
    }

    /// Preserve the typed vanished-slot outcome only when the controller itself completed an exact
    /// resolving read. Never start diagnostic backend I/O after its terminal deadline/cancel gate.
    if (controlled.diagnostics.resolve_observation_completed
        && !controlled.diagnostics.observed_bytes)
    {
        if (MountRenewObservabilityContext * observation = currentMountRenewObservability())
            observation->terminal_classification = MountRenewTerminalClassification::Vanished;
        emitMountEvent(
            event_sink, CasEventType::MountConflict, srid, "vanished", nullptr,
            "mount slot vanished while renewing -- failing closed");
        return terminalResult(
            attempt_start_boot_ms,
            controlled.diagnostics,
            std::make_exception_ptr(Exception(
                ErrorCodes::FILE_DOESNT_EXIST,
                "CAS mount-lease: key '{}' vanished while renewing -- failing closed",
                key)));
    }

    const String reason = fmt::format(
        "CAS mount-lease renewal for key '{}' is unresolved: {}",
        key, describeUnresolvedReason(controlled.diagnostics.unresolved_reason));
    return terminalResult(
        attempt_start_boot_ms,
        controlled.diagnostics,
        makeCasWriteRetryLaterExceptionPtr(reason));
}

void MountLeaseKeeper::terminate()
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
        .min_active = std::numeric_limits<uint64_t>::max(),
        .write_attempt_id = newMountWriteAttemptId(),
    });
    const PutResult result = backend->putOverwrite(key, body, last_token);
    if (result.outcome != PutOutcome::Done)
    {
        if (const auto got = backend->get(key))
        {
            const MountLease current = decodeMountLease(got->bytes);
            if (current.gc_fenced)
                return;
            ProfileEvents::increment(ProfileEvents::CASMountExclusivityViolation);
            throw Exception(
                ErrorCodes::ABORTED,
                "CAS mount-lease: release of key '{}' found a foreign incarnation ({}) and left it untouched",
                key, describeMountHolder(current));
        }
        return;
    }

    seq += 1;
    last_token = result.token;
    emitMountEvent(
        event_sink, CasEventType::MountRelease, srid, "farewell", nullptr,
        "graceful release -- lease stamped already-expired and watermark retired");
}

void MountLeaseKeeper::release()
{
    if (keeper_state != MountLeaseKeeperState::Active)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount-lease: release is allowed only in Active state for key '{}'", key);
    keeper_state = MountLeaseKeeperState::Released;
    terminate();
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
