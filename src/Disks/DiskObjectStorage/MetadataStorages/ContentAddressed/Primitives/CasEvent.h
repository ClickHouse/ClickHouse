#pragma once
#include <base/types.h>
#include <base/extended_types.h>
#include <functional>
#include <map>
#include <utility>

namespace DB::Cas
{

/// Names the append-only audit events emitted by content-addressed storage. The metadata storage
/// converts each `CasEvent` into a `ContentAddressedLogElement` and forwards it to the `SystemLog`;
/// this layer deliberately keeps only pure data so the core and its unit tests do not depend on
/// system-log machinery. The event log reconstructs each entity's lifetime, so this taxonomy
/// includes state-changing decisions, GC transitions, and errors or anomalies rather than only
/// successful user-visible operations.
enum class CasEventType
{
    BlobPut, BlobReuseAdopt, BlobReuseResurrect, BlobRetire, BlobRetireReplaced, BlobDelete, BlobForget,
    ManifestPut, ManifestDelete,
    RefPublish, RefDrop, RefRepoint, RootAdd, RootRemove, RootRepoint, IndegZero,
    GcFoldBegin, GcFoldEnd, GcRetireObserve, GcRetireDecision, GcRecheckVerdict,
    GcFence, GcCursorAdvance, GcShardReclaim, GcFenceOut, GcRebuild, GcFoldClamp,
    /// One GC round anomaly, with the context of the decision it forced. `gc_fold_end` reports only a
    /// COUNT of anomalies, which cannot even distinguish a ref-prefix enumeration disagreement from an
    /// undecodable ref-log body -- so every investigation had to fall back on the rotated text log, which
    /// is how a masked permission error once produced a confident "no occurrences". An anomaly aborts ref
    /// folding; that decision deserves a queryable record, not a counter.
    GcAnomaly,
    GcLeaseAcquire, GcLeaseSteal, GcLeaseHeartbeat,
    BuildStart, BuildPublish, BuildAbort, Precommit, PrecommitRemoved, PrecommitReclaim,
    GateRevalidate, GateResurrect, WatermarkRenew, MountRemount,
    MountClaim, MountRelease, MountConflict,
    /// Operator-driven erasure of a dead pool member's namespace. `decommissionPoolMember` runs as
    /// a writer, never as GC: it claims the member's mount, drains its namespaces and debris, and
    /// deletes the mount slot only after the drain is confirmed. The slot is the interrupted-operation
    /// resume anchor, so a failed drain leaves it terminated for a later retry. `outcome` is one of
    /// "begin", "namespace_removed", or "end".
    MemberDecommission,
    /// Incidental-detection reaction to foreign bytes at a ref-log wedge key owned by this mount.
    /// The mount lease makes the key exclusive, so this is impossible under legitimate
    /// single-writer operation and indicates that the wedge hard contract was violated. The
    /// reaction records the anomaly and fails the local write path closed; it does not treat the
    /// foreign bytes as valid ref-log state.
    ForeignInterference,
    RefResolve, ReadMissing, DanglingAccess,
    CorruptDangle, CorruptDecode, SnapJournalIncoherent, Exception,
};

/// Identifies the kind of object described by a `CasEvent`. `None` is used for events about a
/// protocol action, mount, or anomaly that is not tied to one stored object.
enum class CasEventObjectKind { None, Blob, Manifest, Root, Snap };

/// Pure-data event passed from the content-addressed core to the metadata-storage audit-log sink.
/// Fields that do not apply to an event remain empty or zero. `reason` is mandatory for decisions
/// and must explain why the operation took its outcome; `detail` carries structured facts needed
/// to reconstruct the event without parsing the free-form reason. Hashes are lowercase hexadecimal,
/// tokens identify object incarnations, and the numeric fields identify GC rounds, snapshot
/// generations, or the manifest journal version as applicable.
struct CasEvent
{
    CasEventType type = CasEventType::BlobPut;
    String namespace_;          /// roots/<ns> (empty if N/A)
    String ref_name;            /// the ref name — a mutable directory handle, git-style (empty if N/A)
    CasEventObjectKind object_kind = CasEventObjectKind::None;
    String object_hash;         /// lowercase hex (empty if N/A)
    String token;               /// incarnation token (empty if N/A)
    UInt64 round = 0;
    UInt64 gen = 0;
    UInt64 at_version = 0;
    String outcome;             /// e.g. "ok","adopt","deleted","zeroed" (empty if N/A)
    String reason;              /// REQUIRED: the human-readable WHY of the decision
    std::map<String, String> detail;
};

/// Receives events by value so emission sites can move the complete record, including its `detail`
/// map, into the sink instead of deep-copying it on the emitter thread. Emission sites pass an rvalue
/// for a completed event; the sink consumes that event while converting it to the system-log row.
using CasEventSink = std::function<void(CasEvent)>;

/// Builds and emits a `CasEvent` for a store that owns the event sink. The builder supplies the
/// per-event fields; the emitter supplies the sink owner shared by all events from that store.
///
/// If the store has no sink, the builder is not invoked and no event is constructed; the disabled
/// path is therefore only the sink-presence check. Otherwise, `emit` moves the completed event into
/// `emitEvent`, preserving the fields supplied by the builder without adding a copy. This class is
/// templated to avoid a header-layering cycle (`CasPool.h` includes this header); any `S` exposing
/// `hasEventSink` and `emitEvent` with the expected contracts can be used.
template <typename S>
class EventEmitter
{
public:
    /// Keeps a reference to the store; the store must outlive this short-lived emitter.
    explicit EventEmitter(const S & store_) : store(store_) {}

    template <typename Builder>
    /// Invokes `build` only when the store has an enabled sink, then moves the resulting event into
    /// the store. `Builder` must accept `CasEvent &` and may populate any applicable fields; any
    /// exception from the builder or the store is propagated to the caller.
    void emit(Builder && build) const
    {
        if (!store.hasEventSink())
            return;
        CasEvent event;
        build(event);
        store.emitEvent(std::move(event));
    }

private:
    const S & store;
};

template <typename S>
EventEmitter(const S &) -> EventEmitter<S>;

/// Converts an event taxonomy value to the stable snake_case name stored in the `SystemLog`
/// `event_type` column. Every enumerator must have a mapping because these names are queried by
/// users and are part of the audit-log schema; an unknown value raises a logical-error exception.
String toString(CasEventType type);

/// Converts an object-kind value to the stable snake_case name stored in the `SystemLog`
/// `object_kind` column. An unknown value raises a logical-error exception rather than silently
/// producing an unrecognized schema value.
String toString(CasEventObjectKind kind);

}
