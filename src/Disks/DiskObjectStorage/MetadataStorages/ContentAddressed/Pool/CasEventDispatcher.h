#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>

#include <atomic>
#include <deque>
#include <mutex>

namespace DB::Cas
{

/// Serialized, reentrancy-safe delivery for `CasEvent`s. Every content-addressed component that emits
/// audit events (the `Pool`, the ref ledger, the manifest reader, the mount renewer) routes through
/// the ONE dispatcher owned by its `Pool` instead of calling an installed sink `std::function`
/// directly. That gives two properties the parallel intra-part upload fan-out (stage-1 design §1)
/// newly requires:
///
///  - Serialization: concurrent emitters never run the sink at the same time, so a sink that appends
///    to state without a lock of its own (as every existing sink does) stays correct.
///  - Reentrancy safety: an emission performed FROM INSIDE a sink callback -- the sink calls a ledger
///    read that itself emits -- is queued and drained by the already-running dispatch loop instead of
///    recursing or self-deadlocking on the dispatcher mutex.
///
/// Why one dispatcher and not a per-component locking wrapper: a wrapper that held its own mutex
/// across each component's sink call would establish a `state_mutex -> event_mutex` lock order and
/// deadlock the instant a reentrant sink took `state_mutex` again. The dispatcher never holds `mutex`
/// across the sink call, so reentrancy cannot deadlock it. It is the necessary-but-not-sufficient half
/// of the contract: ledger emission points are additionally restructured to emit AFTER releasing
/// `state_mutex`, so a sink's own reentrant ledger read can take that lock freshly.
class EventDispatcher
{
public:
    /// The sink type is `CasEventSink` (`std::function<void(CasEvent)>`) unchanged: every existing
    /// sink stays valid as-is, and the by-value event preserves the move-the-`detail`-map-into-the-sink
    /// idiom (`CasEvent.h`) rather than forcing a copy at a `const &` boundary.
    using Sink = CasEventSink;

    /// Installs the delivery sink. Pre-traffic only, matching the contract of the setter it replaces
    /// (`Pool::setEventSink`): intended for pre-open wiring or tests with no active mount thread. A
    /// null sink disables delivery.
    void setSink(Sink sink_);

    /// Whether a delivery sink is installed. Lock-free: `sink` is set pre-traffic and never swapped
    /// concurrently with `emit`, so the query-frequency disabled hot path pays no mutex.
    bool hasSink() const noexcept { return has_sink.load(std::memory_order_acquire); }

    /// Delivers `event`, serialized across threads and safe to call reentrantly from within the sink.
    /// Does not propagate a sink exception: a throwing sink is contained per event so one bad event
    /// neither abandons the queued remainder nor leaves the dispatcher wedged (a `draining` flag stuck
    /// true would silently drop every future event). Takes the event by value so the completed record
    /// is moved into the queue and then into the sink, never deep-copied on the emitter thread. The
    /// only exception that can escape is an allocation failure while enqueuing, which happens before
    /// any dispatcher state changes -- the invariant is never broken.
    void emit(CasEvent event);

private:
    std::mutex mutex;
    std::deque<CasEvent> queue;           /// guarded by `mutex`
    bool draining = false;                /// guarded by `mutex`; the draining thread owns delivery
    Sink sink;                            /// swapped under `mutex`; read on the delivery path without it (no concurrent swap in traffic)
    std::atomic<bool> has_sink{false};    /// lock-free mirror of `sink` presence for `hasSink`
};

}
