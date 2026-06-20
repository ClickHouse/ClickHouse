#pragma once

#include <Common/ActionBlocker.h>
#include <base/types.h>
#include <atomic>

namespace DB
{

/// Shared state and decision logic behind the unified SYSTEM STOP/START/PAUSE/CANCEL/REFRESH controls
/// for streaming storages (Kafka, RabbitMQ, NATS, ObjectStorageQueue). Every such storage owns one of
/// these and drives its background task through it, so the control machinery reads the same way across
/// engines even though each engine's durable boundary (Kafka offset commit, RabbitMQ ack, NATS ack,
/// S3Queue file-marked-Processed) and abort mechanism differ.
///
/// The unit of background work is one "cycle" = one `streamToViews()` iteration: drain a block from the
/// source, insert it into the dependent materialized views, then reach the durable boundary. The four
/// verbs map onto three pieces of state, and the ONLY thing that differs between them is whether the
/// cycle running right now is allowed to reach that boundary:
///
///   verb     blocks future cycles?   aborts the in-flight cycle?
///   STOP            yes                      yes   (= PAUSE + CANCEL)
///   PAUSE           yes                      no    (let it finish and commit/ack/mark)
///   CANCEL          no                       yes   (abort, but keep running)
///   START         releases block             —     (resume continuous cycles)
///   REFRESH         —                        —     (run exactly one cycle, even while blocked)
///
/// Backed by:
///   - `blocker`          — STOP/PAUSE block future cycles; START releases. An ActionBlocker so it
///                          plugs into the action-locks manager and the `... ALL BACKGROUND` fan-out.
///   - `cancel_epoch`     — STOP/CANCEL ask the in-flight cycle to abort *before* its durable boundary
///                          (so the source discards its block and the data is redelivered / reprocessed,
///                          or lost for core NATS which has no replay). A monotonic counter rather than a
///                          resettable flag: a consumer (a streaming cycle or a direct SELECT) snapshots
///                          the epoch when it starts and considers itself cancelled once the epoch has
///                          advanced past that snapshot. So a CANCEL aborts exactly the work already in
///                          flight when it arrived, a later reader takes a fresh snapshot and is
///                          unaffected (no stale flag to poison a future direct SELECT), and concurrent
///                          cycles cannot clear each other's request (there is nothing to reset).
///   - `refresh_epoch`    — REFRESH runs one out-of-order cycle even while blocked. A monotonic counter
///                          (like `cancel_epoch`) so every worker of a multi-task engine serves each
///                          REFRESH once. Mirrors `SYSTEM REFRESH VIEW` for refreshable views.
class StreamingBackgroundControl
{
public:
    /// The returned lock is held by the action-locks manager until SYSTEM START releases it.
    [[nodiscard]] ActionLock block() { return blocker.cancel(); }
    bool isBlocked() const { return blocker.isCancelled(); }

    /// Abort whatever is in flight before its durable boundary. Advances a monotonic epoch; a consumer
    /// snapshots `currentCancelEpoch` when it starts and is cancelled once the epoch moves past it.
    void requestCancel() { cancel_epoch.fetch_add(1); }
    UInt64 currentCancelEpoch() const { return cancel_epoch.load(); }
    bool isCancelRequested(UInt64 snapshot) const { return cancel_epoch.load() != snapshot; }

    /// Request one out-of-order cycle for every worker, even while blocked (one per SYSTEM REFRESH).
    void requestRefreshOnce() { refresh_epoch.fetch_add(1); }

    /// True if this worker should run a cycle now: not blocked, or a REFRESH it has not served yet.
    /// `last_seen_refresh_epoch` is per-worker state; call once per wake-up.
    bool claimCycle(UInt64 & last_seen_refresh_epoch)
    {
        const UInt64 epoch = refresh_epoch.load();
        const bool refresh_requested = epoch != last_seen_refresh_epoch;
        last_seen_refresh_epoch = epoch;
        return refresh_requested || !isBlocked();
    }

private:
    ActionBlocker blocker;
    std::atomic<UInt64> cancel_epoch = 0;
    std::atomic<UInt64> refresh_epoch = 0;
};

}
