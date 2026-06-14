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
///   - `refresh_once`     — REFRESH runs exactly one out-of-order cycle even while blocked, then the
///                          block applies again. Mirrors `SYSTEM REFRESH VIEW` for refreshable views.
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

    /// Run exactly one cycle even while blocked.
    void requestRefreshOnce() { refresh_once.store(true); }

    /// Decide whether the background task should run a streaming cycle on this scheduler wake-up:
    /// true when consumption is not blocked, or a SYSTEM REFRESH has requested one out-of-order cycle.
    /// Consumes the one-shot REFRESH request, so call it exactly once per wake-up.
    bool claimCycle()
    {
        const bool one_shot = refresh_once.exchange(false);
        return one_shot || !isBlocked();
    }

private:
    ActionBlocker blocker;
    std::atomic<UInt64> cancel_epoch = 0;
    std::atomic<bool> refresh_once = false;
};

}
