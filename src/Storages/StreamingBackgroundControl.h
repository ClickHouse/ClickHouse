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
/// The unit of background work is one "cycle" = one `streamToViews` iteration: drain a block from the
/// source, insert it into the dependent materialized views, then reach the durable boundary. The five
/// verbs map onto three pieces of state by deciding whether future cycles are blocked and whether the
/// in-flight cycle is aborted before that boundary:
///
///   verb     blocks future cycles?   aborts the in-flight cycle?
///   STOP            yes                      yes   (= PAUSE + CANCEL)
///   PAUSE           yes                      no    (let it finish and commit/ack/mark)
///   CANCEL          no                       yes   (abort, but keep running)
///   START         releases block             -     (resume continuous cycles)
///   REFRESH         -                        -     (run exactly one cycle, even while blocked)
///
/// Backed by:
///   - `blocker`          - STOP/PAUSE block future cycles; START releases. An ActionBlocker so it
///                          plugs into the action-locks manager and the `... ALL BACKGROUND` fan-out.
///   - `cancel_epoch`     - STOP/CANCEL abort the in-flight cycle *before* its durable boundary (the source
///                          discards its block, so it is redelivered/reprocessed, or lost for core NATS). A
///                          monotonic counter, not a resettable flag: each consumer (a streaming cycle or a
///                          direct SELECT) snapshots it at start and is cancelled once it advances, so a stale
///                          flag can't poison a later read and concurrent cycles can't clear each other.
///   - `refresh_epoch`    - REFRESH runs one out-of-order cycle even while blocked. A monotonic counter;
///                          each worker consumes one per cycle, so every worker serves each REFRESH and N
///                          queued REFRESHes run N cycles. Mirrors `SYSTEM REFRESH VIEW` for refreshable views.
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

    /// Run a cycle now? True if not blocked, or a REFRESH is pending. Consumes exactly one pending REFRESH
    /// per call (per-worker `last_seen_refresh_epoch`). This overload assumes `last_seen_refresh_epoch` is
    /// owned by a single worker thread.
    bool claimCycle(UInt64 & last_seen_refresh_epoch)
    {
        if (last_seen_refresh_epoch != refresh_epoch.load())
        {
            ++last_seen_refresh_epoch;
            return true;
        }
        return !isBlocked();
    }

    /// Run a cycle now? True if not blocked, or a REFRESH is pending. Consumes exactly one pending REFRESH
    /// per call (per-worker `last_seen_refresh_epoch`). This overload assumes `last_seen_refresh_epoch` is
    /// shared by tasks that may call this concurrently.
    bool claimCycle(std::atomic<UInt64> & last_seen_refresh_epoch)
    {
        UInt64 seen = last_seen_refresh_epoch.load();
        while (seen != refresh_epoch.load())
        {
            if (last_seen_refresh_epoch.compare_exchange_weak(seen, seen + 1))
                return true;
            /// CAS failed: another caller advanced the counter and `seen` now holds its value; re-check.
        }
        return !isBlocked();
    }

private:
    ActionBlocker blocker;
    std::atomic<UInt64> cancel_epoch = 0;
    std::atomic<UInt64> refresh_epoch = 0;
};

}
