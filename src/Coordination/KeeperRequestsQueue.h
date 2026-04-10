#pragma once

#include <Coordination/KeeperCommon.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <vector>

namespace DB
{

class KeeperRequestsQueue;
class KeeperSubqueue;
using KeeperSubqueuePtr = std::shared_ptr<KeeperSubqueue>;

/// Batch of requests ready for the Raft path.
struct RaftBatch
{
    KeeperRequestsForSessions entries;
    KeeperRequestForSession reconfig;  ///< .request == nullptr if no Reconfig in this batch
    uint64_t bytes{0};
    bool has_reconfig{false};
};

/// One shard of KeeperRequestsQueue. Strict FIFO within the shard.
/// Has its own mutex + deque + atomic backlog counter.
/// Notifies the parent queue's CV when transitioning from empty to non-empty.
class KeeperSubqueue
{
public:
    explicit KeeperSubqueue(KeeperRequestsQueue & parent_);

    /// Push a request. Returns false if the parent's global limit is reached.
    /// If the subqueue was empty before push, notifies the parent's CV.
    bool push(SessionRequestPtr request);

    struct PullResult
    {
        size_t count = 0;
        uint64_t bytes = 0;
    };

    /// Pull requests, transition PendingRaft → InRaft, and append converted
    /// `KeeperRequestForSession` directly to `batch`. No intermediate vector.
    PullResult pullIntoRaftBatch(RaftBatch & batch, size_t max_count, uint64_t max_bytes);

    /// Atomic backlog — readable without lock.
    uint64_t backlog() const { return backlog_count.load(std::memory_order_relaxed); }

private:
    KeeperRequestsQueue & parent;
    std::deque<SessionRequestPtr> queue;
    mutable std::mutex mutex;
    std::atomic<uint64_t> backlog_count{0};
};

/// Sharded concurrent queue for Keeper quorum requests.
///
/// Contains N subqueues (`KeeperSubqueue`). Each session is assigned to
/// one subqueue deterministically at construction time (via `getSubqueueHandle`).
/// The scheduler drains all subqueues round-robin in `waitAndPullRaftBatch`.
class KeeperRequestsQueue
{
public:
    KeeperRequestsQueue(size_t num_subqueues, size_t max_queue_size);

    /// Get the subqueue for this session_id.
    /// Internally: subqueue_idx = session_id % num_subqueues.
    KeeperSubqueuePtr getSubqueue(int64_t session_id);


    /// Poll requests, transition `PendingRaft → InRaft`, extract lightweight
    /// `KeeperRequestForSession` structs for the Raft path. Reconfig requests
    /// are extracted separately into `result.reconfig`.
    ///
    /// Accumulates entries until one of: budget exhausted, reconfig, deadline
    /// expired, or shutdown. The deadline is set from the time of the first
    /// successful pull + `max_batch_time_us`. This builds bigger batches by
    /// waiting for more data within a bounded time window.
    ///
    /// Never returns while the previous batch is still in-flight. Calls
    /// `try_finalize_inflight` every iteration: returns `true` if something
    /// is still in-flight (poll at 50μs), `false` if not (deadline-based
    /// wait when accumulating, indefinite block when idle).
    ///
    /// Never returns an empty batch unless shutdown is requested.
    ///
    /// @param max_batch_time_us  Max time from first pull to batch return.
    RaftBatch waitAndPullRaftBatch(
        size_t max_count,
        uint64_t max_bytes,
        uint64_t max_batch_time_us,
        const std::function<bool()> & try_finalize_inflight);

    /// Signal shutdown.
    void signalShutdown();

    size_t numSubqueues() const { return subqueues.size(); }

    /// Total number of requests across all subqueues.
    uint64_t totalSize() const { return total_size.load(std::memory_order_relaxed); }

    /// Check global admission limit.
    bool isOverLimit() const { return max_queue_size > 0 && total_size.load(std::memory_order_relaxed) >= max_queue_size; }

private:
    friend class KeeperSubqueue;

    /// Called by KeeperSubqueue on every push. Increments total_size under
    /// cv_mutex to prevent a missed-wakeup race with waitForData/waitForDataFor.
    void incrementTotalSizeAndNotify();

    std::vector<KeeperSubqueuePtr> subqueues;
    std::atomic<uint64_t> total_size{0};
    std::atomic<bool> shutdown_requested{false};
    std::condition_variable cv;
    std::mutex cv_mutex;
    size_t last_subqueue{0};
    size_t max_queue_size;
};

}
