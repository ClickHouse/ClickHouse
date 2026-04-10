#include <Coordination/KeeperRequestsQueue.h>
#include <Coordination/SessionRequest.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

#include <algorithm>
#include <chrono>


namespace CurrentMetrics
{
    extern const Metric KeeperOutstandingRequests;
}

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB
{

/// --- KeeperSubqueue ---

KeeperSubqueue::KeeperSubqueue(KeeperRequestsQueue & parent_)
    : parent(parent_)
{
}

bool KeeperSubqueue::push(SessionRequestPtr request, bool bypass_limit)
{
    /// Soft limit — concurrent pushes can overshoot slightly because the
    /// check and increment are not atomic. This is standard backpressure.
    /// Close bypasses the limit to ensure ephemeral cleanup is not delayed.
    if (!bypass_limit && parent.isOverLimit())
        return false;

    {
        std::lock_guard lock(mutex);
        queue.push_back(std::move(request));
        backlog_count.store(queue.size(), std::memory_order_relaxed);
    }

    parent.incrementTotalSizeAndNotify();
    CurrentMetrics::add(CurrentMetrics::KeeperOutstandingRequests);

    return true;
}


KeeperSubqueue::PullResult KeeperSubqueue::pullIntoRaftBatch(
    RaftBatch & batch, size_t max_count, uint64_t max_bytes)
{
    PullResult result;

    std::lock_guard lock(mutex);

    if (queue.empty())
        return result;

    while (!queue.empty())
    {
        auto & req = queue.front();
        uint64_t req_bytes = req->cached_bytes_size;

        /// CAS PendingRaft → InRaft. Uses tryTransitionState to avoid
        /// TOCTOU with finalizeWithErrors (which runs under session mutex,
        /// not subqueue mutex). If CAS fails, the entry was raced — treat as orphan.
        req->tryTransitionState(RequestState::PendingRaft, RequestState::InRaft,
            {{"keeper.outstanding_requests", static_cast<Int64>(parent.totalSize())}});

        /// Entries not in InRaft (e.g. Initial after session cleanup, or
        /// Completed after concurrent finalizeWithErrors) are orphaned.
        /// Handle metrics inline so orphans don't consume the caller's budget.
        if (req->getState() != RequestState::InRaft)
        {
            LOG_TEST(getLogger("KeeperSubqueue"), "Skipping orphaned request in subqueue, state={}",
                static_cast<int>(req->getState()));
            queue.pop_front();
            backlog_count.store(queue.size(), std::memory_order_relaxed);
            parent.total_size.fetch_sub(1, std::memory_order_relaxed);
            CurrentMetrics::sub(CurrentMetrics::KeeperOutstandingRequests);
            continue;
        }

        bool is_reconfig = req->request->getOpNum() == Coordination::OpNum::Reconfig;

        if (is_reconfig)
        {
            batch.has_reconfig = true;
            batch.reconfig = {req->session_id, req->time, req->request, 0, std::nullopt, 0, req->use_xid_64};
        }
        else
        {
            batch.bytes += req_bytes;
            batch.entries.push_back({req->session_id, req->time, req->request, 0, std::nullopt, 0, req->use_xid_64});
        }

        queue.pop_front();
        result.count++;
        result.bytes += req_bytes;

        if (batch.has_reconfig || result.count >= max_count || result.bytes >= max_bytes)
            break;
    }

    backlog_count.store(queue.size(), std::memory_order_relaxed);
    parent.total_size.fetch_sub(result.count, std::memory_order_relaxed);

    CurrentMetrics::sub(CurrentMetrics::KeeperOutstandingRequests, result.count);
    return result;
}

/// --- KeeperRequestsQueue ---

KeeperRequestsQueue::KeeperRequestsQueue(size_t num_subqueues, size_t max_queue_size_)
    : max_queue_size(max_queue_size_)
{
    if (num_subqueues == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "request_queue_num_shards must be greater than 0");
    subqueues.reserve(num_subqueues);
    for (size_t i = 0; i < num_subqueues; ++i)
        subqueues.push_back(std::make_shared<KeeperSubqueue>(*this));
}

KeeperSubqueuePtr KeeperRequestsQueue::getSubqueue(int64_t session_id)
{
    chassert(session_id >= 0);
    size_t idx = static_cast<size_t>(session_id) % subqueues.size();
    return subqueues[idx];
}


RaftBatch KeeperRequestsQueue::waitAndPullRaftBatch(
    size_t max_count,
    uint64_t max_bytes,
    uint64_t max_batch_time_us,
    const std::function<bool()> & try_finalize_inflight)
{
    using clock = std::chrono::steady_clock;
    static constexpr uint64_t poll_interval_us = 50;
    static constexpr size_t initial_reserve_hint = 64;

    RaftBatch result;
    result.entries.reserve(std::min(max_count, initial_reserve_hint));
    size_t num_subs = subqueues.size();

    uint64_t remaining_budget_count = max_count;
    uint64_t remaining_budget_bytes = max_bytes;

    clock::time_point deadline{};
    bool has_data = false;       /// At least one entry has been pulled.
    bool in_flight = true;       /// Previous Raft batch not yet finalized. Start true to force finalization on entry.
    bool batch_ready = false;    /// Budget exhausted or reconfig — stop accumulating, return as soon as !in_flight.

    while (true)
    {
        if (!batch_ready
            && total_size.load(std::memory_order_relaxed) > 0)
        {
            for (size_t i = 0; i < num_subs; ++i)
            {
                size_t idx = (last_subqueue + i) % num_subs;
                if (subqueues[idx]->backlog() == 0)
                    continue;

                auto pulled = subqueues[idx]->pullIntoRaftBatch(result, remaining_budget_count, remaining_budget_bytes);

                if (pulled.count == 0)
                    continue;

                /// Set deadline from the time of the first successful pull.
                if (!has_data)
                {
                    deadline = clock::now() + std::chrono::microseconds(max_batch_time_us);
                    has_data = true;
                }

                remaining_budget_count -= pulled.count;
                remaining_budget_bytes -= std::min(remaining_budget_bytes, pulled.bytes);

                if (result.has_reconfig
                    || remaining_budget_count == 0
                    || remaining_budget_bytes == 0
                    || shutdown_requested.load(std::memory_order_relaxed))
                {
                    last_subqueue = (idx + 1) % num_subs;
                    batch_ready = true;
                    break;
                }
            }
        }

        if (shutdown_requested.load(std::memory_order_relaxed))
            return result;
        
        /// Finalize the previous in-flight Raft result if ready.
        in_flight = try_finalize_inflight();

        /// Return when not in-flight and either budget exhausted, reconfig, or deadline expired.
        if (!in_flight &&
            (
                batch_ready ||
                (has_data && clock::now() >= deadline)
            ))
        {
            /// Advance round-robin pointer even on deadline expiry so that
            /// lower-indexed subqueues don't get systematic priority bias.
            if (!batch_ready && !result.entries.empty())
                last_subqueue = (last_subqueue + 1) % num_subs;
            return result;
        }

        /// Wait for more data, deadline expiry, or inflight completion.
        {
            std::unique_lock lk(cv_mutex);

            auto predicate = [&]
            {
                return total_size.load(std::memory_order_relaxed) > 0
                    || unlikely(shutdown_requested.load(std::memory_order_relaxed));
            };

            if (in_flight)
            {
                /// In-flight: sleep 50μs then re-check. No predicate — we want the
                /// unconditional pause to avoid busy-spinning when batch_ready but
                /// still waiting for the previous append to commit.
                cv.wait_for(lk, std::chrono::microseconds(poll_interval_us));
            }
            else if (has_data)
            {
                /// Have data, accumulating until deadline.
                cv.wait_until(lk, deadline, predicate);
            }
            else
            {
                /// Truly idle: block indefinitely until new data.
                cv.wait(lk, predicate);
            }
        }
    }
}

void KeeperRequestsQueue::incrementTotalSizeAndNotify()
{
    auto old_size = total_size.fetch_add(1, std::memory_order_relaxed);
    if (old_size == 0)
    {
        /// Lock cv_mutex only on the 0→1 transition — the release on
        /// unlock synchronizes with the consumer's acquire on lock,
        /// guaranteeing the predicate recheck sees total_size > 0.
        std::lock_guard lk(cv_mutex);
        cv.notify_one();
    }
}


void KeeperRequestsQueue::signalShutdown()
{
    /// Lock cv_mutex to avoid missed-wakeup race with the idle consumer path
    /// (cv.wait with no timeout). Matches the pattern in incrementTotalSizeAndNotify.
    {
        std::lock_guard lk(cv_mutex);
        shutdown_requested.store(true, std::memory_order_relaxed);
    }
    cv.notify_all();
}

}
