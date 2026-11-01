#include <Common/logger_useful.h>
#include <Interpreters/ProcessList.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/CancellationChecker.h>

#include <base/arithmeticOverflow.h>

#include <chrono>
#include <limits>
#include <mutex>


namespace DB
{

/// Align all timeouts to a grid to allow batching of timeout processing.
/// Tasks may be cancelled slightly later than their exact timeout, but never before.
static constexpr UInt64 CANCELLATION_GRID_MS = 100;

/// The deadline used when `now + timeout` is not representable: the grid boundary just past the
/// largest microsecond count that fits in `Int64`, about 292 thousand years from the epoch.
/// Saturating there moves the deadline later, never earlier, so a query is still never cancelled
/// before its own `max_execution_time`.
static constexpr UInt64 MAX_DEADLINE_MS
    = ((static_cast<UInt64>(std::numeric_limits<Int64>::max()) / 1000 + CANCELLATION_GRID_MS) / CANCELLATION_GRID_MS)
    * CANCELLATION_GRID_MS;

/// `std::condition_variable::wait_for` converts the duration to nanoseconds internally, which
/// overflows for deadlines far in the future, so the worker waits in slices of at most one day and
/// re-arms afterwards. Waking up once a day while an effectively infinite timeout is pending is free.
static constexpr UInt64 MAX_WAIT_MS = 24 * 60 * 60 * 1000;

struct CancellationChecker::QueryToTrack
{
    QueryToTrack(QueryStatusPtr query_, UInt64 timeout_, UInt64 endtime_, OverflowMode overflow_mode_)
        : query(query_)
        , timeout(timeout_)
        , endtime(endtime_)
        , overflow_mode(overflow_mode_)
    {
    }

    QueryStatusPtr query;
    UInt64 timeout;
    UInt64 endtime;
    OverflowMode overflow_mode;
};

void CancellationChecker::cancelTask(CancellationChecker::QueryToTrack task)
{
    if (task.query)
    {
        try
        {
            if (task.overflow_mode == OverflowMode::THROW)
                task.query->cancelQuery(CancelReason::TIMEOUT);
            else
                task.query->checkTimeLimit();
        }
        catch (...)
        {
            /// This function is called from BackgroundSchedulePool which does not allow exceptions.
            /// The query might have been already cancelled by another mechanism, which is fine.
            tryLogCurrentException("CancellationChecker");
        }
    }
}

bool CancellationChecker::CompareEndTime::operator()(
    const CancellationChecker::QueryToTrack & a, const CancellationChecker::QueryToTrack & b) const
{
    return std::tie(a.endtime, a.query) < std::tie(b.endtime, b.query);
}

CancellationChecker::CancellationChecker()
    : stop_thread(false)
    , log(getLogger("CancellationChecker"))
{
}

CancellationChecker & CancellationChecker::getInstance()
{
    static CancellationChecker instance;
    return instance;
}

void CancellationChecker::terminateThread()
{
    std::unique_lock<std::mutex> lock(m);
    LOG_TRACE(log, "Stopping CancellationChecker");
    stop_thread = true;
    cond_var.notify_all();
}

UInt64 CancellationChecker::taskDeadlineMs(std::chrono::steady_clock::time_point now, Int64 timeout_us)
{
    /// Round the current time *up* to a whole microsecond: truncating it drops a sub-microsecond
    /// remainder, and when `now + timeout` then lands exactly on a whole millisecond the alignment
    /// below adds no padding, placing the deadline up to one microsecond before the exact one.
    const Int64 now_us = std::chrono::ceil<std::chrono::microseconds>(now.time_since_epoch()).count();

    /// A `max_execution_time` large enough to overflow the deadline is not representable; saturate
    /// instead of truncating the timeout, so the query is never cancelled ahead of the configured limit.
    Int64 deadline_us = 0;
    if (common::addOverflow(now_us, timeout_us, deadline_us))
        return MAX_DEADLINE_MS;

    /// Round the exact deadline up to a whole millisecond. Rounding either the current time or the
    /// timeout down loses the microsecond precision of `max_execution_time` and can cancel a query
    /// ahead of its own timeout when grid alignment adds no padding.
    const UInt64 deadline_ms = std::chrono::ceil<std::chrono::milliseconds>(std::chrono::microseconds(deadline_us)).count();
    /// Round up to the next grid boundary to enable batching of timeout checks.
    /// This ensures tasks are never cancelled before their timeout, only slightly after.
    return ((deadline_ms + CANCELLATION_GRID_MS - 1) / CANCELLATION_GRID_MS) * CANCELLATION_GRID_MS;
}

bool CancellationChecker::appendTask(const QueryStatusPtr & query, const Int64 timeout_us, OverflowMode overflow_mode)
{
    if (timeout_us <= 0) // Avoid cases when the timeout is less or equal zero
    {
        LOG_TEST(log, "Did not add the task because the timeout is 0, query_id: {}", query->getClientInfo().current_query_id);
        return false;
    }

    std::unique_lock<std::mutex> lock(m);
    LOG_TEST(log, "Added to set. query: {}, timeout: {} microseconds", query->getInfo().query, timeout_us);
    const UInt64 end_time = taskDeadlineMs(std::chrono::steady_clock::now(), timeout_us);
    auto iter = query_set.emplace(query, timeout_us, end_time, overflow_mode);
    if (iter == query_set.begin()) // Only notify if the new task is the earliest one
        cond_var.notify_all();
    return true;
}

void CancellationChecker::appendDoneTasks(const QueryStatusPtr & query)
{
    std::unique_lock lock(m);

    auto it = std::ranges::find(query_set, query, &QueryToTrack::query);
    if (it == query_set.end())
        return;

    LOG_TEST(log, "Removing query {} from done tasks", query->getClientInfo().current_query_id);
    query_set.erase(it);

    // Note that there is no need to notify the worker thread here. Even if we have just removed the earliest task,
    // it will wake up before the next task anyway and fix its timeout to a proper value on wake-up.
    // This optimization avoids unnecessary contention on the mutex.
}

void CancellationChecker::workerFunction()
{
    LOG_TRACE(log, "Started worker function");
    std::vector<QueryToTrack> tasks_to_cancel;

    std::unique_lock<std::mutex> lock(m);

    while (!stop_thread)
    {
        UInt64 now_ms = 0;
        if (!query_set.empty())
        {
            auto now = std::chrono::steady_clock::now();
            now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

            /// Batch all tasks that have reached their deadline.
            /// Since deadlines are aligned to a grid, multiple tasks often expire together.
            while (!query_set.empty())
            {
                auto next_task_it = query_set.begin();
                if (next_task_it->endtime > now_ms || next_task_it->timeout == 0)
                    break;

                LOG_DEBUG(
                    log,
                    "Cancelling the task because of the timeout: {} us, query_id: {}",
                    next_task_it->timeout,
                    next_task_it->query->getClientInfo().current_query_id);

                tasks_to_cancel.push_back(*next_task_it);
                query_set.erase(next_task_it);
            }
        }

        if (!tasks_to_cancel.empty())
        {
            lock.unlock();
            std::ranges::for_each(tasks_to_cancel, cancelTask);
            tasks_to_cancel.clear();
            lock.lock();
            continue;
        }

        /// if there are no queries,
        /// wakeup on first query that was added so we can setup
        /// proper timeout for waking up the thread
        if (query_set.empty())
        {
            armed_deadline = 0;
            cond_var.wait(lock, [&] { return stop_thread || !query_set.empty(); });
        }
        else
        {
            chassert(!query_set.empty());
            /// `appendTask` may insert a deadline earlier than the one this wait is armed for;
            /// the wait must be re-armed then, or the notification is swallowed and the new
            /// deadline fires only when the stale (later) one expires.
            armed_deadline = query_set.begin()->endtime;
            /// The wait is sliced (see `MAX_WAIT_MS`); a slice that elapses without the predicate
            /// becoming true simply re-arms the wait on the next iteration of the loop.
            cond_var.wait_for(
                lock,
                std::chrono::milliseconds(std::min(armed_deadline - now_ms, MAX_WAIT_MS)),
                [&] {
                    /// Use fresh time to avoid spinning when the predicate is re-evaluated after spurious wakeups.
                    UInt64 fresh_now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
                    return stop_thread
                        || (!query_set.empty()
                            && (query_set.begin()->endtime <= fresh_now_ms || query_set.begin()->endtime < armed_deadline));
                });
        }
    }
    armed_deadline = 0;

    /// `terminateThread` flips `stop_thread` to true to signal exit; clear it here while still
    /// holding the lock so the singleton is left in a re-runnable state. This is mainly relevant
    /// to tests that own the worker thread and may want to spin it up again later; the server
    /// starts the worker exactly once during boot, so for the production code path this is a no-op.
    stop_thread = false;
}

UInt64 CancellationChecker::getArmedDeadline()
{
    std::unique_lock<std::mutex> lock(m);
    return armed_deadline;
}

}
