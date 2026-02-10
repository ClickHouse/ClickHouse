#include <Common/logger_useful.h>
#include <Interpreters/ProcessList.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/CancellationChecker.h>

#include <chrono>
#include <mutex>


namespace DB
{

/// Align all timeouts to a grid to allow batching of timeout processing.
/// Tasks may be cancelled slightly later than their exact timeout, but never before.
static constexpr UInt64 CANCELLATION_GRID_MS = 100;

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

void CancellationChecker::appendTask(const QueryStatusPtr & query, const Int64 timeout, OverflowMode overflow_mode)
{
    if (timeout <= 0) // Avoid cases when the timeout is less or equal zero
    {
        LOG_TEST(log, "Did not add the task because the timeout is 0, query_id: {}", query->getClientInfo().current_query_id);
        return;
    }
    std::unique_lock<std::mutex> lock(m);
    LOG_TEST(log, "Added to set. query: {}, timeout: {} milliseconds", query->getInfo().query, timeout);
    const auto now = std::chrono::steady_clock::now();
    const UInt64 now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    /// Round up to the next grid boundary to enable batching of timeout checks.
    /// This ensures tasks are never cancelled before their timeout, only slightly after.
    const UInt64 end_time = ((now_ms + timeout + CANCELLATION_GRID_MS - 1) / CANCELLATION_GRID_MS) * CANCELLATION_GRID_MS;
    auto iter = query_set.emplace(query, timeout, end_time, overflow_mode);
    if (iter == query_set.begin()) // Only notify if the new task is the earliest one
        cond_var.notify_all();
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
                    "Cancelling the task because of the timeout: {} ms, query_id: {}",
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
            cond_var.wait(lock, [&] { return stop_thread || !query_set.empty(); });
        }
        else
        {
            chassert(!query_set.empty());
            cond_var.wait_for(
                lock,
                std::chrono::milliseconds(query_set.begin()->endtime - now_ms),
                [&, now_ms] { return stop_thread || (!query_set.empty() && query_set.begin()->endtime < now_ms); });
        }
    }
}

}
