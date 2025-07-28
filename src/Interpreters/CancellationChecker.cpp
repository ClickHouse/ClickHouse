#include <Common/logger_useful.h>
#include <Interpreters/ProcessList.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/CancellationChecker.h>

#include <chrono>
#include <mutex>


namespace DB
{

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
        if (task.overflow_mode == OverflowMode::THROW)
            task.query->cancelQuery(CancelReason::TIMEOUT);
        else
            task.query->checkTimeLimit();
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

bool CancellationChecker::removeQueryFromSet(QueryStatusPtr query)
{
    auto it = std::ranges::find(query_set, query, &QueryToTrack::query);

    if (it == query_set.end())
        return false;

    LOG_TEST(log, "Removing query {} from done tasks", query->getClientInfo().current_query_id);
    query_set.erase(it);
    return true;
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
    const UInt64 end_time = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() + timeout;
    query_set.emplace(query, timeout, end_time, overflow_mode);
    cond_var.notify_all();
}

void CancellationChecker::appendDoneTasks(const QueryStatusPtr & query)
{
    std::unique_lock<std::mutex> lock(m);
    removeQueryFromSet(query);
    cond_var.notify_all();
}

void CancellationChecker::workerFunction()
{
    LOG_TRACE(log, "Started worker function");
    std::vector<QueryToTrack> tasks_to_cancel;

    std::unique_lock<std::mutex> lock(m);

    while (!stop_thread)
    {
        UInt64 now_ms = 0;
        std::chrono::steady_clock::duration duration_milliseconds = std::chrono::milliseconds(0);

        if (!query_set.empty())
        {
            const auto next_task_it = query_set.begin();

            // Convert UInt64 timeout to std::chrono::steady_clock::time_point
            duration_milliseconds = std::chrono::milliseconds(next_task_it->timeout);

            auto end_time_ms = next_task_it->endtime;
            auto now = std::chrono::steady_clock::now();
            now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            if ((end_time_ms <= now_ms && duration_milliseconds.count() != 0))
            {
                LOG_DEBUG(
                    log,
                    "Cancelling the task because of the timeout: {} ms, query_id: {}",
                    next_task_it->timeout,
                    next_task_it->query->getClientInfo().current_query_id);

                tasks_to_cancel.push_back(*next_task_it);
                query_set.erase(next_task_it);
                continue;
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

        /// if last time we checked there were no queries,
        /// wakeup on first query that was added so we can setup
        /// proper timeout for waking up the thread
        if (!now_ms)
        {
            cond_var.wait(lock, [&] { return stop_thread || !query_set.empty(); });
        }
        else
        {
            chassert(duration_milliseconds.count());
            cond_var.wait_for(
                lock,
                duration_milliseconds,
                [&, now_ms] { return stop_thread || (!query_set.empty() && query_set.begin()->endtime < now_ms); });
        }
    }
}

}
