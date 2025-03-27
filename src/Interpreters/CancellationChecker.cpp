#include <Common/logger_useful.h>
#include <Interpreters/ProcessList.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/CancellationChecker.h>

#include <chrono>
#include <mutex>


namespace DB
{

QueryToTrack::QueryToTrack(
    std::shared_ptr<QueryStatus> query_,
    UInt64 timeout_,
    UInt64 endtime_,
    OverflowMode overflow_mode_)
    : query(query_), timeout(timeout_), endtime(endtime_), overflow_mode(overflow_mode_)
{
}

bool CompareEndTime::operator()(const QueryToTrack& a, const QueryToTrack& b) const
{
    return std::tie(a.endtime, a.query) < std::tie(b.endtime, b.query);
}

CancellationChecker::CancellationChecker() : stop_thread(false), log(getLogger("CancellationChecker"))
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

void CancellationChecker::cancelTask(QueryToTrack task)
{
    if (task.query)
    {
        if (task.overflow_mode == OverflowMode::THROW)
            task.query->cancelQuery(CancelReason::TIMEOUT);
        else
            task.query->checkTimeLimit();
    }
}

bool CancellationChecker::removeQueryFromSet(std::shared_ptr<QueryStatus> query)
{
    auto it = std::find_if(querySet.begin(), querySet.end(), [&](const QueryToTrack& task)
    {
        return task.query == query;
    });

    if (it != querySet.end())
    {
        LOG_TEST(log, "Removing query {} from done tasks", query->getInfo().query);
        querySet.erase(it);
        return true;
    }

    return false;
}

void CancellationChecker::appendTask(const std::shared_ptr<QueryStatus> & query, const Int64 & timeout, OverflowMode overflow_mode)
{
    if (timeout <= 0) // Avoid cases when the timeout is less or equal zero
    {
        LOG_TEST(log, "Did not add the task because the timeout is 0. Query: {}", query->getInfo().query);
        return;
    }
    std::unique_lock<std::mutex> lock(m);
    LOG_TEST(log, "Added to set. query: {}, timeout: {} milliseconds", query->getInfo().query, timeout);
    const auto & now = std::chrono::steady_clock::now();
    const UInt64 & end_time = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() + timeout;
    querySet.emplace(query, timeout, end_time, overflow_mode);
    cond_var.notify_all();
}

void CancellationChecker::appendDoneTasks(const std::shared_ptr<QueryStatus> & query)
{
    std::unique_lock<std::mutex> lock(m);
    removeQueryFromSet(query);
    cond_var.notify_all();
}

void CancellationChecker::workerFunction()
{
    LOG_TRACE(log, "Started worker function");
    std::unique_lock<std::mutex> lock(m);

    while (!stop_thread)
    {
        size_t query_size = 0;
        UInt64 end_time_ms = 0;
        UInt64 duration = 0;
        auto now = std::chrono::steady_clock::now();
        UInt64 now_ms;
        std::chrono::steady_clock::duration duration_milliseconds = std::chrono::milliseconds(0);

        if (!querySet.empty())
        {
            query_size = querySet.size();

            const auto next_task = (*querySet.begin());

            end_time_ms = next_task.endtime;
            duration = next_task.timeout;
            now = std::chrono::steady_clock::now();
            now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

            // Convert UInt64 timeout to std::chrono::steady_clock::time_point
            duration_milliseconds = std::chrono::milliseconds(next_task.timeout);

            if ((end_time_ms <= now_ms && duration_milliseconds.count() != 0))
            {
                LOG_DEBUG(log, "Cancelling the task because of the timeout: {} ms, query_id: {}",
                    duration, next_task.query->getClientInfo().current_query_id);

                cancelTask(next_task);
                querySet.erase(next_task);
                continue;
            }
        }

        if (!duration_milliseconds.count())
            duration_milliseconds = std::chrono::years(1); // we put one year time to wait if we don't have any timeouts

        cond_var.wait_for(lock, duration_milliseconds, [this, end_time_ms, query_size]()
        {
            if (query_size)
                return stop_thread || (!querySet.empty() && (*querySet.begin()).endtime < end_time_ms);
            else
                return stop_thread || !querySet.empty();
        });
    }
}

}
