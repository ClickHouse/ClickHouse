#include <Common/logger_useful.h>
#include <Interpreters/CancellationChecker.h>

#include <__chrono/duration.h>
#include <condition_variable>
#include <mutex>


namespace DB
{

QueryToTrack::QueryToTrack(
    std::shared_ptr<QueryStatus> query_,
    UInt64 timeout_,
    UInt64 endtime_)
    : query(query_), timeout(timeout_), endtime(endtime_)
{
}

bool CompareEndTime::operator()(const QueryToTrack& a, const QueryToTrack& b) const
{
    if (a.endtime != b.endtime)
        return a.endtime < b.endtime;
    else
        return a.query->getClientInfo().current_query_id < b.query->getClientInfo().current_query_id;
}

CancellationChecker::CancellationChecker() : stop_thread(false)
{
}

CancellationChecker::~CancellationChecker()
{
    stop_thread = true;
}

CancellationChecker& CancellationChecker::getInstance()
{
    static CancellationChecker instance;
    return instance;
}

void CancellationChecker::cancelTask(std::shared_ptr<QueryStatus> query)
{
    query->cancelQuery(/*kill=*/false);
}

bool CancellationChecker::removeQueryFromSet(std::shared_ptr<QueryStatus> query)
{
    for (auto it = querySet.begin(); it != querySet.end();)
    {
        if (it->query == query)
        {
            LOG_TRACE(getLogger("CancellationChecker"), "Removing query {} from done tasks", query->getInfo().query);
            it = querySet.erase(it);
            return true;
        }
        else
            ++it;
    }
    return false;
}

void CancellationChecker::appendTask(const std::shared_ptr<QueryStatus> & query, const UInt64 & timeout)
{
    if (!timeout)
    {
        LOG_TRACE(getLogger("CancellationChecker"), "Did not add the task because the timeout is 0. Query: {}", query->getInfo().query);
        return;
    }
    std::unique_lock<std::mutex> lock(m);
    LOG_TRACE(getLogger("CancellationChecker"), "Added to set. query: {}, timeout: {} milliseconds", query->getInfo().query, timeout);
    const auto & now = std::chrono::steady_clock::now();
    const UInt64 & end_time = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() + timeout;
    querySet.emplace(query, timeout, end_time);
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
    LOG_TRACE(getLogger("CancellationChecker"), "Started worker function");
    std::unique_lock<std::mutex> lock(m);

    while (!stop_thread)
    {
        size_t query_size = 0;
        UInt64 end_time_ms = 0;
        UInt64 duration = 0;
        auto now = std::chrono::steady_clock::now();
        UInt64 now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
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
                LOG_TRACE(getLogger("CancellationChecker"), "Cancelling the task because of the timeout: {}, query: {}", duration, next_task.query->getInfo().query);
                cancelTask(next_task.query);
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
