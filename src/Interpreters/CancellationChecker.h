#include <chrono>
#include <memory>
#include <mutex>
#include <Interpreters/ProcessList.h>
#include <Common/logger_useful.h>
#include <base/types.h>

namespace DB
{

struct QueryToTrack
{
    QueryToTrack (std::shared_ptr<QueryStatus> query_, UInt64 timeout_, UInt64 endtime_):
    query(query_),
    timeout(timeout_),
    endtime(endtime_) {}
    std::shared_ptr<QueryStatus> query;
    UInt64 timeout;
    UInt64 endtime;
};

struct CompareEndTime
{
    bool operator()(const QueryToTrack& a, const QueryToTrack& b) const
    {
        return a.endtime < b.endtime;
    }
};

/*
A Singleton class that check if tasks are cancelled or timed out.
Has a priority queue ordered by end time. Checker waits until the
first task in the list is done, then check if this task needs to be cancelled.
If yes, sets a cancellation flag on this task, otherwise removes the task from the queue.
*/
class CancellationChecker
{
private:
    // Private constructor for Singleton pattern
    CancellationChecker() : stop_thread(false)
    {
    }

    ~CancellationChecker()
    {
        stop_thread = true;
    }

    // Priority queue to manage tasks based on endTime
    std::multiset<QueryToTrack, CompareEndTime> querySet;

    std::vector<std::shared_ptr<QueryStatus>> done_tasks;
    std::vector<std::shared_ptr<QueryStatus>> cancelled_tasks;

    std::atomic<bool> stop_thread;
    std::mutex m;
    std::condition_variable cond_var;

    // Function to execute when a task's endTime is reached
    void cancelTask(std::shared_ptr<QueryStatus> query)
    {
        query->cancelQuery(/*kill=*/false);
    }

    bool removeQueryFromSet(std::shared_ptr<QueryStatus> query)
    {
        for (auto it = querySet.begin(); it != querySet.end();)
        {
            if (it->query == query)
            {
                it = querySet.erase(it);
                return true;
            }
            else
                ++it;
        }
        return false;
    }

public:
    // Singleton instance retrieval
    static CancellationChecker& getInstance()
    {
        static CancellationChecker instance;
        return instance;
    }

    // Deleted copy constructor and assignment operator
    CancellationChecker(const CancellationChecker&) = delete;
    CancellationChecker& operator=(const CancellationChecker&) = delete;

    // Method to add a new task to the multiset
    void appendTask(const std::shared_ptr<QueryStatus> & query, const UInt64 & timeout)
    {
        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "added. query: {}, timeout: {} milliseconds", query->getInfo().query, timeout);
        const auto & now = std::chrono::steady_clock::now();
        const UInt64 & end_time = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() + timeout;
        querySet.emplace(query, timeout, end_time);
        cond_var.notify_all();
    }

    // Used when some task is done
    void appendDoneTasks(const std::shared_ptr<QueryStatus> & query)
    {
        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "added to done tasks, query: {}", query->getInfo().query);
        done_tasks.push_back(query);
        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "done tasks size: {}", done_tasks.size());
        cond_var.notify_all();
    }

    // Used when some task is cancelled
    void addToCancelledTasks(const std::shared_ptr<QueryStatus> & query)
    {
        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "added to cancelled tasks, query: {}", query->getInfo().query);
        cancelled_tasks.push_back(query);
        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "cancelled tasks size: {}", cancelled_tasks.size());
        cond_var.notify_all();
    }

    // Worker thread function
    void workerFunction()
    {
        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "workerFunction()");
        std::unique_lock<std::mutex> lock(m);

        while (!stop_thread)
        {
            LOG_TRACE(getLogger("CANCELLATION CHECKER"), "Iteration");
            if (querySet.empty())
            {
                LOG_TRACE(getLogger("CANCELLATION CHECKER"), "minPriorityQueue.empty()");
                // Wait until a new task is added or the thread is stopped
                cond_var.wait(lock, [this]() { return stop_thread || !querySet.empty(); });
            }
            else
            {
                LOG_TRACE(getLogger("CANCELLATION CHECKER"), "else");

                if (!cancelled_tasks.empty())
                {
                    LOG_TRACE(getLogger("CANCELLATION CHECKER"), "Something needs to be cancelled");
                    for (auto it = cancelled_tasks.begin(); it != cancelled_tasks.end();)
                    {
                        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "removing {} from cancelled tasks", (*it)->getInfo().query);
                        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "cancelling this query.");
                        cancelTask(*it);
                        removeQueryFromSet(*it);
                        cancelled_tasks.erase(it);
                    }
                    if (querySet.empty())
                        continue;
                }

                if (!done_tasks.empty())
                {
                    LOG_TRACE(getLogger("CANCELLATION CHECKER"), "Something is done");
                    for (auto it = done_tasks.begin(); it != done_tasks.end();)
                    {
                        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "removing {} from done tasks", (*it)->getInfo().query);
                        removeQueryFromSet(*it);
                        done_tasks.erase(it);
                    }
                    if (querySet.empty())
                        continue;
                }

                const auto next_task = (*querySet.begin());

                const UInt64 end_time_ms = next_task.endtime;
                const auto now = std::chrono::steady_clock::now();
                const UInt64 now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

                // Convert UInt64 timeout to std::chrono::steady_clock::time_point
                const std::chrono::steady_clock::duration duration_milliseconds = std::chrono::milliseconds(next_task.timeout);

                if ((end_time_ms <= now_ms && duration_milliseconds.count() != 0))
                {
                    LOG_TRACE(getLogger("CANCELLATION CHECKER"), "cancel task because of the timeout, end_time_ms: {}, now_ms: {}", end_time_ms, now_ms);
                    cancelTask(next_task.query);
                    querySet.erase(next_task);
                }
                else
                {
                    LOG_TRACE(getLogger("CANCELLATION CHECKER"), "condvar, duration: {}", duration_milliseconds.count());
                    // Wait until the nearest endTime or until a new task is added that might have an earlier endTime or maybe some task cancelled
                    if (duration_milliseconds.count())
                    {
                        cond_var.wait_for(lock, duration_milliseconds, [this, end_time_ms]()
                        {
                            return stop_thread || (!querySet.empty() && (*querySet.begin()).endtime < end_time_ms) || !done_tasks.empty() || !cancelled_tasks.empty();
                        });
                    }
                    else
                    {
                        cond_var.wait(lock, [this, end_time_ms]()
                        {
                            return stop_thread || (!querySet.empty() && (*querySet.begin()).endtime < end_time_ms) || !done_tasks.empty() || !cancelled_tasks.empty();
                        });
                    }
                }
            }
        }
    }
};

}
