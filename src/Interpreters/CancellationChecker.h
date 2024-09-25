#include <chrono>
#include <memory>
#include <mutex>
#include <queue>
#include <Interpreters/ProcessList.h>
#include <Common/logger_useful.h>
#include "base/types.h"

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
        return a.timeout > b.timeout;
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
    std::priority_queue<
        QueryToTrack,
        std::vector<QueryToTrack>,
        CompareEndTime
    > minPriorityQueue;

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

    // Method to add a new task to the priority queue
    void appendTask(std::shared_ptr<QueryStatus> query, UInt64 timeout)
    {
        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "added. query: {}, timeout: {} milliseconds", query->getInfo().query, timeout);
        auto now = std::chrono::steady_clock::now();
        UInt64 end_time = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() + timeout;
        minPriorityQueue.emplace(query, timeout, end_time);
        cond_var.notify_all();
    }

    void appendDoneTasks(std::shared_ptr<QueryStatus> query)
    {
        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "added to done tasks, query: {}", query->getInfo().query);
        done_tasks.push_back(query);
        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "done tasks size: {}", done_tasks.size());
        cond_var.notify_all();
    }

    void addToCancelledTasks(std::shared_ptr<QueryStatus> query)
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
            if (minPriorityQueue.empty())
            {
                LOG_TRACE(getLogger("CANCELLATION CHECKER"), "minPriorityQueue.empty()");
                // Wait until a new task is added or the thread is stopped
                cond_var.wait(lock, [this]() { return stop_thread || !minPriorityQueue.empty(); });
            }
            else
            {
                LOG_TRACE(getLogger("CANCELLATION CHECKER"), "else");
                if (!done_tasks.empty())
                {
                    LOG_TRACE(getLogger("CANCELLATION CHECKER"), "Something is done");
                    for (size_t i = 0; i < done_tasks.size(); i++)
                    {
                        if (done_tasks[i] == minPriorityQueue.top().query)
                        {
                            LOG_TRACE(getLogger("CANCELLATION CHECKER"), "removing {}", done_tasks[i]->getInfo().query);
                            minPriorityQueue.pop();
                            done_tasks.erase(done_tasks.begin() + i);
                        }
                    }
                    if (minPriorityQueue.empty())
                        continue;
                }

                if (!cancelled_tasks.empty())
                {
                    LOG_TRACE(getLogger("CANCELLATION CHECKER"), "Something needs to be cancelled");
                    for (size_t i = 0; i < cancelled_tasks.size(); i++)
                    {
                        if (cancelled_tasks[i] == minPriorityQueue.top().query)
                        {
                            LOG_TRACE(getLogger("CANCELLATION CHECKER"), "cancelling task {}", cancelled_tasks[i]->getInfo().query);
                            cancelTask(minPriorityQueue.top().query);
                            minPriorityQueue.pop();
                            cancelled_tasks.erase(cancelled_tasks.begin() + i);
                        }
                    }
                    if (minPriorityQueue.empty())
                        continue;
                }

                const auto next_task = minPriorityQueue.top();

                {
                    const UInt64 end_time_ms = next_task.endtime; // UInt64 milliseconds
                    const auto now = std::chrono::steady_clock::now();
                    const UInt64 now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

                    // Convert UInt64 timeout to std::chrono::steady_clock::time_point
                    const std::chrono::steady_clock::duration duration_milliseconds = std::chrono::milliseconds(next_task.timeout);
                    const std::chrono::time_point<std::chrono::steady_clock> end_time_point(duration_milliseconds);

                    LOG_TRACE(getLogger("CANCELLATION CHECKER"), "end_time_ms: {}, now_ms: {}, duration: {}, isKilled: {}", end_time_ms, now_ms, duration_milliseconds.count(), next_task.query->isKilled());
                    if ((end_time_ms <= now_ms && duration_milliseconds.count() != 0))
                    {
                        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "cancel task, end_time_ms: {}, now_ms: {}", end_time_ms, now_ms);
                        cancelTask(next_task.query);
                        minPriorityQueue.pop();
                    }
                    else
                    {
                        if (duration_milliseconds.count())
                        {
                            const size_t cancelled_size = cancelled_tasks.size();
                            // Wait until the nearest endTime or until a new task is added that might have an earlier endTime or maybe some task cancelled
                            cond_var.wait_for(lock, duration_milliseconds, [this, end_time_ms, cancelled_size]()
                            {
                                LOG_TRACE(getLogger("CANCELLATION CHECKER"), "cond var condition is: {}, size of queue: {}", stop_thread || (!minPriorityQueue.empty() && minPriorityQueue.top().endtime < end_time_ms), minPriorityQueue.size());
                                return stop_thread || (!minPriorityQueue.empty() && minPriorityQueue.top().endtime < end_time_ms) || cancelled_tasks.size() != cancelled_size;
                            });
                        }
                        else
                        {
                            const size_t cancelled_size = cancelled_tasks.size();
                            LOG_TRACE(getLogger("CANCELLATION CHECKER"), "doesn't have duration, done_size: {}", cancelled_size);
                            cond_var.wait(lock, [this, cancelled_size]()
                            {
                                LOG_TRACE(getLogger("CANCELLATION CHECKER"), "cond var condition is: {}, done_size: {}", stop_thread  || cancelled_tasks.size() != cancelled_size, cancelled_size);
                                return stop_thread  || cancelled_tasks.size() != cancelled_size;
                            });
                        }
                    }
                }
            }
        }
    }
};

}
