#include <chrono>
#include <mutex>
#include <queue>
#include <Interpreters/ProcessList.h>
#include <Common/logger_useful.h>
#include "base/types.h"

namespace DB
{

struct CompareEndTime {
    bool operator()(const std::pair<std::shared_ptr<QueryStatus>, UInt64>& a, const std::pair<std::shared_ptr<QueryStatus>, UInt64>& b) const {
        return a.second > b.second;
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
        std::pair<std::shared_ptr<QueryStatus>, UInt64>,
        std::vector<std::pair<std::shared_ptr<QueryStatus>, UInt64>>,
        CompareEndTime
    > minPriorityQueue;

    std::vector<std::shared_ptr<QueryStatus>> done_tasks;

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
    void appendTask(std::shared_ptr<QueryStatus> query, UInt64 end_time)
    {
        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "added. query: {}, timeout: {} milliseconds", query->getInfo().query, end_time);
        std::unique_lock<std::mutex> lock(m);
        minPriorityQueue.emplace(query, end_time);
        cond_var.notify_all();
    }

    void appendDoneTasks(std::shared_ptr<QueryStatus> query)
    {
        std::unique_lock<std::mutex> lock(m);
        done_tasks.push_back(query);
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
                // Wait until a new task is added or the thread is stopped
                cond_var.wait(lock, [this]() { return stop_thread || !minPriorityQueue.empty(); });
            }
            else if (!done_tasks.empty())
            {
                LOG_TRACE(getLogger("CANCELLATION CHECKER"), "Something is done");
                for (size_t i = 0; i < done_tasks.size(); i++)
                {
                    if (done_tasks[i] == minPriorityQueue.top().first)
                    {
                        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "removing {}", done_tasks[i]->getInfo().query);
                        minPriorityQueue.pop();
                        done_tasks.erase(done_tasks.begin() + i);
                    }
                }
            }
            else
            {
                auto next_task = minPriorityQueue.top();
                UInt64 end_time_ms = next_task.second; // UInt64 milliseconds
                auto now = std::chrono::steady_clock::now();
                UInt64 now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

                // Convert UInt64 end_time_ms to std::chrono::steady_clock::time_point
                const std::chrono::steady_clock::duration duration_milliseconds = std::chrono::milliseconds(end_time_ms);
                const std::chrono::time_point<std::chrono::steady_clock> end_time_point(duration_milliseconds);

                if (end_time_ms <= now_ms)
                {
                    if (end_time_ms != 0)
                    {
                        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "cancel task, end_time_ms: {}, now_ms: {}", end_time_ms, now_ms);
                        // Time to execute func1()
                        cancelTask(next_task.first);
                        // Remove task from queue
                        minPriorityQueue.pop();
                    }
                }
                else
                {
                    LOG_TRACE(getLogger("CANCELLATION CHECKER"), "Set on cond var");
                    // Wait until the nearest endTime or until a new task is added that might have an earlier endTime
                    cond_var.wait_for(lock, duration_milliseconds, [this, end_time_ms]()
                    {
                        LOG_TRACE(getLogger("CANCELLATION CHECKER"), "cond var condition is done");
                        return stop_thread || (!minPriorityQueue.empty() && minPriorityQueue.top().second < end_time_ms);
                    });
                }
            }
        }
    }
};

}
