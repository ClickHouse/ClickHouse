#pragma once

#include <QueryPipeline/SizeLimits.h>
#include <set>
#include <mutex>

namespace Poco
{
class Logger;
}

using LoggerPtr = std::shared_ptr<Poco::Logger>;

namespace DB
{

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;

/*
A Singleton class that checks if tasks are cancelled or timed out.
Has a priority queue ordered by end time. Checker waits until the
first task in the list is done, then checks if this task needs to be cancelled.
If yes, sets a cancellation flag on this task, otherwise removes the task from the queue.
*/
class CancellationChecker
{
private:
    CancellationChecker();

    struct QueryToTrack;

    struct CompareEndTime
    {
        bool operator()(const QueryToTrack & a, const QueryToTrack & b) const;
    };

    // Priority queue to manage tasks based on endTime
    std::multiset<QueryToTrack, CompareEndTime> query_set;

    bool stop_thread;
    std::mutex m;
    std::condition_variable cond_var;

    // Function to execute when a task's endTime is reached
    bool removeQueryFromSet(QueryStatusPtr query);

    static void cancelTask(CancellationChecker::QueryToTrack task);

    const LoggerPtr log;

public:
    // Singleton instance retrieval
    static CancellationChecker & getInstance();

    // Deleted copy constructor and assignment operator
    CancellationChecker(const CancellationChecker &) = delete;
    CancellationChecker & operator=(const CancellationChecker &) = delete;

    void terminateThread();

    // Method to add a new task to the multiset
    void appendTask(const QueryStatusPtr & query, Int64 timeout, OverflowMode overflow_mode);

    // Used when some task is done
    void appendDoneTasks(const QueryStatusPtr & query);

    // Worker thread function
    void workerFunction();
};
}
