#pragma once

#include <Interpreters/ProcessList.h>
#include "QueryPipeline/SizeLimits.h"
#include <atomic>
#include <set>

namespace DB
{

struct QueryToTrack
{
    QueryToTrack(
        std::shared_ptr<QueryStatus> query_,
        UInt64 timeout_,
        UInt64 endtime_,
        OverflowMode overflow_mode_);

    std::shared_ptr<QueryStatus> query;
    UInt64 timeout;
    UInt64 endtime;
    OverflowMode overflow_mode;
};

struct CompareEndTime
{
    bool operator()(const QueryToTrack& a, const QueryToTrack& b) const;
};

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

    // Priority queue to manage tasks based on endTime
    std::multiset<QueryToTrack, CompareEndTime> querySet;

    bool stop_thread;
    std::mutex m;
    std::condition_variable cond_var;

    // Function to execute when a task's endTime is reached
    void cancelTask(QueryToTrack task);
    bool removeQueryFromSet(std::shared_ptr<QueryStatus> query);

    const LoggerPtr log;

public:
    // Singleton instance retrieval
    static CancellationChecker& getInstance();

    // Deleted copy constructor and assignment operator
    CancellationChecker(const CancellationChecker&) = delete;
    CancellationChecker& operator=(const CancellationChecker&) = delete;

    void terminateThread();

    // Method to add a new task to the multiset
    void appendTask(const std::shared_ptr<QueryStatus> & query, const Int64 & timeout, OverflowMode overflow_mode);

    // Used when some task is done
    void appendDoneTasks(const std::shared_ptr<QueryStatus> & query);

    // Worker thread function
    void workerFunction();
};

}
