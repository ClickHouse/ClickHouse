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

    /// The deadline (ms since steady_clock epoch) the worker's wait is currently armed for;
    /// 0 while the worker is not parked on a deadline. Lets tests synchronize with the wait state.
    UInt64 armed_deadline = 0;

    static void cancelTask(CancellationChecker::QueryToTrack task);

    const LoggerPtr log;

public:
    /// Deadlines are aligned to this grid to allow batching of timeout processing.
    /// Tasks may be cancelled slightly later than their exact timeout, but never before.
    static constexpr UInt64 CANCELLATION_GRID_MS = 100;

    // Singleton instance retrieval
    static CancellationChecker & getInstance();

    // Deleted copy constructor and assignment operator
    CancellationChecker(const CancellationChecker &) = delete;
    CancellationChecker & operator=(const CancellationChecker &) = delete;

    void terminateThread();

    // Method to add a new task to the multiset. Returns true if the task was added.
    /// The timeout is in microseconds: flooring it to milliseconds first would arm the deadline
    /// before the timeout the setting names.
    [[nodiscard]] bool appendTask(const QueryStatusPtr & query, Int64 timeout_us, OverflowMode overflow_mode);

    // Used when some task is done
    void appendDoneTasks(const QueryStatusPtr & query);

    // Worker thread function
    void workerFunction();

    // The deadline the worker is currently sleeping toward, 0 when it is not. For tests.
    UInt64 getArmedDeadline();

    /// Grid-aligned deadline, in whole milliseconds since the steady_clock epoch, for a task
    /// started at `now_ns` with a timeout of `timeout_us`. Never earlier than the exact deadline.
    static UInt64 alignedDeadlineMs(UInt64 now_ns, UInt64 timeout_us);
};
}
