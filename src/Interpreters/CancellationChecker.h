#pragma once

#include <QueryPipeline/SizeLimits.h>
#include <chrono>
#include <set>
#include <mutex>

namespace Poco
{
class Logger;
}

using LoggerPtr = std::shared_ptr<Poco::Logger>;

/// gtest generates this class for `TEST(CancellationChecker, RearmsWaitOnEarlierDeadline)`; it is a
/// friend of `CancellationChecker` (below) so the test can read the private `armed_deadline`.
class CancellationChecker_RearmsWaitOnEarlierDeadline_Test;

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
    /// 0 while the worker is not parked on a deadline. The test below reads it (under `m`) to
    /// synchronize with the wait state; it is not part of the public interface.
    UInt64 armed_deadline = 0;
    friend class ::CancellationChecker_RearmsWaitOnEarlierDeadline_Test;

    static void cancelTask(CancellationChecker::QueryToTrack task);

    const LoggerPtr log;

public:
    // Singleton instance retrieval
    static CancellationChecker & getInstance();

    // Deleted copy constructor and assignment operator
    CancellationChecker(const CancellationChecker &) = delete;
    CancellationChecker & operator=(const CancellationChecker &) = delete;

    void terminateThread();

    // Method to add a new task to the multiset. Returns true if the task was added.
    [[nodiscard]] bool appendTask(const QueryStatusPtr & query, Int64 timeout_us, OverflowMode overflow_mode);

    /// The deadline (ms since the steady_clock epoch) for a task appended at `now` with `timeout_us`,
    /// aligned up to the grid the worker batches deadlines on. Never earlier than `now + timeout_us`:
    /// a task cancelled before its own timeout fails with a self-contradictory
    /// `Timeout exceeded: elapsed 999.672 ms, maximum: 1000 ms`.
    static UInt64 taskDeadlineMs(std::chrono::steady_clock::time_point now, Int64 timeout_us);

    // Used when some task is done
    void appendDoneTasks(const QueryStatusPtr & query);

    // Worker thread function
    void workerFunction();
};
}
