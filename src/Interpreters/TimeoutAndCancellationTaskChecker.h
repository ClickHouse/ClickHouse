#ifndef TIMEOUT_AND_CANCELLATION_TASK_CHECKER_H
#define TIMEOUT_AND_CANCELLATION_TASK_CHECKER_H

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/ProcessList.h>
#include <Common/CurrentThread.h>


namespace DB
{

class TimeoutAndCancellationTaskChecker
{
public:

    struct TimedTask
    {
        std::chrono::steady_clock::time_point expiry_time;
        std::shared_ptr<QueryStatus> task;

        bool operator<(const TimedTask& other) const;
    };

    static TimeoutAndCancellationTaskChecker& getInstance(int timeout_microseconds = 300);
    TimeoutAndCancellationTaskChecker(const TimeoutAndCancellationTaskChecker&) = delete;
    TimeoutAndCancellationTaskChecker& operator=(const TimeoutAndCancellationTaskChecker&) = delete;

    void addTask(const std::chrono::steady_clock::time_point& timestamp, const String & query_,
                const IAST * ast,ContextMutablePtr query_context, UInt64 watch_start_nanoseconds);
    void shutdownChecker();

private:
    std::string name = "TimeoutAndCancellationTaskChecker";
    std::set<TimedTask> tasks;
    std::mutex mutex;
    std::condition_variable cv;
    std::atomic<bool> shutdown{false};
    int timeout;
    std::thread worker_thread;

    explicit TimeoutAndCancellationTaskChecker(int timeout_microseconds);
    ~TimeoutAndCancellationTaskChecker();

    void threadFunction();

    void cancelTask();
};

#endif  // TIMEOUT_AND_CANCELLATION_TASK_CHECKER_H

}
