#pragma once

#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentThread.h>

namespace DB
{

struct TSA_SCOPED_LOCKABLE LockGuardWithStopWatch final
{
    std::string caller;
    LoggerRawPtr log;
    UInt64 thread_id{0};

    std::optional<Stopwatch> wait_watch;
    std::optional<std::lock_guard<std::mutex>> lock;
    std::optional<Stopwatch> lock_watch;

    explicit LockGuardWithStopWatch(std::mutex & mutex_, const LoggerPtr & log_, const std::string & caller_) TSA_ACQUIRE(mutex_)
        : caller(caller_)
        , log(log_.get())
    {
        if (CurrentThread::isInitialized())
            thread_id = CurrentThread::get().thread_id;

        wait_watch.emplace(CLOCK_MONOTONIC);
        lock.emplace(mutex_);
        lock_watch.emplace(CLOCK_MONOTONIC);
    }

    ~LockGuardWithStopWatch() TSA_RELEASE()
    {
        /// Must be destroyed first.
        lock.reset();

        if (wait_watch.has_value() && wait_watch->elapsedMilliseconds() > 1000)
        {
            LOG_WARNING(log, "Lock acquisition took {} ms for thread {} in [{}], Stack trace (when copying this message, always include the lines below): \n {}",
                wait_watch->elapsedMilliseconds(), thread_id, caller, StackTrace().toString());
        }

        if (lock_watch.has_value() && lock_watch->elapsedMilliseconds() > 1000)
        {
            LOG_WARNING(log, "Lock was help for {} ms by thread {} in [{}], Stack trace (when copying this message, always include the lines below): \n {}",
                wait_watch->elapsedMilliseconds(), thread_id, caller, StackTrace().toString());
        }
    }

    LockGuardWithStopWatch(LockGuardWithStopWatch const &) = delete;
    LockGuardWithStopWatch& operator=(LockGuardWithStopWatch const&) = delete;
};

}
