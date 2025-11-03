#pragma once

#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentThread.h>

namespace DB
{

struct TSA_SCOPED_LOCKABLE LockGuardWithStopWatch final
{
#if defined(SANITIZER) || !defined(NDEBUG)
    static constexpr auto THRESHOLD_MILLISECONDS = 10 * 1000;
#else
    static constexpr auto THRESHOLD_MILLISECONDS = 1000;
#endif

    const char * caller{nullptr};
    LoggerRawPtr log;
    UInt64 thread_id{0};

    std::optional<Stopwatch> wait_watch;
    std::optional<std::lock_guard<std::mutex>> lock;
    std::optional<Stopwatch> lock_watch;

    explicit LockGuardWithStopWatch(std::mutex & mutex_, LoggerRawPtr log_, const char * caller_) TSA_ACQUIRE(mutex_)
        : caller(caller_)
        , log(log_)
    {
        if (CurrentThread::isInitialized())
            thread_id = CurrentThread::get().thread_id;

        wait_watch.emplace(CLOCK_MONOTONIC);
        lock.emplace(mutex_);
        wait_watch->stop();
        lock_watch.emplace(CLOCK_MONOTONIC);
    }

    explicit LockGuardWithStopWatch(std::mutex & mutex_, LoggerPtr log_, const char * caller_) TSA_ACQUIRE(mutex_)
        : LockGuardWithStopWatch(mutex_, log_.get(), caller_)
    {
    }

    ~LockGuardWithStopWatch() TSA_RELEASE()
    {
        ALLOW_ALLOCATIONS_IN_SCOPE;

        /// Must be destroyed first.
        lock.reset();
        lock_watch->stop();

        if (wait_watch->elapsedMilliseconds() > THRESHOLD_MILLISECONDS)
        {
            LOG_WARNING(log, "Lock acquisition took {} ms for thread {} in [{}], Stack trace (when copying this message, always include the lines below): \n {}",
                wait_watch->elapsedMilliseconds(), thread_id, caller, StackTrace().toString());
        }

        if (lock_watch->elapsedMilliseconds() > THRESHOLD_MILLISECONDS)
        {
            LOG_WARNING(log, "Lock was held for {} ms by thread {} in [{}], Stack trace (when copying this message, always include the lines below): \n {}",
                wait_watch->elapsedMilliseconds(), thread_id, caller, StackTrace().toString());
        }
    }

    LockGuardWithStopWatch(LockGuardWithStopWatch const &) = delete;
    LockGuardWithStopWatch& operator=(LockGuardWithStopWatch const&) = delete;
};

}
