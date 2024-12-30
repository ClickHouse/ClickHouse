#pragma once

#include "config.h"
#include "threadPoolCallbackRunner.h"
#include "IO/WriteBufferFromS3.h"

#include "logger_useful.h"

#include <list>

namespace DB
{

/// TaskTracker takes a Callback which is run by scheduler in some external shared ThreadPool.
/// TaskTracker brings the methods waitIfAny, waitAll/safeWaitAll
/// to help with coordination of the running tasks.

/// Basic exception safety is provided. If exception occurred the object has to be destroyed.
/// No thread safety is provided. Use this object with no concurrency.

class TaskTracker
{
public:
    using Callback = std::function<void()>;

    TaskTracker(ThreadPoolCallbackRunnerUnsafe<void> scheduler_, size_t max_tasks_inflight_, LogSeriesLimiterPtr limited_log_);
    ~TaskTracker();

    static ThreadPoolCallbackRunnerUnsafe<void> syncRunner();

    bool isAsync() const;

    /// waitIfAny collects statuses from already finished tasks
    /// There could be no finished tasks yet, so waitIfAny do nothing useful in that case
    /// the first exception is thrown if any task has failed
    void waitIfAny();

    /// Well, waitAll waits all the tasks until they finish and collects their statuses
    void waitAll();

    /// safeWaitAll does the same as waitAll but mutes the exceptions
    void safeWaitAll();

    void add(Callback && func);

private:
    /// waitTilInflightShrink waits til the number of in-flight tasks beyond the limit `max_tasks_inflight`.
    void waitTilInflightShrink() TSA_NO_THREAD_SAFETY_ANALYSIS;

    void collectFinishedFutures(bool propagate_exceptions) TSA_REQUIRES(mutex);

    const bool is_async;
    ThreadPoolCallbackRunnerUnsafe<void> scheduler;
    const size_t max_tasks_inflight;

    using FutureList = std::list<std::future<void>>;
    FutureList futures;
    LogSeriesLimiterPtr limited_log;

    std::mutex mutex;
    std::condition_variable has_finished TSA_GUARDED_BY(mutex);
    using FinishedList = std::list<FutureList::iterator>;
    FinishedList finished_futures TSA_GUARDED_BY(mutex);
};

}
