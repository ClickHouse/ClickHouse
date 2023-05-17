#pragma once

#include "config.h"

#if USE_AWS_S3

#include "WriteBufferFromS3.h"

#include <list>

namespace DB
{

/// That class is used only in WriteBufferFromS3 for now.
/// Therefore it declared as a part of  WriteBufferFromS3.
/// TaskTracker takes a Callback which is run by scheduler in some external shared ThreadPool.
/// TaskTracker brings the methods waitReady, waitAll/safeWaitAll
/// to help with coordination of the running tasks.

class WriteBufferFromS3::TaskTracker
{
public:
    using Callback = std::function<void()>;

    explicit TaskTracker(ThreadPoolCallbackRunner<void> scheduler_, size_t max_tasks_inflight_ = 0);
    ~TaskTracker();

    static ThreadPoolCallbackRunner<void> syncRunner();

    bool isAsync() const;
    size_t consumeReady();
    void waitAny();
    void waitAll();
    void safeWaitAll();
    void add(Callback && func);

private:
    void waitInFlight();

    const bool is_async;
    ThreadPoolCallbackRunner<void> scheduler;
    const size_t max_tasks_inflight;

    using FutureList = std::list<std::future<void>>;
    FutureList futures;
    Poco::Logger * log = &Poco::Logger::get("TaskTracker");

    std::mutex mutex;
    std::condition_variable cond_var;
    using FinishedList = std::list<FutureList::iterator>;
    FinishedList finished_futures;
};

}

#endif
