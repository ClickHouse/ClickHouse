#pragma once

#include "config.h"

#if USE_AWS_S3

#include "WriteBufferFromS3.h"

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

    explicit TaskTracker(ThreadPoolCallbackRunner<void> scheduler_);
    ~TaskTracker();

    static ThreadPoolCallbackRunner<void> syncRunner();

    bool isAsync() const;
    void waitReady();
    void waitAll();
    void safeWaitAll();
    void add(Callback && func);

private:
    bool is_async;
    ThreadPoolCallbackRunner<void> scheduler;
    std::list<std::future<void>> futures;
    Poco::Logger * log = &Poco::Logger::get("TaskTracker");
};

}

#endif
