#pragma once

#include "config.h"

#if USE_AWS_S3

#include "WriteBufferFromS3.h"

namespace DB
{

class WriteBufferFromS3::TaskTracker
{
public:
    using Callback = std::function<void()>;

    explicit TaskTracker(ThreadPoolCallbackRunner<void> scheduler_);
    ~TaskTracker();

    static ThreadPoolCallbackRunner<void> syncRunner();

    bool isAsync() const;
    void getReady();
    void getAll();
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
