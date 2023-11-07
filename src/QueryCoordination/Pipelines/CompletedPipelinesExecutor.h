#pragma once

#include <QueryPipeline/QueryPipeline.h>
#include <Common/ThreadPool.h>

namespace DB
{

using setExceptionCallback = std::function<void(std::exception_ptr exception_)>;

class CompletedPipelinesExecutor
{
public:
    CompletedPipelinesExecutor(std::vector<QueryPipeline> & pipelines_, std::vector<Int32> & fragment_ids_);

    ~CompletedPipelinesExecutor();

    /// This callback will be called each interactive_timeout_ms (if it is not 0).
    /// If returns true, query would be cancelled.
    void setCancelCallback(std::function<bool()> is_cancelled, size_t interactive_timeout_ms_);

    void setExceptionCallback(setExceptionCallback exception_callback_)
    {
        exception_callback = exception_callback_;
    }

    void execute();
    void asyncExecute();

    void waitFinish();
    void cancel();

    struct Data;
    struct Datas;

private:
    Poco::Logger * log;
    std::vector<QueryPipeline> pipelines;
    std::vector<Int32> fragment_ids;
    std::function<bool()> is_cancelled_callback;
    size_t interactive_timeout_ms = 0;
    std::unique_ptr<Datas> datas;
    Poco::Event datas_init;

    ThreadFromGlobalPool thread;

    DB::setExceptionCallback exception_callback;

    std::atomic_bool cancelled = false;
    std::atomic_bool cancelled_reading = false;
};

}
