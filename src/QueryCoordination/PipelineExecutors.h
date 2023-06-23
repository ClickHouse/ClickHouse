#pragma once

#include <Processors/Executors/PipelineExecutor.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/ThreadPool.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

using onFinishCallBack = std::function<void()>;

class PipelineExecutors
{
public:
    PipelineExecutors()
    {
        cleaner = std::make_unique<ThreadFromGlobalPool>(&PipelineExecutors::cleanerThread, this);
    }

    void execute(QueryPipeline & pipeline, onFinishCallBack call_back);
    void cleanerThread();

    struct Data
    {
        PipelineExecutorPtr executor;
        std::exception_ptr exception;
        std::atomic_bool is_finished = false;
        std::atomic_bool has_exception = false;
        ThreadFromGlobalPool thread;
        onFinishCallBack finish_call_back;

        ~Data()
        {
            if (thread.joinable())
                thread.join();

            finish_call_back();
        }

        void rethrowExceptionIfHas()
        {
            if (has_exception)
            {
                has_exception = false;
                std::rethrow_exception(exception);
            }
        }
    };

private:
    std::list<std::unique_ptr<Data>> executors;

    std::mutex data_queue_mutex;

    std::unique_ptr<ThreadFromGlobalPool> cleaner;

    bool shutdown = false;
};

}
