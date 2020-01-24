#pragma once
#include <Processors/Executors/PipelineExecutor.h>

namespace DB
{

class AsyncExecutor
{
public:
    explicit AsyncExecutor(PipelineExecutorPtr executor_, size_t num_threads_, ThreadGroupStatusPtr thread_group_)
        : executor(std::move(executor_)), num_threads(num_threads_), thread_group(std::move(thread_group_)) {}

    void cancel() { executor->cancel(); }

    bool hasException() const { return has_exception; }

    void rethrowIfException()
    {
        if (has_exception)
        {
            has_exception = false;
            std::rethrow_exception(exception);
        }
    }

    ~AsyncExecutor()
    {
        if (has_exception)
            tryLogException(exception, "AsyncExecutor", "Unhandled exception");

        if (thread.joinable())
            thread.join();
    }

private:
    PipelineExecutorPtr executor;
    size_t num_threads;
    ThreadGroupStatusPtr thread_group;

    std::atomic_bool has_exception = false;
    std::exception_ptr exception;

    ThreadFromGlobalPool thread{&AsyncExecutor::run, this};

    void run()
    {
        if (thread_group)
            CurrentThread::attachTo(thread_group);

        SCOPE_EXIT(
                if (thread_group)
                    CurrentThread::detachQueryIfNotDetached();
        );

        CurrentMetrics::Increment query_thread_metric_increment{CurrentMetrics::QueryThread};
        setThreadName("QueryPipelineEx");

        try
        {
            executor->execute(num_threads);
        }
        catch (...)
        {
            has_exception = true;
            exception = std::current_exception();
        }
    }
};

using AsyncExecutorPtr = std::unique_ptr<AsyncExecutor>;

}
