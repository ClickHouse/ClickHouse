#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPipeline.h>
#include <Common/setThreadName.h>
#include <common/scope_guard_safe.h>
#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

CompletedPipelineExecutor::CompletedPipelineExecutor(QueryPipeline & pipeline_) : pipeline(pipeline_)
{
    if (!pipeline.completed())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for CompletedPipelineExecutor must be completed");
}

void CompletedPipelineExecutor::setCancelCallback(std::function<bool()> is_cancelled, size_t interactive_timeout_ms_)
{
    is_cancelled_callback = is_cancelled;
    interactive_timeout_ms = interactive_timeout_ms_;
}

void CompletedPipelineExecutor::execute()
{
    PipelineExecutor executor(pipeline.processors, pipeline.process_list_element);

    if (interactive_timeout_ms)
    {
        bool is_done = false;
        std::mutex mutex;
        std::exception_ptr exception;
        auto thread_group = CurrentThread::getGroup();

        ThreadFromGlobalPool thread([&]()
        {
            setThreadName("QueryPipelineEx");

            try
            {
                if (thread_group)
                    CurrentThread::attachTo(thread_group);

                SCOPE_EXIT_SAFE(
                    if (thread_group)
                        CurrentThread::detachQueryIfNotDetached();
                );

                executor.execute(pipeline.getNumThreads());
            }
            catch (...)
            {
                exception = std::current_exception();
            }
            std::lock_guard lock(mutex);
            is_done = true;
        });

        {
            std::condition_variable condvar;
            std::unique_lock lock(mutex);
            while (!is_done)
            {
                condvar.wait_for(lock, std::chrono::milliseconds(interactive_timeout_ms), [&]() { return is_done; });

                if (is_cancelled_callback())
                {
                    executor.cancel();
                    is_done = true;
                }
            }
        }
        thread.join();
        if (exception)
            std::rethrow_exception(exception);
    }
    else
        executor.execute(pipeline.getNumThreads());
}

}
