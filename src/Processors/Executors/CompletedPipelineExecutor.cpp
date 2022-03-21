#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Poco/Event.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct CompletedPipelineExecutor::Data
{
    PipelineExecutorPtr executor;
    std::exception_ptr exception;
    std::atomic_bool is_finished = false;
    std::atomic_bool has_exception = false;
    ThreadFromGlobalPool thread;
    Poco::Event finish_event;

    ~Data()
    {
        if (thread.joinable())
            thread.join();
    }
};

static void threadFunction(CompletedPipelineExecutor::Data & data, ThreadGroupStatusPtr thread_group, size_t num_threads)
{
    setThreadName("QueryPipelineEx");

    try
    {
        if (thread_group)
            CurrentThread::attachTo(thread_group);

        data.executor->execute(num_threads);
    }
    catch (...)
    {
        data.exception = std::current_exception();
        data.has_exception = true;
    }

    data.is_finished = true;
    data.finish_event.set();
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
        data = std::make_unique<Data>();
        data->executor = std::make_shared<PipelineExecutor>(pipeline.processors, pipeline.process_list_element);

        auto func = [&, thread_group = CurrentThread::getGroup()]()
        {
            threadFunction(*data, thread_group, pipeline.getNumThreads());
        };

        data->thread = ThreadFromGlobalPool(std::move(func));

        while (!data->is_finished)
        {
            if (data->finish_event.tryWait(interactive_timeout_ms))
                break;

            if (is_cancelled_callback())
                data->executor->cancel();
        }

        if (data->has_exception)
            std::rethrow_exception(data->exception);
    }
    else
        executor.execute(pipeline.getNumThreads());
}

CompletedPipelineExecutor::~CompletedPipelineExecutor()
{
    try
    {
        if (data && data->executor)
            data->executor->cancel();
    }
    catch (...)
    {
        tryLogCurrentException("PullingAsyncPipelineExecutor");
    }
}

}
