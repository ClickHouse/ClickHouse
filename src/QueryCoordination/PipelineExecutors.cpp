#include <QueryCoordination/PipelineExecutors.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Common/scope_guard_safe.h>

namespace DB
{

struct PipelineExecutors::Data
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

    void rethrowExceptionIfHas()
    {
        if (has_exception)
        {
            has_exception = false;
            std::rethrow_exception(exception);
        }
    }
};

static void threadFunction(PipelineExecutors::Data & data, ThreadGroupPtr thread_group, size_t num_threads)
{
    SCOPE_EXIT_SAFE(
        if (thread_group)
            CurrentThread::detachFromGroupIfNotDetached();
    );
    setThreadName("FragmentPipeEx");

    try
    {
        if (thread_group)
            CurrentThread::attachToGroup(thread_group);

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


void PipelineExecutors::execute(QueryPipeline & pipeline)
{
    std::unique_ptr<Data> data = std::make_unique<Data>();

    /// TODO lock
    executors.emplace_back(std::move(data));

    data->executor = std::make_shared<PipelineExecutor>(pipeline.processors, pipeline.process_list_element);
    data->executor->setReadProgressCallback(pipeline.getReadProgressCallback());

    auto func = [&, thread_group = CurrentThread::getGroup()]()
    {
        threadFunction(*data, thread_group, pipeline.getNumThreads());
    };

    data->thread = ThreadFromGlobalPool(std::move(func));
}

void PipelineExecutors::cleanerThread()
{
    // TODO lock
    while (!shutdown)
    {
        for (auto it = executors.begin(); it != executors.end();)
        {
            if ((*it)->is_finished)
            {
                it = executors.erase(it);
            }
            else
            {
                it++;
            }
        }

        /// TODO sleep
    }
}

}
