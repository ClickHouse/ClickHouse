#include <QueryCoordination/PipelineExecutors.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Common/scope_guard_safe.h>

namespace DB
{

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

    data->executor = std::make_shared<PipelineExecutor>(pipeline.processors, pipeline.process_list_element);
    data->executor->setReadProgressCallback(pipeline.getReadProgressCallback());

    auto func = [&, data_ptr = data.get(), thread_group = CurrentThread::getGroup()]()
    {
        threadFunction(*data_ptr, thread_group, pipeline.getNumThreads());
    };

    data->thread = ThreadFromGlobalPool(std::move(func));

    std::lock_guard lock(data_queue_mutex);
    executors.emplace_back(std::move(data));
}

void PipelineExecutors::cleanerThread()
{
    while (!shutdown)
    {
        {
            std::lock_guard lock(data_queue_mutex);
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
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

}
