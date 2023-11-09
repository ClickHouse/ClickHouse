#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Formats/LazyOutputFormat.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryCoordination/Pipelines/CompletedPipelinesExecutor.h>
#include <QueryCoordination/Pipelines/RemotePipelinesManager.h>
#include <QueryCoordination/QueryCoordinationExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

struct QueryCoordinationExecutor::Data
{
    PipelineExecutorPtr executor;
    LazyOutputFormat * lazy_format = nullptr;
    std::atomic_bool is_finished = false;
    ThreadFromGlobalPool thread;
    Poco::Event finish_event;

    ~Data()
    {
        if (thread.joinable())
            thread.join();
    }
};

QueryCoordinationExecutor::QueryCoordinationExecutor(
    std::shared_ptr<PullingAsyncPipelineExecutor> pulling_async_pipeline_executor_,
    std::shared_ptr<CompletedPipelinesExecutor> completed_pipelines_executor_,
    std::shared_ptr<RemotePipelinesManager> remote_pipelines_manager_)
    : log(&Poco::Logger::get("QueryCoordinationExecutor"))
    , pulling_async_pipeline_executor(pulling_async_pipeline_executor_)
    , completed_pipelines_executor(completed_pipelines_executor_)
    , remote_pipelines_manager(remote_pipelines_manager_)
{
}

QueryCoordinationExecutor::~QueryCoordinationExecutor()
{
    try
    {
        cancel();
    }
    catch (...)
    {
        tryLogCurrentException("QueryCoordinationExecutor");
    }
}

const Block & QueryCoordinationExecutor::getHeader() const
{
    return pulling_async_pipeline_executor->getHeader();
}


bool QueryCoordinationExecutor::pull(Block & block, uint64_t milliseconds)
{
    if (!begin_execute)
    {
        auto exception_callback = [this](std::exception_ptr exception_) { setException(exception_); };

        if (completed_pipelines_executor)
        {
            completed_pipelines_executor->setExceptionCallback(exception_callback);
            completed_pipelines_executor->asyncExecute();
        }

        if (remote_pipelines_manager)
        {
            remote_pipelines_manager->setExceptionCallback(exception_callback);
            remote_pipelines_manager->asyncReceiveReporter();
        }
        begin_execute = true;
    }

    rethrowExceptionIfHas();

    bool is_execution_finished = !pulling_async_pipeline_executor->pull(block, milliseconds);

    if (is_execution_finished)
    {
        if (completed_pipelines_executor)
            completed_pipelines_executor->waitFinish();

        if (remote_pipelines_manager)
            remote_pipelines_manager->waitFinish();
    }

    return !is_execution_finished;
}

void QueryCoordinationExecutor::cancel()
{
    LOG_DEBUG(log, "cancel");

    /// Cancel execution if it wasn't finished.
    cancelWithExceptionHandling(
        [&]()
        {
            if (pulling_async_pipeline_executor)
                pulling_async_pipeline_executor->cancel();
        });

    cancelWithExceptionHandling(
        [&]()
        {
            if (completed_pipelines_executor)
                completed_pipelines_executor->cancel();
        });

    cancelWithExceptionHandling(
        [&]()
        {
            if (remote_pipelines_manager)
                remote_pipelines_manager->cancel();
        });

    LOG_DEBUG(log, "cancelled");

    /// Rethrow exception to not swallow it in destructor.
    rethrowExceptionIfHas();
}

void QueryCoordinationExecutor::cancelReading()
{
    //    if (!data)
    //        return;
    //
    //    /// Stop reading from source if pipeline wasn't finished.
    //    cancelWithExceptionHandling([&]()
    //    {
    //        if (!data->is_finished && data->executor)
    //            data->executor->cancelReading();
    //    });
}

void QueryCoordinationExecutor::cancelWithExceptionHandling(CancelFunc && cancel_func)
{
    try
    {
        cancel_func();
    }
    catch (...)
    {
        /// Store exception only of during query execution there was no
        /// exception, since only one exception can be re-thrown.
        setException(std::current_exception());
    }
}

Chunk QueryCoordinationExecutor::getTotals()
{
    return pulling_async_pipeline_executor->getTotals();
}

Chunk QueryCoordinationExecutor::getExtremes()
{
    return pulling_async_pipeline_executor->getExtremes();
}

Block QueryCoordinationExecutor::getTotalsBlock()
{
    return pulling_async_pipeline_executor->getTotalsBlock();
}

Block QueryCoordinationExecutor::getExtremesBlock()
{
    return pulling_async_pipeline_executor->getExtremesBlock();
}

ProfileInfo & QueryCoordinationExecutor::getProfileInfo()
{
    return pulling_async_pipeline_executor->getProfileInfo();
}

void QueryCoordinationExecutor::setException(std::exception_ptr exception_)
{
    std::lock_guard lock(mutex);
    if (!has_exception)
    {
        has_exception = true;
        exception = exception_;
    }
}

void QueryCoordinationExecutor::rethrowExceptionIfHas()
{
    std::lock_guard lock(mutex);
    if (has_exception)
    {
        has_exception = false;
        std::rethrow_exception(exception);
    }
}

}
