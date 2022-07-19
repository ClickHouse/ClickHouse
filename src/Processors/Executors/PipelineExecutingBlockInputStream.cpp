#include <Processors/Executors/PipelineExecutingBlockInputStream.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PipelineExecutingBlockInputStream::PipelineExecutingBlockInputStream(QueryPipeline pipeline_)
    : pipeline(std::make_unique<QueryPipeline>(std::move(pipeline_)))
{
}

PipelineExecutingBlockInputStream::~PipelineExecutingBlockInputStream() = default;

Block PipelineExecutingBlockInputStream::getHeader() const
{
    if (executor)
        return executor->getHeader();

    if (async_executor)
        return async_executor->getHeader();

    return pipeline->getHeader();
}

void PipelineExecutingBlockInputStream::createExecutor()
{
    if (pipeline->getNumThreads() > 1)
        async_executor = std::make_unique<PullingAsyncPipelineExecutor>(*pipeline);
    else
        executor = std::make_unique<PullingPipelineExecutor>(*pipeline);

    is_execution_started = true;
}

void PipelineExecutingBlockInputStream::readPrefixImpl()
{
    createExecutor();
}

Block PipelineExecutingBlockInputStream::readImpl()
{
    if (!is_execution_started)
        createExecutor();

    Block block;
    bool can_continue = true;
    while (can_continue)
    {
        if (executor)
            can_continue = executor->pull(block);
        else
            can_continue = async_executor->pull(block);

        if (block)
            return block;
    }

    totals = executor ? executor->getTotalsBlock()
                      : async_executor->getTotalsBlock();

    extremes = executor ? executor->getExtremesBlock()
                        : async_executor->getExtremesBlock();

    return {};
}

inline static void throwIfExecutionStarted(bool is_execution_started, const char * method)
{
    if (is_execution_started)
        throw Exception(String("Cannot call ") + method +
                        " for PipelineExecutingBlockInputStream because execution was started",
                        ErrorCodes::LOGICAL_ERROR);
}

void PipelineExecutingBlockInputStream::cancel(bool kill)
{
    IBlockInputStream::cancel(kill);

    if (is_execution_started)
    {
        executor ? executor->cancel()
                 : async_executor->cancel();
    }
}

void PipelineExecutingBlockInputStream::setProgressCallback(const ProgressCallback & callback)
{
    throwIfExecutionStarted(is_execution_started, "setProgressCallback");
    pipeline->setProgressCallback(callback);
}

void PipelineExecutingBlockInputStream::setProcessListElement(QueryStatus * elem)
{
    throwIfExecutionStarted(is_execution_started, "setProcessListElement");
    IBlockInputStream::setProcessListElement(elem);
    pipeline->setProcessListElement(elem);
}

void PipelineExecutingBlockInputStream::setLimits(const StreamLocalLimits & limits_)
{
    throwIfExecutionStarted(is_execution_started, "setLimits");

    if (limits_.mode == LimitsMode::LIMITS_TOTAL)
        throw Exception("Total limits are not supported by PipelineExecutingBlockInputStream",
                        ErrorCodes::LOGICAL_ERROR);

    /// Local limits may be checked by IBlockInputStream itself.
    IBlockInputStream::setLimits(limits_);
}

void PipelineExecutingBlockInputStream::setQuota(const std::shared_ptr<const EnabledQuota> &)
{
    throw Exception("Quota is not supported by PipelineExecutingBlockInputStream",
                    ErrorCodes::LOGICAL_ERROR);
}

}
