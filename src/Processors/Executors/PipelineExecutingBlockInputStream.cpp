#include <Processors/Executors/PipelineExecutingBlockInputStream.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>

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
    return executor ? executor->getHeader()
                    : pipeline->getHeader();
}

void PipelineExecutingBlockInputStream::readPrefixImpl()
{
    executor = std::make_unique<PullingPipelineExecutor>(*pipeline);
}

Block PipelineExecutingBlockInputStream::readImpl()
{
    if (!executor)
        executor = std::make_unique<PullingPipelineExecutor>(*pipeline);

    Block block;
    while (executor->pull(block))
    {
        if (block)
            return block;
    }

    return {};
}

inline static void throwIfExecutionStarted(bool is_execution_started, const char * method)
{
    if (is_execution_started)
        throw Exception(String("Cannot call ") + method +
                        " for PipelineExecutingBlockInputStream because execution was started",
                        ErrorCodes::LOGICAL_ERROR);
}

inline static void throwIfExecutionNotStarted(bool is_execution_started, const char * method)
{
    if (!is_execution_started)
        throw Exception(String("Cannot call ") + method +
                        " for PipelineExecutingBlockInputStream because execution was not started",
                        ErrorCodes::LOGICAL_ERROR);
}

void PipelineExecutingBlockInputStream::cancel(bool kill)
{
    throwIfExecutionNotStarted(executor != nullptr, "cancel");
    IBlockInputStream::cancel(kill);
    executor->cancel();
}


void PipelineExecutingBlockInputStream::setProgressCallback(const ProgressCallback & callback)
{
    throwIfExecutionStarted(executor != nullptr, "setProgressCallback");
    pipeline->setProgressCallback(callback);
}

void PipelineExecutingBlockInputStream::setProcessListElement(QueryStatus * elem)
{
    throwIfExecutionStarted(executor != nullptr, "setProcessListElement");
    IBlockInputStream::setProcessListElement(elem);
    pipeline->setProcessListElement(elem);
}

void PipelineExecutingBlockInputStream::setLimits(const IBlockInputStream::LocalLimits & limits_)
{
    throwIfExecutionStarted(executor != nullptr, "setLimits");

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

void PipelineExecutingBlockInputStream::addTotalRowsApprox(size_t)
{
    throw Exception("Progress is not supported by PipelineExecutingBlockInputStream",
                    ErrorCodes::LOGICAL_ERROR);
}


}
