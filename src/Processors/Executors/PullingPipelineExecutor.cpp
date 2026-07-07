#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Formats/PullingOutputFormat.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Sources/NullSource.h>
#include <Interpreters/ProcessList.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PullingPipelineExecutor::PullingPipelineExecutor(QueryPipeline & pipeline_) : pipeline(pipeline_)
{
    if (!pipeline.pulling())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for PullingPipelineExecutor must be pulling");

    pulling_format = std::make_shared<PullingOutputFormat>(pipeline.output->getSharedHeader(), has_data_flag);
    pipeline.complete(pulling_format);
}

PullingPipelineExecutor::~PullingPipelineExecutor()
{
    try
    {
        cancel();
    }
    catch (...)
    {
        tryLogCurrentException("PullingPipelineExecutor");
    }
}

const Block & PullingPipelineExecutor::getHeader() const
{
    return pulling_format->getPort(IOutputFormat::PortKind::Main).getHeader();
}

const SharedHeader & PullingPipelineExecutor::getSharedHeader() const
{
    return pulling_format->getPort(IOutputFormat::PortKind::Main).getSharedHeader();
}

bool PullingPipelineExecutor::pull(Chunk & chunk)
{
    if (!executor)
    {
        executor = std::make_shared<PipelineExecutor>(pipeline.processors, pipeline.process_list_element);
        executor->setReadProgressCallback(pipeline.getReadProgressCallback());
    }

    if (!executor->checkTimeLimitSoft())
        return false;

    if (!executor->executeStep(&has_data_flag))
        return false;

    chunk = pulling_format->getChunk();
    return true;
}

bool PullingPipelineExecutor::pull(Block & block)
{
    Chunk chunk;

    if (!pull(chunk))
        return false;

    if (!chunk)
    {
        /// In case if timeout exceeded.
        block.clear();
        return true;
    }

    block = pulling_format->getPort(IOutputFormat::PortKind::Main).getHeader().cloneWithColumns(chunk.detachColumns());
    if (auto agg_info = chunk.getChunkInfos().get<AggregatedChunkInfo>())
    {
        block.info.bucket_num = agg_info->bucket_num;
        block.info.is_overflows = agg_info->is_overflows;
        block.info.out_of_order_buckets = agg_info->out_of_order_buckets;
    }

    return true;
}

void PullingPipelineExecutor::cancel()
{
    /// Cancel execution if it wasn't finished.
    if (executor)
        executor->cancel();
}

Chunk PullingPipelineExecutor::getTotals()
{
    return pulling_format->getTotals();
}

Chunk PullingPipelineExecutor::getExtremes()
{
    return pulling_format->getExtremes();
}

Block PullingPipelineExecutor::getTotalsBlock()
{
    auto totals = getTotals();

    if (totals.empty())
        return {};

    const auto & header = pulling_format->getPort(IOutputFormat::PortKind::Totals).getHeader();
    return header.cloneWithColumns(totals.detachColumns());
}

Block PullingPipelineExecutor::getExtremesBlock()
{
    auto extremes = getExtremes();

    if (extremes.empty())
        return {};

    const auto & header = pulling_format->getPort(IOutputFormat::PortKind::Extremes).getHeader();
    return header.cloneWithColumns(extremes.detachColumns());
}

ProfileInfo & PullingPipelineExecutor::getProfileInfo()
{
    return pulling_format->getProfileInfo();
}

PipelineExecutor::ExecutionStatus PullingPipelineExecutor::getExecutionStatus() const
{
    if (!executor)
        return PipelineExecutor::ExecutionStatus::NotStarted;

    auto status = executor->getExecutionStatus();

    if (!pipeline.process_list_element)
        return status;

    /// `ExecutorHolder::cancel` forwards to the no-arg `PipelineExecutor::cancel`, which always reports
    /// `CancelledByUser` regardless of the `CancelReason` recorded by `cancelQuery`. Likewise,
    /// `PipelineExecutor::cancel(CancelledByTimeout)` only transitions `Executing` → `CancelledByTimeout`,
    /// so a timeout that fires before the first `executeStep` leaves the executor at `NotStarted`.
    /// Consult the `QueryStatus` to recover the actual reason in both cases.
    if (pipeline.process_list_element->isKilled()
        && (status == PipelineExecutor::ExecutionStatus::NotStarted
            || status == PipelineExecutor::ExecutionStatus::CancelledByUser))
    {
        if (pipeline.process_list_element->getCancelReason() == CancelReason::TIMEOUT)
            return PipelineExecutor::ExecutionStatus::CancelledByTimeout;
        return PipelineExecutor::ExecutionStatus::CancelledByUser;
    }

    /// Soft timeout can also fire without going through `cancelQuery` (`overflow_mode='break'`).
    if (status == PipelineExecutor::ExecutionStatus::NotStarted
        && !pipeline.process_list_element->checkTimeLimitSoft())
        return PipelineExecutor::ExecutionStatus::CancelledByTimeout;

    return status;
}

}
