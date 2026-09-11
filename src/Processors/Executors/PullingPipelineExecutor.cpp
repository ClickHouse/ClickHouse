#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/Runtime/PipelineExecutor.h>
#include <Processors/Formats/PullingOutputFormat.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Sources/NullSource.h>

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

    const size_t num_rows = chunk.getNumRows();
    block = pulling_format->getPort(IOutputFormat::PortKind::Main).getHeader().cloneWithColumns(chunk.detachColumns());

    /// A block takes its number of rows from its columns, so without them the count has to be kept aside.
    if (block.columns() == 0)
        block.info.num_rows_without_columns = num_rows;

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

}
