#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/PullingOutputFormat.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

PullingPipelineExecutor::PullingPipelineExecutor(QueryPipeline & pipeline_) : pipeline(pipeline_)
{
    pulling_format = std::make_shared<PullingOutputFormat>(pipeline.getHeader(), has_data_flag);
    pipeline.setOutputFormat(pulling_format);
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

bool PullingPipelineExecutor::pull(Chunk & chunk)
{
    if (!executor)
        executor = pipeline.execute();

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

    if (auto chunk_info = chunk.getChunkInfo())
    {
        if (const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(chunk_info.get()))
        {
            block.info.bucket_num = agg_info->bucket_num;
            block.info.is_overflows = agg_info->is_overflows;
        }
    }

    return true;
}

void PullingPipelineExecutor::cancel()
{
    /// Cancel execution if it wasn't finished.
    if (executor)
        executor->cancel();

    /// Read all data and finish execution.
    Chunk chunk;
    while (pull(chunk));
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

BlockStreamProfileInfo & PullingPipelineExecutor::getProfileInfo()
{
    return pulling_format->getProfileInfo();
}

}
