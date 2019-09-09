#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataStreams/RemoteBlockInputStream.h>

namespace DB
{

SourceFromInputStream::SourceFromInputStream(BlockInputStreamPtr stream_, bool force_add_aggregating_info_)
    : ISource(stream_->getHeader())
    , force_add_aggregating_info(force_add_aggregating_info_)
    , stream(std::move(stream_))
{
    auto & sample = getPort().getHeader();
    for (auto & type : sample.getDataTypes())
        if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
            has_aggregate_functions = true;
}

void SourceFromInputStream::addTotalsPort()
{
    if (has_totals_port)
        throw Exception("Totals port was already added for SourceFromInputStream.", ErrorCodes::LOGICAL_ERROR);

    outputs.emplace_back(outputs.front().getHeader(), this);
    has_totals_port = true;
}

IProcessor::Status SourceFromInputStream::prepare()
{
    auto status = ISource::prepare();

    if (status == Status::Finished)
    {
        is_generating_finished = true;

        /// Read postfix and get totals if needed.
        if (!is_stream_finished)
            return Status::Ready;

        if (has_totals_port)
        {
            auto & totals_out = outputs.back();

            if (totals_out.isFinished())
                return Status::Finished;

            if (has_totals)
            {
                if (!totals_out.canPush())
                    return Status::PortFull;

                totals_out.push(std::move(totals));
                has_totals = false;
            }

            totals_out.finish();
        }
    }

    return status;
}

void SourceFromInputStream::work()
{
    if (!is_generating_finished)
    {
        try
        {
            ISource::work();
        }
        catch (...)
        {
            /// Won't read suffix in case of exception.
            is_stream_finished = true;
            throw;
        }

        return;
    }

    if (is_stream_finished)
        return;

    /// Don't cancel for RemoteBlockInputStream (otherwise readSuffix can stack)
    if (!typeid_cast<const RemoteBlockInputStream *>(stream.get()))
        stream->cancel(false);

    stream->readSuffix();

    if (auto totals_block = stream->getTotals())
    {
        totals.setColumns(totals_block.getColumns(), 1);
        has_totals = true;
    }

    is_stream_finished = true;
}

Chunk SourceFromInputStream::generate()
{
    if (is_stream_finished)
        return {};

    if (!is_stream_started)
    {
        stream->readPrefix();
        is_stream_started = true;
    }

    auto block = stream->read();
    if (!block)
    {
        stream->readSuffix();

        if (auto totals_block = stream->getTotals())
        {
            totals.setColumns(totals_block.getColumns(), 1);
            has_totals = true;
        }

        is_stream_finished = true;
        return {};
    }

    assertBlocksHaveEqualStructure(getPort().getHeader(), block, "SourceFromInputStream");

    UInt64 num_rows = block.rows();
    Chunk chunk(block.getColumns(), num_rows);

    if (force_add_aggregating_info || has_aggregate_functions)
    {
        auto info = std::make_shared<AggregatedChunkInfo>();
        info->bucket_num = block.info.bucket_num;
        info->is_overflows = block.info.is_overflows;
        chunk.setChunkInfo(std::move(info));
    }

    return chunk;
}

}
