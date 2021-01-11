#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataStreams/RemoteBlockInputStream.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SourceFromInputStream::SourceFromInputStream(BlockInputStreamPtr stream_, bool force_add_aggregating_info_)
    : ISourceWithProgress(stream_->getHeader())
    , force_add_aggregating_info(force_add_aggregating_info_)
    , stream(std::move(stream_))
{
    init();
}

void SourceFromInputStream::init()
{
    const auto & sample = getPort().getHeader();
    for (auto & type : sample.getDataTypes())
        if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
            has_aggregate_functions = true;
}

void SourceFromInputStream::addTotalsPort()
{
    if (totals_port)
        throw Exception("Totals port was already added for SourceFromInputStream.", ErrorCodes::LOGICAL_ERROR);

    outputs.emplace_back(outputs.front().getHeader(), this);
    totals_port = &outputs.back();
}

void SourceFromInputStream::addExtremesPort()
{
    if (extremes_port)
        throw Exception("Extremes port was already added for SourceFromInputStream.", ErrorCodes::LOGICAL_ERROR);

    outputs.emplace_back(outputs.front().getHeader(), this);
    extremes_port = &outputs.back();
}

IProcessor::Status SourceFromInputStream::prepare()
{
    auto status = ISource::prepare();

    if (status == Status::Finished)
    {
        is_generating_finished = true;

        /// Read postfix and get totals if needed.
        if (!is_stream_finished && !isCancelled())
            return Status::Ready;

        if (totals_port && !totals_port->isFinished())
        {
            if (has_totals)
            {
                if (!totals_port->canPush())
                    return Status::PortFull;

                totals_port->push(std::move(totals));
                has_totals = false;
            }

            totals_port->finish();
        }

        if (extremes_port && !extremes_port->isFinished())
        {
            if (has_extremes)
            {
                if (!extremes_port->canPush())
                    return Status::PortFull;

                extremes_port->push(std::move(extremes));
                has_extremes = false;
            }

            extremes_port->finish();
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

    if (rows_before_limit)
    {
        const auto & info = stream->getProfileInfo();
        if (info.hasAppliedLimit())
            rows_before_limit->add(info.getRowsBeforeLimit());
    }

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
    if (!block && !isCancelled())
    {
        if (rows_before_limit)
        {
            const auto & info = stream->getProfileInfo();
            if (info.hasAppliedLimit())
                rows_before_limit->add(info.getRowsBeforeLimit());
        }

        stream->readSuffix();

        if (auto totals_block = stream->getTotals())
        {
            if (totals_block.rows() > 0) /// Sometimes we can get empty totals. Skip it.
            {
                totals.setColumns(totals_block.getColumns(), totals_block.rows());
                has_totals = true;
            }
        }

        if (auto extremes_block = stream->getExtremes())
        {
            if (extremes_block.rows() > 0) /// Sometimes we can get empty extremes. Skip it.
            {
                extremes.setColumns(extremes_block.getColumns(), extremes_block.rows());
                has_extremes = true;
            }
        }

        is_stream_finished = true;
        return {};
    }

    if (isCancelled())
        return {};

#ifndef NDEBUG
    assertBlocksHaveEqualStructure(getPort().getHeader(), block, "SourceFromInputStream");
#endif

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
