#include <Processors/Sources/RemoteSource.h>
#include <DataStreams/RemoteQueryExecutor.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <DataTypes/DataTypeAggregateFunction.h>

namespace DB
{

RemoteSource::RemoteSource(RemoteQueryExecutorPtr executor, bool add_aggregation_info_)
    : SourceWithProgress(executor->getHeader(), false)
    , add_aggregation_info(add_aggregation_info_), query_executor(std::move(executor))
{
    /// Add AggregatedChunkInfo if we expect DataTypeAggregateFunction as a result.
    const auto & sample = getPort().getHeader();
    for (auto & type : sample.getDataTypes())
        if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
            add_aggregation_info = true;
}

RemoteSource::~RemoteSource() = default;

Chunk RemoteSource::generate()
{
    if (!was_query_sent)
    {
        /// Progress method will be called on Progress packet.
        query_executor->setProgressCallback([this](const Progress & value) { progress(value); });

        /// Get rows_before_limit result for remote query from ProfileInfo packet.
        query_executor->setProfileInfoCallback([this](const BlockStreamProfileInfo & info)
        {
            if (rows_before_limit && info.hasAppliedLimit())
                rows_before_limit->set(info.getRowsBeforeLimit());
        });

        query_executor->sendQuery();

        was_query_sent = true;
    }

    auto block = query_executor->read();

    if (!block)
    {
        query_executor->finish();
        return {};
    }

    UInt64 num_rows = block.rows();
    Chunk chunk(block.getColumns(), num_rows);

    if (add_aggregation_info)
    {
        auto info = std::make_shared<AggregatedChunkInfo>();
        info->bucket_num = block.info.bucket_num;
        info->is_overflows = block.info.is_overflows;
        chunk.setChunkInfo(std::move(info));
    }

    return chunk;
}

void RemoteSource::onCancel()
{
    query_executor->cancel();
}


RemoteTotalsSource::RemoteTotalsSource(RemoteQueryExecutorPtr executor)
    : ISource(executor->getHeader())
    , query_executor(std::move(executor))
{
}

RemoteTotalsSource::~RemoteTotalsSource() = default;

Chunk RemoteTotalsSource::generate()
{
    if (auto block = query_executor->getTotals())
    {
        UInt64 num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

    return {};
}


RemoteExtremesSource::RemoteExtremesSource(RemoteQueryExecutorPtr executor)
    : ISource(executor->getHeader())
    , query_executor(std::move(executor))
{
}

RemoteExtremesSource::~RemoteExtremesSource() = default;

Chunk RemoteExtremesSource::generate()
{
    if (auto block = query_executor->getExtremes())
    {
        UInt64 num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

    return {};
}


Pipe createRemoteSourcePipe(
    RemoteQueryExecutorPtr query_executor,
    bool add_aggregation_info, bool add_totals, bool add_extremes)
{
    Pipe pipe(std::make_shared<RemoteSource>(query_executor, add_aggregation_info));

    if (add_totals)
    {
        auto totals_source = std::make_shared<RemoteTotalsSource>(query_executor);
        pipe.setTotalsPort(&totals_source->getPort());
        pipe.addProcessors({std::move(totals_source)});
    }

    if (add_extremes)
    {
        auto extremes_source = std::make_shared<RemoteExtremesSource>(query_executor);
        pipe.setExtremesPort(&extremes_source->getPort());
        pipe.addProcessors({std::move(extremes_source)});
    }

    return pipe;
}

}
