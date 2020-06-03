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


RemoteTotalsSource::RemoteTotalsSource(Block header) : ISource(std::move(header)) {}
RemoteTotalsSource::~RemoteTotalsSource() = default;

Chunk RemoteTotalsSource::generate()
{
    /// Check use_count instead of comparing with nullptr just in case.
    /// setQueryExecutor() may be called from other thread, but there shouldn't be any race,
    /// because totals end extremes are always read after main data.
    if (query_executor.use_count())
    {
        if (auto block = query_executor->getTotals())
        {
            UInt64 num_rows = block.rows();
            return Chunk(block.getColumns(), num_rows);
        }
    }

    return {};
}


RemoteExtremesSource::RemoteExtremesSource(Block header) : ISource(std::move(header)) {}
RemoteExtremesSource::~RemoteExtremesSource() = default;

Chunk RemoteExtremesSource::generate()
{
    if (query_executor.use_count())
    {
        if (auto block = query_executor->getExtremes())
        {
            UInt64 num_rows = block.rows();
            return Chunk(block.getColumns(), num_rows);
        }
    }

    return {};
}

}
