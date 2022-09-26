#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <QueryPipeline/RemoteQueryExecutorReadContext.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <DataTypes/DataTypeAggregateFunction.h>

namespace DB
{

RemoteSource::RemoteSource(RemoteQueryExecutorPtr executor, bool add_aggregation_info_, bool async_read_)
    : SourceWithProgress(executor->getHeader(), false)
    , add_aggregation_info(add_aggregation_info_), query_executor(std::move(executor))
    , async_read(async_read_)
{
    /// Add AggregatedChunkInfo if we expect DataTypeAggregateFunction as a result.
    const auto & sample = getPort().getHeader();
    for (auto & type : sample.getDataTypes())
        if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
            add_aggregation_info = true;
}

RemoteSource::~RemoteSource() = default;

ISource::Status RemoteSource::prepare()
{
    /// Check if query was cancelled before returning Async status. Otherwise it may lead to infinite loop.
    if (was_query_canceled)
    {
        getPort().finish();
        return Status::Finished;
    }

    if (is_async_state)
        return Status::Async;

    Status status = SourceWithProgress::prepare();
    /// To avoid resetting the connection (because of "unfinished" query) in the
    /// RemoteQueryExecutor it should be finished explicitly.
    if (status == Status::Finished)
    {
        query_executor->finish(&read_context);
        is_async_state = false;
    }
    return status;
}

std::optional<Chunk> RemoteSource::tryGenerate()
{
    /// onCancel() will do the cancel if the query was sent.
    if (was_query_canceled)
        return {};

    if (!was_query_sent)
    {
        /// Progress method will be called on Progress packet.
        query_executor->setProgressCallback([this](const Progress & value) { progress(value); });

        /// Get rows_before_limit result for remote query from ProfileInfo packet.
        query_executor->setProfileInfoCallback([this](const ProfileInfo & info)
        {
            if (rows_before_limit && info.hasAppliedLimit())
                rows_before_limit->set(info.getRowsBeforeLimit());
        });

        query_executor->sendQuery();

        was_query_sent = true;
    }

    Block block;

    if (async_read)
    {
        auto res = query_executor->read(read_context);
        if (std::holds_alternative<int>(res))
        {
            fd = std::get<int>(res);
            is_async_state = true;
            return Chunk();
        }

        is_async_state = false;

        block = std::get<Block>(std::move(res));
    }
    else
        block = query_executor->read();

    if (!block)
    {
        query_executor->finish(&read_context);
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
    was_query_canceled = true;
    query_executor->cancel(&read_context);
    // is_async_state = false;
}

void RemoteSource::onUpdatePorts()
{
    if (getPort().isFinished())
    {
        was_query_canceled = true;
        query_executor->finish(&read_context);
        // is_async_state = false;
    }
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
    bool add_aggregation_info, bool add_totals, bool add_extremes, bool async_read)
{
    Pipe pipe(std::make_shared<RemoteSource>(query_executor, add_aggregation_info, async_read));

    if (add_totals)
        pipe.addTotalsSource(std::make_shared<RemoteTotalsSource>(query_executor));

    if (add_extremes)
        pipe.addExtremesSource(std::make_shared<RemoteExtremesSource>(query_executor));

    return pipe;
}

}
