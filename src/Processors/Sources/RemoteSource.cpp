#include <variant>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <QueryPipeline/RemoteQueryExecutorReadContext.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <DataTypes/DataTypeAggregateFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

RemoteSource::RemoteSource(RemoteQueryExecutorPtr executor, bool add_aggregation_info_, bool async_read_, bool async_query_sending_)
    : ISource(executor->getHeader(), false)
    , add_aggregation_info(add_aggregation_info_), query_executor(std::move(executor))
    , async_read(async_read_)
    , async_query_sending(async_query_sending_)
{
    /// Add AggregatedChunkInfo if we expect DataTypeAggregateFunction as a result.
    const auto & sample = getPort().getHeader();
    for (auto & type : sample.getDataTypes())
        if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
            add_aggregation_info = true;
}

RemoteSource::~RemoteSource() = default;

void RemoteSource::setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_)
{
    /// Remove leaf limits for remote source.
    StorageLimitsList list;
    for (const auto & value : *storage_limits_)
        list.emplace_back(StorageLimits{value.local_limits, {}});

    storage_limits = std::make_shared<const StorageLimitsList>(std::move(list));
}

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

    Status status = ISource::prepare();
    /// To avoid resetting the connection (because of "unfinished" query) in the
    /// RemoteQueryExecutor it should be finished explicitly.
    if (status == Status::Finished)
    {
        query_executor->finish();
        is_async_state = false;
        return status;
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
        query_executor->setProgressCallback([this](const Progress & value)
        {
            if (value.total_rows_to_read)
                addTotalRowsApprox(value.total_rows_to_read);
            if (value.total_bytes_to_read)
                addTotalBytes(value.total_bytes_to_read);
            progress(value.read_rows, value.read_bytes);
        });

        /// Get rows_before_limit result for remote query from ProfileInfo packet.
        query_executor->setProfileInfoCallback([this](const ProfileInfo & info)
        {
            if (rows_before_limit)
            {
                if (info.hasAppliedLimit())
                    rows_before_limit->add(info.getRowsBeforeLimit());
                else
                    manually_add_rows_before_limit_counter = true; /// Remote subquery doesn't contain a limit
            }
        });

        if (async_query_sending)
        {
            int fd_ = query_executor->sendQueryAsync();
            if (fd_ >= 0)
            {
                fd = fd_;
                is_async_state = true;
                return Chunk();
            }

            is_async_state = false;
        }
        else
        {
            query_executor->sendQuery();
        }

        was_query_sent = true;
    }

    Block block;

    if (async_read)
    {
        auto res = query_executor->readAsync();

        if (res.getType() == RemoteQueryExecutor::ReadResult::Type::Nothing)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got an empty packet from the RemoteQueryExecutor. This is a bug");

        if (res.getType() == RemoteQueryExecutor::ReadResult::Type::FileDescriptor)
        {
            fd = res.getFileDescriptor();
            is_async_state = true;
            return Chunk();
        }

        if (res.getType() == RemoteQueryExecutor::ReadResult::Type::ParallelReplicasToken)
        {
            is_async_state = false;
            return Chunk();
        }

        is_async_state = false;

        block = res.getBlock();
    }
    else
        block = query_executor->readBlock();

    if (!block)
    {
        if (manually_add_rows_before_limit_counter)
            rows_before_limit->add(rows);

        query_executor->finish();
        return {};
    }

    UInt64 num_rows = block.rows();
    rows += num_rows;
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
    query_executor->cancel();
}

void RemoteSource::onUpdatePorts()
{
    if (getPort().isFinished())
    {
        was_query_canceled = true;
        query_executor->finish();
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
    bool add_aggregation_info, bool add_totals, bool add_extremes, bool async_read, bool async_query_sending)
{
    Pipe pipe(std::make_shared<RemoteSource>(query_executor, add_aggregation_info, async_read, async_query_sending));

    if (add_totals)
        pipe.addTotalsSource(std::make_shared<RemoteTotalsSource>(query_executor));

    if (add_extremes)
        pipe.addExtremesSource(std::make_shared<RemoteExtremesSource>(query_executor));

    return pipe;
}

}
