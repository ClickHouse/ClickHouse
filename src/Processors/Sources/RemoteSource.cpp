#include <Columns/ColumnBLOB.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Processors/IProcessor.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <QueryPipeline/RemoteQueryExecutorReadContext.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <Common/Exception.h>
#include <Common/Logger.h>

#include <Processors/Transforms/SortChunksBySequenceNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

RemoteSource::RemoteSource(
    RemoteQueryExecutorPtr executor,
    bool add_aggregation_info_,
    bool async_read_,
    bool async_query_sending_,
    bool add_totals_port,
    bool add_extremes_port)
    : ISource(executor->getSharedHeader(), false)
    , add_aggregation_info(add_aggregation_info_)
    , query_executor(std::move(executor))
    , async_read(async_read_)
    , async_query_sending(async_query_sending_)
{
    /// ISource binds its `output` reference to `outputs.front`, and OutputPorts is a std::list,
    /// so appending here leaves that reference valid.
    if (add_totals_port)
        totals_port = &outputs.emplace_back(getPort().getHeader(), this);

    if (add_extremes_port)
        extremes_port = &outputs.emplace_back(getPort().getHeader(), this);

    /// Add AggregatedChunkInfo if we expect DataTypeAggregateFunction as a result.
    const auto & sample = getPort().getHeader();
    for (auto & type : sample.getDataTypes())
        if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
            add_aggregation_info = true;

    /// Progress method will be called on Progress packet.
    query_executor->setProgressCallback([this](const Progress & value)
    {
        if (value.total_rows_to_read)
            addTotalRowsApprox(value.total_rows_to_read);
        if (value.total_bytes_to_read)
            addTotalBytes(value.total_bytes_to_read);
        progress(value.read_rows, value.read_bytes);
    });

    query_executor->setProfileInfoCallback(
        [this](const ProfileInfo & info)
        {
            if (rows_before_limit)
            {
                if (info.hasAppliedLimit())
                    rows_before_limit->add(info.getRowsBeforeLimit());
                else
                    manually_add_rows_before_limit_counter = true; /// Remote subquery doesn't contain a limit
            }

            if (rows_before_aggregation)
            {
                if (info.hasAppliedAggregation())
                    rows_before_aggregation->add(info.getRowsBeforeAggregation());
            }
        });
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

ISource::Status RemoteSource::prepareAuxPorts()
{
    /// totals/extremes are moved out of the shared executor here - by the same node that received
    /// them - after the main stream is drained, so no two threads ever touch them.
    /// Ports are emitted independently, because a consumer may make them needed one at a time.
    if (totals_port && !totals_emitted)
    {
        if (!totals_port->isFinished())
        {
            if (!totals_port->canPush())
                return Status::PortFull;

            if (auto block = query_executor->getTotals(); !block.empty())
                totals_port->push(Chunk(block.getColumns(), block.rows()));
        }

        totals_port->finish();
        totals_emitted = true;
    }

    if (extremes_port && !extremes_emitted)
    {
        if (!extremes_port->isFinished())
        {
            if (!extremes_port->canPush())
                return Status::PortFull;

            if (auto block = query_executor->getExtremes(); !block.empty())
                extremes_port->push(Chunk(block.getColumns(), block.rows()));
        }

        extremes_port->finish();
        extremes_emitted = true;
    }

    return Status::Finished;
}

ISource::Status RemoteSource::prepare()
{
    /// Check if query was cancelled before returning Async status. Otherwise it may lead to infinite loop.
    if (isCancelled())
    {
        getPort().finish();
        if (totals_port)
            totals_port->finish();
        if (extremes_port)
            extremes_port->finish();
        return Status::Finished;
    }

    if (main_output_finished)
        return prepareAuxPorts();

#if defined(OS_LINUX) || defined(OS_DARWIN)
    if (async_query_sending && !was_query_sent && fd < 0)
    {
        startup_event_fd.write();
        return Status::Async;
    }
#endif

    if (is_async_state)
        return Status::Async;

    if (query_executor->isFinished())
    {
        getPort().finish();
        main_output_finished = true;
        return prepareAuxPorts();
    }

    Status status = ISource::prepare();
    /// To avoid resetting the connection (because of "unfinished" query) in the
    /// RemoteQueryExecutor it should be finished explicitly.
    if (status == Status::Finished)
    {
        is_async_state = false;
        need_drain = true;
        main_output_finished = true;
        return Status::Ready;
    }

    return status;
}

int RemoteSource::schedule()
{
#if defined(OS_LINUX) || defined(OS_DARWIN)
    return (fd < 0 ? startup_event_fd.fd : fd);
#else
    return fd;
#endif
}

void RemoteSource::work()
{
    /// Connection drain is a heavy operation that may take a long time.
    /// Therefore we move connection drain from prepare() to work(), and drain multiple connections in parallel.
    /// See issue: https://github.com/ClickHouse/ClickHouse/issues/60844
    if (need_drain)
    {
        query_executor->finish();
        return;
    }

    if (preprocessed_packet)
    {
        preprocessed_packet = false;
        return;
    }

    ISource::work();
}

void RemoteSource::onAsyncJobReady()
{
    chassert(async_read || async_query_sending);

    if (!was_query_sent)
        return;

    chassert(!preprocessed_packet);
    preprocessed_packet = query_executor->processParallelReplicaPacketIfAny();
    if (preprocessed_packet)
        is_async_state = false;
}

std::optional<Chunk> RemoteSource::tryGenerate()
{
    /// onCancel() will do the cancel if the query was sent.
    if (isCancelled())
        return {};

    if (!was_query_sent)
    {
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

    if (block.empty())
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
        info->out_of_order_buckets = block.info.out_of_order_buckets;
        chunk.getChunkInfos().add(std::move(info));
    }

    return chunk;
}

void RemoteSource::onCancel() noexcept
{
    try
    {
        query_executor->cancel();
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("RemoteSource"), "Error occurs on cancellation.");
    }
}

void RemoteSource::onUpdatePorts()
{
    if (isCancelled())
        return;
    if (getPort().isFinished())
        query_executor->finish();
}


void UnmarshallBlocksTransform::transform(Chunk & chunk)
{
    const auto rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
    {
        if (const auto * col = typeid_cast<const ColumnBLOB *>(column.get()))
            column = col->convertFrom();
    }
    chunk.setColumns(std::move(columns), rows);
}

Pipe createRemoteSourcePipe(
    RemoteQueryExecutorPtr query_executor,
    bool add_aggregation_info,
    bool add_totals,
    bool add_extremes,
    bool async_read,
    bool async_query_sending,
    size_t parallel_marshalling_threads)
{
    chassert(parallel_marshalling_threads);

    auto source = std::make_shared<RemoteSource>(
        query_executor, add_aggregation_info, async_read, async_query_sending, add_totals, add_extremes);

    auto * main_port = &source->getPort();
    auto * totals_port = source->getTotalsPort();
    auto * extremes_port = source->getExtremesPort();

    Pipe pipe(std::move(source), main_port, totals_port, extremes_port);

    /// The totals/extremes ports now exist from pipe construction, so both simple transforms must
    /// opt out of them explicitly to keep running on the main streams only.
    pipe.addSimpleTransform([&](const SharedHeader & header, Pipe::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != Pipe::StreamType::Main)
            return nullptr;
        return std::make_shared<AddSequenceNumber>(header);
    });

    pipe.resize(parallel_marshalling_threads);
    pipe.addSimpleTransform([&](const SharedHeader & header, Pipe::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != Pipe::StreamType::Main)
            return nullptr;
        return std::make_shared<UnmarshallBlocksTransform>(header);
    });
    pipe.addTransform(std::make_shared<SortChunksBySequenceNumber>(pipe.getHeader(), parallel_marshalling_threads));

    return pipe;
}

}
