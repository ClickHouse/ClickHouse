#include <cstddef>
#include <vector>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <QueryPipeline/RemoteQueryExecutorReadContext.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include "Columns/ColumnBlob.h"
#include "Processors/IProcessor.h"
#include "base/types.h"

#include <Processors/ISimpleTransform.h>

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

ISource::Status RemoteSource::prepare()
{
    /// Check if query was cancelled before returning Async status. Otherwise it may lead to infinite loop.
    if (isCancelled())
    {
        getPort().finish();
        return Status::Finished;
    }

#if defined(OS_LINUX)
    if (async_query_sending && !was_query_sent && fd < 0)
    {
        startup_event_fd.write();
        return Status::Async;
    }
#endif

    if (is_async_state)
        return Status::Async;

    if (executor_finished)
        return Status::Finished;

    Status status = ISource::prepare();
    /// To avoid resetting the connection (because of "unfinished" query) in the
    /// RemoteQueryExecutor it should be finished explicitly.
    if (status == Status::Finished)
    {
        is_async_state = false;
        need_drain = true;
        return Status::Ready;
    }

    return status;
}

int RemoteSource::schedule()
{
#if defined(OS_LINUX)
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
        executor_finished = true;
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

struct ChunkSequenceNumber : public ChunkInfoCloneable<ChunkSequenceNumber>
{
    explicit ChunkSequenceNumber(UInt64 sequence_number_)
        : sequence_number(sequence_number_)
    {
    }

    UInt64 sequence_number = 0;
};

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
        chunk.getChunkInfos().add(std::move(info));
    }

    auto info = std::make_shared<ChunkSequenceNumber>(++chunk_sequence_number);
    chunk.getChunkInfos().add(std::move(info));

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
    if (getPort().isFinished())
    {
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

struct ConvertBlobColumnsTransform : ISimpleTransform
{
public:
    explicit ConvertBlobColumnsTransform(const Block & header_)
        : ISimpleTransform(header_, header_, false)
    {
    }

    String getName() const override { return "ConvertBlobColumnsTransform"; }

    void transform(Chunk & chunk) override
    {
        const auto rows = chunk.getNumRows();
        auto columns = chunk.detachColumns();
        for (auto & column : columns)
        {
            if (const auto * col = typeid_cast<const ColumnBlob *>(column.get()))
                column = col->convertFrom();
        }
        chunk.setColumns(std::move(columns), rows);
    }
};

class SortChunksBySequenceNumber : public IProcessor
{
public:
    SortChunksBySequenceNumber(const Block & header_, size_t num_inputs_)
        : IProcessor(InputPorts{num_inputs_, header_}, {header_})
        , num_inputs(num_inputs_)
        , chunk_snums(num_inputs, -1)
        , chunks(num_inputs)
        , is_input_finished(num_inputs, false)
    {
    }

    String getName() const override { return "SortChunksBySequenceNumber"; }

    IProcessor::Status prepare() final
    {
        auto & output = outputs.front();

        if (output.isFinished())
        {
            for (auto & input : inputs)
                input.close();
            return Status::Finished;
        }

        if (!output.canPush())
        {
            for (auto & input : inputs)
                input.setNotNeeded();
            return Status::PortFull;
        }

        const bool pushed_to_output = tryPushChunk();

        bool need_data = false;
        bool all_finished = true;

        /// Try read anything.
        auto in = inputs.begin();
        for (size_t input_num = 0; input_num < num_inputs; ++input_num, ++in)
        {
            if (in->isFinished())
            {
                is_input_finished[input_num] = true;
                continue;
            }

            if (chunk_snums[input_num] != -1)
            {
                all_finished = false;
                continue;
            }

            in->setNeeded();

            if (!in->hasData())
            {
                need_data = true;
                all_finished = false;
                continue;
            }

            auto chunk = in->pull();
            addChunk(std::move(chunk), input_num);

            if (in->isFinished())
            {
                is_input_finished[input_num] = true;
            }
            else
            {
                /// If chunk was pulled, then we need data from this port.
                need_data = true;
                all_finished = false;
            }
        }

        if (pushed_to_output)
            return Status::PortFull;

        if (tryPushChunk())
            return Status::PortFull;

        if (need_data)
            return Status::NeedData;

        if (!all_finished)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "SortChunksBySequenceNumber has read bucket, but couldn't push it.");

        output.finish();
        return Status::Finished;
    }

    bool tryPushChunk()
    {
        auto & output = outputs.front();

        bool can_peak_next = false;
        Int64 min_snum = std::numeric_limits<Int64>::max();
        size_t min_snum_idx = 0;
        for (size_t i = 0; i < num_inputs; ++i)
        {
            if (is_input_finished[i] && chunk_snums[i] == -1)
                continue;

            if (chunk_snums[i] == -1)
                return false;

            can_peak_next = true;
            if (chunk_snums[i] < min_snum)
            {
                min_snum = chunk_snums[i];
                min_snum_idx = i;
            }
        }

        if (!can_peak_next)
            return false;

        auto & chunk = chunks[min_snum_idx];
        output.push(std::move(chunk));
        chunk_snums[min_snum_idx] = -1;
        return true;
    }

    void addChunk(Chunk chunk, size_t input)
    {
        auto info = chunk.getChunkInfos().get<ChunkSequenceNumber>();
        if (!info)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk should have ChunkSequenceNumber in SortChunksBySequenceNumber.");

        chunk_snums[input] = info->sequence_number;
        chunks[input] = std::move(chunk);
    }

private:
    const size_t num_inputs;
    std::vector<Int64> chunk_snums;
    std::vector<Chunk> chunks;
    std::vector<bool> is_input_finished;
};

Pipe createRemoteSourcePipe(
    RemoteQueryExecutorPtr query_executor,
    bool add_aggregation_info, bool add_totals, bool add_extremes, bool async_read, bool async_query_sending)
{
    Pipe pipe(std::make_shared<RemoteSource>(query_executor, add_aggregation_info, async_read, async_query_sending));

    if (add_totals)
        pipe.addTotalsSource(std::make_shared<RemoteTotalsSource>(query_executor));

    if (add_extremes)
        pipe.addExtremesSource(std::make_shared<RemoteExtremesSource>(query_executor));

    const size_t threads = 8;
    pipe.resize(threads);
    pipe.addSimpleTransform([&](const Block & header) { return std::make_shared<ConvertBlobColumnsTransform>(header); });
    pipe.addTransform(std::make_shared<SortChunksBySequenceNumber>(pipe.getHeader(), threads));

    return pipe;
}

}
