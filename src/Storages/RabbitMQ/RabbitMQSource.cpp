#include <Storages/RabbitMQ/RabbitMQSource.h>

#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/RabbitMQ/RabbitMQConsumer.h>
#include <Common/logger_useful.h>
#include <IO/EmptyReadBuffer.h>
#include <base/sleep.h>

namespace DB
{

static std::pair<Block, Block> getHeaders(const StorageSnapshotPtr & storage_snapshot)
{
    auto non_virtual_header = storage_snapshot->metadata->getSampleBlockNonMaterialized();
    auto virtual_header = storage_snapshot->getSampleBlockForColumns(
                {"_exchange_name", "_channel_id", "_delivery_tag", "_redelivered", "_message_id", "_timestamp"});

    return {non_virtual_header, virtual_header};
}

static Block getSampleBlock(const Block & non_virtual_header, const Block & virtual_header)
{
    auto header = non_virtual_header;
    for (const auto & column : virtual_header)
        header.insert(column);

    return header;
}

RabbitMQSource::RabbitMQSource(
    StorageRabbitMQ & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    ContextPtr context_,
    const Names & columns,
    size_t max_block_size_,
    UInt64 max_execution_time_,
    bool ack_in_suffix_)
    : RabbitMQSource(
        storage_,
        storage_snapshot_,
        getHeaders(storage_snapshot_),
        context_,
        columns,
        max_block_size_,
        max_execution_time_,
        ack_in_suffix_)
{
}

RabbitMQSource::RabbitMQSource(
    StorageRabbitMQ & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    std::pair<Block, Block> headers,
    ContextPtr context_,
    const Names & columns,
    size_t max_block_size_,
    UInt64 max_execution_time_,
    bool ack_in_suffix_)
    : ISource(getSampleBlock(headers.first, headers.second))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , context(context_)
    , column_names(columns)
    , max_block_size(max_block_size_)
    , ack_in_suffix(ack_in_suffix_)
    , non_virtual_header(std::move(headers.first))
    , virtual_header(std::move(headers.second))
    , log(&Poco::Logger::get("RabbitMQSource"))
    , max_execution_time_ms(max_execution_time_)
{
    storage.incrementReader();
}


RabbitMQSource::~RabbitMQSource()
{
    storage.decrementReader();

    if (!consumer)
        return;

    storage.pushConsumer(consumer);
}


bool RabbitMQSource::needChannelUpdate()
{
    if (!consumer)
        return false;

    return consumer->needChannelUpdate();
}


void RabbitMQSource::updateChannel()
{
    if (!consumer)
        return;

    consumer->updateChannel(storage.getConnection());
}

Chunk RabbitMQSource::generate()
{
    auto chunk = generateImpl();
    if (!chunk && ack_in_suffix)
        sendAck();

    return chunk;
}

Chunk RabbitMQSource::generateImpl()
{
    if (!consumer)
    {
        auto timeout = std::chrono::milliseconds(context->getSettingsRef().rabbitmq_max_wait_ms.totalMilliseconds());
        consumer = storage.popConsumer(timeout);
    }

    if (is_finished || !consumer || consumer->isConsumerStopped())
    {
        LOG_TRACE(log, "RabbitMQSource is stopped (is_finished: {}, consumer_stopped: {})",
                  is_finished, consumer ? toString(consumer->isConsumerStopped()) : "No consumer");
        return {};
    }

    /// Currently it is one time usage source: to make sure data is flushed
    /// strictly by timeout or by block size.
    is_finished = true;

    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();
    EmptyReadBuffer empty_buf;
    auto input_format = FormatFactory::instance().getInput(
        storage.getFormatName(), empty_buf, non_virtual_header, context, max_block_size, std::nullopt, 1);

    StreamingFormatExecutor executor(non_virtual_header, input_format);
    size_t total_rows = 0;

    RabbitMQConsumer::CommitInfo current_commit_info;
    while (true)
    {
        size_t new_rows = 0;

        if (consumer->hasPendingMessages())
        {
            if (auto buf = consumer->consume())
                new_rows = executor.execute(*buf);
        }

        if (new_rows)
        {
            const auto exchange_name = storage.getExchange();
            const auto & message = consumer->currentMessage();

            for (size_t i = 0; i < new_rows; ++i)
            {
                virtual_columns[0]->insert(exchange_name);
                virtual_columns[1]->insert(message.channel_id);
                virtual_columns[2]->insert(message.delivery_tag);
                virtual_columns[3]->insert(message.redelivered);
                virtual_columns[4]->insert(message.message_id);
                virtual_columns[5]->insert(message.timestamp);
            }

            total_rows += new_rows;
            current_commit_info = {message.delivery_tag, message.channel_id};
        }
        else if (total_rows == 0)
        {
            break;
        }

        bool is_time_limit_exceeded = false;
        UInt64 remaining_execution_time = 0;
        if (max_execution_time_ms)
        {
            uint64_t elapsed_time_ms = total_stopwatch.elapsedMilliseconds();
            is_time_limit_exceeded = max_execution_time_ms <= elapsed_time_ms;
            if (!is_time_limit_exceeded)
                remaining_execution_time = max_execution_time_ms - elapsed_time_ms;
        }

        if (total_rows >= max_block_size || consumer->isConsumerStopped() || is_time_limit_exceeded)
        {
            break;
        }
        else if (new_rows == 0)
        {
            if (remaining_execution_time)
                consumer->waitForMessages(remaining_execution_time);
            else
                consumer->waitForMessages();
        }
    }

    LOG_TEST(
        log,
        "Flushing {} rows (max block size: {}, time: {} / {} ms)",
        total_rows, max_block_size, total_stopwatch.elapsedMilliseconds(), max_execution_time_ms);

    if (total_rows == 0)
        return {};

    auto result_columns  = executor.getResultColumns();
    for (auto & column : virtual_columns)
        result_columns.push_back(std::move(column));

    commit_info = current_commit_info;
    return Chunk(std::move(result_columns), total_rows);
}


bool RabbitMQSource::sendAck()
{
    return consumer && consumer->ackMessages(commit_info);
}

bool RabbitMQSource::sendNack()
{
    return consumer && consumer->nackMessages(commit_info);
}

}
