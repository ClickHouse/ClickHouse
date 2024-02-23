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

static std::pair<Block, Block> getHeaders(StorageRabbitMQ & storage_, const StorageSnapshotPtr & storage_snapshot)
{
    auto non_virtual_header = storage_snapshot->metadata->getSampleBlockNonMaterialized();
    auto virtual_header = storage_snapshot->getSampleBlockForColumns(storage_.getVirtuals().getNames());

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
    StreamingHandleErrorMode handle_error_mode_,
    bool ack_in_suffix_)
    : RabbitMQSource(
        storage_,
        storage_snapshot_,
        getHeaders(storage_, storage_snapshot_),
        context_,
        columns,
        max_block_size_,
        max_execution_time_,
        handle_error_mode_,
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
    StreamingHandleErrorMode handle_error_mode_,
    bool ack_in_suffix_)
    : ISource(getSampleBlock(headers.first, headers.second))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , context(context_)
    , column_names(columns)
    , max_block_size(max_block_size_)
    , handle_error_mode(handle_error_mode_)
    , ack_in_suffix(ack_in_suffix_)
    , non_virtual_header(std::move(headers.first))
    , virtual_header(std::move(headers.second))
    , log(getLogger("RabbitMQSource"))
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
        return {};

    /// Currently it is one time usage source: to make sure data is flushed
    /// strictly by timeout or by block size.
    is_finished = true;

    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();
    EmptyReadBuffer empty_buf;
    auto input_format = FormatFactory::instance().getInput(
        storage.getFormatName(), empty_buf, non_virtual_header, context, max_block_size, std::nullopt, 1);

    std::optional<String> exception_message;
    size_t total_rows = 0;

    auto on_error = [&](const MutableColumns & result_columns, Exception & e)
    {
        if (handle_error_mode == StreamingHandleErrorMode::STREAM)
        {
            exception_message = e.message();
            for (const auto & column : result_columns)
            {
                // We could already push some rows to result_columns
                // before exception, we need to fix it.
                auto cur_rows = column->size();
                if (cur_rows > total_rows)
                    column->popBack(cur_rows - total_rows);

                // All data columns will get default value in case of error.
                column->insertDefault();
            }

            return 1;
        }
        else
        {
            throw std::move(e);
        }
    };

    StreamingFormatExecutor executor(non_virtual_header, input_format, on_error);

    RabbitMQConsumer::CommitInfo current_commit_info;
    while (true)
    {
        exception_message.reset();
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
                if (handle_error_mode == StreamingHandleErrorMode::STREAM)
                {
                    if (exception_message)
                    {
                        virtual_columns[6]->insertData(message.message.data(), message.message.size());
                        virtual_columns[7]->insertData(exception_message->data(), exception_message->size());
                    }
                    else
                    {
                        virtual_columns[6]->insertDefault();
                        virtual_columns[7]->insertDefault();
                    }
                }
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
    if (!consumer)
        return false;

    if (!consumer->ackMessages(commit_info))
        return false;

    return true;
}

}
