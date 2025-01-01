#include <Storages/RabbitMQ/RabbitMQSource.h>

#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <IO/EmptyReadBuffer.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <base/sleep.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace Setting
{
    extern const SettingsMilliseconds rabbitmq_max_wait_ms;
}

static std::pair<Block, Block> getHeaders(const StorageSnapshotPtr & storage_snapshot, const Names & column_names)
{
    auto all_columns_header = storage_snapshot->metadata->getSampleBlock();

    auto non_virtual_header = storage_snapshot->metadata->getSampleBlockNonMaterialized();
    auto virtual_header = storage_snapshot->virtual_columns->getSampleBlock();

    for (const auto & column_name : column_names)
    {
        if (non_virtual_header.has(column_name) || virtual_header.has(column_name))
            continue;
        const auto & column = all_columns_header.getByName(column_name);
        non_virtual_header.insert(column);
    }

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
    bool nack_broken_messages_,
    bool ack_in_suffix_,
    LoggerPtr log_)
    : RabbitMQSource(
        storage_,
        storage_snapshot_,
        getHeaders(storage_snapshot_, columns),
        context_,
        columns,
        max_block_size_,
        max_execution_time_,
        handle_error_mode_,
        nack_broken_messages_,
        ack_in_suffix_,
        log_)
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
    bool nack_broken_messages_,
    bool ack_in_suffix_,
    LoggerPtr log_)
    : ISource(getSampleBlock(headers.first, headers.second))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , context(context_)
    , column_names(columns)
    , max_block_size(max_block_size_)
    , handle_error_mode(handle_error_mode_)
    , ack_in_suffix(ack_in_suffix_)
    , nack_broken_messages(nack_broken_messages_)
    , non_virtual_header(std::move(headers.first))
    , virtual_header(std::move(headers.second))
    , log(log_)
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
    {
        LOG_TEST(log, "Will send ack on select");
        sendAck();
    }

    return chunk;
}

Chunk RabbitMQSource::generateImpl()
{
    if (!consumer)
    {
        auto timeout = std::chrono::milliseconds(context->getSettingsRef()[Setting::rabbitmq_max_wait_ms].totalMilliseconds());
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

    std::optional<String> exception_message;
    size_t total_rows = 0;

    auto on_error = [&](const MutableColumns & result_columns, const ColumnCheckpoints & checkpoints, Exception & e)
    {
        if (handle_error_mode == StreamingHandleErrorMode::STREAM)
        {
            exception_message = e.message();
            for (size_t i = 0; i < result_columns.size(); ++i)
            {
                // We could already push some rows to result_columns before exception, we need to fix it.
                result_columns[i]->rollback(*checkpoints[i]);

                // All data columns will get default value in case of error.
                result_columns[i]->insertDefault();
            }

            return 1;
        }

        throw std::move(e);
    };

    StreamingFormatExecutor executor(non_virtual_header, input_format, on_error);

    /// Channel id will not change during read.
    while (true)
    {
        exception_message.reset();
        size_t new_rows = 0;

        if (consumer->hasPendingMessages())
        {
            /// A buffer containing a single RabbitMQ message.
            if (auto buf = consumer->consume())
            {
                new_rows = executor.execute(*buf);
            }
        }

        if (new_rows)
        {
            const auto exchange_name = storage.getExchange();
            const auto & message = consumer->currentMessage();

            LOG_TEST(log, "Pulled {} rows, message delivery tag: {}, "
                     "previous delivery tag: {}, redelivered: {}, failed delivery tags by this moment: {}, exception message: {}",
                     new_rows, message.delivery_tag, commit_info.delivery_tag, message.redelivered,
                     commit_info.failed_delivery_tags.size(),
                     exception_message.has_value() ? exception_message.value() : "None");

            commit_info.channel_id = message.channel_id;

            if (exception_message.has_value() && nack_broken_messages)
            {
                commit_info.failed_delivery_tags.push_back(message.delivery_tag);
            }
            else
            {
                chassert(!commit_info.delivery_tag || message.redelivered || commit_info.delivery_tag < message.delivery_tag);
                commit_info.delivery_tag = std::max(commit_info.delivery_tag, message.delivery_tag);
            }

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
        if (new_rows == 0)
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
