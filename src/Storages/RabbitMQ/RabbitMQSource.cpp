#include <exception>
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

    std::vector<String> exception_messages;
    size_t total_rows = 0;

    size_t input_format_allow_errors_num = context->getSettingsRef().input_format_allow_errors_num;
    size_t num_rows_with_errors = 0;
    auto on_error = [&](std::exception_ptr e)
    {
        if (handle_error_mode != StreamingHandleErrorMode::STREAM)
        {
            if (input_format_allow_errors_num >= ++num_rows_with_errors)
                return;

            std::rethrow_exception(e);
        }
        exception_messages.emplace_back(getExceptionMessage(e, false));
    };

    StreamingFormatExecutor executor(non_virtual_header, input_format, on_error);

    RabbitMQConsumer::CommitInfo current_commit_info;
    while (true)
    {
        exception_messages.clear();
        size_t new_rows = 0;

        if (consumer->hasPendingMessages())
        {
            if (auto buf = consumer->consume())
                new_rows = executor.execute(*buf);
        }

        if (new_rows || !exception_messages.empty())
        {
            const auto exchange_name = storage.getExchange();
            const auto & message = consumer->currentMessage();

            size_t new_rows_with_errors = new_rows + exception_messages.size();
            /// FIXME: we can do better, get information from particular message
            virtual_columns[0]->insertMany(exchange_name, new_rows_with_errors);
            virtual_columns[1]->insertMany(message.channel_id, new_rows_with_errors);
            virtual_columns[2]->insertMany(message.delivery_tag, new_rows_with_errors);
            virtual_columns[3]->insertMany(message.redelivered, new_rows_with_errors);
            virtual_columns[4]->insertMany(message.message_id, new_rows_with_errors);
            virtual_columns[5]->insertMany(message.timestamp, new_rows_with_errors);

            if (handle_error_mode == StreamingHandleErrorMode::STREAM)
            {
                virtual_columns[6]->insertManyDefaults(new_rows);
                virtual_columns[7]->insertManyDefaults(new_rows);

                /// FIXME: we can do better, by reusing reason/row from ErrorEntry
                virtual_columns[6]->insertMany(Field(message.message.data(), message.message.size()), exception_messages.size());
                for (const auto & exception_message : exception_messages)
                {
                    virtual_columns[7]->insertData(exception_message.data(), exception_message.size());
                }
            }

            total_rows += new_rows_with_errors;
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

    auto result_columns = executor.getResultColumns();

    size_t result_block_rows = result_columns.front()->size();
    size_t virtual_block_rows = virtual_columns.front()->size();
    size_t errors = virtual_block_rows - result_block_rows;
    if (errors)
    {
        for (auto & column : result_columns)
            column->insertManyDefaults(errors);
    }

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
