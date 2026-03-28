#include <Storages/SQS/SQSSource.h>
#include <Storages/SQS/StorageSQS.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/EmptyReadBuffer.h>
#include <Processors/Formats/IInputFormat.h>

#include "config.h"

#if USE_AWS_SQS

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

SQSSource::SQSSource(
    StorageSQS & storage_,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    const String & format_,
    const Block & sample_block_,
    UInt64 max_block_size_,
    UInt64 max_execution_time_ms_,
    ContextPtr context_,
    bool skip_broken_messages_,
    UInt64 skip_broken_messages_count_,
    const String & dead_letter_queue_url_,
    bool auto_delete_)
    : ISource(sample_block_.getColumnsWithTypeAndName())
    , storage(storage_)
    , sample_block(sample_block_)
    , context(context_)
    , format(format_)
    , max_block_size(max_block_size_)
    , skip_broken_messages(skip_broken_messages_)
    , skip_broken_messages_count(skip_broken_messages_count_)
    , dead_letter_queue_url(dead_letter_queue_url_)
    , auto_delete(auto_delete_)
    , max_execution_time_ms(max_execution_time_ms_)
    , pending_ack(max_block_size_ + 1)
{
}

SQSSource::~SQSSource()
{
    pending_ack.clear();

    if (consumer)
        storage.pushConsumer(consumer);
}

Chunk SQSSource::generate()
{
    if (!consumer)
        consumer = storage.popConsumer();

    if (!consumer || is_finished)
        return {};

    /// Each call to generate() produces at most one block.
    is_finished = true;

    size_t total_rows = 0;
    size_t broken_count = 0;
    MutableColumns result_columns = sample_block.cloneEmptyColumns();

    while (total_rows < max_block_size)
    {
        if (consumer->isStopped())
            break;

        if (max_execution_time_ms > 0 && total_stopwatch.elapsedMilliseconds() >= max_execution_time_ms)
            break;

        auto message_opt = consumer->getMessage();
        if (!message_opt)
        {
            if (!consumer->receive())
                break;
            continue;
        }

        auto & message = *message_opt;
        if (message.data.empty())
            continue;

        read_buf = std::make_unique<ReadBufferFromString>(message.data);
        auto input_format = FormatFactory::instance().getInput(format, *read_buf, sample_block, context, max_block_size);

        Chunk chunk;
        bool format_error = false;

        try
        {
            chunk = input_format->read();
        }
        catch (const Exception & e)
        {
            LOG_ERROR(getLogger("SQSSource"), "Error parsing SQS message {}: {}", message.message_id, e.message());
            format_error = true;
        }

        if (format_error || !chunk.hasRows())
        {
            if (format_error)
            {
                if (!dead_letter_queue_url.empty())
                {
                    consumer->moveMessageToDLQ(message);
                }
                else if (skip_broken_messages)
                {
                    ++broken_count;
                    if (broken_count > skip_broken_messages_count && skip_broken_messages_count > 0)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "SQS: Too many broken messages (more than {})", skip_broken_messages_count);
                    consumer->deleteMessage(message.receipt_handle);
                }
                else
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "SQS: Failed to parse message {}. "
                        "Use sqs_skip_broken_messages to skip broken messages or "
                        "sqs_dead_letter_queue_url to route them to a DLQ.",
                        message.message_id);
                }
            }
            continue;
        }

        /// Append chunk rows to result_columns.
        /// Virtual columns are handled by StorageSnapshot - they will appear in sample_block
        /// if the user requested them, but the format only fills non-virtual columns.
        const auto & chunk_columns = chunk.getColumns();
        const size_t num_rows = chunk.getNumRows();

        for (size_t i = 0; i < result_columns.size(); ++i)
        {
            const auto & col_name = sample_block.getByPosition(i).name;

            if (col_name == "_message_id")
                for (size_t r = 0; r < num_rows; ++r)
                    result_columns[i]->insert(message.message_id);
            else if (col_name == "_receive_count")
                for (size_t r = 0; r < num_rows; ++r)
                    result_columns[i]->insert(message.receive_count);
            else if (col_name == "_sent_timestamp")
                for (size_t r = 0; r < num_rows; ++r)
                    result_columns[i]->insert(message.sent_timestamp);
            else if (col_name == "_message_group_id")
                for (size_t r = 0; r < num_rows; ++r)
                    result_columns[i]->insert(message.message_group_id);
            else if (col_name == "_message_deduplication_id")
                for (size_t r = 0; r < num_rows; ++r)
                    result_columns[i]->insert(message.message_deduplication_id);
            else if (col_name == "_sequence_number")
                for (size_t r = 0; r < num_rows; ++r)
                    result_columns[i]->insert(message.sequence_number);
            else
            {
                /// Find matching column in chunk by position (non-virtual columns come first)
                if (i < chunk_columns.size())
                    result_columns[i]->insertRangeFrom(*chunk_columns[i], 0, num_rows);
            }
        }

        pending_ack.tryPush(message);
        total_rows += num_rows;
    }

    if (total_rows == 0)
        return {};

    if (auto_delete)
        deleteProcessedMessages();

    return Chunk(std::move(result_columns), total_rows);
}

void SQSSource::deleteProcessedMessages()
{
    if (!consumer)
        return;

    SQSConsumer::Message msg;
    while (pending_ack.tryPop(msg))
        consumer->deleteMessage(msg.receipt_handle);
}

}

#endif // USE_AWS_SQS
