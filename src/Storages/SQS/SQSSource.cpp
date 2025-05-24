#include <Storages/SQS/SQSSource.h>
#include <Storages/SQS/SQSConsumer.h>

#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/EmptyReadBuffer.h>
#include <Processors/Formats/IInputFormat.h>
#include <Columns/IColumn.h>

#include "config.h"

#if USE_AWS_SQS

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_SQS;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

SQSSource::SQSSource(
    StorageSQS & storage_,
    const StorageSnapshotPtr & /* storage_snapshot */,
    const String & format_,
    const Block & sample_block_,
    UInt64 max_block_size_,
    UInt64 max_execution_time_,
    ContextPtr context_,
    bool skip_invalid_messages_,
    String dead_letter_queue_url_,
    bool is_read_,
    bool auto_delete_)
    : ISource(sample_block_.getColumnsWithTypeAndName())
    , storage(storage_)
    , sample_block(sample_block_)
    , context(context_)
    , format(format_)
    , max_block_size(max_block_size_)
    , skip_invalid_messages(skip_invalid_messages_)
    , dead_letter_queue_url(dead_letter_queue_url_)
    , is_read(is_read_)
    , auto_delete(auto_delete_)
    , max_execution_time_ms(max_execution_time_)
    , queue(100)
{
}

Chunk SQSSource::generate()
{
    if (!consumer)
        consumer = storage.popConsumer();

    if (!consumer || is_finished)
        return {};

    is_finished = true;

    size_t total_rows = 0;
    MutableColumns columns = sample_block.cloneEmptyColumns();
    
    while (total_rows < max_block_size)
    {
        if (consumer->isStopped())
            break;
        
        bool is_time_limit_exceeded = false;
        if (max_execution_time_ms)
        {
            uint64_t elapsed_time_ms = total_stopwatch.elapsedMilliseconds();
            is_time_limit_exceeded = max_execution_time_ms <= elapsed_time_ms;
        }
        if (is_time_limit_exceeded)
            break;
        
        // Receive message from consumer queue
        auto message_opt = consumer->getMessage();
        if (!message_opt)
        {
            if (!consumer->receive())
            {
                break;
            }
            continue;
        }
        
        auto message = message_opt.value();
        if (message.data.empty())
        {
            continue;
        }
        
        read_buf = std::make_unique<ReadBufferFromString>(message.data);

        auto input_format = FormatFactory::instance().getInput(
            format, *read_buf, sample_block, context, max_block_size);
        
        if (!input_format)
        {
            if (!dead_letter_queue_url.empty())
            {
                consumer->moveMessageToDLQ(message);
            }
            else if (skip_invalid_messages)
            {
                consumer->deleteMessage(message.receipt_handle);
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to create input format for SQS message");
            }
            continue;
        }
        
        Chunk chunk;
        try 
        {
            chunk = input_format->read();
        } 
        catch (const Exception & e)
        {
            LOG_ERROR(&Poco::Logger::get("SQSSource"), "Error reading data from SQS message: {}", e.message());

            if (e.code() == ErrorCodes::BAD_ARGUMENTS || 
                e.code() == ErrorCodes::CANNOT_CONNECT_SQS ||
                e.displayText().find("parse") != String::npos || 
                e.displayText().find("format") != String::npos)
            {
                // If the problem is in data format and DLQ is configured, move the message there
                if (!dead_letter_queue_url.empty())
                {
                    LOG_WARNING(&Poco::Logger::get("SQSSource"), "Error in data format in message, moving to DLQ");
                    consumer->moveMessageToDLQ(message);
                }
                else if (skip_invalid_messages)
                {
                    // Otherwise, delete the broken message
                    LOG_WARNING(&Poco::Logger::get("SQSSource"), "Deleting message with data format error");
                    consumer->deleteMessage(message.receipt_handle);
                } else {
                    throw;
                }
            }
            continue;
        }
        if (!chunk.hasRows())
        {
            continue;
        }
        
        if (total_rows + chunk.getNumRows() > max_block_size && total_rows > 0)
        {
            break;
        }
        
        // Add the rows to our columns
        for (size_t i = 0; i < columns.size(); ++i)
        {
            const auto & column_name = sample_block.getColumnsWithTypeAndName()[i].name;
            
            // Add virtual columns for all rows in the chunk
            if (column_name.starts_with("_"))
            {
                size_t num_rows = chunk.getNumRows();
                for (size_t row = 0; row < num_rows; ++row)
                {
                    if (column_name == "_message_id")
                    {
                        columns[i]->insert(message.message_id);
                    }
                    else if (column_name == "_receive_count")
                    {
                        columns[i]->insert(message.receive_count);
                    }
                    else if (column_name == "_sent_timestamp")
                    {
                        columns[i]->insert(message.sent_timestamp);
                    }
                    else if (column_name == "_message_group_id")
                    {
                        columns[i]->insert(message.message_group_id);
                    }
                    else if (column_name == "_message_deduplication_id")
                    {
                        columns[i]->insert(message.message_deduplication_id);
                    }
                    else if (column_name == "_sequence_number")
                    {
                        columns[i]->insert(message.sequence_number);
                    }
                }
            }
            else
            {
                // Process regular columns
                const auto & src_column = chunk.getColumns()[i];
                if (src_column && columns[i])
                {
                    size_t num_rows = chunk.getNumRows();
                    columns[i]->insertRangeFrom(*src_column, 0, num_rows);
                }
            }
        }
        
        if (!queue.tryPush(message))
        {
            continue;
        }
        total_rows += chunk.getNumRows();
    }
    
    if (total_rows == 0)
    {
        return {};
    }

    if (is_read && auto_delete)
    {
        deleteMessages();
    }
    
    return Chunk(std::move(columns), total_rows);
}

void SQSSource::deleteMessages()
{
    SQSConsumer::Message message;
    while (queue.tryPop(message))
    {
        consumer->deleteMessage(message.receipt_handle);
    }
}

SQSSource::~SQSSource()
{
    queue.clear();

    if (consumer)
    {
        storage.pushConsumer(consumer);
    }
}

}

#endif // USE_AWS_SQS
