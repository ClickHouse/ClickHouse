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
    auto * logger = &Poco::Logger::get("SQSSource");
    LOG_INFO(logger, "SQS Source generate() called");

    if (!consumer)
    {
        consumer = storage.popConsumer();
    }

    if (!consumer)
    {
        LOG_INFO(logger, "No consumer");
        return {};
    }

    if (is_finished)
    {
        LOG_INFO(logger, "SQS Source is finished");
        return {};
    }

    is_finished = true;

    // Process messages from SQS
    size_t total_rows = 0;
    MutableColumns columns = sample_block.cloneEmptyColumns();

    LOG_INFO(logger, "ConsumerID: {}, Processing messages, max_block_size: {}", 
        consumer->consumer_id, max_block_size);
    
    while (total_rows < max_block_size)
    {
        if (consumer->isStopped())
        {
            LOG_INFO(logger, "ConsumerID: {}, Consumer is stopped", 
                consumer->consumer_id);
            break;
        }
        
        bool is_time_limit_exceeded = false;
        if (max_execution_time_ms)
        {
            uint64_t elapsed_time_ms = total_stopwatch.elapsedMilliseconds();
            is_time_limit_exceeded = max_execution_time_ms <= elapsed_time_ms;
        }
        if (is_time_limit_exceeded)
        {
            LOG_INFO(logger, "ConsumerID: {}, Time limit exceeded", 
                consumer->consumer_id);
            break;
        }
        
        // Receive message from consumer queue
        auto message_opt = consumer->getMessage();
        if (!message_opt)
        {
            LOG_INFO(logger, "ConsumerID: {}, No messages in consumer queue", 
                consumer->consumer_id);

            if (!consumer->receive())
            {
                LOG_INFO(logger, "ConsumerID: {}, No messages available to receive from SQS", 
                    consumer->consumer_id);
                break;
            }

            LOG_INFO(logger, "ConsumerID: {}, Messages were read from SQS, now they are in the consumer queue", 
                consumer->consumer_id);
            continue;
        }
        
        auto message = message_opt.value();
        if (message.data.empty())
        {
            LOG_INFO(logger, "ConsumerID: {}, Empty message", 
                consumer->consumer_id);
            continue;
        }
        
        LOG_INFO(logger, "ConsumerID: {}, Processing message {}", 
            consumer->consumer_id, message.data);

        read_buf = std::make_unique<ReadBufferFromString>(message.data);

        // Create input format for parsing
        auto input_format = FormatFactory::instance().getInput(
            format, *read_buf, sample_block, context, max_block_size);
        
        if (!input_format)
        {
            LOG_ERROR(logger, "Failed to create input format for SQS message");

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
        
        // Read data from the input format
        Chunk chunk;
        try 
        {
            chunk = input_format->read();
            
            LOG_INFO(logger, "ConsumerID: {}, Chunk read successfully", 
                consumer->consumer_id);
        } 
        catch (const Exception & e)
        {
            LOG_ERROR(logger, "Error reading data from SQS message: {}", e.message());
            
            // Check if the error is related to data format
            if (e.code() == ErrorCodes::BAD_ARGUMENTS || 
                e.code() == ErrorCodes::CANNOT_CONNECT_SQS ||
                e.displayText().find("parse") != String::npos || 
                e.displayText().find("format") != String::npos)
            {
                // If the problem is in data format and DLQ is configured, move the message there
                if (!dead_letter_queue_url.empty())
                {
                    LOG_WARNING(logger, "Error in data format in message, moving to DLQ");
                    consumer->moveMessageToDLQ(message);
                }
                else if (skip_invalid_messages)
                {
                    // Otherwise, delete the broken message
                    LOG_WARNING(logger, "Deleting message with data format error");
                    consumer->deleteMessage(message.receipt_handle);
                } else {
                    // Throw an exception
                    throw;
                }
            }
            continue;
        }
        if (!chunk.hasRows())
        {
            LOG_INFO(logger, "ConsumerID: {}, No rows in chunk", 
                consumer->consumer_id);
            continue;
        }
        
        LOG_INFO(logger, "ConsumerID: {}, Total rows: {}, Max block size: {}", 
            consumer->consumer_id, total_rows, max_block_size);
        // Check if we need to start a new chunk because of max_block_size
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
                        LOG_INFO(logger, "ConsumerID: {}, Inserting message_id: {}", 
                            consumer->consumer_id, message.message_id);
                        columns[i]->insert(message.message_id);
                    }
                    else if (column_name == "_receive_count")
                    {
                        LOG_INFO(logger, "ConsumerID: {}, Inserting receive_count: {}", 
                            consumer->consumer_id, message.receive_count);
                        columns[i]->insert(message.receive_count);
                    }
                    else if (column_name == "_sent_timestamp")
                    {
                        LOG_INFO(logger, "ConsumerID: {}, Inserting sent_timestamp: {}", 
                            consumer->consumer_id, message.sent_timestamp);
                        columns[i]->insert(message.sent_timestamp);
                    }
                    else if (column_name == "_message_group_id")
                    {
                        LOG_INFO(logger, "ConsumerID: {}, Inserting message_group_id: {}", 
                            consumer->consumer_id, message.message_group_id);
                        columns[i]->insert(message.message_group_id);
                    }
                    else if (column_name == "_message_deduplication_id")
                    {
                        LOG_INFO(logger, "ConsumerID: {}, Inserting message_deduplication_id: {}", 
                            consumer->consumer_id, message.message_deduplication_id);
                        columns[i]->insert(message.message_deduplication_id);
                    }
                    else if (column_name == "_sequence_number")
                    {
                        LOG_INFO(logger, "ConsumerID: {}, Inserting sequence_number: {}", 
                            consumer->consumer_id, message.sequence_number);
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
                    LOG_INFO(logger, "ConsumerID: {}, Inserting regular column: {}", 
                        consumer->consumer_id, column_name);
                    size_t num_rows = chunk.getNumRows();
                    columns[i]->insertRangeFrom(*src_column, 0, num_rows);
                }
            }
        }
        
        if (queue.tryPush(message))
        {
            LOG_INFO(logger, "ConsumerID: {}, Pushed message to queue", 
                consumer->consumer_id);
        }
        else
        {
            LOG_INFO(logger, "ConsumerID: {}, Queue is full, skipping message", 
                consumer->consumer_id);
            continue;
        }
        total_rows += chunk.getNumRows();
    }
    
    if (total_rows == 0)
    {
        LOG_INFO(logger, "ConsumerID: {}, No rows in chunk", 
            consumer->consumer_id);
        return {};
    }

    if (is_read && auto_delete)
    {
        LOG_INFO(logger, "ConsumerID: {}, Deleting messages because of auto_delete and is_read", 
            consumer->consumer_id);
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
        LOG_INFO(&Poco::Logger::get("SQSSource"), "Pushing consumer to storage");
        storage.pushConsumer(consumer);
    }
    else
    {
        LOG_INFO(&Poco::Logger::get("SQSSource"), "No consumer to push to storage");
    }
}

}  // namespace DB

#endif // USE_AWS_SQS
