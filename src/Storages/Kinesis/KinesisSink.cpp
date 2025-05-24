#include <Storages/Kinesis/KinesisSink.h>

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/Block.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Processors/Formats/IOutputFormat.h>

#include "config.h"

#if USE_AWS_KINESIS

#include <aws/kinesis/model/PutRecordRequest.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int KINESIS_ERROR;
    extern const int BAD_ARGUMENTS;
}

KinesisSink::KinesisSink(
    const StorageMetadataPtr & metadata_snapshot,
    const Aws::Kinesis::KinesisClient & client_,
    const String & stream_name_,
    const String & format_name_,
    size_t max_rows_per_message_,
    ContextPtr context_)
    : SinkToStorage(metadata_snapshot->getSampleBlock())
    , client(client_)
    , stream_name(stream_name_)
    , format_name(format_name_)
    , max_rows_per_message(max_rows_per_message_)
    , context(context_)
    , sample_block(metadata_snapshot->getSampleBlock())
{
    if (stream_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Kinesis stream name cannot be empty");
    
    if (max_rows_per_message == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Kinesis max_rows_per_message cannot be zero");
}

void KinesisSink::consume(Chunk & chunk)
{
    if (chunk.getNumRows() == 0)
        return;

    Block block = getHeader().cloneWithColumns(chunk.detachColumns());
    size_t rows = block.rows();
    total_rows += rows;
    
    for (size_t offset = 0; offset < rows; offset += max_rows_per_message)
    {
        size_t current_chunk_size = std::min(max_rows_per_message, rows - offset);
        
        Block current_block = block.cloneEmpty();
        auto columns = block.getColumns();
        
        Columns current_columns;
        for (const auto & column : columns)
        {
            current_columns.emplace_back(column->cut(offset, current_chunk_size));
        }
        
        current_block.setColumns(current_columns);
        
        String message;
        WriteBufferFromString string_buf(message);
        
        auto output_format = FormatFactory::instance().getOutputFormat(
            format_name, string_buf, current_block.cloneEmpty(), context);
        
        output_format->write(current_block);
        output_format->finalize();
        string_buf.finalize();
        
        sendMessage(message);
    }
}

void KinesisSink::onFinish()
{
    if (total_rows > 0)
        LOG_DEBUG(&Poco::Logger::get("KinesisSink"), "Finished sending {} rows to Kinesis stream", total_rows);
}

void KinesisSink::sendMessage(const String & message)
{
    Aws::Kinesis::Model::PutRecordRequest request;
    request.SetStreamName(stream_name);
    
    Aws::Utils::ByteBuffer data(
        reinterpret_cast<const unsigned char*>(message.data()),
        message.size());
    request.SetData(data);
    
    request.SetPartitionKey(generatePartitionKey(message));
    
    auto outcome = client.PutRecord(request);
    
    if (!outcome.IsSuccess())
    {
        const auto & error = outcome.GetError();
        throw Exception(
            ErrorCodes::KINESIS_ERROR,
            "Failed to send message to Kinesis stream ({}): {} ({})",
            stream_name,
            error.GetMessage(),
            error.GetExceptionName());
    }
    
    LOG_TRACE(&Poco::Logger::get("KinesisSink"), 
        "Successfully sent message to shard {}, sequence number: {}", 
        outcome.GetResult().GetShardId(),
        outcome.GetResult().GetSequenceNumber());
}

String KinesisSink::generatePartitionKey(const String & message)
{
    return std::to_string(std::hash<String>{}(message));
    
    // Alternative:
    // 1. Use UUID
    // 2. Use timestamp
    // 3. Extract key value from message
    // 4. Use random value if uniform distribution is needed
}

}

#endif // USE_AWS_KINESIS
