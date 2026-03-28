#include <Storages/SQS/SQSSink.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromString.h>

#include "config.h"

#if USE_AWS_SQS

#include <aws/sqs/model/SendMessageRequest.h>
#include <aws/sqs/model/SendMessageResult.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_SQS;
    extern const int BAD_ARGUMENTS;
}

SQSSink::SQSSink(
    StorageMetadataPtr metadata_snapshot,
    const Aws::SQS::SQSClient & client_,
    const String & queue_url_,
    const String & format_name_,
    size_t max_rows_per_message_,
    ContextPtr context_)
    : SinkToStorage(metadata_snapshot->getSampleBlock())
    , client(client_)
    , queue_url(queue_url_)
    , format_name(format_name_)
    , max_rows_per_message(max_rows_per_message_)
    , context(context_)
    , log(getLogger("SQSSink"))
{
    if (queue_url.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SQS queue URL cannot be empty");
    if (max_rows_per_message == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "sqs_max_rows_per_message cannot be zero");
}

void SQSSink::consume(Chunk & chunk)
{
    if (chunk.getNumRows() == 0)
        return;

    Block block = getHeader().cloneWithColumns(chunk.detachColumns());
    const size_t rows = block.rows();
    total_rows += rows;

    for (size_t offset = 0; offset < rows; offset += max_rows_per_message)
    {
        const size_t chunk_size = std::min(max_rows_per_message, rows - offset);

        Columns current_columns;
        current_columns.reserve(block.columns());
        for (const auto & column : block.getColumns())
            current_columns.emplace_back(column->cut(offset, chunk_size));

        Block current_block = block.cloneEmpty();
        current_block.setColumns(current_columns);

        String message;
        WriteBufferFromString buf(message);
        auto output_format = FormatFactory::instance().getOutputFormat(format_name, buf, current_block, context);
        output_format->write(current_block);
        output_format->finalize();
        buf.finalize();

        sendMessage(message);
    }
}

void SQSSink::onFinish()
{
    if (total_rows > 0)
        LOG_DEBUG(log, "Finished sending {} rows to SQS queue {}", total_rows, queue_url);
}

void SQSSink::sendMessage(const String & message)
{
    Aws::SQS::Model::SendMessageRequest request;
    request.SetQueueUrl(queue_url);
    request.SetMessageBody(message);

    auto outcome = client.SendMessage(request);
    if (!outcome.IsSuccess())
    {
        const auto & error = outcome.GetError();
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_SQS,
            "Failed to send message to SQS queue {}: {} ({})",
            queue_url,
            error.GetMessage(),
            error.GetExceptionName());
    }
}

}

#endif // USE_AWS_SQS
