#include <Storages/Kinesis/KinesisSink.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/IOutputFormat.h>

#include <openssl/md5.h>

#include "config.h"

#if USE_AWS_KINESIS

#include <aws/kinesis/model/PutRecordRequest.h>
#include <aws/kinesis/model/PutRecordResult.h>
#include <aws/core/utils/Array.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_KINESIS;
    extern const int BAD_ARGUMENTS;
}

KinesisSink::KinesisSink(
    StorageMetadataPtr metadata_snapshot,
    std::shared_ptr<Aws::Kinesis::KinesisClient> client_,
    const String & stream_name_,
    const String & format_name_,
    size_t max_rows_per_message_,
    ContextPtr context_)
    : SinkToStorage(std::make_shared<const Block>(metadata_snapshot->getSampleBlock()))
    , client(std::move(client_))
    , stream_name(stream_name_)
    , format_name(format_name_)
    , max_rows_per_message(max_rows_per_message_)
    , context(context_)
    , log(getLogger("KinesisSink"))
{
    if (stream_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Kinesis stream name cannot be empty");
    if (max_rows_per_message == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "kinesis_max_rows_per_message cannot be zero");
}

void KinesisSink::consume(Chunk & chunk)
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

        String data;
        WriteBufferFromString buf(data);
        auto output_format = FormatFactory::instance().getOutputFormat(format_name, buf, current_block, context);
        output_format->write(current_block);
        output_format->finalize();
        buf.finalize();

        sendRecord(data, generatePartitionKey(data));
    }
}

void KinesisSink::onFinish()
{
    if (total_rows > 0)
        LOG_DEBUG(log, "Finished sending {} rows to Kinesis stream {}", total_rows, stream_name);
}

void KinesisSink::sendRecord(const String & data, const String & partition_key)
{
    Aws::Kinesis::Model::PutRecordRequest request;
    request.SetStreamName(stream_name);
    request.SetPartitionKey(partition_key);
    request.SetData(Aws::Utils::ByteBuffer(
        reinterpret_cast<const unsigned char *>(data.data()),
        data.size()));

    auto outcome = client->PutRecord(request);
    if (!outcome.IsSuccess())
    {
        const auto & error = outcome.GetError();
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_KINESIS,
            "Failed to put record to Kinesis stream {}: {} ({})",
            stream_name, error.GetMessage(), error.GetExceptionName());
    }
}

String KinesisSink::generatePartitionKey(const String & data)
{
    unsigned char digest[MD5_DIGEST_LENGTH];
    MD5(reinterpret_cast<const unsigned char *>(data.data()), data.size(), digest);

    char hex[33];
    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i)
        snprintf(hex + i * 2, 3, "%02x", digest[i]);
    hex[32] = '\0';

    return String(hex);
}

}

#endif // USE_AWS_KINESIS
