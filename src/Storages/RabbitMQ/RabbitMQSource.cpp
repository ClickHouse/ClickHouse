#include <Storages/RabbitMQ/RabbitMQSource.h>

#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>

namespace DB
{

static std::pair<Block, Block> getHeaders(StorageRabbitMQ & storage, const StorageMetadataPtr & metadata_snapshot)
{
    auto non_virtual_header = metadata_snapshot->getSampleBlockNonMaterialized();
    auto virtual_header = metadata_snapshot->getSampleBlockForColumns(
                {"_exchange_name", "_channel_id", "_delivery_tag", "_redelivered", "_message_id", "_timestamp"},
                storage.getVirtuals(), storage.getStorageID());

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
    const StorageMetadataPtr & metadata_snapshot_,
    ContextPtr context_,
    const Names & columns,
    size_t max_block_size_,
    bool ack_in_suffix_)
    : RabbitMQSource(
        storage_,
        metadata_snapshot_,
        getHeaders(storage_, metadata_snapshot_),
        context_,
        columns,
        max_block_size_,
        ack_in_suffix_)
{
}

RabbitMQSource::RabbitMQSource(
    StorageRabbitMQ & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    std::pair<Block, Block> headers,
    ContextPtr context_,
    const Names & columns,
    size_t max_block_size_,
    bool ack_in_suffix_)
    : SourceWithProgress(getSampleBlock(headers.first, headers.second))
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , column_names(columns)
    , max_block_size(max_block_size_)
    , ack_in_suffix(ack_in_suffix_)
    , non_virtual_header(std::move(headers.first))
    , virtual_header(std::move(headers.second))
{
}


RabbitMQSource::~RabbitMQSource()
{
    if (!buffer)
        return;

    storage.pushReadBuffer(buffer);
}


bool RabbitMQSource::needChannelUpdate()
{
    if (!buffer)
        return false;

    return buffer->needChannelUpdate();
}


void RabbitMQSource::updateChannel()
{
    if (!buffer)
        return;

    buffer->updateAckTracker();

    if (storage.updateChannel(buffer->getChannel()))
        buffer->setupChannel();
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
    if (!buffer)
    {
        auto timeout = std::chrono::milliseconds(context->getSettingsRef().rabbitmq_max_wait_ms.totalMilliseconds());
        buffer = storage.popReadBuffer(timeout);
    }

    if (!buffer || is_finished)
        return {};

    is_finished = true;

    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();
    auto input_format = FormatFactory::instance().getInputFormat(
            storage.getFormatName(), *buffer, non_virtual_header, context, max_block_size);

    StreamingFormatExecutor executor(non_virtual_header, input_format);

    size_t total_rows = 0;

    while (true)
    {
        if (buffer->eof())
            break;

        auto new_rows = executor.execute();

        if (new_rows)
        {
            auto exchange_name = storage.getExchange();
            auto channel_id = buffer->getChannelID();
            auto delivery_tag = buffer->getDeliveryTag();
            auto redelivered = buffer->getRedelivered();
            auto message_id = buffer->getMessageID();
            auto timestamp = buffer->getTimestamp();

            buffer->updateAckTracker({delivery_tag, channel_id});

            for (size_t i = 0; i < new_rows; ++i)
            {
                virtual_columns[0]->insert(exchange_name);
                virtual_columns[1]->insert(channel_id);
                virtual_columns[2]->insert(delivery_tag);
                virtual_columns[3]->insert(redelivered);
                virtual_columns[4]->insert(message_id);
                virtual_columns[5]->insert(timestamp);
            }

            total_rows = total_rows + new_rows;
        }

        buffer->allowNext();

        if (total_rows >= max_block_size || buffer->queueEmpty() || buffer->isConsumerStopped() || !checkTimeLimit())
            break;
    }

    if (total_rows == 0)
        return {};

    auto result_columns  = executor.getResultColumns();
    for (auto & column : virtual_columns)
        result_columns.push_back(std::move(column));

    return Chunk(std::move(result_columns), total_rows);
}


bool RabbitMQSource::sendAck()
{
    if (!buffer)
        return false;

    if (!buffer->ackMessages())
        return false;

    return true;
}

}
