#include <Storages/RabbitMQ/RabbitMQBlockInputStream.h>

#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>

namespace DB
{

RabbitMQBlockInputStream::RabbitMQBlockInputStream(
    StorageRabbitMQ & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    ContextPtr context_,
    const Names & columns,
    size_t max_block_size_,
    bool ack_in_suffix_)
    : storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , column_names(columns)
    , max_block_size(max_block_size_)
    , ack_in_suffix(ack_in_suffix_)
    , non_virtual_header(metadata_snapshot->getSampleBlockNonMaterialized())
    , sample_block(non_virtual_header)
    , virtual_header(metadata_snapshot->getSampleBlockForColumns(
                {"_exchange_name", "_channel_id", "_delivery_tag", "_redelivered", "_message_id", "_timestamp"},
                storage.getVirtuals(), storage.getStorageID()))
{
    for (const auto & column : virtual_header)
        sample_block.insert(column);
}


RabbitMQBlockInputStream::~RabbitMQBlockInputStream()
{
    if (!buffer)
        return;

    storage.pushReadBuffer(buffer);
}


Block RabbitMQBlockInputStream::getHeader() const
{
    return sample_block;
}


void RabbitMQBlockInputStream::readPrefixImpl()
{
    auto timeout = std::chrono::milliseconds(context->getSettingsRef().rabbitmq_max_wait_ms.totalMilliseconds());
    buffer = storage.popReadBuffer(timeout);
}


bool RabbitMQBlockInputStream::needChannelUpdate()
{
    if (!buffer)
        return false;

    return buffer->needChannelUpdate();
}


void RabbitMQBlockInputStream::updateChannel()
{
    if (!buffer)
        return;

    buffer->updateAckTracker();

    if (storage.updateChannel(buffer->getChannel()))
        buffer->setupChannel();
}


Block RabbitMQBlockInputStream::readImpl()
{
    if (!buffer || finished)
        return Block();

    finished = true;

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
        return Block();

    auto result_block  = non_virtual_header.cloneWithColumns(executor.getResultColumns());
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));

    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    return result_block;
}


void RabbitMQBlockInputStream::readSuffixImpl()
{
    if (ack_in_suffix)
        sendAck();
}

bool RabbitMQBlockInputStream::sendAck()
{
    if (!buffer)
        return false;

    if (!buffer->ackMessages())
        return false;

    return true;
}

}
