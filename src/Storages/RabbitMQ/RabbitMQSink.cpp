#include <Storages/RabbitMQ/RabbitMQSink.h>
#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>
#include <Formats/FormatFactory.h>
#include <common/logger_useful.h>


namespace DB
{

RabbitMQSink::RabbitMQSink(
    StorageRabbitMQ & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    ContextPtr context_)
    : SinkToStorage(metadata_snapshot_->getSampleBlockNonMaterialized())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
{
}


void RabbitMQSink::onStart()
{
    if (!storage.exchangeRemoved())
        storage.unbindExchange();

    buffer = storage.createWriteBuffer();
    buffer->activateWriting();

    auto format_settings = getFormatSettings(context);
    format_settings.protobuf.allow_multiple_rows_without_delimiter = true;

    child = FormatFactory::instance().getOutputStream(storage.getFormatName(), *buffer,
        getPort().getHeader(), context,
        [this](const Columns & /* columns */, size_t /* rows */)
        {
            buffer->countRow();
        },
        format_settings);
}


void RabbitMQSink::consume(Chunk chunk)
{
    child->write(getPort().getHeader().cloneWithColumns(chunk.detachColumns()));
}


void RabbitMQSink::onFinish()
{
    child->writeSuffix();

    if (buffer)
        buffer->updateMaxWait();
}

}
