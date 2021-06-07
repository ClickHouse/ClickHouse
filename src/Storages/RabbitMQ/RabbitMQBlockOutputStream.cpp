#include <Storages/RabbitMQ/RabbitMQBlockOutputStream.h>
#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>
#include <Formats/FormatFactory.h>
#include <common/logger_useful.h>


namespace DB
{

RabbitMQBlockOutputStream::RabbitMQBlockOutputStream(
    StorageRabbitMQ & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    ContextPtr context_)
    : storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
{
}


Block RabbitMQBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlockNonMaterialized();
}


void RabbitMQBlockOutputStream::writePrefix()
{
    if (!storage.exchangeRemoved())
        storage.unbindExchange();

    buffer = storage.createWriteBuffer();
    buffer->activateWriting();

    auto format_settings = getFormatSettings(context);
    format_settings.protobuf.allow_multiple_rows_without_delimiter = true;

    child = FormatFactory::instance().getOutputStream(storage.getFormatName(), *buffer,
        getHeader(), context,
        [this](const Columns & /* columns */, size_t /* rows */)
        {
            buffer->countRow();
        },
        format_settings);
}


void RabbitMQBlockOutputStream::write(const Block & block)
{
    child->write(block);
}


void RabbitMQBlockOutputStream::writeSuffix()
{
    child->writeSuffix();

    if (buffer)
        buffer->updateMaxWait();
}

}
