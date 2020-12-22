#include <Storages/RabbitMQ/RabbitMQBlockOutputStream.h>
#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>
#include <Formats/FormatFactory.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_IO_BUFFER;
}


RabbitMQBlockOutputStream::RabbitMQBlockOutputStream(
    StorageRabbitMQ & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const Context & context_)
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
    if (!buffer)
        throw Exception("Failed to create RabbitMQ producer!", ErrorCodes::CANNOT_CREATE_IO_BUFFER);

    buffer->activateWriting();

    auto format_settings = getFormatSettings(context);
    format_settings.protobuf.allow_many_rows_no_delimiters = true;

    child = FormatFactory::instance().getOutput(storage.getFormatName(), *buffer,
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
