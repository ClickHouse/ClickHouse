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
        StorageRabbitMQ & storage_, const Context & context_) : storage(storage_), context(context_)
{
}


Block RabbitMQBlockOutputStream::getHeader() const
{
    return storage.getSampleBlockNonMaterialized();
}


void RabbitMQBlockOutputStream::writePrefix()
{
    buffer = storage.createWriteBuffer();
    if (!buffer)
        throw Exception("Failed to create RabbitMQ producer!", ErrorCodes::CANNOT_CREATE_IO_BUFFER);

    child = FormatFactory::instance().getOutput(
            storage.getFormatName(), *buffer, getHeader(), context, [this](const Columns & /* columns */, size_t /* rows */)
            {
                buffer->count_row();
            });
}


void RabbitMQBlockOutputStream::write(const Block & block)
{
    child->write(block);

    if (buffer)
        buffer->flush();
}


void RabbitMQBlockOutputStream::writeSuffix()
{
    child->writeSuffix();
}

}
