#include <Storages/RabbitMQ/RabbitMQBlockOutputStream.h>
#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>
#include <Formats/FormatFactory.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern int CANNOT_CREATE_IO_BUFFER;
}


RabbitMQBlockOutputStream::RabbitMQBlockOutputStream(
        StorageRabbitMQ & storage_, const Context & context_, Poco::Logger * log_) : storage(storage_), context(context_), log(log_)
{
}


Block RabbitMQBlockOutputStream::getHeader() const
{
    return storage.getSampleBlockNonMaterialized();
}


void RabbitMQBlockOutputStream::writePrefix()
{
    LOG_TRACE(log, "write prefix");

    buffer = storage.createWriteBuffer();
    if (!buffer)
        throw Exception("Failed to create RabbitMQ producer!", ErrorCodes::CANNOT_CREATE_IO_BUFFER);

    child = FormatFactory::instance().getOutput(
            storage.getFormatName(), *buffer, getHeader(), context, [this](const Columns & /*columns*/, size_t /*row*/)
            {
                buffer->count_row();
            });
}


void RabbitMQBlockOutputStream::write(const Block & block)
{
    LOG_TRACE(log, "write");
    child->write(block);
}


void RabbitMQBlockOutputStream::writeSuffix()
{
    LOG_TRACE(log, "write suffix");
    child->writeSuffix();
}
}
