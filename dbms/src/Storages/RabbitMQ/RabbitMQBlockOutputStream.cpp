#include <Storages/RabbitMQ/RabbitMQBlockOutputStream.h>
#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>

#include <Formats/FormatFactory.h>


namespace DB
{

    namespace ErrorCodes
    {

        extern int CANNOT_CREATE_IO_BUFFER;

    }

    /// These functions are to be implemeneted

    RabbitMQBlockOutputStream::RabbitMQBlockOutputStream(
            StorageRabbitMQ & storage_, const Context & context_) : storage(storage_), context(context_)
    {
    }

    RabbitMQBlockOutputStream::~RabbitMQBlockOutputStream()
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

        child = FormatFactory::instance().getOutput(storage.getFormatName(), *buffer, getHeader(), context, [this]{ buffer->count_row(); });
    }

    void RabbitMQBlockOutputStream::write(const Block & block)
    {
        child->write(block);
    }
}