#include "KafkaBlockOutputStream.h"

#include <Formats/FormatFactory.h>

namespace DB
{

namespace ErrorCodes
{

extern int CANNOT_CREATE_IO_BUFFER;

}

KafkaBlockOutputStream::KafkaBlockOutputStream(StorageKafka & storage_, const Context & context_) : storage(storage_), context(context_)
{
}

KafkaBlockOutputStream::~KafkaBlockOutputStream()
{
}

Block KafkaBlockOutputStream::getHeader() const
{
    return storage.getSampleBlockNonMaterialized();
}

void KafkaBlockOutputStream::writePrefix()
{
    buffer = storage.createWriteBuffer();
    if (!buffer)
        throw Exception("Failed to create Kafka producer!", ErrorCodes::CANNOT_CREATE_IO_BUFFER);

    child = FormatFactory::instance().getOutput(storage.getFormatName(), *buffer, getHeader(), context, [this]{ buffer->count_row(); });
}

void KafkaBlockOutputStream::write(const Block & block)
{
    child->write(block);
}

void KafkaBlockOutputStream::writeSuffix()
{
    child->writeSuffix();
    flush();
}

void KafkaBlockOutputStream::flush()
{
    if (buffer)
        buffer->flush();
}

}
