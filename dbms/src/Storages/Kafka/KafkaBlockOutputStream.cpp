#include "KafkaBlockOutputStream.h"

#include <Formats/FormatFactory.h>

namespace DB {

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
        return; // TODO: throw exception!

    child = FormatFactory::instance().getOutput(storage.getFormatName(), *buffer, getHeader(), context, [this]{ buffer->count_row(); });
}

void KafkaBlockOutputStream::write(const Block & block)
{
    child->write(block);
}

void KafkaBlockOutputStream::writeSuffix()
{
    child.reset();
    flush();
}

void KafkaBlockOutputStream::flush()
{
    if (buffer)
        buffer->flush();
}

} // namespace DB
