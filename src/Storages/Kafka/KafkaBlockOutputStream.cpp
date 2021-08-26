#include <Storages/Kafka/KafkaBlockOutputStream.h>

#include <Formats/FormatFactory.h>
#include <Storages/Kafka/WriteBufferToKafkaProducer.h>

namespace DB
{

KafkaBlockOutputStream::KafkaBlockOutputStream(
    StorageKafka & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const ContextPtr & context_)
    : storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
{
}

Block KafkaBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlockNonMaterialized();
}

void KafkaBlockOutputStream::writePrefix()
{
    buffer = storage.createWriteBuffer(getHeader());

    auto format_settings = getFormatSettings(context);
    format_settings.protobuf.allow_multiple_rows_without_delimiter = true;

    child = FormatFactory::instance().getOutputStream(storage.getFormatName(), *buffer,
        getHeader(), context,
        [this](const Columns & columns, size_t row)
        {
            buffer->countRow(columns, row);
        },
        format_settings);
}

void KafkaBlockOutputStream::write(const Block & block)
{
    child->write(block);
}

void KafkaBlockOutputStream::writeSuffix()
{
    if (child)
        child->writeSuffix();
    flush();
}

void KafkaBlockOutputStream::flush()
{
    if (buffer)
        buffer->flush();
}

}
