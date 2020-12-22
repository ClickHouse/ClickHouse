#include <Storages/Kafka/KafkaBlockOutputStream.h>

#include <Formats/FormatFactory.h>
#include <Storages/Kafka/WriteBufferToKafkaProducer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_IO_BUFFER;
}

KafkaBlockOutputStream::KafkaBlockOutputStream(
    StorageKafka & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const std::shared_ptr<Context> & context_)
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
    if (!buffer)
        throw Exception("Failed to create Kafka producer!", ErrorCodes::CANNOT_CREATE_IO_BUFFER);

    auto format_settings = getFormatSettings(*context);
    format_settings.protobuf.allow_many_rows_no_delimiters = true;

    child = FormatFactory::instance().getOutput(storage.getFormatName(), *buffer,
        getHeader(), *context,
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
