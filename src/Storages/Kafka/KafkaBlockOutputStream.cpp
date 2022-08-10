#include <Storages/Kafka/KafkaBlockOutputStream.h>

#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/Kafka/WriteBufferToKafkaProducer.h>

namespace DB
{

KafkaSink::KafkaSink(
    StorageKafka & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const ContextPtr & context_)
    : SinkToStorage(metadata_snapshot_->getSampleBlockNonMaterialized())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
{
}

void KafkaSink::onStart()
{
    buffer = storage.createWriteBuffer(getHeader());

    auto format_settings = getFormatSettings(context);
    format_settings.protobuf.allow_multiple_rows_without_delimiter = true;

    format = FormatFactory::instance().getOutputFormat(storage.getFormatName(), *buffer,
        getHeader(), context,
        [this](const Columns & columns, size_t row)
        {
            buffer->countRow(columns, row);
        },
        format_settings);
}

void KafkaSink::consume(Chunk chunk)
{
    format->write(getHeader().cloneWithColumns(chunk.detachColumns()));
}

void KafkaSink::onFinish()
{
    if (format)
        format->finalize();
    //flush();

    if (buffer)
        buffer->flush();
}

}
