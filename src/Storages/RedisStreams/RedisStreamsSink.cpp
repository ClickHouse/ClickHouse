#include <Storages/RedisStreams/RedisStreamsSink.h>

#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/RedisStreams/WriteBufferToRedisStreams.h>

namespace DB
{

RedisStreamsSink::RedisStreamsSink(
    StorageRedisStreams & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const ContextPtr & context_)
    : SinkToStorage(metadata_snapshot_->getSampleBlockNonMaterialized())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
{
}

void RedisStreamsSink::onStart()
{
    buffer = storage.createWriteBuffer();

    auto format_settings = getFormatSettings(context);
    format_settings.protobuf.allow_multiple_rows_without_delimiter = true;

    format = FormatFactory::instance().getOutputFormat(storage.getFormatName(), *buffer,
        getHeader(), context,
        [this](const Columns & /* columns */, size_t /* rows */)
        {
            buffer->countRow();
        },
        format_settings);
}

void RedisStreamsSink::consume(Chunk chunk)
{
    format->write(getHeader().cloneWithColumns(chunk.detachColumns()));
}

void RedisStreamsSink::onFinish()
{
    if (format)
        format->finalize();
}

}
