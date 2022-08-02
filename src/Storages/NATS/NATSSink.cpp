#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/NATS/NATSSink.h>
#include <Storages/NATS/StorageNATS.h>
#include <Storages/NATS/WriteBufferToNATSProducer.h>
#include <Common/logger_useful.h>


namespace DB
{

NATSSink::NATSSink(
    StorageNATS & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    ContextPtr context_,
    ProducerBufferPtr buffer_)
    : SinkToStorage(metadata_snapshot_->getSampleBlockNonMaterialized())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , buffer(buffer_)
{
}


void NATSSink::onStart()
{
    buffer->activateWriting();

    auto format_settings = getFormatSettings(context);
    format_settings.protobuf.allow_multiple_rows_without_delimiter = true;

    format = FormatFactory::instance().getOutputFormat(storage.getFormatName(), *buffer, getHeader(), context,
        [this](const Columns & /* columns */, size_t /* rows */)
        {
            buffer->countRow();
        },
        format_settings);
}


void NATSSink::consume(Chunk chunk)
{
    std::lock_guard lock(cancel_mutex);
    if (cancelled)
        return;
    format->write(getHeader().cloneWithColumns(chunk.detachColumns()));
}


void NATSSink::onFinish()
{
    std::lock_guard lock(cancel_mutex);
    finalize();
}

void NATSSink::onException()
{
    std::lock_guard lock(cancel_mutex);
    finalize();
}

void NATSSink::onCancel()
{
    std::lock_guard lock(cancel_mutex);
    finalize();
    cancelled = true;
}

void NATSSink::finalize()
{
    format->finalize();
    buffer->updateMaxWait();
    buffer->finalize();
}

}
