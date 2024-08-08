#include <Storages/MessageQueueSink.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

MessageQueueSink::MessageQueueSink(
    const Block & header,
    const String & format_name_,
    size_t max_rows_per_message_,
    std::unique_ptr<IMessageProducer> producer_,
    const String & storage_name_,
    const ContextPtr & context_)
    : SinkToStorage(header), format_name(format_name_), max_rows_per_message(max_rows_per_message_), producer(std::move(producer_)), storage_name(storage_name_), context(context_)
{
}

void MessageQueueSink::onStart()
{
    LOG_TEST(
        getLogger("MessageQueueSink"),
        "Executing startup for MessageQueueSink");

    initialize();
    producer->start(context);

    buffer = std::make_unique<WriteBufferFromOwnString>();

    auto format_settings = getFormatSettings(context);
    format_settings.protobuf.allow_multiple_rows_without_delimiter = true;

    format = FormatFactory::instance().getOutputFormat(format_name, *buffer, getHeader(), context, format_settings);
    row_format = dynamic_cast<IRowOutputFormat *>(format.get());
}

void MessageQueueSink::onFinish()
{
    producer->finish();
}

void MessageQueueSink::consume(Chunk & chunk)
{
    const auto & columns = chunk.getColumns();
    if (columns.empty())
        return;

    if (row_format)
    {
        size_t row = 0;
        while (row < chunk.getNumRows())
        {
            row_format->writePrefixIfNeeded();
            size_t i = 0;
            for (; i < max_rows_per_message && row < chunk.getNumRows(); ++i, ++row)
            {
                if (i != 0)
                    row_format->writeRowBetweenDelimiter();
                row_format->writeRow(columns, row);
            }
            row_format->finalize();
            row_format->resetFormatter();
            producer->produce(buffer->str(), i, columns, row - 1);
            /// Reallocate buffer if it's capacity is large then DBMS_DEFAULT_BUFFER_SIZE,
            /// because most likely in this case we serialized abnormally large row
            /// and won't need this large allocated buffer anymore.
            buffer->restart(DBMS_DEFAULT_BUFFER_SIZE);
        }
    }
    else
    {
        format->write(getHeader().cloneWithColumns(chunk.detachColumns()));
        format->finalize();
        producer->produce(buffer->str(), chunk.getNumRows(), columns, chunk.getNumRows() - 1);
        format->resetFormatter();
        buffer->restart();
    }
}


void MessageQueueSink::onCancel() noexcept
{
    try
    {
        onFinish();
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("MessageQueueSink"), "Error occurs on cancellation.");
    }
}

}
