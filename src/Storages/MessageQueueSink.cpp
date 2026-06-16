#include <Storages/MessageQueueSink.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <numeric>

namespace DB
{

MessageQueueSink::MessageQueueSink(
    SharedHeader header,
    const String & format_name_,
    size_t max_rows_per_message_,
    std::unique_ptr<IMessageProducer> producer_,
    const String & storage_name_,
    const ContextPtr & context_)
    : MessageQueueSink(
          header,
          header,
          [&]()
          {
              std::vector<size_t> identity_indices(header->columns());
              std::iota(identity_indices.begin(), identity_indices.end(), size_t{0});
              return identity_indices;
          }(),
          format_name_,
          max_rows_per_message_,
          std::move(producer_),
          storage_name_,
          context_)
{
}

MessageQueueSink::MessageQueueSink(
    SharedHeader header,
    SharedHeader format_header_,
    std::vector<size_t> format_column_indices_,
    const String & format_name_,
    size_t max_rows_per_message_,
    std::unique_ptr<IMessageProducer> producer_,
    const String & storage_name_,
    const ContextPtr & context_)
    : SinkToStorage(header)
    , format_name(format_name_)
    , max_rows_per_message(max_rows_per_message_)
    , format_header(std::move(format_header_))
    , format_column_indices(std::move(format_column_indices_))
    , producer(std::move(producer_))
    , storage_name(storage_name_)
    , context(context_)
{
}

MessageQueueSink::~MessageQueueSink()
{
    if (isCancelled())
    {
        if (format)
            format->cancel();

        if (buffer)
            buffer->cancel();

        if (producer)
            producer->cancel();
    }
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

    format = FormatFactory::instance().getOutputFormat(format_name, *buffer, *format_header, context, format_settings);
    row_format = dynamic_cast<IRowOutputFormat *>(format.get());
}

void MessageQueueSink::onFinish()
{
    if (format)
        format->finalize();
    if (buffer)
        buffer->finalize();
    if (producer)
        producer->finish();
}

void MessageQueueSink::onException(std::exception_ptr /* exception */)
{
    onFinish();
}

Columns MessageQueueSink::extractFormatColumns(const Columns & columns) const
{
    Columns format_columns;
    format_columns.reserve(format_column_indices.size());
    for (size_t index : format_column_indices)
        format_columns.push_back(columns[index]);
    return format_columns;
}

void MessageQueueSink::consume(Chunk & chunk)
{
    const auto & columns = chunk.getColumns();

    if (columns.empty())
        return;

    const auto format_columns = extractFormatColumns(columns);

    /// The formatter might hold pointers to buffer (e.g. if PeekableWriteBuffer is used), which means the formatter
    /// needs to be reset after buffer might reallocate its memory. In this exact case after restarting the buffer.
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
                row_format->writeRow(format_columns, row);
            }
            row_format->finalize();
            buffer->finalize();
            producer->produce(buffer->str(), i, columns, row - 1);
            /// Reallocate buffer if it's capacity is large then DBMS_DEFAULT_BUFFER_SIZE,
            /// because most likely in this case we serialized abnormally large row
            /// and won't need this large allocated buffer anymore.
            buffer->restart(DBMS_DEFAULT_BUFFER_SIZE);
            row_format->resetFormatter();
        }
    }
    else
    {
        format->write(format_header->cloneWithColumns(format_columns));
        format->finalize();
        buffer->finalize();
        producer->produce(buffer->str(), chunk.getNumRows(), columns, chunk.getNumRows() - 1);
        buffer->restart();
        format->resetFormatter();
    }
}

}
