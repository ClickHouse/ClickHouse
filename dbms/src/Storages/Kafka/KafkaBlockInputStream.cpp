#include <Storages/Kafka/KafkaBlockInputStream.h>

#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>


namespace DB
{
KafkaBlockInputStream::KafkaBlockInputStream(
    StorageKafka & storage_, const Context & context_, const Names & columns, size_t max_block_size_, bool commit_in_suffix_)
    : storage(storage_)
    , context(context_)
    , column_names(columns)
    , max_block_size(max_block_size_)
    , commit_in_suffix(commit_in_suffix_)
    , non_virtual_header(storage.getSampleBlockNonMaterialized()) /// FIXME: add materialized columns support
    , virtual_header(storage.getSampleBlockForColumns({"_topic", "_key", "_offset", "_partition", "_timestamp"}))

{
    context.setSetting("input_format_skip_unknown_fields", 1u); // Always skip unknown fields regardless of the context (JSON or TSKV)
    context.setSetting("input_format_allow_errors_ratio", 0.);
    context.setSetting("input_format_allow_errors_num", storage.skipBroken());

    if (!storage.getSchemaName().empty())
        context.setSetting("format_schema", storage.getSchemaName());
}

KafkaBlockInputStream::~KafkaBlockInputStream()
{
    if (!claimed)
        return;

    if (broken)
        buffer->unsubscribe();

    storage.pushReadBuffer(buffer);
}

Block KafkaBlockInputStream::getHeader() const
{
    return storage.getSampleBlockForColumns(column_names);
}

void KafkaBlockInputStream::readPrefixImpl()
{
    auto timeout = std::chrono::milliseconds(context.getSettingsRef().kafka_max_wait_ms.totalMilliseconds());
    buffer = storage.popReadBuffer(timeout);
    claimed = !!buffer;

    if (!buffer)
        return;

    buffer->subscribe(storage.getTopics());

    broken = true;
}

Block KafkaBlockInputStream::readImpl()
{
    if (!buffer)
        return Block();

    MutableColumns result_columns  = non_virtual_header.cloneEmptyColumns();
    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    auto row_callback = [&]
    {
        virtual_columns[0]->insert(buffer->currentTopic());     // "topic"
        virtual_columns[1]->insert(buffer->currentKey());       // "key"
        virtual_columns[2]->insert(buffer->currentOffset());    // "offset"
        virtual_columns[3]->insert(buffer->currentPartition()); // "partition"

        auto timestamp = buffer->currentTimestamp();
        if (timestamp)
            virtual_columns[4]->insert(std::chrono::duration_cast<std::chrono::seconds>(timestamp->get_timestamp()).count()); // "timestamp"
        else
            virtual_columns[4]->insert(Field());
    };

    auto input_format = FormatFactory::instance().getInputFormat(
        storage.getFormatName(), *buffer, non_virtual_header, context, max_block_size, row_callback);
    auto input_stream = std::make_shared<InputStreamFromInputFormat>(input_format);

    /// Reads all polled messages at once
    auto read_kafka_messages = [&]
    {
        size_t rows = 0;

        while (auto block = input_stream->read())
        {
            rows += block.rows();

            auto columns = block.mutateColumns();
            for (size_t i = 0, s = columns.size(); i < s; ++i)
                result_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());
        }

        return rows;
    };

    size_t total_rows = 0;
    while (total_rows < max_block_size)
    {
        auto new_rows = read_kafka_messages();
        total_rows = total_rows + new_rows;

        buffer->allowPoll();

        if (!new_rows || !checkTimeLimit())
            break;
    }

    if (total_rows == 0)
        return Block();

    auto result_block  = non_virtual_header.cloneWithColumns(std::move(result_columns));
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));

    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    return ConvertingBlockInputStream(
               context,
               std::make_shared<OneBlockInputStream>(result_block),
               getHeader(),
               ConvertingBlockInputStream::MatchColumnsMode::Name)
        .read();
}

void KafkaBlockInputStream::readSuffixImpl()
{
    broken = false;

    if (commit_in_suffix)
        commit();
}

void KafkaBlockInputStream::commit()
{
    if (!buffer)
        return;

    buffer->commit();
}

}
