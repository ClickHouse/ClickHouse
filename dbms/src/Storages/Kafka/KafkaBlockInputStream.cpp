#include <Storages/Kafka/KafkaBlockInputStream.h>

#include <DataStreams/BlocksBlockInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>

namespace DB
{

KafkaBlockInputStream::KafkaBlockInputStream(
    StorageKafka & storage_, const Context & context_, const Names & columns, size_t max_block_size_)
    : storage(storage_), context(context_), column_names(columns), max_block_size(max_block_size_)
{
    context.setSetting("input_format_skip_unknown_fields", 1u); // Always skip unknown fields regardless of the context (JSON or TSKV)
    context.setSetting("input_format_allow_errors_ratio", 0.);
    context.setSetting("input_format_allow_errors_num", storage.skipBroken());

    if (!storage.getSchemaName().empty())
        context.setSetting("format_schema", storage.getSchemaName());

    virtual_columns = storage.getSampleBlockForColumns({"_topic", "_key", "_offset", "_partition", "_timestamp"}).cloneEmptyColumns();
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

    auto non_virtual_header = storage.getSampleBlockNonMaterialized(); /// FIXME: add materialized columns support
    auto read_callback = [this]
    {
        virtual_columns[0]->insert(buffer->currentTopic());     // "topic"
        virtual_columns[1]->insert(buffer->currentKey());       // "key"
        virtual_columns[2]->insert(buffer->currentOffset());    // "offset"
        virtual_columns[3]->insert(buffer->currentPartition()); // "partition"

        auto timestamp = buffer->currentTimestamp();
        if (timestamp)
            virtual_columns[4]->insert(std::chrono::duration_cast<std::chrono::seconds>(timestamp->get_timestamp()).count()); // "timestamp"
    };

    auto blocks = std::make_shared<Blocks>();

    auto read_whole_message = [&, this]
    {
        auto child = FormatFactory::instance().getInput(
            storage.getFormatName(), *buffer, non_virtual_header, context, max_block_size, read_callback);
        UInt64 rows = 0;

        while (Block block = child->read())
        {
            Block virtual_block = storage.getSampleBlockForColumns({"_topic", "_key", "_offset", "_partition", "_timestamp"})
                                      .cloneWithColumns(std::move(virtual_columns));
            virtual_columns
                = storage.getSampleBlockForColumns({"_topic", "_key", "_offset", "_partition", "_timestamp"}).cloneEmptyColumns();

            for (const auto & column : virtual_block.getColumnsWithTypeAndName())
                block.insert(column);

            /// FIXME: materialize MATERIALIZED columns here.

            rows += block.rows();
            blocks->emplace_back(std::move(block));
        }

        return rows;
    };

    UInt64 total_rows = 0;
    while (total_rows < max_block_size)
    {
        auto new_rows = read_whole_message();
        total_rows += new_rows;

        buffer->allowNext();

        if (!new_rows || !checkTimeLimit())
            break;
    }

    return ConvertingBlockInputStream(
               context,
               std::make_shared<BlocksBlockInputStream>(blocks, getHeader()),
               getHeader(),
               ConvertingBlockInputStream::MatchColumnsMode::Name)
        .read();
}

void KafkaBlockInputStream::readSuffixImpl()
{
    if (!buffer)
        return;

    buffer->commit();

    broken = false;
}

}
