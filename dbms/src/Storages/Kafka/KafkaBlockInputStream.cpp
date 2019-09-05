#include <Storages/Kafka/KafkaBlockInputStream.h>

#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
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

    const auto & limits_ = getLimits();
    const size_t poll_timeout = buffer->pollTimeout();
    size_t rows_portion_size = poll_timeout ? std::min<size_t>(max_block_size, limits_.max_execution_time.totalMilliseconds() / poll_timeout) : max_block_size;
    rows_portion_size = std::max(rows_portion_size, 1ul);

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

    auto child = FormatFactory::instance().getInput(
        storage.getFormatName(), *buffer, non_virtual_header, context, max_block_size, rows_portion_size, read_callback);
    child->setLimits(limits_);
    addChild(child);

    broken = true;
}

Block KafkaBlockInputStream::readImpl()
{
    if (!buffer)
        return Block();

    Block block = children.back()->read();
    if (!block)
        return block;

    Block virtual_block = storage.getSampleBlockForColumns({"_topic", "_key", "_offset", "_partition", "_timestamp"}).cloneWithColumns(std::move(virtual_columns));
    virtual_columns = storage.getSampleBlockForColumns({"_topic", "_key", "_offset", "_partition", "_timestamp"}).cloneEmptyColumns();

    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        block.insert(column);

    /// FIXME: materialize MATERIALIZED columns here.

    return ConvertingBlockInputStream(
               context, std::make_shared<OneBlockInputStream>(block), getHeader(), ConvertingBlockInputStream::MatchColumnsMode::Name)
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
