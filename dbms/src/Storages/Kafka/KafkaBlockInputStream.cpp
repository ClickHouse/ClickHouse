#include <Storages/Kafka/KafkaBlockInputStream.h>

#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>

namespace DB
{

KafkaBlockInputStream::KafkaBlockInputStream(
    StorageKafka & storage_, const Context & context_, const Names & columns, size_t max_block_size_, bool commit_in_suffix_)
    : storage(storage_), context(context_), column_names(columns), max_block_size(max_block_size_), commit_in_suffix(commit_in_suffix_)
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

    auto merge_blocks = [] (Block & block1, Block && block2)
    {
        if (!block1)
        {
            // Need to make sure that resulting block has the same structure
            block1 = std::move(block2);
            return;
        }

        if (!block2)
            return;

        auto columns1 = block1.mutateColumns();
        auto columns2 = block2.mutateColumns();
        for (size_t i = 0, s = columns1.size(); i < s; ++i)
            columns1[i]->insertRangeFrom(*columns2[i], 0, columns2[i]->size());
        block1.setColumns(std::move(columns1));
    };

    auto read_kafka_message = [&, this]
    {
        Block result;
        auto child = FormatFactory::instance().getInput(
            storage.getFormatName(), *buffer, non_virtual_header, context, max_block_size, read_callback);
        const auto virtual_header = storage.getSampleBlockForColumns({"_topic", "_key", "_offset", "_partition", "_timestamp"});

        while (auto block = child->read())
        {
            auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));
            virtual_columns = virtual_header.cloneEmptyColumns();

            for (const auto & column : virtual_block.getColumnsWithTypeAndName())
                block.insert(column);

            /// FIXME: materialize MATERIALIZED columns here.

            merge_blocks(result, std::move(block));
        }

        return result;
    };

    Block single_block;

    UInt64 total_rows = 0;
    while (total_rows < max_block_size)
    {
        auto new_block = read_kafka_message();
        auto new_rows = new_block.rows();
        total_rows += new_rows;
        merge_blocks(single_block, std::move(new_block));

        buffer->allowNext();

        if (!new_rows || !checkTimeLimit())
            break;
    }

    if (!single_block)
        return Block();

    return ConvertingBlockInputStream(
               context,
               std::make_shared<OneBlockInputStream>(single_block),
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
