#include <Storages/Kafka/KafkaBlockInputStream.h>

#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>

#include <common/logger_useful.h>

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

    virtual_columns = virtual_header.cloneEmptyColumns();
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

    Block single_block;
    MutableColumns single_block_columns;

    auto input_format = FormatFactory::instance().getInputFormat(
        storage.getFormatName(), *buffer, non_virtual_header, context, max_block_size, read_callback);

    InputPort port(input_format->getPort().getHeader(), input_format.get());
    connect(input_format->getPort(), port);
    port.setNeeded();

    auto read_kafka_message = [&]
    {
        size_t new_rows = 0;

        while (true)
        {
            auto status = input_format->prepare();

            switch (status)
            {
                case IProcessor::Status::Ready:
                    input_format->work();
                    break;

                case IProcessor::Status::Finished:
                    input_format->resetParser();
                    return new_rows;

                case IProcessor::Status::PortFull:
                {
                    auto block = input_format->getPort().getHeader().cloneWithColumns(port.pull().detachColumns());
                    new_rows = new_rows + block.rows();

                    /// FIXME: materialize MATERIALIZED columns here.
                    if (!single_block)
                    {
                        single_block = std::move(block);
                        single_block_columns = single_block.mutateColumns();
                    } else {
                        // assertBlocksHaveEqualStructure(single_block, block, "KafkaBlockInputStream");
                        auto block_columns = block.getColumns();
                        for (size_t i = 0, s = block_columns.size(); i < s; ++i)
                            single_block_columns[i]->insertRangeFrom(*block_columns[i], 0, block_columns[i]->size());
                    }
                    break;
                }
                case IProcessor::Status::NeedData:
                case IProcessor::Status::Async:
                case IProcessor::Status::Wait:
                case IProcessor::Status::ExpandPipeline:
                    throw Exception("Source processor returned status " + IProcessor::statusToName(status), ErrorCodes::LOGICAL_ERROR);
            }
        }
    };

    size_t total_rows = 0;
    while (total_rows < max_block_size)
    {
        auto new_rows = read_kafka_message();
        total_rows = total_rows + new_rows;
        buffer->allowNext();
        if (!new_rows || !checkTimeLimit())
            break;
    }

    if (total_rows == 0)
        return Block();

    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));
    // LOG_TRACE(&Poco::Logger::get("kkkkkkk"), "virtual_block have now " << virtual_block.rows() << " rows");
    // LOG_TRACE(&Poco::Logger::get("kkkkkkk"), "single_block have now " << single_block.rows() << " rows");

    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        single_block.insert(column);

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
