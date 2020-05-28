#include <Storages/Kafka/KafkaBlockInputStream.h>

#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
KafkaBlockInputStream::KafkaBlockInputStream(
    StorageKafka & storage_, const Context & context_, const Names & columns, size_t max_block_size_, bool commit_in_suffix_)
    : storage(storage_)
    , context(context_)
    , column_names(columns)
    , max_block_size(max_block_size_)
    , commit_in_suffix(commit_in_suffix_)
    , non_virtual_header(storage.getSampleBlockNonMaterialized())
    , virtual_header(storage.getSampleBlockForColumns({"_topic", "_key", "_offset", "_partition", "_timestamp","_timestamp_ms"}))

{
    context.setSetting("input_format_skip_unknown_fields", 1u); // Always skip unknown fields regardless of the context (JSON or TSKV)
    context.setSetting("input_format_allow_errors_ratio", 0.);
    context.setSetting("input_format_allow_errors_num", storage.skipBroken());

    if (!storage.getSchemaName().empty())
        context.setSetting("format_schema", storage.getSchemaName());
}

KafkaBlockInputStream::~KafkaBlockInputStream()
{
    if (!buffer)
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

    if (!buffer)
        return;

    buffer->subscribe();

    broken = true;
}

Block KafkaBlockInputStream::readImpl()
{
    if (!buffer || finished)
        return Block();

    finished = true;
    // now it's one-time usage InputStream
    // one block of the needed size (or with desired flush timeout) is formed in one internal iteration
    // otherwise external iteration will reuse that and logic will became even more fuzzy

    MutableColumns result_columns  = non_virtual_header.cloneEmptyColumns();
    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    auto input_format = FormatFactory::instance().getInputFormat(
        storage.getFormatName(), *buffer, non_virtual_header, context, max_block_size);

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
                    auto chunk = port.pull();

                    // that was returning bad value before https://github.com/ClickHouse/ClickHouse/pull/8005
                    // if will be backported should go together with #8005
                    auto chunk_rows = chunk.getNumRows();
                    new_rows += chunk_rows;

                    auto columns = chunk.detachColumns();
                    for (size_t i = 0, s = columns.size(); i < s; ++i)
                    {
                        result_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());
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

    while (true)
    {
        // some formats (like RowBinaryWithNamesAndTypes / CSVWithNames)
        // throw an exception from readPrefix when buffer in empty
        if (buffer->eof())
            break;

        auto new_rows = read_kafka_message();

        buffer->storeLastReadMessageOffset();

        auto topic         = buffer->currentTopic();
        auto key           = buffer->currentKey();
        auto offset        = buffer->currentOffset();
        auto partition     = buffer->currentPartition();
        auto timestamp_raw = buffer->currentTimestamp();

        for (size_t i = 0; i < new_rows; ++i)
        {
            virtual_columns[0]->insert(topic);
            virtual_columns[1]->insert(key);
            virtual_columns[2]->insert(offset);
            virtual_columns[3]->insert(partition);
            if (timestamp_raw)
            {
                auto ts = timestamp_raw->get_timestamp();
                virtual_columns[4]->insert(std::chrono::duration_cast<std::chrono::seconds>(ts).count());
                virtual_columns[5]->insert(DecimalField<Decimal64>(std::chrono::duration_cast<std::chrono::milliseconds>(ts).count(),3));
            }
            else
            {
                virtual_columns[4]->insertDefault();
                virtual_columns[5]->insertDefault();
            }
        }

        total_rows = total_rows + new_rows;
        buffer->allowNext();

        if (buffer->hasMorePolledMessages())
        {
            continue;
        }
        if (total_rows >= max_block_size || !checkTimeLimit())
        {
            break;
        }
    }

    if (buffer->polledDataUnusable() || total_rows == 0)
        return Block();

    /// MATERIALIZED columns can be added here, but I think
    // they are not needed here:
    // and it's misleading to use them here,
    // as columns 'materialized' that way stays 'ephemeral'
    // i.e. will not be stored anythere
    // If needed any extra columns can be added using DEFAULT they can be added at MV level if needed.

    auto result_block  = non_virtual_header.cloneWithColumns(std::move(result_columns));
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));

    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    return ConvertingBlockInputStream(
               std::make_shared<OneBlockInputStream>(result_block),
               getHeader(),
               ConvertingBlockInputStream::MatchColumnsMode::Name)
        .read();
}

void KafkaBlockInputStream::readSuffixImpl()
{
    if (commit_in_suffix)
        commit();
}

void KafkaBlockInputStream::commit()
{
    if (!buffer)
        return;

    buffer->commit();

    broken = false;
}

}
