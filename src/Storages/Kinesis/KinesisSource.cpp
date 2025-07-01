#include <Storages/Kinesis/KinesisSource.h>
#include <Storages/Kinesis/KinesisConsumer.h>

#include <Common/logger_useful.h>
#include <IO/ReadBufferFromString.h>
#include <Formats/FormatFactory.h>
#include <Columns/IColumn.h>

#include "config.h"

#if USE_AWS_KINESIS

namespace DB
{

KinesisSource::KinesisSource(
    StorageKinesis & storage_,
    const StorageSnapshotPtr & /* storage_snapshot_ */,
    const String & format_,
    const Block & sample_block_,
    UInt64 max_block_size_,
    UInt64 max_execution_time_ms_,
    ContextPtr context_,
    bool is_read_)
    : ISource(sample_block_.getColumnsWithTypeAndName())
    , storage(storage_)
    , consumer(storage.popConsumer())
    , sample_block(sample_block_)
    , context(context_)
    , format(format_)
    , max_block_size(max_block_size_)
    , max_execution_time_ms(max_execution_time_ms_)
    , is_read(is_read_)
{
}

Chunk KinesisSource::generate()
{
    if (!consumer)
        consumer = storage.popConsumer();
    if (!consumer || is_finished)
        return {};
    
    is_finished = true;

    size_t total_rows = 0;
    MutableColumns columns = sample_block.cloneEmptyColumns();
    
    while (total_rows < max_block_size)
    {
        if (!consumer->isRunning())
            break;

        bool is_time_limit_exceeded = false;
        if (max_execution_time_ms)
        {
            uint64_t elapsed_time_ms = total_stopwatch.elapsedMilliseconds();
            is_time_limit_exceeded = max_execution_time_ms <= elapsed_time_ms;
        }
        if (is_time_limit_exceeded)
            break;
        
        auto message_opt = consumer->getMessage();
        if (!message_opt)
        {
            if (!consumer->receive())
                break;
            message_opt = consumer->getMessage();
            if (!message_opt)
                break;
        }

        auto message = message_opt.value();
        if (message.data.empty())
            continue;
        
        read_buf = std::make_unique<ReadBufferFromString>(message.data);

        auto input_format = FormatFactory::instance().getInput(
            format, *read_buf, sample_block, context, max_block_size);
        
        if (!input_format)
            continue;
        
        Chunk chunk;
        try 
        {
            chunk = input_format->read();
        }
        catch (const Exception & e)
        {
            LOG_ERROR(&Poco::Logger::get("KinesisSource"), "KinesisSource consumer_name: {}, Error reading data from Kinesis message: {}", 
                consumer->consumer_name, e.message());
        }

        if (!chunk.hasRows())
            continue;

        total_rows += chunk.getNumRows();

        for (size_t i = 0; i < columns.size(); ++i)
        {
            const auto & column_name = sample_block.getColumnsWithTypeAndName()[i].name;

            if (column_name.starts_with("_"))
            {
                size_t num_rows = chunk.getNumRows();
                for (size_t row = 0; row < num_rows; ++row)
                {
                    if (column_name == "_sequence_number")
                    {
                        columns[i]->insert(message.sequence_number);
                    }
                    else if (column_name == "_partition_key")
                    {
                        columns[i]->insert(message.partition_key);
                    }
                    else if (column_name == "_shard_id")
                    {
                        columns[i]->insert(message.shard_id);
                    }
                    else if (column_name == "_approximate_arrival_timestamp")
                    {
                        columns[i]->insert(message.approximate_arrival_timestamp);
                    }
                    else if (column_name == "_received_at")
                    {
                        columns[i]->insert(message.received_at);
                    }
                }
            } 
            else
            {
                const auto & src_column = chunk.getColumns()[i];
                if (src_column && columns[i])
                {
                    size_t num_rows = chunk.getNumRows();
                    columns[i]->insertRangeFrom(*src_column, 0, num_rows);
                }
            }
        }
    }

    if (is_read)
        consumer->commit();

    return Chunk(std::move(columns), total_rows);
}

void KinesisSource::commit()
{
    if (consumer)
        consumer->commit();
}

void KinesisSource::rollback()
{
    if (consumer)
        consumer->rollback();
}

KinesisSource::~KinesisSource()
{
    if (consumer)
        storage.pushConsumer(consumer);
}

}

#endif // USE_AWS_KINESIS
