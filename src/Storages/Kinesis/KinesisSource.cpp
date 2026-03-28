#include <Storages/Kinesis/KinesisSource.h>
#include <Storages/Kinesis/StorageKinesis.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <Processors/Formats/IInputFormat.h>

#include "config.h"

#if USE_AWS_KINESIS

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

KinesisSource::KinesisSource(
    StorageKinesis & storage_,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    const String & format_,
    const Block & sample_block_,
    UInt64 max_block_size_,
    UInt64 max_execution_time_ms_,
    ContextPtr context_,
    bool skip_broken_messages_,
    UInt64 skip_broken_messages_count_)
    : ISource(std::make_shared<const Block>(sample_block_))
    , storage(storage_)
    , sample_block(sample_block_)
    , context(context_)
    , format(format_)
    , max_block_size(max_block_size_)
    , max_execution_time_ms(max_execution_time_ms_)
    , skip_broken_messages(skip_broken_messages_)
    , skip_broken_messages_count(skip_broken_messages_count_)
{
}

KinesisSource::~KinesisSource()
{
    if (consumer)
        storage.pushConsumer(consumer);
}

Chunk KinesisSource::generate()
{
    if (!consumer)
        consumer = storage.popConsumer();

    if (!consumer || is_finished)
        return {};

    is_finished = true;

    size_t total_rows = 0;
    size_t broken_count = 0;
    MutableColumns result_columns = sample_block.cloneEmptyColumns();

    while (total_rows < max_block_size)
    {
        if (consumer->isStopped())
            break;

        if (max_execution_time_ms > 0 && total_stopwatch.elapsedMilliseconds() >= max_execution_time_ms)
            break;

        auto message_opt = consumer->getMessage();
        if (!message_opt)
        {
            if (!consumer->receive())
                break;
            continue;
        }

        auto & message = *message_opt;
        if (message.data.empty())
            continue;

        auto read_buf = std::make_unique<ReadBufferFromString>(message.data);
        auto input_format = FormatFactory::instance().getInput(format, *read_buf, sample_block, context, max_block_size);

        Chunk chunk;
        bool format_error = false;

        try
        {
            chunk = input_format->read();
        }
        catch (const Exception & e)
        {
            LOG_ERROR(getLogger("KinesisSource"), "Error parsing Kinesis record {}/{}: {}",
                message.shard_id, message.sequence_number, e.message());
            format_error = true;
        }

        if (!chunk.hasRows())
            continue;

        if (format_error)
        {
            if (!skip_broken_messages)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Kinesis: Failed to parse record {}/{}. "
                    "Use kinesis_skip_broken_messages to skip broken records.",
                    message.shard_id, message.sequence_number);

            ++broken_count;
            if (skip_broken_messages_count > 0 && broken_count > skip_broken_messages_count)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Kinesis: Too many broken messages (more than {})", skip_broken_messages_count);

            continue;
        }

        const auto & chunk_columns = chunk.getColumns();
        const size_t num_rows = chunk.getNumRows();

        for (size_t i = 0; i < result_columns.size(); ++i)
        {
            const auto & col_name = sample_block.getByPosition(i).name;

            if (col_name == "_sequence_number")
                for (size_t r = 0; r < num_rows; ++r)
                    result_columns[i]->insert(message.sequence_number);
            else if (col_name == "_partition_key")
                for (size_t r = 0; r < num_rows; ++r)
                    result_columns[i]->insert(message.partition_key);
            else if (col_name == "_shard_id")
                for (size_t r = 0; r < num_rows; ++r)
                    result_columns[i]->insert(message.shard_id);
            else if (col_name == "_approximate_arrival_timestamp")
                for (size_t r = 0; r < num_rows; ++r)
                    result_columns[i]->insert(message.approximate_arrival_timestamp);
            else if (i < chunk_columns.size())
                result_columns[i]->insertRangeFrom(*chunk_columns[i], 0, num_rows);
        }

        total_rows += num_rows;
    }

    if (total_rows == 0)
        return {};

    return Chunk(std::move(result_columns), total_rows);
}

}

#endif // USE_AWS_KINESIS
