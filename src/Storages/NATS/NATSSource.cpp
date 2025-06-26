#include <Storages/NATS/NATSSource.h>

#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <IO/EmptyReadBuffer.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/NATS/NATSConsumer.h>

namespace DB
{
namespace Setting
{
    extern const SettingsMilliseconds rabbitmq_max_wait_ms;
}

static std::pair<Block, Block> getHeaders(const StorageSnapshotPtr & storage_snapshot)
{
    auto non_virtual_header = storage_snapshot->metadata->getSampleBlockNonMaterialized();
    auto virtual_header = storage_snapshot->virtual_columns->getSampleBlock();

    return {non_virtual_header, virtual_header};
}

static Block getSampleBlock(const Block & non_virtual_header, const Block & virtual_header)
{
    auto header = non_virtual_header;
    for (const auto & column : virtual_header)
        header.insert(column);

    return header;
}

NATSSource::NATSSource(
    StorageNATS & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    ContextPtr context_,
    const Names & columns,
    size_t max_block_size_,
    StreamingHandleErrorMode handle_error_mode_)
    : NATSSource(storage_, storage_snapshot_, getHeaders(storage_snapshot_), context_, columns, max_block_size_, handle_error_mode_)
{
}

NATSSource::NATSSource(
    StorageNATS & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    std::pair<Block, Block> headers,
    ContextPtr context_,
    const Names & columns,
    size_t max_block_size_,
    StreamingHandleErrorMode handle_error_mode_)
    : ISource(getSampleBlock(headers.first, headers.second))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , context(context_)
    , column_names(columns)
    , max_block_size(max_block_size_)
    , handle_error_mode(handle_error_mode_)
    , non_virtual_header(std::move(headers.first))
    , virtual_header(std::move(headers.second))
{
}


NATSSource::~NATSSource()
{
    if (!consumer)
        return;

    if (unsubscribe_on_destroy)
        consumer->unsubscribe();

    storage.pushConsumer(consumer);
}

bool NATSSource::checkTimeLimit() const
{
    if (max_execution_time != 0)
    {
        auto elapsed_ns = total_stopwatch.elapsed();

        if (elapsed_ns > static_cast<UInt64>(max_execution_time.totalMicroseconds()) * 1000)
            return false;
    }

    return true;
}

Chunk NATSSource::generate()
{
    if (!consumer)
    {
        auto timeout = std::chrono::milliseconds(context->getSettingsRef()[Setting::rabbitmq_max_wait_ms].totalMilliseconds());
        consumer = storage.popConsumer(timeout);

        if (consumer && !consumer->isSubscribed())
        {
            consumer->subscribe();
            unsubscribe_on_destroy = true;
        }
    }

    if (!consumer || is_finished)
        return {};

    is_finished = true;

    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();
    EmptyReadBuffer empty_buf;
    auto input_format = FormatFactory::instance().getInput(
        storage.getFormatName(), empty_buf, non_virtual_header, context, max_block_size, std::nullopt, FormatParserGroup::singleThreaded(context->getSettingsRef()));
    std::optional<String> exception_message;
    size_t total_rows = 0;
    auto on_error = [&](const MutableColumns & result_columns, const ColumnCheckpoints & checkpoints, Exception & e)
    {
        if (handle_error_mode == StreamingHandleErrorMode::STREAM)
        {
            exception_message = e.message();
            for (size_t i = 0; i < result_columns.size(); ++i)
            {
                // We could already push some rows to result_columns before exception, we need to fix it.
                result_columns[i]->rollback(*checkpoints[i]);

                // All data columns will get default value in case of error.
                result_columns[i]->insertDefault();
            }

            return 1;
        }

        throw std::move(e);
    };

    StreamingFormatExecutor executor(non_virtual_header, input_format, on_error);

    while (true)
    {
        if (consumer->queueEmpty())
            break;

        exception_message.reset();
        size_t new_rows = 0;
        if (auto buf = consumer->consume())
            new_rows = executor.execute(*buf);

        if (new_rows)
        {
            auto subject = consumer->getSubject();
            virtual_columns[0]->insertMany(subject, new_rows);
            if (handle_error_mode == StreamingHandleErrorMode::STREAM)
            {
                if (exception_message)
                {
                    const auto & current_message = consumer->getCurrentMessage();
                    virtual_columns[1]->insertData(current_message.data(), current_message.size());
                    virtual_columns[2]->insertData(exception_message->data(), exception_message->size());

                }
                else
                {
                    virtual_columns[1]->insertDefault();
                    virtual_columns[2]->insertDefault();
                }
            }

            total_rows = total_rows + new_rows;
        }

        if (total_rows >= max_block_size || consumer->queueEmpty() || consumer->isConsumerStopped() || !checkTimeLimit())
            break;
    }

    if (total_rows == 0)
        return {};

    auto result_columns = executor.getResultColumns();
    for (auto & column : virtual_columns)
        result_columns.push_back(std::move(column));

    return Chunk(std::move(result_columns), total_rows);
}

}
