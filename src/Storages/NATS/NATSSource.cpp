#include <Storages/NATS/NATSSource.h>

#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/NATS/NATSConsumer.h>
#include <IO/EmptyReadBuffer.h>

namespace DB
{

static std::pair<Block, Block> getHeaders(StorageNATS & storage, const StorageSnapshotPtr & storage_snapshot)
{
    auto non_virtual_header = storage_snapshot->metadata->getSampleBlockNonMaterialized();
    auto virtual_header = storage_snapshot->getSampleBlockForColumns(storage.getVirtuals().getNames());

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
    : NATSSource(storage_, storage_snapshot_, getHeaders(storage_, storage_snapshot_), context_, columns, max_block_size_, handle_error_mode_)
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
    storage.incrementReader();
}


NATSSource::~NATSSource()
{
    storage.decrementReader();

    if (!consumer)
        return;

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
        auto timeout = std::chrono::milliseconds(context->getSettingsRef().rabbitmq_max_wait_ms.totalMilliseconds());
        consumer = storage.popConsumer(timeout);
        consumer->subscribe();
    }

    if (!consumer || is_finished)
        return {};

    is_finished = true;

    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();
    EmptyReadBuffer empty_buf;
    auto input_format = FormatFactory::instance().getInput(
        storage.getFormatName(), empty_buf, non_virtual_header, context, max_block_size, std::nullopt, 1);
    std::vector<String> exception_messages;
    size_t total_rows = 0;

    size_t input_format_allow_errors_num = context->getSettingsRef().input_format_allow_errors_num;
    size_t num_rows_with_errors = 0;
    auto on_error = [&](std::exception_ptr e)
    {
        if (handle_error_mode != StreamingHandleErrorMode::STREAM)
        {
            if (input_format_allow_errors_num >= ++num_rows_with_errors)
                return;

            std::rethrow_exception(e);
        }
        exception_messages.emplace_back(getExceptionMessage(e, false));
    };

    StreamingFormatExecutor executor(non_virtual_header, input_format, on_error);

    while (true)
    {
        if (consumer->queueEmpty())
            break;

        exception_messages.clear();
        size_t new_rows = 0;
        if (auto buf = consumer->consume())
            new_rows = executor.execute(*buf);

        if (new_rows || !exception_messages.empty())
        {
            size_t new_rows_with_errors = new_rows + exception_messages.size();
            auto subject = consumer->getSubject();
            virtual_columns[0]->insertMany(subject, new_rows_with_errors);
            if (handle_error_mode == StreamingHandleErrorMode::STREAM)
            {
                virtual_columns[1]->insertManyDefaults(new_rows);
                virtual_columns[2]->insertManyDefaults(new_rows);

                /// FIXME: we can do better, by reusing reason/row from ErrorEntry
                const auto & current_message = consumer->getCurrentMessage();
                virtual_columns[1]->insertMany(Field(current_message.data(), current_message.size()), exception_messages.size());

                for (const auto & exception_message : exception_messages)
                {
                    virtual_columns[2]->insertData(exception_message.data(), exception_message.size());
                }
            }

            total_rows += new_rows_with_errors;
        }

        if (total_rows >= max_block_size || consumer->queueEmpty() || consumer->isConsumerStopped() || !checkTimeLimit())
            break;
    }

    if (total_rows == 0)
        return {};

    auto result_columns = executor.getResultColumns();

    size_t result_block_rows = result_columns.front()->size();
    size_t virtual_block_rows = virtual_columns.front()->size();
    size_t errors = virtual_block_rows - result_block_rows;
    if (errors)
    {
        for (auto & column : result_columns)
            column->insertManyDefaults(errors);
    }

    for (auto & column : virtual_columns)
        result_columns.push_back(std::move(column));

    return Chunk(std::move(result_columns), total_rows);
}

}
