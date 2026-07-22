#include <Storages/Pulsar/PulsarSource.h>

#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/EmptyReadBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/StorageSnapshot.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace Setting
{
    extern const SettingsMilliseconds pulsar_max_wait_ms;
}

PulsarSource::PulsarSource(
    StoragePulsar & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    const Names & columns_,
    size_t max_block_size_,
    LoggerPtr log_,
    UInt64 max_execution_time_,
    bool commit_in_suffix_)
    : ISource(std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(columns_)))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , context(context_)
    , column_names(columns_)
    , max_block_size(max_block_size_)
    , log(log_)
    , max_execution_time(max_execution_time_)
    , handle_error_mode(storage.getStreamingHandleErrorMode())
    , commit_in_suffix(commit_in_suffix_)
    , non_virtual_header(storage_snapshot->metadata->getSampleBlockNonMaterialized())
    , virtual_header(storage_snapshot->metadata->virtuals.getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::Reader))
{
}

PulsarSource::~PulsarSource()
{
    if (consumer)
        storage.pushConsumer(consumer);
}

Chunk PulsarSource::generate()
{
    auto chunk = generateImpl();
    if (!chunk && commit_in_suffix)
        commit();

    return chunk;
}

void PulsarSource::commit()
{
    if (consumer)
        consumer->commit();
}

void PulsarSource::rollback()
{
    if (consumer)
        consumer->rollback();
}

Chunk PulsarSource::generateImpl()
{
    if (!consumer)
    {
        auto timeout = std::chrono::milliseconds(context->getSettingsRef()[Setting::pulsar_max_wait_ms].totalMilliseconds());
        consumer = storage.popConsumer(timeout);
    }

    if (is_finished || !consumer)
        return {};

    Stopwatch stopwatch;

    is_finished = true;
    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    auto put_error_to_stream = handle_error_mode == StreamingHandleErrorMode::STREAM;

    EmptyReadBuffer empty_buf;
    auto input_format = FormatFactory::instance().getInput(
        storage.getFormatName(),
        empty_buf,
        non_virtual_header,
        context,
        max_block_size,
        std::nullopt,
        FormatParserSharedResources::singleThreaded(context->getSettingsRef()));

    std::optional<std::string> exception_message;
    size_t total_rows = 0;

    auto on_error = [&](const MutableColumns & result_columns, const ColumnCheckpoints & checkpoints, Exception & e)
    {
        if (put_error_to_stream)
        {
            exception_message = e.message();
            for (size_t i = 0; i < result_columns.size(); ++i)
            {
                // We could already push some rows to result_columns
                // before exception, we need to fix it.
                result_columns[i]->rollback(*checkpoints[i]);

                // all data columns will get default value in case of error
                result_columns[i]->insertDefault();
            }

            return 1;
        }
        else
        {
            throw std::move(e);
        }
    };

    StreamingFormatExecutor executor(non_virtual_header, input_format, std::move(on_error));

    /// An exit condition for reading from an empty topic: without it, a direct SELECT
    /// (which has no time limit) would poll forever. Mirrors Kafka's failed-poll counter.
    static constexpr size_t MAX_FAILED_POLL_ATTEMPTS = 10;
    size_t failed_poll_attempts = 0;

    while (true)
    {
        size_t new_rows = 0;
        exception_message.reset();
        if (auto buf = consumer->consume())
            new_rows = executor.execute(*buf);
        else
            ++failed_poll_attempts;

        if (new_rows)
        {
            auto topic = consumer->currentTopic();
            auto ordering_key = consumer->currentOrderingKey();
            auto partition_key = consumer->currentPartitionKey();
            auto timestamp_raw = consumer->currentTimestamp();

            for (size_t i = 0; i < new_rows; ++i)
            {
                virtual_columns[0]->insert(topic);
                virtual_columns[1]->insert(ordering_key);
                virtual_columns[2]->insert(partition_key);
                if (timestamp_raw)
                {
                    auto ts = std::chrono::milliseconds(timestamp_raw);
                    virtual_columns[3]->insert(std::chrono::duration_cast<std::chrono::seconds>(ts).count());
                    virtual_columns[4]->insert(
                        DecimalField<Decimal64>(std::chrono::duration_cast<std::chrono::milliseconds>(ts).count(), 3));
                }
                else
                {
                    virtual_columns[3]->insertDefault();
                    virtual_columns[4]->insertDefault();
                }
                if (put_error_to_stream)
                {
                    if (exception_message)
                    {
                        const auto & payload = consumer->currentPayload();
                        virtual_columns[5]->insertData(payload.data(), payload.size());
                        virtual_columns[6]->insertData(exception_message->data(), exception_message->size());
                    }
                    else
                    {
                        virtual_columns[5]->insertDefault();
                        virtual_columns[6]->insertDefault();
                    }
                }
            }
            total_rows += new_rows;
        }

        if (total_rows >= max_block_size || (max_execution_time != 0 && stopwatch.elapsedMilliseconds() > max_execution_time)
            || failed_poll_attempts >= MAX_FAILED_POLL_ATTEMPTS)
            break;
    }

    if (total_rows == 0)
        return {};

    auto result_block = non_virtual_header.cloneWithColumns(executor.getResultColumns());
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));

    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    auto converting_dag = ActionsDAG::makeConvertingActions(
        result_block.cloneEmpty().getColumnsWithTypeAndName(),
        getPort().getHeader().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context);

    auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
    converting_actions->execute(result_block);

    return Chunk(result_block.getColumns(), result_block.rows());
}
}
