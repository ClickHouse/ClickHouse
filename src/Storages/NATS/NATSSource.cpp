#include <Storages/NATS/NATSSource.h>

#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/EmptyReadBuffer.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/NATS/INATSConsumer.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace Setting
{
    extern const SettingsMilliseconds rabbitmq_max_wait_ms;
    extern const SettingsUInt64 interactive_delay;
}

namespace ErrorCodes
{
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char nats_fail_resubscribe_within_query[];
}

static std::pair<Block, Block> getHeaders(const StorageSnapshotPtr & storage_snapshot)
{
    auto non_virtual_header = storage_snapshot->metadata->getSampleBlockNonMaterialized();
    auto virtual_header = storage_snapshot->metadata->virtuals.getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::Reader);

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
    StreamingHandleErrorMode handle_error_mode_,
    std::optional<UInt64> cancel_epoch_)
    : NATSSource(storage_, storage_snapshot_, getHeaders(storage_snapshot_), context_, columns, max_block_size_, handle_error_mode_, cancel_epoch_)
{
}

NATSSource::NATSSource(
    StorageNATS & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    std::pair<Block, Block> headers,
    ContextPtr context_,
    const Names & columns,
    size_t max_block_size_,
    StreamingHandleErrorMode handle_error_mode_,
    std::optional<UInt64> cancel_epoch_)
    : ISource(std::make_shared<const Block>(getSampleBlock(headers.first, headers.second)))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , context(context_)
    , log(getLogger("NATSSource (" + storage_.getStorageID().getFullTableName() + ")"))
    , column_names(columns)
    , max_block_size(max_block_size_)
    , handle_error_mode(handle_error_mode_)
    , non_virtual_header(std::move(headers.first))
    , virtual_header(std::move(headers.second))
    , cancel_epoch(cancel_epoch_.value_or(storage_.currentCancelEpoch()))
{
}


NATSSource::~NATSSource()
{
    if (!consumer)
        return;

    /// What a direct `SELECT` committed is acknowledged by `generate` already, so whatever it still
    /// holds goes back to the broker while the subscription is alive; otherwise the broker hides it
    /// until the ACK deadline.
    if (unsubscribe_on_destroy)
    {
        /// The subscription of a direct `SELECT` ends with it, together with its local queue.
        consumer->finishAndReturnUnprocessed(INATSConsumer::SkippedMessages::ReturnToBroker);
        consumer->unsubscribe();
    }
    else if (!background_streaming)
    {
        /// A direct `SELECT` that got a consumer still subscribed by the streaming task leaves it
        /// subscribed, and its local queue to whoever takes the consumer next.
        consumer->returnConsumed();
    }
    else
    {
        /// A streaming cycle keeps the subscription and the local queue for the next cycle. Its
        /// insert failed if it did not acknowledge, and the messages are redelivered.
        consumer->dropConsumed();
    }

    storage.pushConsumer(consumer);
}

bool NATSSource::checkTimeLimit() const
{
    if (max_execution_time != 0)
    {
        auto elapsed_ns = total_stopwatch.elapsed();

        /// Compare in whole microseconds: converting the timeout to nanoseconds overflows for huge values.
        if (elapsed_ns / 1000 > static_cast<UInt64>(max_execution_time.totalMicroseconds()))
            return false;
    }

    return true;
}

Chunk NATSSource::generate()
{
    auto chunk = generateImpl();

    if (!chunk && commit_on_select && !consumption_aborted && consumer)
        consumer->ackConsumed();

    return chunk;
}

Chunk NATSSource::generateImpl()
{
    if (!consumer)
    {
        auto timeout = std::chrono::milliseconds(context->getSettingsRef()[Setting::rabbitmq_max_wait_ms].totalMilliseconds());
        consumer = storage.popConsumer(timeout);

        if (consumer && !consumer->isSubscribed())
        {
            consumer->dropBuffered();
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
        storage.getFormatName(),
        empty_buf,
        non_virtual_header,
        context,
        max_block_size,
        std::nullopt,
        FormatParserSharedResources::singleThreaded(context->getSettingsRef()));
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
        if (isCancelled() || storage.isConsumeCancelRequested(cancel_epoch))
        {
            consumption_aborted = true;
            return {};
        }

        /// A direct read holds its consumer until it ends, so a subscription that stopped consuming
        /// is recovered here rather than by `StorageNATS::resubscribeStaleConsumers`. Only a
        /// consumer that holds no message which may still owe rows is recovered, because the
        /// recovery returns everything it holds to the broker. A skipped message owes nothing:
        /// a streaming cycle acknowledges it, as its skip is final, while a direct `SELECT` has not
        /// reached its commit point in `generate` yet and returns it.
        /// `unsubscribe_on_destroy` is kept: a streaming consumer stays subscribed for the next cycle.
        if (total_rows == 0 && !consumer->hasConsumedMessages() && consumer->queueEmpty() && consumer->needsResubscribe())
        {
            LOG_INFO(log, "A subscription stopped consuming from the NATS server, resubscribing within a running query");
            consumer->finishAndReturnUnprocessed(
                background_streaming ? INATSConsumer::SkippedMessages::Acknowledge
                                     : INATSConsumer::SkippedMessages::ReturnToBroker);
            consumer->unsubscribe();

            try
            {
                fiu_do_on(FailPoints::nats_fail_resubscribe_within_query,
                {
                    throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure of resubscribing within a running query");
                });
                consumer->subscribe();
            }
            catch (...)
            {
                /// A consumer the streaming task subscribed no longer reports that it needs to be
                /// recovered once it is unsubscribed, so `subscribeConsumers` has to subscribe it.
                if (!unsubscribe_on_destroy)
                    storage.markConsumersNotReady();
                if (!background_streaming)
                    throw;

                /// Let the other sources of the cycle insert what they have.
                tryLogCurrentException(log, "Cannot resubscribe a NATS consumer");
                return {};
            }
        }

        if (consumer->isConsumerStopped() || !checkTimeLimit())
            break;

        exception_message.reset();
        size_t new_rows = 0;

        ReadBufferPtr buf;
        if (wait_for_flush_interval)
            buf = consumer->consume(std::max<UInt64>(100, context->getSettingsRef()[Setting::interactive_delay] / 1000));
        else
            buf = consumer->consume();

        if (buf)
        {
            new_rows = executor.execute(*buf);

            /// Passed over by `nats_skip_broken_messages`.
            if (new_rows == 0)
                consumer->markLastConsumedSkipped();
        }
        else if (!wait_for_flush_interval)
            break;

        if (new_rows)
        {
            auto subject = consumer->getSubject();
            virtual_columns[0]->insertMany(subject, new_rows);
            virtual_columns[1]->insertMany(storage.getStorageID().getTableName(), new_rows);
            if (handle_error_mode == StreamingHandleErrorMode::STREAM)
            {
                if (exception_message)
                {
                    const auto & current_message = consumer->getCurrentMessage();
                    virtual_columns[2]->insertData(current_message);
                    virtual_columns[3]->insertData(*exception_message);
                }
                else
                {
                    virtual_columns[2]->insertDefault();
                    virtual_columns[3]->insertDefault();
                }
            }

            total_rows = total_rows + new_rows;
        }

        if (total_rows >= max_block_size)
            break;
    }

    if (isCancelled() || storage.isConsumeCancelRequested(cancel_epoch))
    {
        consumption_aborted = true;
        return {};
    }

    if (total_rows == 0)
        return {};

    auto result_columns = executor.getResultColumns();
    for (auto & column : virtual_columns)
        result_columns.push_back(std::move(column));

    return Chunk(std::move(result_columns), total_rows);
}

}
