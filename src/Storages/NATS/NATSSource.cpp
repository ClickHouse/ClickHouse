#include <Storages/NATS/NATSSource.h>

#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/EmptyReadBuffer.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/NATS/INATSConsumer.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace Setting
{
    extern const SettingsMilliseconds rabbitmq_max_wait_ms;
    extern const SettingsUInt64 interactive_delay;
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

    consumer->dropConsumed();

    if (unsubscribe_on_destroy)
        consumer->unsubscribe();

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

        /// A JetStream pull subscription survives a reconnect client side, but the server has
        /// discarded the pull request it was waiting for. Direct reads do not return the consumer
        /// to `StorageNATS` until this source is destroyed, so recover it here instead of waiting
        /// for the background streaming task to notice it.
        /// Nothing the consumer holds locally can outlive its subscription: a `natsMsg` keeps a
        /// plain pointer to the `natsSubscription` it arrived on, and `natsMsg_Ack` follows it to
        /// reach the JetStream context and the connection, so acknowledging a message whose
        /// subscription has been destroyed reads freed memory. So recovery prefers a cycle that
        /// starts holding nothing: no rows in the current output block, whose ACK handles
        /// `StorageNATS` needs until it has inserted them, and nothing left in the local queue,
        /// which the cycles before this one insert and acknowledge.
        /// Those checks are only a snapshot - `onMsg` runs on the NATS client thread and the
        /// drain inside `unsubscribe` delivers whatever the subscription still has - so what the
        /// consumer does turn out to hold is returned to the broker instead of being destroyed,
        /// while the subscription it arrived on is still alive.
        /// Emitting no rows is not the same as having consumed nothing: `consume` takes a message
        /// before it is parsed, and `nats_skip_broken_messages` turns a message that yields no rows
        /// into an ordinary outcome. Such a message is not waiting to be inserted, so the recovery
        /// does not have to wait for it: `markLastConsumedSkipped` moves it aside as soon as it
        /// turns out to have produced nothing. Only a background streaming cycle, which never
        /// inserts such a message and whose skip is therefore already final when it happens,
        /// acknowledges it here rather than handing it back, which keeps the skip instead of
        /// showing the same malformed input to the next cycle. A direct `SELECT` consumes only what
        /// it has committed, and this recovery runs long before the commit point in `generate` - a
        /// query cancelled in between must leave the message for the next reader, whatever
        /// `nats_commit_on_select` says - so there a skipped message goes back to the broker like
        /// the rest of what the consumer holds. The redelivery is skipped again right away and is
        /// acknowledged with everything else once the query does commit.
        /// What the guard below waits for is a message that still owes rows to this query.
        /// `unsubscribe_on_destroy` keeps its previous value: a background streaming consumer must
        /// stay subscribed when this source is destroyed, so the next streaming cycle keeps
        /// consuming where this one left off. Only a consumer this source subscribed from an
        /// unsubscribed state (the direct `SELECT` case above) is unsubscribed on destroy.
        if (total_rows == 0 && !consumer->hasConsumedMessages() && consumer->queueEmpty() && consumer->needsResubscribe())
        {
            LOG_INFO(log, "A subscription stopped consuming from the NATS server, resubscribing within a running query");
            consumer->finishAndReturnUnprocessed(
                background_streaming ? INATSConsumer::SkippedMessages::Acknowledge
                                     : INATSConsumer::SkippedMessages::ReturnToBroker);
            consumer->unsubscribe();
            consumer->subscribe();
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

            /// A message that parsed into no rows is one `nats_skip_broken_messages` passed over
            /// (with `handle_error_mode = 'stream'` a malformed message still yields its error row).
            /// Nothing is waiting for it, so it must not hold back a reconnect recovery.
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
