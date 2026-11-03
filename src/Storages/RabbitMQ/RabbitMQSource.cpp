#include <Storages/RabbitMQ/RabbitMQSource.h>

#include <IO/WriteHelpers.h>
#include <Core/Settings.h>
#include <Common/DateLUT.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/EmptyReadBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/DeadLetterQueue.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace Setting
{
extern const SettingsMilliseconds rabbitmq_max_wait_ms;
}

/// Cap for the self-driven wait when the flush interval is 0 (i.e. no per-cycle time budget), so a
/// REFRESH-while-stopped cycle never parks a pool worker indefinitely.
static constexpr uint64_t DEFAULT_LOOP_DRIVE_WAIT_MS = 5000;
/// Slice between event-loop iterations while self-driving the loop waiting for a message.
static constexpr uint64_t LOOP_DRIVE_POLL_MS = 50;

static std::pair<Block, Block> getHeaders(const StorageSnapshotPtr & storage_snapshot, const Names & column_names)
{
    auto all_columns_header = storage_snapshot->metadata->getSampleBlock();

    auto non_virtual_header = storage_snapshot->metadata->getSampleBlockNonMaterialized();
    auto virtual_header = storage_snapshot->metadata->virtuals.getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::Reader);

    for (const auto & column_name : column_names)
    {
        if (non_virtual_header.has(column_name) || virtual_header.has(column_name))
            continue;
        const auto & column = all_columns_header.getByName(column_name);
        non_virtual_header.insert(column);
    }

    return {non_virtual_header, virtual_header};
}

static Block getSampleBlock(const Block & non_virtual_header, const Block & virtual_header)
{
    auto header = non_virtual_header;
    for (const auto & column : virtual_header)
        header.insert(column);

    return header;
}

RabbitMQSource::RabbitMQSource(
    StorageRabbitMQ & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    ContextPtr context_,
    const Names & columns,
    size_t max_block_size_,
    UInt64 max_execution_time_,
    StreamingHandleErrorMode handle_error_mode_,
    bool nack_broken_messages_,
    bool ack_in_suffix_,
    LoggerPtr log_,
    std::optional<UInt64> cancel_epoch_,
    bool drive_loop_on_worker_)
    : RabbitMQSource(
        storage_,
        storage_snapshot_,
        getHeaders(storage_snapshot_, columns),
        context_,
        columns,
        max_block_size_,
        max_execution_time_,
        handle_error_mode_,
        nack_broken_messages_,
        ack_in_suffix_,
        log_,
        cancel_epoch_,
        drive_loop_on_worker_)
{
}

RabbitMQSource::RabbitMQSource(
    StorageRabbitMQ & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    std::pair<Block, Block> headers,
    ContextPtr context_,
    const Names & columns,
    size_t max_block_size_,
    UInt64 max_execution_time_,
    StreamingHandleErrorMode handle_error_mode_,
    bool nack_broken_messages_,
    bool ack_in_suffix_,
    LoggerPtr log_,
    std::optional<UInt64> cancel_epoch_,
    bool drive_loop_on_worker_)
    : ISource(std::make_shared<const Block>(getSampleBlock(headers.first, headers.second)))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , context(context_)
    , column_names(columns)
    , max_block_size(max_block_size_)
    , handle_error_mode(handle_error_mode_)
    , ack_in_suffix(ack_in_suffix_)
    , nack_broken_messages(nack_broken_messages_)
    , drive_loop_on_worker(drive_loop_on_worker_)
    , non_virtual_header(std::move(headers.first))
    , virtual_header(std::move(headers.second))
    , cancel_epoch(cancel_epoch_.value_or(storage_.currentCancelEpoch()))
    , log(log_)
    , max_execution_time_ms(max_execution_time_)
{
    storage.incrementReader();
}


RabbitMQSource::~RabbitMQSource()
{
    storage.decrementReader();

    if (!consumer)
        return;

    storage.pushConsumer(consumer);
}


bool RabbitMQSource::needChannelUpdate()
{
    if (!consumer)
        return false;

    return consumer->needChannelUpdate();
}


void RabbitMQSource::updateChannel()
{
    if (!consumer)
        return;

    consumer->updateChannel(storage.getConnection());
}

uint64_t RabbitMQSource::driveBudgetMs() const
{
    return max_execution_time_ms ? max_execution_time_ms : DEFAULT_LOOP_DRIVE_WAIT_MS;
}

bool RabbitMQSource::driveLoopUntilMessage()
{
    const uint64_t drive_budget_ms = driveBudgetMs();

    auto is_cancelled = [this]{ return storage.isConsumeCancelRequested(cancel_epoch); };
    auto & handler = storage.getConnection().getHandler();
    /// A finite flush interval caps the whole cycle from its entry (drive_stopwatch). An unlimited
    /// cycle (flush=0) instead bounds only each wait for the next delivery by this per-call idle
    /// window, so a live backlog drains in full while an empty queue still releases the permit.
    Stopwatch idle_stopwatch{CLOCK_MONOTONIC_COARSE};
    while (!consumer->hasPendingMessages())
    {
        if (consumer->isConsumerStopped() || is_cancelled())
            return false;
        const uint64_t elapsed_ms = max_execution_time_ms
            ? drive_stopwatch->elapsedMilliseconds()
            : idle_stopwatch.elapsedMilliseconds();
        if (elapsed_ms >= drive_budget_ms)
            return false;

        /// Drive the AMQP event loop on this worker instead of parking on the consumer's condition
        /// variable: delivery would otherwise depend on the RabbitMQLoopingTask getting a
        /// MessageBrokerSchedulePool worker, which a SYSTEM REFRESH ALL BACKGROUND fan-out can starve.
        /// iterateLoop() is a no-op when the looping task already owns the loop (try_lock).
        handler.iterateLoop();
        if (!consumer->hasPendingMessages())
            consumer->waitForMessages(std::min(LOOP_DRIVE_POLL_MS, drive_budget_ms - elapsed_ms), is_cancelled);
    }

    return true;
}

Chunk RabbitMQSource::generate()
{
    auto chunk = generateImpl();
    if (!chunk && ack_in_suffix)
    {
        if (consumption_aborted)
        {
            LOG_TEST(log, "Will requeue messages on aborted select");
            sendNack(/*requeue=*/true);
        }
        else
        {
            LOG_TEST(log, "Will send ack on select");
            sendAck();
        }
    }

    return chunk;
}

Chunk RabbitMQSource::generateImpl()
{
    if (!consumer)
    {
        auto timeout = std::chrono::milliseconds(context->getSettingsRef()[Setting::rabbitmq_max_wait_ms].totalMilliseconds());
        consumer = storage.popConsumer(timeout);
    }

    if (is_finished || !consumer || consumer->isConsumerStopped())
    {
        LOG_TRACE(
            log,
            "RabbitMQSource is stopped (is_finished: {}, consumer_stopped: {})",
            is_finished,
            consumer ? toString(consumer->isConsumerStopped()) : "No consumer");
        return {};
    }

    /// Currently it is one time usage source: to make sure data is flushed
    /// strictly by timeout or by block size.
    is_finished = true;

    /// Start the self-driving cycle deadline now, so it bounds the whole cycle regardless of whether any
    /// poll returns rows (a steady backlog otherwise never re-enters driveLoopUntilMessage).
    if (drive_loop_on_worker)
        drive_stopwatch.emplace(CLOCK_MONOTONIC_COARSE);

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
    bool is_dead_letter = false;

    auto on_error = [&](const MutableColumns & result_columns, const ColumnCheckpoints & checkpoints, Exception & e)
    {
        switch (handle_error_mode)
        {
            case StreamingHandleErrorMode::STREAM:
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
            case StreamingHandleErrorMode::DEAD_LETTER_QUEUE:
            {
                exception_message = e.message();
                for (size_t i = 0; i < result_columns.size(); ++i)
                {
                    // We could already push some rows to result_columns before exception, we need to fix it.
                    result_columns[i]->rollback(*checkpoints[i]);
                }

                is_dead_letter = true;
                return 0;
            }
            case StreamingHandleErrorMode::DEFAULT:
                throw std::move(e);
        }
    };

    StreamingFormatExecutor executor(non_virtual_header, input_format, on_error);

    bool aborted = false;

    /// Channel id will not change during read.
    while (true)
    {
        if (storage.isConsumeCancelRequested(cancel_epoch))
        {
            aborted = true;
            break;
        }

        exception_message.reset();
        size_t new_rows = 0;
        is_dead_letter = false;

        if (consumer->hasPendingMessages())
        {
            /// A buffer containing a single RabbitMQ message.
            if (auto buf = consumer->consume())
            {
                try
                {
                    new_rows = executor.execute(*buf);
                }
                catch (...)
                {
                    /// The message was already dequeued by `consume`. Record its
                    /// delivery tag so that `nackMessages` in `streamToViews`
                    /// can properly reject it. Without this, the tag is lost and
                    /// the message stays unacked in RabbitMQ forever.
                    /// See https://github.com/ClickHouse/ClickHouse/issues/73541
                    const auto & message = consumer->currentMessage();
                    commit_info.channel_id = message.channel_id;
                    commit_info.delivery_tag = std::max(commit_info.delivery_tag, message.delivery_tag);
                    throw;
                }
            }
        }

        if (new_rows || is_dead_letter)
        {
            const auto exchange_name = storage.getExchange();
            const auto & message = consumer->currentMessage();

            LOG_TEST(
                log,
                "Pulled {} rows, message delivery tag: {}, "
                "previous delivery tag: {}, redelivered: {}, failed delivery tags by this moment: {}, exception message: {}",
                new_rows,
                message.delivery_tag,
                commit_info.delivery_tag,
                message.redelivered,
                commit_info.failed_delivery_tags.size(),
                exception_message.has_value() ? exception_message.value() : "None");

            commit_info.channel_id = message.channel_id;

            if (exception_message.has_value() && nack_broken_messages)
            {
                commit_info.failed_delivery_tags.push_back(message.delivery_tag);
            }
            else
            {
                chassert(!commit_info.delivery_tag || message.redelivered || commit_info.delivery_tag < message.delivery_tag);
                commit_info.delivery_tag = std::max(commit_info.delivery_tag, message.delivery_tag);
            }

            for (size_t i = 0; i < new_rows; ++i)
            {
                virtual_columns[0]->insert(exchange_name);
                virtual_columns[1]->insert(message.channel_id);
                virtual_columns[2]->insert(message.delivery_tag);
                virtual_columns[3]->insert(message.redelivered);
                virtual_columns[4]->insert(message.message_id);
                virtual_columns[5]->insert(message.timestamp);
                virtual_columns[6]->insert(storage.getStorageID().getTableName());
                if (handle_error_mode == StreamingHandleErrorMode::STREAM)
                {
                    if (exception_message)
                    {
                        virtual_columns[7]->insertData(message.message);
                        virtual_columns[8]->insertData(*exception_message);
                    }
                    else
                    {
                        virtual_columns[7]->insertDefault();
                        virtual_columns[8]->insertDefault();
                    }
                }
            }

            if (is_dead_letter)
            {
                chassert(exception_message);
                const auto time_now = std::chrono::system_clock::now();
                auto storage_id = storage.getStorageID();

                auto dead_letter_queue = context->getDeadLetterQueue();
                if (!dead_letter_queue)
                    LOG_WARNING(log, "Table system.dead_letter_queue is not configured, skipping message");
                else
                    dead_letter_queue->add([&](DeadLetterQueueElement & element) { element =
                        DeadLetterQueueElement{
                            .table_engine = DeadLetterQueueElement::StreamType::RabbitMQ,
                            .event_time = timeInSeconds(time_now),
                            .event_time_microseconds = timeInMicroseconds(time_now),
                            .database = storage_id.database_name,
                            .table = storage_id.table_name,
                            .raw_message = message.message,
                            .error = exception_message.value(),
                            .details = DeadLetterQueueElement::RabbitMQDetails{
                                .exchange_name = exchange_name,
                                .message_id = message.message_id,
                                .timestamp = message.timestamp,
                                .redelivered = message.redelivered,
                                .delivery_tag = message.delivery_tag,
                                .channel_id = message.channel_id
                            }
                        }; });
            }

            total_rows += new_rows;
        }
        else if (total_rows == 0)
        {
            /// Empty first poll: self-drive the loop instead of returning empty. See driveLoopUntilMessage.
            if (drive_loop_on_worker && !consumer->isConsumerStopped() && driveLoopUntilMessage())
                continue;
            break;
        }

        bool is_time_limit_exceeded = false;
        UInt64 remaining_execution_time = 0;
        /// Both paths bound the cycle by the flush interval (max_execution_time_ms; 0 == unlimited,
        /// drain up to max_block_size). The drive path measures elapsed from cycle entry via the
        /// per-source drive_stopwatch; the normal path uses the construction-time total_stopwatch.
        const uint64_t budget_ms = max_execution_time_ms;
        if (budget_ms)
        {
            uint64_t elapsed_time_ms = (drive_loop_on_worker ? *drive_stopwatch : total_stopwatch).elapsedMilliseconds();
            is_time_limit_exceeded = budget_ms <= elapsed_time_ms;
            if (!is_time_limit_exceeded)
                remaining_execution_time = budget_ms - elapsed_time_ms;
        }

        if (total_rows >= max_block_size || consumer->isConsumerStopped() || is_time_limit_exceeded)
        {
            break;
        }
        if (new_rows == 0)
        {
            /// A false return (idle budget reached / stopped / cancelled) ends the cycle.
            if (drive_loop_on_worker)
            {
                if (driveLoopUntilMessage())
                    continue;
                break;
            }
            auto is_cancelled = [this]{ return storage.isConsumeCancelRequested(cancel_epoch); };
            if (remaining_execution_time)
                consumer->waitForMessages(remaining_execution_time, is_cancelled);
            else
                consumer->waitForMessages(std::nullopt, is_cancelled);
        }
    }

    if (aborted || storage.isConsumeCancelRequested(cancel_epoch))
    {
        consumption_aborted = true;
        LOG_TRACE(log, "Consumption interrupted: discarding in-flight block of {} rows", total_rows);
        return {};
    }

    LOG_TEST(
        log,
        "Flushing {} rows (max block size: {}, time: {} / {} ms)",
        total_rows,
        max_block_size,
        total_stopwatch.elapsedMilliseconds(),
        max_execution_time_ms);

    if (total_rows == 0)
        return {};

    auto result_columns = executor.getResultColumns();
    for (auto & column : virtual_columns)
        result_columns.push_back(std::move(column));

    return Chunk(std::move(result_columns), total_rows);
}


bool RabbitMQSource::sendAck()
{
    return consumer && consumer->ackMessages(commit_info);
}

bool RabbitMQSource::sendNack(bool requeue)
{
    return consumer && consumer->nackMessages(commit_info, requeue);
}

}
