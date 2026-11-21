#include <Storages/RabbitMQ/RabbitMQStreamSource.h>

#include <Columns/IColumn.h>
#include <Core/Streaming/CursorTree.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/EmptyReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DeadLetterQueue.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/DateLUT.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <amqpcpp.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_CONNECT_RABBITMQ;
}

/// The connection of a streaming read is driven only by this source, so it sends the heartbeats itself. The interval is
/// far below the heartbeat timeout that a broker suggests (60 seconds by default), and a heartbeat frame is tiny.
static constexpr uint64_t HEARTBEAT_INTERVAL_MS = 1000;

/// The columns of the table that the parser fills: the ordinary columns and the requested `MATERIALIZED` ones
/// (filled with the defaults of their types, as by a direct `SELECT` from the table), including the parents
/// of the requested subcolumns.
static Block getParserHeader(const StorageSnapshotPtr & storage_snapshot, const Names & column_names)
{
    const auto & columns = storage_snapshot->metadata->getColumns();
    auto all_columns_header = storage_snapshot->metadata->getSampleBlock();
    auto header = storage_snapshot->metadata->getSampleBlockNonMaterialized();

    for (const auto & column_name : column_names)
    {
        auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, column_name);
        if (!column)
            continue;   /// A virtual column.
        const auto name_in_storage = column->getNameInStorage();
        if (!header.has(name_in_storage))
            header.insert(all_columns_header.getByName(name_in_storage));
    }

    return header;
}

RabbitMQStreamSource::RabbitMQStreamSource(
    Params params_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    const Names & columns,
    const StreamSettings & stream_settings_,
    LoggerPtr log_)
    : ISource(std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(columns)))
    , params(std::move(params_))
    , storage_snapshot(storage_snapshot_)
    , context(context_)
    , stream_settings(stream_settings_)
    , log(std::move(log_))
    , non_virtual_header(getParserHeader(storage_snapshot_, columns))
    , virtual_header(storage_snapshot->metadata->virtuals.getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::Reader))
{
}

RabbitMQStreamSource::~RabbitMQStreamSource()
{
    try
    {
        if (channel && channel->usable())
            channel->close();
        channel.reset();
        if (connection)
            connection->disconnect();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot close the connection of a streaming read from RabbitMQ");
    }
}

void RabbitMQStreamSource::start()
{
    /// A queue name may contain dots, and the cursor tree splits the keys of the query by dots. So the offsets are taken
    /// from the collapsed form of the tree, where the path of an offset is the queue name.
    std::map<String, Int64> cursor_offsets;
    if (stream_settings.cursor)
    {
        for (const auto & entry : cursorTreeToMap(stream_settings.cursor))
        {
            const auto & tuple = entry.safeGet<Tuple>();
            cursor_offsets[tuple.at(0).safeGet<String>()] = tuple.at(1).safeGet<Int64>();
        }
    }

    for (const auto & queue : params.queues)
    {
        Int64 last_offset = -1;
        if (auto it = cursor_offsets.find(queue); it != cursor_offsets.end())
        {
            last_offset = it->second;
            /// The read starts at the next offset, which must be representable.
            if (last_offset < 0 || last_offset == std::numeric_limits<Int64>::max())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid offset {} in the cursor for queue {}", last_offset, queue);
        }
        last_offsets[queue] = last_offset;
    }

    /// The connection is published only once it is established: the destructor disconnects it, which requires
    /// the AMQP connection inside, and creating that may throw.
    auto new_connection = std::make_unique<RabbitMQConnection>(params.configuration, log);
    if (!new_connection->connect())
        throw Exception(ErrorCodes::CANNOT_CONNECT_RABBITMQ, "Cannot connect to {}", new_connection->connectionInfoForLog());
    connection = std::move(new_connection);
    heartbeat_watch.restart();

    channel = connection->createChannel();
    channel->onError([this](const char * message)
    {
        channel_error = message;
        connection->getHandler().stopBlockingLoop();
    });

    /// A stream queue requires a prefetch limit: it delivers no more than that many unacknowledged messages.
    channel->setQos(static_cast<uint16_t>(std::clamp<size_t>(params.max_block_size, 1, 1000)));

    for (const auto & [queue, last_offset] : last_offsets)
    {
        AMQP::Table arguments;
        if (last_offset >= 0)
            arguments.set("x-stream-offset", AMQP::LongLong(last_offset + 1));
        else
            arguments.set("x-stream-offset", "first");

        channel->consume(queue, arguments)
            .onReceived([this, queue_name = queue](const AMQP::Message & message, uint64_t delivery_tag, bool redelivered)
            {
                Message result{
                    .queue = queue_name,
                    .offset = message.headers().contains("x-stream-offset")
                        ? static_cast<int64_t>(message.headers().get("x-stream-offset")) : -1,
                    .delivery_tag = delivery_tag,
                    .body = String(message.body(), message.bodySize()),
                    .message_id = message.hasMessageID() ? message.messageID() : "",
                    .timestamp = message.hasTimestamp() ? message.timestamp() : 0,
                    .redelivered = redelivered,
                };
                received.push_back(std::move(result));
                connection->getHandler().stopBlockingLoop();
            })
            .onError([this, queue_name = queue](const char * message)
            {
                channel_error = fmt::format("cannot consume from queue {}: {}", queue_name, message);
                connection->getHandler().stopBlockingLoop();
            });

        LOG_DEBUG(log, "Streaming RabbitMQ queue {} from offset {}", queue,
            last_offset >= 0 ? std::to_string(last_offset + 1) : "first");
    }
}

void RabbitMQStreamSource::reportFinalCursor() const
{
    /// Only an incremental refresh of a materialized view has the holder of the cursor.
    auto streaming_cursor = context->getStreamingCursor();
    if (!streaming_cursor)
        return;

    auto cursor = std::make_shared<CursorTreeNode>();
    for (const auto & [queue, last_offset] : last_offsets)
        if (last_offset >= 0)
            cursor->setValue(queue, last_offset);
    streaming_cursor->merge(cursor);
}

std::optional<Chunk> RabbitMQStreamSource::tryGenerate()
{
    /// An idle stream returns an empty chunk after a poll, so that the thread runs other processors in the meantime
    /// (e.g. the other tables of a `Merge` table) instead of waiting here.
    auto chunk = generate();
    if (!chunk && (stream_finished || isCancelled()))
        return std::nullopt;
    return chunk;
}

bool RabbitMQStreamSource::waitForMessage(uint64_t timeout_ms)
{
    Stopwatch watch;
    while (received.empty() && !isCancelled())
    {
        if (heartbeat_watch.elapsedMilliseconds() >= HEARTBEAT_INTERVAL_MS)
        {
            connection->heartbeat();
            heartbeat_watch.restart();
        }

        const uint64_t elapsed_ms = watch.elapsedMilliseconds();
        if (elapsed_ms >= timeout_ms)
            break;

        /// Drive the event loop of the connection until a message arrives (the callback stops the loop), but not longer
        /// than until the next heartbeat is due.
        connection->getHandler().startBlockingLoopWithTimeout(std::min(timeout_ms - elapsed_ms, HEARTBEAT_INTERVAL_MS));

        if (!channel_error.empty())
            throw Exception(ErrorCodes::CANNOT_CONNECT_RABBITMQ,
                "Error in the channel of a streaming read from RabbitMQ: {}", channel_error);
    }
    return !received.empty();
}

Chunk RabbitMQStreamSource::generate()
{
    if (stream_finished)
        return {};

    if (!started)
    {
        start();
        started = true;
    }

    const bool bounded = !stream_settings.subscribe_for_updates;

    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    EmptyReadBuffer empty_buf;
    auto parser_shared_resources = FormatParserSharedResources::singleThreaded(context->getSettingsRef());
    auto create_format = [&]
    {
        return FormatFactory::instance().getInput(
            params.format_name, empty_buf, non_virtual_header, context, params.max_block_size, std::nullopt, parser_shared_resources);
    };

    std::optional<String> exception_message;
    bool is_dead_letter = false;
    auto on_error = [&](const MutableColumns & result_columns, const ColumnCheckpoints & checkpoints, Exception & e) -> size_t
    {
        switch (params.handle_error_mode)
        {
            case StreamingHandleErrorMode::STREAM:
            {
                exception_message = e.message();
                for (size_t i = 0; i < result_columns.size(); ++i)
                {
                    result_columns[i]->rollback(*checkpoints[i]);
                    result_columns[i]->insertDefault();
                }
                return 1;
            }
            case StreamingHandleErrorMode::DEAD_LETTER_QUEUE:
            {
                /// The message produces no rows and is recorded in `system.dead_letter_queue`.
                exception_message = e.message();
                for (size_t i = 0; i < result_columns.size(); ++i)
                    result_columns[i]->rollback(*checkpoints[i]);
                is_dead_letter = true;
                return 0;
            }
            case StreamingHandleErrorMode::DEFAULT:
                throw std::move(e);
        }
    };

    StreamingFormatExecutor executor(non_virtual_header, std::move(create_format), std::move(on_error));

    const Field table_name = params.storage_id.getTableName();
    size_t total_rows = 0;

    /// The rows already read are returned once the flush interval passes since the start of the chunk, even if messages
    /// keep arriving faster than the poll timeout.
    Stopwatch flush_watch;
    const uint64_t flush_interval_ms = params.flush_interval_ms;

    while (!isCancelled() && total_rows < params.max_block_size)
    {
        if (received.empty())
        {
            uint64_t timeout_ms = params.poll_timeout_ms;
            if (total_rows && flush_interval_ms)
            {
                const uint64_t elapsed_ms = flush_watch.elapsedMilliseconds();
                if (elapsed_ms >= flush_interval_ms)
                    break;
                timeout_ms = std::min(timeout_ms, flush_interval_ms - elapsed_ms);
            }

            if (!waitForMessage(timeout_ms))
            {
                if (total_rows || isCancelled())
                    break;

                /// RabbitMQ does not report the end of a stream queue, so a bounded stream ends when it is idle.
                if (bounded)
                {
                    reportFinalCursor();
                    stream_finished = true;
                }
                break;
            }
        }

        auto message = std::move(received.front());
        received.pop_front();

        if (message.offset < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "A message of queue {} has no `x-stream-offset` header, so the queue is not a stream queue", message.queue);

        /// A stream queue does not remove an acknowledged message; the acknowledgement only grants credit for more messages.
        channel->ack(message.delivery_tag);

        /// A stream queue starts the delivery at the beginning of the chunk of the requested offset, so the messages
        /// before it may come again.
        if (message.offset <= last_offsets[message.queue])
            continue;

        exception_message.reset();
        is_dead_letter = false;
        ReadBufferFromMemory buf(message.body.data(), message.body.size());
        const size_t new_rows = message.body.empty() ? 0 : executor.execute(buf);
        last_offsets[message.queue] = message.offset;

        if (is_dead_letter)
            addToDeadLetterQueue(message, *exception_message);

        for (size_t i = 0; i < new_rows; ++i)
        {
            for (size_t column_index = 0; column_index < virtual_columns.size(); ++column_index)
            {
                const auto & name = virtual_header.getByPosition(column_index).name;
                auto & column = virtual_columns[column_index];
                if (name == "_exchange_name")
                    column->insert(params.exchange_name);
                else if (name == "_delivery_tag")
                    column->insert(message.delivery_tag);
                else if (name == "_redelivered")
                    column->insert(message.redelivered);
                else if (name == "_message_id")
                    column->insert(message.message_id);
                else if (name == "_timestamp")
                    column->insert(message.timestamp);
                else if (name == "_table")
                    column->insert(table_name);
                else if (name == "_raw_message" && exception_message)
                    column->insertData(message.body.data(), message.body.size());
                else if (name == "_error" && exception_message)
                    column->insertData(exception_message->data(), exception_message->size());
                else
                    column->insertDefault();
            }
        }
        total_rows += new_rows;
    }

    if (!total_rows)
        return {};

    auto result_block = non_virtual_header.cloneWithColumns(executor.getResultColumns());
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));

    /// The requested columns, in the order of the header. A subcolumn is extracted from its parsed parent column.
    const auto & columns = storage_snapshot->metadata->getColumns();
    const auto & header = getPort().getHeader();
    Columns output_columns;
    output_columns.reserve(header.columns());
    for (const auto & output : header)
    {
        if (auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, output.name))
        {
            const auto & parent = result_block.getByName(column->getNameInStorage());
            if (column->isSubcolumn())
                output_columns.push_back(column->getTypeInStorage()->getSubcolumn(column->getSubcolumnName(), parent.column));
            else
                output_columns.push_back(parent.column);
        }
        else
        {
            output_columns.push_back(virtual_block.getByName(output.name).column);
        }
    }

    return Chunk(std::move(output_columns), total_rows);
}

void RabbitMQStreamSource::addToDeadLetterQueue(const Message & message, const String & error) const
{
    auto dead_letter_queue = context->getDeadLetterQueue();
    if (!dead_letter_queue)
    {
        LOG_WARNING(log, "Table system.dead_letter_queue is not configured, skipping message");
        return;
    }

    const auto time_now = std::chrono::system_clock::now();
    dead_letter_queue->add([&](DeadLetterQueueElement & element)
    {
        element = DeadLetterQueueElement{
            .table_engine = DeadLetterQueueElement::StreamType::RabbitMQ,
            .event_time = timeInSeconds(time_now),
            .event_time_microseconds = timeInMicroseconds(time_now),
            .database = params.storage_id.database_name,
            .table = params.storage_id.table_name,
            .raw_message = message.body,
            .error = error,
            .details = DeadLetterQueueElement::RabbitMQDetails{
                .exchange_name = params.exchange_name,
                .message_id = message.message_id,
                .timestamp = message.timestamp,
                .redelivered = message.redelivered,
                .delivery_tag = message.delivery_tag,
                .channel_id = std::to_string(channel->id()),
            },
        };
    });
}

}
