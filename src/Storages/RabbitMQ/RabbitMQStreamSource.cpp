#include <Storages/RabbitMQ/RabbitMQStreamSource.h>

#include <Columns/IColumn.h>
#include <Core/Streaming/CursorTree.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/EmptyReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/logger_useful.h>

#include <amqpcpp.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_CONNECT_RABBITMQ;
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
    , non_virtual_header(storage_snapshot->metadata->getSampleBlockNonMaterialized())
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
            if (last_offset < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid offset {} in the cursor for queue {}", last_offset, queue);
        }
        last_offsets[queue] = last_offset;
    }

    connection = std::make_unique<RabbitMQConnection>(params.configuration, log);
    if (!connection->connect())
        throw Exception(ErrorCodes::CANNOT_CONNECT_RABBITMQ, "Cannot connect to {}", connection->connectionInfoForLog());

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
    auto on_error = [&](const MutableColumns & result_columns, const ColumnCheckpoints & checkpoints, Exception & e) -> size_t
    {
        if (params.handle_error_mode != StreamingHandleErrorMode::STREAM)
            throw std::move(e);

        exception_message = e.message();
        for (size_t i = 0; i < result_columns.size(); ++i)
        {
            result_columns[i]->rollback(*checkpoints[i]);
            result_columns[i]->insertDefault();
        }
        return 1;
    };

    StreamingFormatExecutor executor(non_virtual_header, std::move(create_format), std::move(on_error));

    const Field table_name = params.table_name;
    size_t total_rows = 0;

    while (!isCancelled() && total_rows < params.max_block_size)
    {
        if (received.empty())
        {
            /// Drive the event loop of the connection until a message arrives (the callback stops the loop).
            const bool got_event = connection->getHandler().startBlockingLoopWithTimeout(params.poll_timeout_ms);

            if (!channel_error.empty())
                throw Exception(ErrorCodes::CANNOT_CONNECT_RABBITMQ,
                    "Error in the channel of a streaming read from RabbitMQ: {}", channel_error);

            if (received.empty())
            {
                if (total_rows)
                    break;

                /// RabbitMQ does not report the end of a stream queue, so a bounded stream ends when it is idle.
                if (bounded && !got_event)
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
        if (message.offset >= 0 && message.offset <= last_offsets[message.queue])
            continue;

        exception_message.reset();
        ReadBufferFromMemory buf(message.body.data(), message.body.size());
        const size_t new_rows = message.body.empty() ? 0 : executor.execute(buf);
        last_offsets[message.queue] = message.offset;

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
    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    auto converting_dag = ActionsDAG::makeConvertingActions(
        result_block.cloneEmpty().getColumnsWithTypeAndName(),
        getPort().getHeader().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context);
    ExpressionActions(std::move(converting_dag)).execute(result_block);

    return Chunk(result_block.getColumns(), result_block.rows());
}

}
