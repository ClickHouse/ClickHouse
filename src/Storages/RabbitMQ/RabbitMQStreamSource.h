#pragma once

#include <Core/Block.h>
#include <Core/StreamingHandleErrorMode.h>
#include <Core/Streaming/Settings.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/ISource.h>
#include <Storages/RabbitMQ/RabbitMQConnection.h>
#include <Storages/StorageSnapshot.h>
#include <Common/Logger.h>

#include <deque>
#include <map>
#include <memory>
#include <optional>


namespace DB
{

/// Reads the stream queues (`x-queue-type = stream`) of a `RabbitMQ` table for a streaming query
/// (`SELECT ... FROM rabbitmq_table STREAM`).
///
/// A stream queue keeps its messages after they are consumed and lets a consumer start at an offset.
/// Unlike a direct `SELECT` from the table, this source uses its own connection and does not take
/// the messages away from the consumers of the table. It starts after the offsets in the cursor of the query:
///     {'<queue name>': <offset of the last read message>, ...}
/// A queue absent in the cursor is read from the beginning.
///
/// RabbitMQ does not report the end of a stream queue, so a bounded stream (`STREAM BOUNDED`) finishes
/// once no message arrives during `poll_timeout_ms`.
class RabbitMQStreamSource final : public ISource
{
public:
    struct Params
    {
        RabbitMQConfiguration configuration;
        std::vector<String> queues;
        String exchange_name;
        String format_name;
        StreamingHandleErrorMode handle_error_mode = StreamingHandleErrorMode::DEFAULT;
        size_t max_block_size = 0;
        size_t poll_timeout_ms = 0;
        String table_name;
    };

    RabbitMQStreamSource(
        Params params_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const Names & columns,
        const StreamSettings & stream_settings_,
        LoggerPtr log_);

    ~RabbitMQStreamSource() override;

    String getName() const override { return "RabbitMQStreamSource"; }

    Chunk generate() override;
    std::optional<Chunk> tryGenerate() override;

private:
    struct Message
    {
        String queue;
        Int64 offset = 0;
        uint64_t delivery_tag = 0;
        String body;
        String message_id;
        UInt64 timestamp = 0;
        bool redelivered = false;
    };

    void start();
    void reportFinalCursor() const;

    Params params;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    const StreamSettings stream_settings;
    LoggerPtr log;

    const Block non_virtual_header;
    const Block virtual_header;

    std::unique_ptr<RabbitMQConnection> connection;
    std::unique_ptr<AMQP::TcpChannel> channel;
    String channel_error;

    /// Filled by the callbacks of the channel, which run while this source drives the event loop.
    std::deque<Message> received;

    /// Queue name -> the offset of the last read message (or of the cursor), -1 if nothing was read.
    std::map<String, Int64> last_offsets;
    bool started = false;
    bool stream_finished = false;
};

}
