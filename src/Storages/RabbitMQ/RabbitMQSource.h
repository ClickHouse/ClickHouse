#pragma once

#include <Core/StreamingHandleErrorMode.h>
#include <Processors/ISource.h>
#include <Storages/RabbitMQ/RabbitMQConsumer.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>

#include <optional>


namespace DB
{

class RabbitMQSource final : public ISource
{

public:
    RabbitMQSource(
            StorageRabbitMQ & storage_,
            const StorageSnapshotPtr & storage_snapshot_,
            ContextPtr context_,
            const Names & columns,
            size_t max_block_size_,
            UInt64 max_execution_time_,
            StreamingHandleErrorMode handle_error_mode_,
            bool nack_broken_messages_,
            bool ack_in_suffix,
            LoggerPtr log_,
            std::optional<UInt64> cancel_epoch_ = {},
            bool drive_loop_on_worker_ = false,
            UInt64 first_poll_timeout_ms_ = 0,
            std::optional<std::vector<StorageID>> streaming_views_ = {});

    ~RabbitMQSource() override;

    String getName() const override { return storage.getName(); }
    void updateChannel(RabbitMQConnection & connection) { consumer->updateChannel(connection); }
    String getChannelID() const { return consumer->getChannelID(); }

    Chunk generate() override;

    bool hasPendingMessages() const { return consumer && consumer->hasPendingMessages(); }
    bool needChannelUpdate();
    void updateChannel();
    bool sendAck();
    bool sendNack(bool requeue = false);

    bool wasConsumptionAborted() const { return consumption_aborted; }
    bool endedOnViewsChange() const { return ended_on_views_change; }

private:
    StorageRabbitMQ & storage;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    const Names column_names;
    const size_t max_block_size;
    const StreamingHandleErrorMode handle_error_mode;
    const bool ack_in_suffix;
    const bool nack_broken_messages;

    bool is_finished = false;
    bool consumption_aborted = false;
    bool ended_on_views_change = false;
    /// Set only for a REFRESH-while-stopped background cycle; see driveLoopUntilMessage.
    bool drive_loop_on_worker = false;
    /// How long an empty first poll waits for a message, like Kafka's poll timeout (0: return at once).
    const UInt64 first_poll_timeout_ms;
    /// Background streaming only: the ready views the insert pipeline of this cycle was built for.
    const std::optional<std::vector<StorageID>> streaming_views;
    const Block non_virtual_header;
    const Block virtual_header;
    const UInt64 cancel_epoch;

    LoggerPtr log;
    RabbitMQConsumerPtr consumer;

    uint64_t max_execution_time_ms = 0;
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};
    /// Per-source deadline for the self-driving REFRESH cycle (see driveLoopUntilMessage).
    std::optional<Stopwatch> drive_stopwatch;

    RabbitMQConsumer::CommitInfo commit_info;

    RabbitMQSource(
        StorageRabbitMQ & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        std::pair<Block, Block> headers,
        ContextPtr context_,
        const Names & columns,
        size_t max_block_size_,
        UInt64 max_execution_time_,
        StreamingHandleErrorMode handle_error_mode_,
        bool nack_broken_messages_,
        bool ack_in_suffix,
        LoggerPtr log_,
        std::optional<UInt64> cancel_epoch_ = {},
        bool drive_loop_on_worker_ = false,
        UInt64 first_poll_timeout_ms_ = 0,
        std::optional<std::vector<StorageID>> streaming_views_ = {});

    Chunk generateImpl();

    /// True if the ready dependent views differ from `streaming_views` (always false without them).
    bool viewsChanged() const;

    /// Budget of driveLoopUntilMessage: for a REFRESH cycle the flush interval when set, else a finite
    /// default (the flush interval may be 0); for streaming the poll timeout, capped by the flush interval.
    uint64_t driveBudgetMs() const;

    /// Drive the AMQP loop on this worker until a message is pending (bounded, cancel-aware).
    /// Returns true if a message is now pending, false if it timed out / was cancelled / stopped / paused.
    bool driveLoopUntilMessage();
};

}
