#pragma once

#include <Core/StreamingHandleErrorMode.h>
#include <Processors/ISource.h>
#include <Storages/RabbitMQ/RabbitMQConsumer.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>


namespace DB
{

class RabbitMQSource : public ISource
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
            LoggerPtr log_);

    ~RabbitMQSource() override;

    String getName() const override { return storage.getName(); }
    void updateChannel(RabbitMQConnection & connection) { consumer->updateChannel(connection); }
    String getChannelID() const { return consumer->getChannelID(); }

    Chunk generate() override;

    bool hasPendingMessages() const { return consumer && consumer->hasPendingMessages(); }
    bool needChannelUpdate();
    void updateChannel();
    bool sendAck();
    bool sendNack();

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
    const Block non_virtual_header;
    const Block virtual_header;

    LoggerPtr log;
    RabbitMQConsumerPtr consumer;

    uint64_t max_execution_time_ms = 0;
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};

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
        LoggerPtr log_);

    Chunk generateImpl();
};

}
