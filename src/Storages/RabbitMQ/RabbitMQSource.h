#pragma once

#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>
#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>


namespace DB
{

class RabbitMQSource : public SourceWithProgress
{

public:
    RabbitMQSource(
            StorageRabbitMQ & storage_,
            const StorageSnapshotPtr & storage_snapshot_,
            ContextPtr context_,
            const Names & columns,
            size_t max_block_size_,
            bool ack_in_suffix = false);

    ~RabbitMQSource() override;

    String getName() const override { return storage.getName(); }
    ConsumerBufferPtr getBuffer() { return buffer; }

    Chunk generate() override;

    bool queueEmpty() const { return !buffer || buffer->queueEmpty(); }
    bool needChannelUpdate();
    void updateChannel();
    bool sendAck();

private:
    StorageRabbitMQ & storage;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    Names column_names;
    const size_t max_block_size;
    bool ack_in_suffix;

    bool is_finished = false;
    const Block non_virtual_header;
    const Block virtual_header;

    ConsumerBufferPtr buffer;

    RabbitMQSource(
        StorageRabbitMQ & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        std::pair<Block, Block> headers,
        ContextPtr context_,
        const Names & columns,
        size_t max_block_size_,
        bool ack_in_suffix);

    Chunk generateImpl();
};

}
