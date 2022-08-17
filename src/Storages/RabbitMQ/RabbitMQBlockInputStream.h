#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>
#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>


namespace DB
{

class RabbitMQBlockInputStream : public IBlockInputStream
{

public:
    RabbitMQBlockInputStream(
            StorageRabbitMQ & storage_,
            const StorageMetadataPtr & metadata_snapshot_,
            ContextPtr context_,
            const Names & columns,
            size_t max_block_size_,
            bool ack_in_suffix = true);

    ~RabbitMQBlockInputStream() override;

    String getName() const override { return storage.getName(); }
    Block getHeader() const override;
    ConsumerBufferPtr getBuffer() { return buffer; }

    void readPrefixImpl() override;
    Block readImpl() override;
    void readSuffixImpl() override;

    bool queueEmpty() const { return !buffer || buffer->queueEmpty(); }
    bool needChannelUpdate();
    void updateChannel();
    bool sendAck();

private:
    StorageRabbitMQ & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    Names column_names;
    const size_t max_block_size;
    bool ack_in_suffix;

    bool finished = false;
    const Block non_virtual_header;
    Block sample_block;
    const Block virtual_header;

    ConsumerBufferPtr buffer;
};

}
