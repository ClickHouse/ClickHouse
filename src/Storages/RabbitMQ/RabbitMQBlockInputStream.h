#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>
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
            const Context & context_,
            const Names & columns);

    ~RabbitMQBlockInputStream() override;

    String getName() const override { return storage.getName(); }
    Block getHeader() const override;

    void readPrefixImpl() override;
    Block readImpl() override;

private:
    StorageRabbitMQ & storage;
    StorageMetadataPtr metadata_snapshot;
    Context context;
    Names column_names;
    bool finished = false;
    bool claimed = false;
    const Block non_virtual_header;
    const Block virtual_header;

    ConsumerBufferPtr buffer;
};

}
