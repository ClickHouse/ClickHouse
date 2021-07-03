#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>


namespace DB
{

class RabbitMQBlockOutputStream : public IBlockOutputStream
{

public:
    explicit RabbitMQBlockOutputStream(StorageRabbitMQ & storage_, const StorageMetadataPtr & metadata_snapshot_, ContextPtr context_);

    Block getHeader() const override;

    void writePrefix() override;
    void write(const Block & block) override;
    void writeSuffix() override;

private:
    StorageRabbitMQ & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    ProducerBufferPtr buffer;
    BlockOutputStreamPtr child;
};
}
