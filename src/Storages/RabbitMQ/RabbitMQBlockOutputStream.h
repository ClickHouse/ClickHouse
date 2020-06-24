#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/Context.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>


namespace DB
{

class RabbitMQBlockOutputStream : public IBlockOutputStream
{

public:
    explicit RabbitMQBlockOutputStream(StorageRabbitMQ & storage_, const StorageMetadataPtr & metadata_snapshot_, const Context & context_);

    Block getHeader() const override;

    void writePrefix() override;
    void write(const Block & block) override;
    void writeSuffix() override;

private:
    StorageRabbitMQ & storage;
    StorageMetadataPtr metadata_snapshot;
    Context context;
    ProducerBufferPtr buffer;
    BlockOutputStreamPtr child;
};
}
