#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/Context.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>

namespace DB
{

class RabbitMQBlockOutputStream : public IBlockOutputStream
{
public:
    explicit RabbitMQBlockOutputStream(StorageRabbitMQ & storage_, const Context & context_);
    ~RabbitMQBlockOutputStream() override;

    Block getHeader() const override;

    void writePrefix() override;
    void write(const Block & block) override;

private:
    StorageRabbitMQ & storage;
    Context context;
    ProducerBufferPtr buffer;
    BlockOutputStreamPtr child;
};

}