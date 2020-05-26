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
            const Context & context_,
            const Names & columns,
            Poco::Logger * log_);

    ~RabbitMQBlockInputStream() override;

    String getName() const override { return storage.getName(); }
    Block getHeader() const override;

    void readPrefixImpl() override;
    Block readImpl() override;
    ///void readSuffixImpl() override;

private:
    StorageRabbitMQ & storage;
    Context context;
    Names column_names;
    Poco::Logger * log;
    bool finished = false, claimed = false;
    const Block non_virtual_header, virtual_header;

    ConsumerBufferPtr buffer;
};

}
