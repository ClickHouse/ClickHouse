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
            StorageRabbitMQ & storage_, const Context & context_, const Names & columns,
            size_t max_block_size_, Poco::Logger * log_, bool commit_in_suffix = true);
    ~RabbitMQBlockInputStream() override;

    String getName() const override { return storage.getName(); }
    Block getHeader() const override;

    void readPrefixImpl() override;
    Block readImpl() override;
    void readSuffixImpl() override;

    void commit();
    void commitNotSubscribed(const Names & routing_keys);
    void startProcessing();

private:
    StorageRabbitMQ & storage;
    Context context;
    Names column_names;
    UInt64 max_block_size;
    Poco::Logger * log;
    bool commit_in_suffix, claimed = false;

    const Block non_virtual_header, virtual_header;

    ConsumerBufferPtr buffer;
};
}
