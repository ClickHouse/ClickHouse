#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>

#include <Storages/RabbitMQ/Buffer_fwd.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

#include <Poco/Semaphore.h>
#include <ext/shared_ptr_helper.h>

#include <mutex>
#include <atomic>


namespace DB
{

using ChannelPtr = std::shared_ptr<AMQP::TcpChannel>;

class StorageRabbitMQ final: public ext::shared_ptr_helper<StorageRabbitMQ>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageRabbitMQ>;
public:
    std::string getName() const override { return "RabbitMQ"; }

    bool supportsSettings() const override { return true; }

    void startup() override;
    void shutdown() override;

    Pipes read(
            const Names & column_names,
            const SelectQueryInfo & query_info,
            const Context & context,
            QueryProcessingStage::Enum processed_stage,
            size_t max_block_size,
            unsigned num_streams) override;

    BlockOutputStreamPtr write(
            const ASTPtr & query,
            const Context & context) override;

    void pushReadBuffer(ConsumerBufferPtr buf);
    ConsumerBufferPtr popReadBuffer();
    ConsumerBufferPtr popReadBuffer(std::chrono::milliseconds timeout);

    ProducerBufferPtr createWriteBuffer();

    RabbitMQHandler & getHandler() { return connection_handler; }
    const Names & getRoutingKeys() const { return routing_keys; }

    const String & getFormatName() const { return format_name; }
    const auto & skipBroken() const { return skip_broken; }

protected:
    StorageRabbitMQ(
            const StorageID & table_id_,
            Context & context_,
            const ColumnsDescription & columns_,
            const String & host_port_, const Names & routing_keys_, const String & exchange_name, 
            const String & format_name_, char row_delimiter_,
            size_t num_consumers_, UInt64 max_block_size_, size_t skip_broken);

private:
    Context global_context;

    const String host_port;
    Names routing_keys;
    const String exchange_name;

    const String format_name;
    char row_delimiter;
    size_t num_consumers;
    UInt64 max_block_size;
    size_t num_created_consumers = 0;
    size_t skip_broken;

    Poco::Logger * log;

    Poco::Semaphore semaphore;
    std::mutex mutex;
    std::vector<ConsumerBufferPtr> buffers; /// available buffers for RabbitMQ consumers

    RabbitMQHandler connection_handler;
    AMQP::Address address;
    AMQP::TcpConnection connection;

    ChannelPtr publishing_channel; /// Shared between all publishers
    ChannelPtr consumer_channel; /// Unique to consumer

    BackgroundSchedulePool::TaskHolder task;
    std::atomic<bool> stream_cancelled{false};

    ConsumerBufferPtr createReadBuffer();

    void threadFunc();
    bool streamToViews();
    bool checkDependencies(const StorageID & table_id);
    void initQueues(ChannelPtr consumer_channel, String key);
};
}
