#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <Poco/Semaphore.h>
#include <ext/shared_ptr_helper.h>
#include <mutex>
#include <atomic>
#include <Storages/RabbitMQ/Buffer_fwd.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <event2/event.h>


namespace DB
{

using ChannelPtr = std::shared_ptr<AMQP::TcpChannel>;

class StorageRabbitMQ final: public ext::shared_ptr_helper<StorageRabbitMQ>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageRabbitMQ>;

public:
    std::string getName() const override { return "RabbitMQ"; }

    bool supportsSettings() const override { return true; }
    bool noPushingToViews() const override { return true; }

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

    const String & getExchangeName() const { return exchange_name; }
    const String & getRoutingKey() const { return routing_key; }

    const String & getFormatName() const { return format_name; }
    NamesAndTypesList getVirtuals() const override;

    const void pingConnection() { connection.heartbeat(); }

protected:
    StorageRabbitMQ(
            const StorageID & table_id_,
            Context & context_,
            const ColumnsDescription & columns_,
            const String & host_port_,
            const String & routing_key_,
            const String & exchange_name_, 
            const String & format_name_,
            char row_delimiter_,
            size_t num_consumers_, 
            size_t num_queues_, 
            bool hash_exchange);

private:
    Context global_context;
    Context rabbitmq_context;

    String routing_key;
    const String exchange_name;

    const String format_name;
    char row_delimiter;
    size_t num_consumers;
    size_t num_created_consumers = 0;
    bool bind_by_id;
    size_t num_queues;
    const bool hash_exchange;

    Poco::Logger * log;
    std::pair<String, UInt16> parsed_address;
    std::pair<String, String> login_password;

    event_base * evbase;
    RabbitMQHandler eventHandler;
    AMQP::TcpConnection connection; /// Connection for all consumers

    Poco::Semaphore semaphore;
    std::mutex mutex;
    std::vector<ConsumerBufferPtr> buffers; /// available buffers for RabbitMQ consumers

    size_t next_channel_id = 1; /// Must >= 1 because it is used as a binding key, which has to be > 0
    bool update_channel_id = false;

    BackgroundSchedulePool::TaskHolder task;
    std::atomic<bool> stream_cancelled{false};

    ConsumerBufferPtr createReadBuffer();

    void threadFunc();
    bool streamToViews();
    bool checkDependencies(const StorageID & table_id);
};

}
