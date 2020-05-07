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

    void pushReadBuffer(ConsumerBufferPtr buf, bool make_init);
    ConsumerBufferPtr popReadBuffer();
    ConsumerBufferPtr popReadBuffer(std::chrono::milliseconds timeout);

    ProducerBufferPtr createWriteBuffer();

    const String & getExchangeName() const { return exchange_name; }
    const Names & getRoutingKeys() const { return routing_keys; }

    const String & getFormatName() const { return format_name; }
    const auto & skipBroken() const { return skip_broken; }

    NamesAndTypesList getVirtuals() const override;

    RabbitMQHandler & getConsumerHandler() { return consumersEventHandler; }

protected:
    StorageRabbitMQ(
            const StorageID & table_id_,
            Context & context_,
            const ColumnsDescription & columns_,
            const String & host_port_,
            const Names & routing_keys_, const String & exchange_name, 
            const String & format_name_, char row_delimiter_,
            size_t num_consumers_, size_t skip_broken);


private:
    Context global_context;
    Context rabbitmq_context;

    Names routing_keys;
    const String exchange_name;

    const String format_name;
    char row_delimiter;
    size_t num_consumers;
    UInt64 max_block_size;
    size_t num_created_consumers = 0;
    size_t skip_broken;

    Poco::Logger * log;

    std::pair<std::string, UInt16> parsed_address;

    Poco::Semaphore semaphore;
    std::mutex mutex;
    std::vector<ConsumerBufferPtr> buffers; /// available buffers for RabbitMQ consumers

    /* There are several reasons for making separate connections: to limit the number of channels per connection, to make
     * event loops more deterministic and not shared between publishers and consumers, to avoid library being overloaded with 
     * callbacks. And it is simply recommended to use separate connections to publish and consume. */
    event_base * consumersEvbase;
    event_base * producersEvbase;

    RabbitMQHandler consumersEventHandler;
    RabbitMQHandler producersEventHandler;

    AMQP::TcpConnection consumersConnection;
    AMQP::TcpConnection producersConnection;

    BackgroundSchedulePool::TaskHolder task;
    std::atomic<bool> stream_cancelled{false};

    ConsumerBufferPtr createReadBuffer();

    void threadFunc();
    bool streamToViews();
    bool checkDependencies(const StorageID & table_id);
};

}
