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
#include <Common/thread_local_rng.h>
#include <amqpcpp/libuv.h>
#include <uv.h>
#include <random>


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

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const Context & context) override;

    void pushReadBuffer(ConsumerBufferPtr buf);
    ConsumerBufferPtr popReadBuffer();
    ConsumerBufferPtr popReadBuffer(std::chrono::milliseconds timeout);

    ProducerBufferPtr createWriteBuffer();

    const String & getFormatName() const { return format_name; }
    NamesAndTypesList getVirtuals() const override;
    const auto & getSchemaName() const { return schema_name; }

    const String getExchange() const { return exchange_name; }
    bool checkBridge() const { return !exchange_removed.load(); }
    void unbindExchange();

    bool connectionRunning() { return event_handler->connectionRunning(); }
    bool restoreConnection();
    ChannelPtr getChannel();

protected:
    StorageRabbitMQ(
            const StorageID & table_id_,
            Context & context_,
            const ColumnsDescription & columns_,
            const String & host_port_,
            const Names & routing_keys_,
            const String & exchange_name_,
            const String & format_name_,
            char row_delimiter_,
            const String & schema_name_,
            const String & exchange_type_,
            size_t num_consumers_,
            size_t num_queues_,
            const String & queue_base_,
            const String & deadletter_exchange,
            const bool persistent_);

private:
    Context global_context;
    Context rabbitmq_context;

    Names routing_keys;
    const String exchange_name;
    AMQP::ExchangeType exchange_type;

    const String format_name;
    char row_delimiter;
    const String schema_name;
    size_t num_consumers;
    size_t num_created_consumers = 0;
    bool hash_exchange;
    size_t num_queues;
    String queue_base;
    const String deadletter_exchange;
    const bool persistent;

    Poco::Logger * log;
    std::pair<String, UInt16> parsed_address;
    std::pair<String, String> login_password;

    std::shared_ptr<uv_loop_t> loop;
    std::shared_ptr<RabbitMQHandler> event_handler;
    std::shared_ptr<AMQP::TcpConnection> connection; /// Connection for all consumers

    Poco::Semaphore semaphore;
    std::mutex mutex;
    std::vector<ConsumerBufferPtr> buffers; /// available buffers for RabbitMQ consumers

    String unique_strbase;
    String sharding_exchange, bridge_exchange, consumer_exchange;
    std::once_flag flag;
    size_t producer_id = 0, consumer_id = 0;
    bool loop_started = false;
    std::atomic<bool> exchange_removed = false, wait_confirm = true;
    ChannelPtr setup_channel;
    std::mutex connection_mutex, restore_connection;

    BackgroundSchedulePool::TaskHolder streaming_task;
    BackgroundSchedulePool::TaskHolder heartbeat_task;
    BackgroundSchedulePool::TaskHolder looping_task;

    std::atomic<bool> stream_cancelled{false};

    ConsumerBufferPtr createReadBuffer();

    void threadFunc();
    void heartbeatFunc();
    void loopingFunc();
    void initExchange();
    void bindExchange();

    void pingConnection() { connection->heartbeat(); }
    bool streamToViews();
    bool checkDependencies(const StorageID & table_id);

    String getRandomName()
    {
        std::uniform_int_distribution<int> distribution('a', 'z');
        String random_str(32, ' ');
        for (auto & c : random_str)
            c = distribution(thread_local_rng);
        return random_str;
    }
};

}
