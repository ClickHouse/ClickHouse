#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Poco/Semaphore.h>
#include <base/shared_ptr_helper.h>
#include <mutex>
#include <atomic>
#include <Storages/RabbitMQ/Buffer_fwd.h>
#include <Storages/RabbitMQ/RabbitMQSettings.h>
#include <Storages/RabbitMQ/RabbitMQConnection.h>
#include <Common/thread_local_rng.h>
#include <amqpcpp/libuv.h>
#include <uv.h>
#include <random>


namespace DB
{

class StorageRabbitMQ final: public shared_ptr_helper<StorageRabbitMQ>, public IStorage, WithContext
{
    friend struct shared_ptr_helper<StorageRabbitMQ>;

public:
    std::string getName() const override { return "RabbitMQ"; }

    bool noPushingToViews() const override { return true; }

    void startup() override;
    void shutdown() override;

    /// This is a bad way to let storage know in shutdown() that table is going to be dropped. There are some actions which need
    /// to be done only when table is dropped (not when detached). Also connection must be closed only in shutdown, but those
    /// actions require an open connection. Therefore there needs to be a way inside shutdown() method to know whether it is called
    /// because of drop query. And drop() method is not suitable at all, because it will not only require to reopen connection, but also
    /// it can be called considerable time after table is dropped (for example, in case of Atomic database), which is not appropriate for the case.
    void checkTableCanBeDropped() const override { drop_table = true; }

    /// Always return virtual columns in addition to required columns
    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context) override;

    void pushReadBuffer(ConsumerBufferPtr buf);
    ConsumerBufferPtr popReadBuffer();
    ConsumerBufferPtr popReadBuffer(std::chrono::milliseconds timeout);

    ProducerBufferPtr createWriteBuffer();

    const String & getFormatName() const { return format_name; }
    NamesAndTypesList getVirtuals() const override;

    String getExchange() const { return exchange_name; }
    void unbindExchange();

    bool updateChannel(ChannelPtr & channel);
    void updateQueues(std::vector<String> & queues_) { queues_ = queues; }
    void prepareChannelForBuffer(ConsumerBufferPtr buffer);

    void incrementReader();
    void decrementReader();

protected:
    StorageRabbitMQ(
            const StorageID & table_id_,
            ContextPtr context_,
            const ColumnsDescription & columns_,
            std::unique_ptr<RabbitMQSettings> rabbitmq_settings_,
            bool is_attach_);

private:
    ContextMutablePtr rabbitmq_context;
    std::unique_ptr<RabbitMQSettings> rabbitmq_settings;

    const String exchange_name;
    const String format_name;
    AMQP::ExchangeType exchange_type;
    Names routing_keys;
    char row_delimiter;
    const String schema_name;
    size_t num_consumers;
    size_t num_queues;
    String queue_base;
    Names queue_settings_list;

    /// For insert query. Mark messages as durable.
    const bool persistent;

    /// A table setting. It is possible not to perform any RabbitMQ setup, which is supposed to be consumer-side setup:
    /// declaring exchanges, queues, bindings. Instead everything needed from RabbitMQ table is to connect to a specific queue.
    /// This solution disables all optimizations and is not really optimal, but allows user to fully control all RabbitMQ setup.
    bool use_user_setup;

    bool hash_exchange;
    Poco::Logger * log;

    RabbitMQConnectionPtr connection; /// Connection for all consumers
    RabbitMQConfiguration configuration;

    size_t num_created_consumers = 0;
    Poco::Semaphore semaphore;
    std::mutex buffers_mutex;
    std::vector<ConsumerBufferPtr> buffers; /// available buffers for RabbitMQ consumers

    String unique_strbase; /// to make unique consumer channel id

    /// maximum number of messages in RabbitMQ queue (x-max-length). Also used
    /// to setup size of inner buffer for received messages
    uint32_t queue_size;

    String sharding_exchange, bridge_exchange, consumer_exchange;
    size_t consumer_id = 0; /// counter for consumer buffer, needed for channel id

    std::vector<String> queues;

    std::once_flag flag; /// remove exchange only once
    std::mutex task_mutex;
    BackgroundSchedulePool::TaskHolder streaming_task;
    BackgroundSchedulePool::TaskHolder looping_task;
    BackgroundSchedulePool::TaskHolder connection_task;

    uint64_t milliseconds_to_wait;

    /**
     * ╰( ͡° ͜ʖ ͡° )つ──☆* Evil atomics:
     */
    /// Needed for tell MV or producer background tasks
    /// that they must finish as soon as possible.
    std::atomic<bool> shutdown_called{false};
    /// Counter for producer buffers, needed for channel id.
    /// Needed to generate unique producer buffer identifiers.
    std::atomic<size_t> producer_id = 1;
    /// Has connection background task completed successfully?
    /// It is started only once -- in constructor.
    std::atomic<bool> rabbit_is_ready = false;
    /// Allow to remove exchange only once.
    std::atomic<bool> exchange_removed = false;
    /// For select query we must be aware of the end of streaming
    /// to be able to turn off the loop.
    std::atomic<size_t> readers_count = 0;
    std::atomic<bool> mv_attached = false;

    /// In select query we start event loop, but do not stop it
    /// after that select is finished. Then in a thread, which
    /// checks for MV we also check if we have select readers.
    /// If not - we turn off the loop. The checks are done under
    /// mutex to avoid having a turned off loop when select was
    /// started.
    std::mutex loop_mutex;

    size_t read_attempts = 0;
    mutable bool drop_table = false;
    bool is_attach;

    ConsumerBufferPtr createReadBuffer();
    void initializeBuffers();
    bool initialized = false;

    /// Functions working in the background
    void streamingToViewsFunc();
    void loopingFunc();
    void connectionFunc();

    void startLoop();
    void stopLoop();
    void stopLoopIfNoReaders();

    static Names parseSettings(String settings_list);
    static AMQP::ExchangeType defineExchangeType(String exchange_type_);
    static String getTableBasedName(String name, const StorageID & table_id);

    ContextMutablePtr addSettings(ContextPtr context) const;
    size_t getMaxBlockSize() const;
    void deactivateTask(BackgroundSchedulePool::TaskHolder & task, bool wait, bool stop_loop);

    void initRabbitMQ();
    void cleanupRabbitMQ() const;

    void initExchange(AMQP::TcpChannel & rabbit_channel);
    void bindExchange(AMQP::TcpChannel & rabbit_channel);
    void bindQueue(size_t queue_id, AMQP::TcpChannel & rabbit_channel);

    bool streamToViews();
    bool checkDependencies(const StorageID & table_id);

    static String getRandomName()
    {
        std::uniform_int_distribution<int> distribution('a', 'z');
        String random_str(32, ' ');
        for (auto & c : random_str)
            c = distribution(thread_local_rng);
        return random_str;
    }
};

}
