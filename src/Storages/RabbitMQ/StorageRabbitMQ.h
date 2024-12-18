#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Core/StreamingHandleErrorMode.h>
#include <Storages/IStorage.h>
#include <Poco/Semaphore.h>
#include <mutex>
#include <atomic>
#include <Storages/RabbitMQ/RabbitMQConsumer.h>
#include <Storages/RabbitMQ/RabbitMQConnection.h>
#include <Common/thread_local_rng.h>
#include <amqpcpp/libuv.h>
#include <uv.h>
#include <random>


namespace DB
{
struct RabbitMQSettings;
using RabbitMQConsumerPtr = std::shared_ptr<RabbitMQConsumer>;

class StorageRabbitMQ final: public IStorage, WithContext
{
public:
    StorageRabbitMQ(
            const StorageID & table_id_,
            ContextPtr context_,
            const ColumnsDescription & columns_,
            const String & comment,
            std::unique_ptr<RabbitMQSettings> rabbitmq_settings_,
            LoadingStrictnessLevel mode);

    ~StorageRabbitMQ() override;

    std::string getName() const override { return "RabbitMQ"; }

    bool noPushingToViews() const override { return true; }

    void startup() override;
    void shutdown(bool is_drop) override;

    /// This is a bad way to let storage know in shutdown() that table is going to be dropped. There are some actions which need
    /// to be done only when table is dropped (not when detached). Also connection must be closed only in shutdown, but those
    /// actions require an open connection. Therefore there needs to be a way inside shutdown() method to know whether it is called
    /// because of drop query. And drop() method is not suitable at all, because it will not only require to reopen connection, but also
    /// it can be called considerable time after table is dropped (for example, in case of Atomic database), which is not appropriate for the case.
    void checkTableCanBeDropped([[ maybe_unused ]] ContextPtr query_context) const override { drop_table = true; }

    /// Always return virtual columns in addition to required columns
    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        bool async_insert) override;

    /// We want to control the number of rows in a chunk inserted into RabbitMQ
    bool prefersLargeBlocks() const override { return false; }

    void pushConsumer(RabbitMQConsumerPtr consumer);
    RabbitMQConsumerPtr popConsumer();
    RabbitMQConsumerPtr popConsumer(std::chrono::milliseconds timeout);

    const String & getFormatName() const { return format_name; }

    String getExchange() const { return exchange_name; }
    void unbindExchange();

    RabbitMQConnection & getConnection() { return *connection; }

    void incrementReader();
    void decrementReader();

private:
    ContextMutablePtr rabbitmq_context;
    std::unique_ptr<RabbitMQSettings> rabbitmq_settings;

    const String exchange_name;
    const String format_name;
    AMQP::ExchangeType exchange_type;
    Names routing_keys;
    const String schema_name;
    size_t num_consumers;
    size_t num_queues;
    String queue_base;
    Names queue_settings_list;
    size_t max_rows_per_message;
    bool reject_unhandled_messages = false;

    LoggerPtr log;

    /// For insert query. Mark messages as durable.
    const bool persistent;

    /// A table setting. It is possible not to perform any RabbitMQ setup, which is supposed to be consumer-side setup:
    /// declaring exchanges, queues, bindings. Instead everything needed from RabbitMQ table is to connect to a specific queue.
    /// This solution disables all optimizations and is not really optimal, but allows user to fully control all RabbitMQ setup.
    bool use_user_setup;

    bool hash_exchange;

    RabbitMQConnectionPtr connection; /// Connection for all consumers
    RabbitMQConfiguration configuration;

    size_t num_created_consumers = 0;
    Poco::Semaphore semaphore;
    std::mutex consumers_mutex;
    std::vector<RabbitMQConsumerPtr> consumers; /// available RabbitMQ consumers
    std::vector<std::weak_ptr<RabbitMQConsumer>> consumers_ref;

    String unique_strbase; /// to make unique consumer channel id

    /// maximum number of messages in RabbitMQ queue (x-max-length). Also used
    /// to setup size of inner consumer for received messages
    uint32_t queue_size;

    String sharding_exchange, bridge_exchange, consumer_exchange;
    size_t consumer_id = 0; /// counter for consumer, needed for channel id

    std::vector<String> queues;

    std::once_flag flag; /// remove exchange only once
    std::mutex task_mutex;
    BackgroundSchedulePool::TaskHolder streaming_task;
    BackgroundSchedulePool::TaskHolder looping_task;
    BackgroundSchedulePool::TaskHolder init_task;

    uint64_t milliseconds_to_wait;

    /**
     * ╰( ͡° ͜ʖ ͡° )つ──☆* Evil atomics:
     */
    /// Needed for tell MV or producer background tasks
    /// that they must finish as soon as possible.
    std::atomic<bool> shutdown_called{false};
    /// Counter for producers, needed for channel id.
    /// Needed to generate unique producer identifiers.
    std::atomic<size_t> producer_id = 1;
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

    RabbitMQConsumerPtr createConsumer();
    std::atomic<bool> initialized = false;

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

    void bindExchange(AMQP::TcpChannel & rabbit_channel);
    void bindQueue(size_t queue_id, AMQP::TcpChannel & rabbit_channel);

    void streamToViewsImpl();
    /// Return true on successful stream attempt.
    bool tryStreamToViews();
    bool hasDependencies(const StorageID & table_id);

    static VirtualColumnsDescription createVirtuals(StreamingHandleErrorMode handle_error_mode);

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
