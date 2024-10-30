#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Core/StreamingHandleErrorMode.h>
#include <Storages/IStorage.h>
#include <Storages/Kafka/KafkaConsumer.h>
#include <Common/Macros.h>
#include <Common/SettingsChanges.h>
#include <Common/ThreadPool_fwd.h>

#include <Poco/Semaphore.h>

#include <condition_variable>
#include <mutex>
#include <list>
#include <atomic>
#include <cppkafka/cppkafka.h>

namespace DB
{

struct KafkaSettings;
class ReadFromStorageKafka;
class StorageSystemKafkaConsumers;
class ThreadStatus;

template <typename TStorageKafka>
struct KafkaInterceptors;

using KafkaConsumerPtr = std::shared_ptr<KafkaConsumer>;
using ConsumerPtr = std::shared_ptr<cppkafka::Consumer>;

/** Implements a Kafka queue table engine that can be used as a persistent queue / buffer,
  * or as a basic building block for creating pipelines with a continuous insertion / ETL.
  */
class StorageKafka final : public IStorage, WithContext
{
    using KafkaInterceptors = KafkaInterceptors<StorageKafka>;
    friend KafkaInterceptors;

public:
    StorageKafka(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        const String & comment,
        std::unique_ptr<KafkaSettings> kafka_settings_,
        const String & collection_name_);

    ~StorageKafka() override;

    std::string getName() const override { return "Kafka"; }

    bool noPushingToViews() const override { return true; }

    void startup() override;
    void shutdown(bool is_drop) override;

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
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr context,
        bool async_insert) override;

    /// We want to control the number of rows in a chunk inserted into Kafka
    bool prefersLargeBlocks() const override { return false; }

    void pushConsumer(KafkaConsumerPtr consumer);
    KafkaConsumerPtr popConsumer();
    KafkaConsumerPtr popConsumer(std::chrono::milliseconds timeout);

    const auto & getFormatName() const { return format_name; }

    StreamingHandleErrorMode getStreamingHandleErrorMode() const;

    struct SafeConsumers
    {
        std::shared_ptr<IStorage> storage_ptr;
        std::unique_lock<std::mutex> lock;
        std::vector<KafkaConsumerPtr> & consumers;
    };

    SafeConsumers getSafeConsumers() { return {shared_from_this(), std::unique_lock(mutex), consumers};  }

private:
    friend class ReadFromStorageKafka;

    // Configuration and state
    std::unique_ptr<KafkaSettings> kafka_settings;
    Macros::MacroExpansionInfo macros_info;
    const Names topics;
    const String brokers;
    const String group;
    const String client_id;
    const String format_name;
    const size_t max_rows_per_message;
    const String schema_name;
    const size_t num_consumers; /// total number of consumers
    LoggerPtr log;
    const bool intermediate_commit;
    const SettingsChanges settings_adjustments;

    std::atomic<bool> mv_attached = false;

    std::vector<KafkaConsumerPtr> consumers;

    std::mutex mutex;
    std::condition_variable cv;
    std::condition_variable cleanup_cv;

    // Stream thread
    struct TaskContext
    {
        BackgroundSchedulePool::TaskHolder holder;
        std::atomic<bool> stream_cancelled {false};
        explicit TaskContext(BackgroundSchedulePool::TaskHolder&& task_) : holder(std::move(task_))
        {
        }
    };
    std::vector<std::shared_ptr<TaskContext>> tasks;
    bool thread_per_consumer = false;

    std::unique_ptr<ThreadFromGlobalPool> cleanup_thread;

    /// For memory accounting in the librdkafka threads.
    std::mutex thread_statuses_mutex;
    std::list<std::shared_ptr<ThreadStatus>> thread_statuses;

    /// Creates KafkaConsumer object without real consumer (cppkafka::Consumer)
    KafkaConsumerPtr createKafkaConsumer(size_t consumer_number);
    /// Returns full consumer related configuration, also the configuration
    /// contains global kafka properties.
    cppkafka::Configuration getConsumerConfiguration(size_t consumer_number);
    /// Returns full producer related configuration, also the configuration
    /// contains global kafka properties.
    cppkafka::Configuration getProducerConfiguration();

    /// If named_collection is specified.
    String collection_name;

    std::atomic<bool> shutdown_called = false;

    void threadFunc(size_t idx);

    size_t getPollMaxBatchSize() const;
    size_t getMaxBlockSize() const;
    size_t getPollTimeoutMillisecond() const;

    bool streamToViews();

    void cleanConsumers();
};

}
