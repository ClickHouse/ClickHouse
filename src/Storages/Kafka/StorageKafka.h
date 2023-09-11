#pragma once

#include <Common/Macros.h>
#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Storages/Kafka/KafkaConsumer.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Common/SettingsChanges.h>

#include <Poco/Semaphore.h>

#include <mutex>
#include <list>
#include <atomic>

namespace cppkafka
{

class Configuration;

}

namespace DB
{

class StorageSystemKafkaConsumers;

struct StorageKafkaInterceptors;

using KafkaConsumerPtr = std::shared_ptr<KafkaConsumer>;
using KafkaConsumerWeakPtr = std::weak_ptr<KafkaConsumer>;

/** Implements a Kafka queue table engine that can be used as a persistent queue / buffer,
  * or as a basic building block for creating pipelines with a continuous insertion / ETL.
  */
class StorageKafka final : public IStorage, WithContext
{
    friend struct StorageKafkaInterceptors;

public:
    StorageKafka(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        std::unique_ptr<KafkaSettings> kafka_settings_,
        const String & collection_name_);

    std::string getName() const override { return "Kafka"; }

    bool noPushingToViews() const override { return true; }

    void startup() override;
    void shutdown() override;

    Pipe read(
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

    NamesAndTypesList getVirtuals() const override;
    Names getVirtualColumnNames() const;
    HandleKafkaErrorMode getHandleKafkaErrorMode() const { return kafka_settings->kafka_handle_error_mode; }

    struct SafeConsumers
    {
        std::shared_ptr<IStorage> storage_ptr;
        std::unique_lock<std::mutex> lock;
        std::vector<KafkaConsumerWeakPtr> & consumers;
    };

    SafeConsumers getSafeConsumers() { return {shared_from_this(), std::unique_lock(mutex), all_consumers};  }

private:
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
    Poco::Logger * log;
    Poco::Semaphore semaphore;
    const bool intermediate_commit;
    const SettingsChanges settings_adjustments;

    std::atomic<bool> mv_attached = false;

    /// Can differ from num_consumers in case of exception in startup() (or if startup() hasn't been called).
    /// In this case we still need to be able to shutdown() properly.
    size_t num_created_consumers = 0; /// number of actually created consumers.

    std::vector<KafkaConsumerPtr> consumers; /// available consumers
    std::vector<KafkaConsumerWeakPtr> all_consumers; /// busy (belong to a KafkaSource) and vacant consumers

    std::mutex mutex;

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

    /// For memory accounting in the librdkafka threads.
    std::mutex thread_statuses_mutex;
    std::list<std::shared_ptr<ThreadStatus>> thread_statuses;

    SettingsChanges createSettingsAdjustments();
    KafkaConsumerPtr createConsumer(size_t consumer_number);

    /// If named_collection is specified.
    String collection_name;

    std::atomic<bool> shutdown_called = false;

    // Update Kafka configuration with values from CH user configuration.
    void updateConfiguration(cppkafka::Configuration & kafka_config, std::shared_ptr<KafkaConsumerWeakPtr>);
    void updateConfiguration(cppkafka::Configuration & kafka_config)
    {
        updateConfiguration(kafka_config, std::make_shared<KafkaConsumerWeakPtr>());
    }

    String getConfigPrefix() const;
    void threadFunc(size_t idx);

    size_t getPollMaxBatchSize() const;
    size_t getMaxBlockSize() const;
    size_t getPollTimeoutMillisecond() const;

    static Names parseTopics(String topic_list);
    static String getDefaultClientId(const StorageID & table_id_);

    bool streamToViews();
    bool checkDependencies(const StorageID & table_id);

};

}
