#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Storages/Kafka/Buffer_fwd.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Common/SettingsChanges.h>

#include <Poco/Semaphore.h>
#include <ext/shared_ptr_helper.h>

#include <mutex>
#include <list>
#include <atomic>

namespace cppkafka
{

class Configuration;

}

namespace DB
{

struct StorageKafkaInterceptors;

/** Implements a Kafka queue table engine that can be used as a persistent queue / buffer,
  * or as a basic building block for creating pipelines with a continuous insertion / ETL.
  */
class StorageKafka final : public ext::shared_ptr_helper<StorageKafka>, public IStorage, WithContext
{
    friend struct ext::shared_ptr_helper<StorageKafka>;
    friend struct StorageKafkaInterceptors;

public:
    std::string getName() const override { return "Kafka"; }

    bool noPushingToViews() const override { return true; }

    void startup() override;
    void shutdown() override;

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr context) override;

    void pushReadBuffer(ConsumerBufferPtr buf);
    ConsumerBufferPtr popReadBuffer();
    ConsumerBufferPtr popReadBuffer(std::chrono::milliseconds timeout);

    ProducerBufferPtr createWriteBuffer(const Block & header);

    const auto & getFormatName() const { return format_name; }

    NamesAndTypesList getVirtuals() const override;
    Names getVirtualColumnNames() const;
    HandleKafkaErrorMode getHandleKafkaErrorMode() const { return kafka_settings->kafka_handle_error_mode; }
protected:
    StorageKafka(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        std::unique_ptr<KafkaSettings> kafka_settings_);

private:
    // Configuration and state
    std::unique_ptr<KafkaSettings> kafka_settings;
    const Names topics;
    const String brokers;
    const String group;
    const String client_id;
    const String format_name;
    const char row_delimiter; /// optional row delimiter for generating char delimited stream in order to make various input stream parsers happy.
    const String schema_name;
    const size_t num_consumers; /// total number of consumers
    Poco::Logger * log;
    Poco::Semaphore semaphore;
    const bool intermediate_commit;
    const SettingsChanges settings_adjustments;

    /// Can differ from num_consumers in case of exception in startup() (or if startup() hasn't been called).
    /// In this case we still need to be able to shutdown() properly.
    size_t num_created_consumers = 0; /// number of actually created consumers.

    std::vector<ConsumerBufferPtr> buffers; /// available buffers for Kafka consumers

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

    /// Handle error mode
    HandleKafkaErrorMode handle_error_mode;

    SettingsChanges createSettingsAdjustments();
    ConsumerBufferPtr createReadBuffer(const size_t consumer_number);

    // Update Kafka configuration with values from CH user configuration.
    void updateConfiguration(cppkafka::Configuration & conf);
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
