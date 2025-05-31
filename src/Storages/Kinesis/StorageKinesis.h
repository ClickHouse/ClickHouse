#pragma once

#include <Storages/IStorage.h>
#include <Storages/Kinesis/KinesisSettings.h>

#include <Core/BackgroundSchedulePool.h>
#include <Poco/Semaphore.h>

#include <mutex>
#include <atomic>

#include "config.h"

#if USE_AWS_KINESIS

#include <aws/core/Aws.h>
#include <aws/kinesis/KinesisClient.h>

namespace DB
{

struct ShardState;

class KinesisConsumer;
using KinesisConsumerPtr = std::shared_ptr<KinesisConsumer>;

class KinesisShardsBalancer;
using KinesisShardsBalancerPtr = std::shared_ptr<KinesisShardsBalancer>;
struct TaskContext
{
    BackgroundSchedulePool::TaskHolder holder;
    std::atomic<bool> stream_cancelled{false};
    
    explicit TaskContext(BackgroundSchedulePool::TaskHolder && task_)
        : holder(std::move(task_)) {}
};

class StorageKinesis final : public IStorage, WithContext
{
public:
    StorageKinesis(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        const String & comment,
        std::shared_ptr<KinesisSettings> kinesis_settings_);

    std::string getName() const override { return "Kinesis"; }

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
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        bool async_insert) override;

    // API для работы с потребителями
    KinesisConsumerPtr createConsumer();
    void pushConsumer(KinesisConsumerPtr consumer);
    KinesisConsumerPtr popConsumer();
    KinesisConsumerPtr popConsumer(std::chrono::milliseconds timeout);

    void saveCheckpoints();
    std::map<String, ShardState> loadCheckpoints();
    void createCheckpointsTableIfNotExists();
    void showCheckpointsTable();

    // Получение настроек
    size_t getMaxBlockSize() const;
    size_t getFlushIntervalMs() const;
    size_t getNumConsumers() const { return kinesis_settings->num_consumers; }
    bool getEnhancedFanOut() const { return kinesis_settings->enhanced_fan_out; }
    const String & getConsumerName() const { return kinesis_settings->consumer_name; }
    const String & getStreamName() const { return kinesis_settings->stream_name; }
    StreamingHandleErrorMode getStreamingHandleErrorMode() const;

private:
    void streamingToViewsFunc();
    bool tryProcessMessages();
    bool recreateClient();
    void scheduleNextExecution(bool success);

    void saveOffsets(
        ContextMutablePtr op_context,
        const StorageID & table_id, 
        const String & stream_name, 
        const std::map<String, ShardState> & states);
    
    std::map<String, ShardState> loadOffsets(
        const StorageID & table_id, 
        const String & stream_name);

    std::shared_ptr<KinesisSettings> kinesis_settings;
    LoggerPtr log;

    // AWS client management
    String endpoint_override;
    Aws::SDKOptions aws_sdk_options;
    std::shared_ptr<Aws::Kinesis::KinesisClient> client;

    // Consumers
    std::mutex mutex;
    Poco::Semaphore semaphore;
    std::vector<KinesisConsumerPtr> consumers;
    std::vector<std::weak_ptr<KinesisConsumer>> consumers_ref;

    KinesisShardsBalancerPtr shards_balancer;
    
    // Background tasks management
    std::vector<std::shared_ptr<TaskContext>> tasks;
    BackgroundSchedulePool::TaskHolder streaming_task;
    std::atomic<bool> shutdown_called{false};
    std::atomic<bool> mv_attached{false};
    std::atomic<bool> connection_ok{true};
};

}

#endif // USE_AWS_KINESIS
