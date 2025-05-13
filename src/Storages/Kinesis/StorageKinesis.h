/*
    WIP
*/


#pragma once

#include <Storages/IStorage.h>
#include <Storages/Kinesis/KinesisSettings.h>
#include <Storages/Kinesis/KinesisShardsBalancer.h>

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

class KinesisConsumer;
using KinesisConsumerPtr = std::shared_ptr<KinesisConsumer>;

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

    // Получение настроек
    size_t getMaxBlockSize() const;
    size_t getFlushIntervalMs() const;
    size_t getNumConsumers() const { return kinesis_settings->num_consumers; }
    bool getEnhancedFanOut() const { return kinesis_settings->enhanced_fan_out; }
    const String & getConsumerName() const { return kinesis_settings->consumer_name; }
    const String & getStreamName() const { return kinesis_settings->stream_name; }
    StreamingHandleErrorMode getStreamingHandleErrorMode() const;

private:
    // Метод потока для стриминга данных в материализованные представления
    void threadFunc(size_t idx);
    bool streamToViewsFunc();
    bool recreateClient();

    std::shared_ptr<KinesisSettings> kinesis_settings;
    LoggerPtr log;

    // Управление клиентами AWS
    String endpoint_override;
    Aws::SDKOptions aws_sdk_options;
    std::shared_ptr<Aws::Kinesis::KinesisClient> client;

    // Потребители
    std::mutex mutex;
    Poco::Semaphore semaphore;
    std::vector<KinesisConsumerPtr> consumers;
    std::vector<std::weak_ptr<KinesisConsumer>> consumers_ref;
    
    // Управление фоновыми задачами
    std::vector<std::shared_ptr<TaskContext>> tasks;
    BackgroundSchedulePool::TaskHolder streaming_task;
    std::atomic<bool> shutdown_called{false};
    std::atomic<bool> mv_attached{false};
    std::atomic<bool> connection_ok{true};

    // Дополнительные настройки и параметры
    bool thread_per_consumer;
};

}

#endif // USE_AWS_KINESIS
