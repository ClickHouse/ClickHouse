#pragma once

#include <memory>
#include <mutex>
#include <atomic>
#include <vector>
#include <chrono>

#include <Poco/Semaphore.h>
#include <Storages/IStorage.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include "config.h"

#if USE_AWS_SQS

#include <aws/sqs/SQSClient.h>
#include <Storages/SQS/SQSConsumer.h>
#include <Storages/SQS/SQSSettings.h>

namespace DB
{

class StorageSQS final : public IStorage, WithContext
{
public:
    StorageSQS(
        const StorageID & table_id,
        ContextPtr context,
        const ColumnsDescription & columns,
        const String & comment,
        std::unique_ptr<SQSSettings> sqs_settings);

    ~StorageSQS() override;

    std::string getName() const override { return "SQS"; }

    bool isMessageQueue() const override { return true; }
    bool noPushingToViewsOnInserts() const override { return true; }
    bool prefersLargeBlocks() const override { return false; }

    bool supportsDynamicSubcolumns() const override { return true; }
    bool supportsSubcolumns() const override { return true; }

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

    const SQSSettings & getSettings() const { return *sqs_settings; }

    void pushConsumer(SQSConsumerPtr consumer);
    SQSConsumerPtr popConsumer();
    SQSConsumerPtr popConsumer(std::chrono::milliseconds timeout);

    static VirtualColumnsDescription createVirtuals();

private:
    std::unique_ptr<SQSSettings> sqs_settings;
    String endpoint_override;
    LoggerPtr log;

    std::shared_ptr<Aws::SQS::SQSClient> client;

    std::mutex mutex;
    Poco::Semaphore semaphore;
    std::vector<SQSConsumerPtr> consumers;
    std::vector<std::weak_ptr<SQSConsumer>> consumers_ref;

    std::atomic<bool> shutdown_called{false};
    std::atomic<bool> connection_ok{true};

    BackgroundSchedulePoolTaskHolder streaming_task;

    SQSConsumerPtr createConsumer();
    std::shared_ptr<Aws::SQS::SQSClient> createClient() const;

    void streamingToViewsFunc();
    bool tryStreamToViews();
    void scheduleNextExecution(bool success);
};

}

#endif // USE_AWS_SQS
