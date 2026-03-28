#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include <Poco/Semaphore.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/IStorage.h>

#include "config.h"

#if USE_AWS_KINESIS

#include <aws/kinesis/KinesisClient.h>
#include <Storages/Kinesis/KinesisConsumer.h>
#include <Storages/Kinesis/KinesisSettings.h>

namespace DB
{

class StorageKinesis final : public IStorage, WithContext
{
public:
    StorageKinesis(
        const StorageID & table_id,
        ContextPtr context,
        const ColumnsDescription & columns,
        const String & comment,
        std::unique_ptr<KinesisSettings> kinesis_settings);

    ~StorageKinesis() override;

    std::string getName() const override { return "Kinesis"; }

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

    const KinesisSettings & getSettings() const { return *kinesis_settings; }

    void pushConsumer(KinesisConsumerPtr consumer);
    KinesisConsumerPtr popConsumer();
    KinesisConsumerPtr popConsumer(std::chrono::milliseconds timeout);

    static VirtualColumnsDescription createVirtuals();

private:
    std::unique_ptr<KinesisSettings> kinesis_settings;
    LoggerPtr log;

    std::shared_ptr<Aws::Kinesis::KinesisClient> client;

    std::mutex mutex;
    Poco::Semaphore semaphore;
    std::vector<KinesisConsumerPtr> consumers;
    std::vector<std::weak_ptr<KinesisConsumer>> consumers_ref;

    std::atomic<bool> shutdown_called{false};
    std::atomic<bool> connection_ok{true};

    BackgroundSchedulePoolTaskHolder streaming_task;

    std::shared_ptr<Aws::Kinesis::KinesisClient> createClient() const;
    KinesisConsumerPtr createConsumer(std::map<String, KinesisShardState> shard_states) const;

    std::vector<std::map<String, KinesisShardState>> listAndDistributeShards(
        const std::map<String, KinesisShardState> & checkpoints) const;

    void createCheckpointTableIfNeeded() const;
    std::map<String, KinesisShardState> loadCheckpoints() const;
    void saveCheckpoints(const std::map<String, KinesisShardState> & states) const;

    void streamingToViewsFunc();
    bool tryStreamToViews();
    void scheduleNextExecution(bool success);
};

}

#endif // USE_AWS_KINESIS