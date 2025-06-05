#pragma once

#include <memory>
#include <mutex>
#include <atomic>
#include <vector>
#include <chrono>

#include <Poco/Semaphore.h>
#include <Storages/IStorage.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/Block.h>
#include <Common/ThreadPool.h>

#include "config.h"

#if USE_AWS_SQS

#include <aws/core/Aws.h>
#include <aws/sqs/SQSClient.h>
#include <Storages/SQS/SQSConsumer.h>

namespace DB
{

struct SQSSettings;
using SQSConsumerPtr = std::shared_ptr<SQSConsumer>;
class Chunk;

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int SYNTAX_ERROR;
}

class StorageSQS final : public IStorage, WithContext
{
public:
    StorageSQS(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        const String & comment,
        std::shared_ptr<SQSSettings> sqs_settings_);

    std::string getName() const override { return "SQS"; }

    void startup() override;
    void shutdown(bool is_drop) override;
    bool isShuttingDown() const { return shutdown_called; }

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

    bool prefersLargeBlocks() const override { return false; }

    void pushConsumer(SQSConsumerPtr consumer);
    SQSConsumerPtr popConsumer();
    SQSConsumerPtr popConsumer(std::chrono::milliseconds timeout);

private:
    std::shared_ptr<SQSSettings> sqs_settings;
    String endpoint_override;
    LoggerPtr log;
    
    std::shared_ptr<Aws::SQS::SQSClient> client;
    Aws::SDKOptions aws_sdk_options;
    
    std::mutex mutex;
    Poco::Semaphore semaphore;
    std::vector<SQSConsumerPtr> consumers;
    std::vector<std::weak_ptr<SQSConsumer>> consumers_ref;
    
    std::atomic<bool> shutdown_called{false};
    std::atomic<bool> connection_ok{true};
    std::atomic<size_t> empty_batch_count{0};
    
    BackgroundSchedulePoolTaskHolder streaming_task;
    
    SQSConsumerPtr createConsumer();
    void streamingToViewsFunc();
    
    bool tryProcessMessages();
    bool recreateClient();
    void scheduleNextExecution(bool success);
};

} 

#endif // USE_AWS_SQS
