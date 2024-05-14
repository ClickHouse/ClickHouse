#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Storages/Pulsar/PulsarConsumer.h>
#include <Storages/Pulsar/PulsarSettings.h>
#include <pulsar/Client.h>
#include <Poco/Semaphore.h>

#include <atomic>
#include <mutex>

namespace DB
{

using PulsarConsumerPtr = std::shared_ptr<PulsarConsumer>;
using ConsumerPtr = std::shared_ptr<pulsar::Consumer>;
using ProducerPtr = std::shared_ptr<pulsar::Producer>;

class ReadFromStoragePulsar;

class StoragePulsar final : public IStorage, WithContext
{
    friend class ReadFromStoragePulsar;

public:
    StoragePulsar(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_, 
        std::unique_ptr<PulsarSettings> pulsar_settings_);

    ~StoragePulsar() override = default;

    std::string getName() const override { return "Pulsar"; }

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

    SinkToStoragePtr
    write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    /// We want to control the number of rows in a chunk inserted into Pulsar
    bool prefersLargeBlocks() const override { return false; }

    String getFormatName() const { return format_name; }

    void pushConsumer(PulsarConsumerPtr consumer);
    PulsarConsumerPtr popConsumer();
    PulsarConsumerPtr popConsumer(std::chrono::milliseconds timeout);


    size_t getPollTimeoutMilliseconds() const;
    size_t getPollMaxBatchSize() const;
    size_t getMaxBlockSize() const;
    StreamingHandleErrorMode getStreamingHandleErrorMode() const;

private:
    std::unique_ptr<PulsarSettings> pulsar_settings;

    const String format_name;
    const size_t num_consumers;
    const size_t max_rows_per_message;
    LoggerPtr log;


    pulsar::Client pulsar_client;

    std::atomic<bool> shutdown_called{false};
    std::atomic<bool> mv_attached{false};

    Names topics;

    std::vector<PulsarConsumerPtr> consumers;
    std::mutex consumers_mutex;
    Poco::Semaphore semaphore;
    BackgroundSchedulePool::TaskHolder streamer;

    void createConsumer(pulsar::Consumer & consumer);
    ProducerPtr createProducer();

    Names parseTopics(String topics) const;

    void streaming();
    bool checkDependencies(const StorageID & table_id);
    void streamToViews();


    VirtualColumnsDescription createVirtuals();
};

}
