#pragma once

#include <Common/Macros.h>
#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStreamingStorage.h>
#include <Storages/Pulsar/PulsarConsumer.h>
#include <Storages/Pulsar/PulsarSettings.h>
#include <pulsar/Client.h>
#include <Poco/Semaphore.h>

#include <mutex>

namespace DB
{

using PulsarConsumerPtr = std::shared_ptr<PulsarConsumer>;
using ConsumerPtr = std::shared_ptr<pulsar::Consumer>;
using ProducerPtr = std::shared_ptr<pulsar::Producer>;

class ReadFromStoragePulsar;

class StoragePulsar final : public IStreamingStorage, WithContext
{
    friend class ReadFromStoragePulsar;

public:
    StoragePulsar(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        std::unique_ptr<PulsarSettings> pulsar_settings_,
        LoadingStrictnessLevel mode);

    ~StoragePulsar() override = default;

    std::string getName() const override { return "Pulsar"; }

    bool isMessageQueue() const override { return true; }

    bool noPushingToViewsOnInserts() const override { return true; }

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

    /// Return a consumer taken with `popConsumer`. A usable consumer goes back to the pool;
    /// one that hit a terminal receive error is dropped and its slot is recreated by the
    /// background initialization task.
    void returnConsumer(PulsarConsumerPtr consumer);


    size_t getPollTimeoutMilliseconds() const;
    size_t getPollMaxBatchSize() const;
    size_t getMaxBlockSize() const;
    StreamingHandleErrorMode getStreamingHandleErrorMode() const;

private:
    std::unique_ptr<PulsarSettings> pulsar_settings;

    /// The broker-facing string settings support macro substitution, e.g. {database} and {table}.
    Macros::MacroExpansionInfo macros_info;
    const String format_name;
    const size_t num_consumers;
    const size_t max_rows_per_message;
    const String group_name;
    const String schema_name;
    LoggerPtr log;


    pulsar::Client pulsar_client;

    Names topics;

    std::vector<PulsarConsumerPtr> consumers;
    std::mutex consumers_mutex;
    /// The number of live consumers, both pooled and popped by sources. When it is below
    /// `num_consumers` (a subscribe failure on server startup, or a dropped poisoned consumer),
    /// `init_task` keeps recreating the missing ones until the pool is complete again.
    size_t created_consumers = 0;
    Poco::Semaphore semaphore;
    BackgroundSchedulePool::TaskHolder streamer;
    BackgroundSchedulePool::TaskHolder init_task;

    /// Owned by the single streaming task; used by `stream_control.claimCycle`.
    UInt64 last_seen_refresh_epoch = 0;

    void createConsumer(pulsar::Consumer & consumer);
    /// Create consumers until there are `num_consumers` of them. Throws on the first failure.
    void createConsumers();
    /// The body of `init_task`: retries `createConsumers` until it succeeds.
    void initConsumersFunc();
    ProducerPtr createProducer();

    Names parseTopics(String topic_list) const;

    void scheduleStreamingTasksImpl() override;

    void streaming();
    bool checkDependencies(const StorageID & table_id);
    bool streamToViews(UInt64 cycle_epoch);

    ContextMutablePtr addSettings(ContextPtr local_context) const;

    VirtualColumnsDescription createVirtuals();
};

}
