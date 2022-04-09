#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Storages/Redis/Buffer_fwd.h>
#include <Storages/Redis/RedisSettings.h>
#include <Common/SettingsChanges.h>

#include <Poco/Semaphore.h>
#include <base/shared_ptr_helper.h>
#include <redis++/redis++.h>

#include <mutex>
#include <list>
#include <atomic>

namespace cppRedis
{

class Configuration;

}

namespace DB
{

struct StorageRedisInterceptors;

/** Implements a Redis queue table engine that can be used as a persistent queue / buffer,
  * or as a basic building block for creating pipelines with a continuous insertion / ETL.
  */
class StorageRedis final : public shared_ptr_helper<StorageRedis>, public IStorage, WithContext
{
    friend struct shared_ptr_helper<StorageRedis>;
    friend struct StorageRedisInterceptors;
    using RedisPtr = std::shared_ptr<sw::redis::Redis>;

public:
    std::string getName() const override { return "Redis"; }

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

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr context) override;

    void pushReadBuffer(ConsumerBufferPtr buf);
    ConsumerBufferPtr popReadBuffer();
    ConsumerBufferPtr popReadBuffer(std::chrono::milliseconds timeout);

    ProducerBufferPtr createWriteBuffer();

    const auto & getFormatName() const { return "JSONEachRow"; }

    NamesAndTypesList getVirtuals() const override;
    Names getVirtualColumnNames() const;
protected:
    StorageRedis(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        std::unique_ptr<RedisSettings> Redis_settings_,
        const String & collection_name_);

private:
    // Configuration and state
    std::unique_ptr<RedisSettings> redis_settings;
    const Names streams;
    const String brokers;
    const String group;
    const String client_id;
    const size_t num_consumers; /// total number of consumers
    Poco::Logger * log;
    Poco::Semaphore semaphore;
    const bool intermediate_commit;
    const SettingsChanges settings_adjustments;

    std::atomic<bool> mv_attached = false;

    /// Can differ from num_consumers in case of exception in startup() (or if startup() hasn't been called).
    /// In this case we still need to be able to shutdown() properly.
    size_t num_created_consumers = 0; /// number of actually created consumers.

    std::vector<ConsumerBufferPtr> buffers; /// available buffers for Redis consumers

    RedisPtr redis;

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

    SettingsChanges createSettingsAdjustments();
    ConsumerBufferPtr createReadBuffer(const std::string& id);

    /// If named_collection is specified.
    String collection_name;

    uint64_t milliseconds_to_wait;

    void threadFunc(size_t idx);

    size_t getPollMaxBatchSize() const;
    size_t getMaxBlockSize() const;
    size_t getPollTimeoutMillisecond() const;

    static Names parseStreams(String stream_list);
    static String getDefaultClientId(const StorageID & table_id_);

    bool streamToViews();
    bool checkDependencies(const StorageID & table_id);
};

}
