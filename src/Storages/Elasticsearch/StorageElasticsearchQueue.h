#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/Types.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Disks/IDisk.h>
#include <IO/HTTPHeaderEntries.h>
#include <Storages/IStorage.h>
#include <Storages/VirtualColumnsDescription.h>
#include <Common/Macros.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <Poco/JSON/Object.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/URI.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

namespace DB
{

struct ConnectionTimeouts;
struct ElasticsearchQueueSettings;

class StorageElasticsearchQueue final : public IStorage, WithContext
{
public:
    struct Row
    {
        Poco::JSON::Object::Ptr hit;
        Poco::JSON::Object::Ptr source;
    };

    struct Batch
    {
        std::vector<Row> rows;
        String checkpoint;
        zkutil::ZooKeeperPtr checkpoint_zookeeper;
        zkutil::EphemeralNodeHolder::Ptr checkpoint_lock;
    };

    StorageElasticsearchQueue(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        const String & comment,
        std::unique_ptr<ElasticsearchQueueSettings> settings_,
        const String & relative_data_path_,
        LoadingStrictnessLevel mode,
        const String & collection_name_);

    ~StorageElasticsearchQueue() override;

    std::string getName() const override;

    bool isMessageQueue() const override { return true; }

    bool noPushingToViewsOnInserts() const override { return true; }

    void startup() override;
    void shutdown(bool is_drop) override;
    void drop() override;
    void renameInMemory(const StorageID & new_table_id) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    Batch pollBatch(size_t max_rows);
    void commit(
        const String & new_checkpoint,
        const zkutil::EphemeralNodeHolder::Ptr & checkpoint_lock = nullptr,
        const zkutil::ZooKeeperPtr & checkpoint_zookeeper = nullptr);

    size_t getPollMaxBatchSize() const;
    size_t getMaxBlockSize() const;
    size_t getFlushIntervalMilliseconds() const;
    size_t getConsumerRescheduleMilliseconds() const;
    bool commitOnSelect() const;

    static VirtualColumnsDescription createVirtuals();

private:
    friend class ReadFromStorageElasticsearchQueue;

    struct TaskContext
    {
        BackgroundSchedulePoolTaskHolder holder;
        std::atomic<bool> stream_cancelled {false};
        explicit TaskContext(BackgroundSchedulePoolTaskHolder && task_) : holder(std::move(task_))
        {
        }
    };

    std::unique_ptr<ElasticsearchQueueSettings> settings;
    Macros::MacroExpansionInfo macros_info;
    const String url;
    const String index;
    const String cursor_field;
    const String tiebreaker_field;
    const String query_body;
    const String user;
    const String password;
    const String api_key;
    const String auth_type;
    const String bearer_token;
    const bool use_point_in_time;
    const String pit_keep_alive;
    const String keeper_path;
    const String keeper_checkpoint_name;
    const String collection_name;

    LoggerPtr log;
    DiskPtr disk;
    String metadata_base_path;

    std::shared_ptr<TaskContext> task;
    std::atomic<bool> mv_attached = false;
    std::atomic<bool> shutdown_called = false;

    mutable std::mutex mutex;
    String checkpoint;

    String getCheckpointPath() const;
    void loadCheckpoint();
    void storeCheckpoint(const String & new_checkpoint) const;
    String getKeeperCheckpointPath() const;
    String getKeeperLockPath() const;
    zkutil::ZooKeeperPtr getZooKeeper() const;
    void initializeKeeperCheckpoint() const;
    String loadKeeperCheckpoint() const;
    void storeKeeperCheckpoint(const String & new_checkpoint, const zkutil::ZooKeeperPtr & zookeeper = nullptr) const;
    std::pair<zkutil::ZooKeeperPtr, zkutil::EphemeralNodeHolder::Ptr> tryAcquireKeeperCheckpointLock() const;

    void validateAuthenticationSettings() const;
    ConnectionTimeouts getHTTPTimeouts() const;
    HTTPHeaderEntries makeAuthHeaders() const;
    void setBasicCredentials(Poco::Net::HTTPBasicCredentials & credentials) const;
    String executeHTTPRequest(const String & method, const Poco::URI & uri, const String & body_string) const;
    String openPointInTime() const;
    void closePointInTime(const String & pit_id) const;
    Poco::JSON::Object::Ptr makeSearchBody(size_t max_rows, const String & search_after, const String & pit_id) const;
    String executeSearchRequest(size_t max_rows, const String & search_after) const;

    void threadFunc();
    bool streamToViews();
    bool checkDependencies(const StorageID & table_id) const;
    size_t getTableDependentCount() const;
};

}
