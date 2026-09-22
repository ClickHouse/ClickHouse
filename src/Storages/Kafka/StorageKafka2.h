#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/Block_fwd.h>
#include <Core/StreamingHandleErrorMode.h>
#include <Core/Types.h>
#include <Storages/IStorage.h>
#include <Storages/Kafka/IKafkaExceptionInfoSink.h>
#include <Storages/Kafka/KafkaConsumer2.h>
#include <Storages/Kafka/Kafka_fwd.h>
#include <Storages/Kafka/KeeperHandlingConsumer.h>
#include <Storages/IStreamingStorage.h>
#include <Common/Macros.h>
#include <Common/SettingsChanges.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <atomic>
#include <filesystem>
#include <list>
#include <mutex>
#include <optional>
#include <rdkafka.h>

namespace cppkafka
{

class Configuration;

}

namespace DB
{

namespace AWSMSKIAMAuth { struct OAuthBearerTokenRefreshContext; }

struct KafkaSettings;
class Kafka2Source;
class ReadFromStorageKafka2;
template <typename TStorageKafka>
struct KafkaInterceptors;
class ThreadStatus;

/// Implements a Kafka queue table engine that can be used as a persistent queue / buffer,
/// or as a basic building block for creating pipelines with a continuous insertion / ETL.
///
/// It is similar to the already existing StorageKafka, it instead of storing the offsets
/// in Kafka, its main source of information about offsets is Keeper. On top of the
/// offsets, it also stores the number of messages (intent size) it tried to insert from
/// each topic. By storing the intent sizes it is possible to retry the same batch of
/// messages in case of any errors and giving deduplication a chance to deduplicate
/// blocks.
///
/// To not complicate things too much, the current implementation makes sure to fetch
/// messages only from a single topic-partition on a single thread at a time by
/// manipulating the queues of librdkafka. By pulling from multiple topic-partitions
/// the order of messages are not guaranteed, therefore they would have different
/// hashes for deduplication.
///
/// For the committed offsets we try to mimic the same behavior as Kafka does: if the last
/// read offset is `n`, then we save the offset `n + 1`, same as Kafka does.
class StorageKafka2 final : public IStreamingStorage, WithContext
{
    using KafkaInterceptors = KafkaInterceptors<StorageKafka2>;
    friend KafkaInterceptors;
    friend class Kafka2Source;
    friend class ReadFromStorageKafka2;

public:
    using KeeperHandlingConsumerPtr = std::shared_ptr<KeeperHandlingConsumer>;
    struct SafeConsumers
    {
        std::shared_ptr<IStorage> storage_ptr;
        std::unique_lock<std::mutex> lock;
        std::vector<KeeperHandlingConsumerPtr> & consumers;
    };

    StorageKafka2(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        const String & comment,
        std::unique_ptr<KafkaSettings> kafka_settings_,
        const String & collection_name_);

    ~StorageKafka2() override;

    std::string getName() const override { return Kafka::TABLE_ENGINE_NAME; }

    bool isMessageQueue() const override { return true; }

    bool noPushingToViewsOnInserts() const override { return true; }

    void startup() override;
    void shutdown(bool is_drop) override;

    void drop() override;

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
    write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

    /// We want to control the number of rows in a chunk inserted into Kafka
    bool prefersLargeBlocks() const override { return false; }

    const auto & getFormatName() const { return format_name; }

    StreamingHandleErrorMode getHandleKafkaErrorMode() const;

    bool supportsColumnsWithDynamicStructure() const override { return true; }
    bool supportsSubcolumns() const override { return true; }

    const KafkaSettings & getKafkaSettings() const { return *kafka_settings; }

    /// Returns the existing OAuth context, or installs `candidate` if none exists yet. Thread-safe.
    std::shared_ptr<AWSMSKIAMAuth::OAuthBearerTokenRefreshContext>
    ensureOAuthContext(std::shared_ptr<AWSMSKIAMAuth::OAuthBearerTokenRefreshContext> candidate)
    {
        std::lock_guard lock(oauth_context_mutex);
        if (!oauth_context)
            oauth_context = std::move(candidate);
        return oauth_context;
    }

    SafeConsumers getSafeConsumers() { return {shared_from_this(), std::unique_lock(consumers_mutex), consumers}; }

private:
    // Stream thread
    struct TaskContext
    {
        BackgroundSchedulePoolTaskHolder holder;
        std::atomic<bool> stream_cancelled{false};
        UInt64 last_seen_refresh_epoch = 0;
        explicit TaskContext(BackgroundSchedulePoolTaskHolder && task_)
            : holder(std::move(task_))
        {
        }
    };

    struct BlocksAndGuard
    {
        BlocksList blocks;
        KeeperHandlingConsumer::OffsetGuard guard;
    };

    // Configuration and state
    mutable std::mutex keeper_mutex;
    zkutil::ZooKeeperPtr keeper;
    const String keeper_path;
    const std::filesystem::path fs_keeper_path;
    String replica_path;
    std::unique_ptr<KafkaSettings> kafka_settings;
    Macros::MacroExpansionInfo macros_info;
    const Names topics;
    const String brokers;
    const String group;
    const String client_id;
    const String format_name;
    const size_t max_rows_per_message;
    const String schema_name;
    const size_t num_consumers; /// total number of consumers
    LoggerPtr log;
    const SettingsChanges settings_adjustments;

    /// Partition affinity settings, enabled when both are > 0.
    UInt64 partition_shard_num = 0;
    UInt64 shard_count = 0;
    /// Can differ from num_consumers in case of exception in startup() (or if startup() hasn't been called).
    /// In this case we still need to be able to shutdown() properly.
    size_t num_created_consumers = 0; /// number of actually created consumers.
    mutable std::mutex oauth_context_mutex;
    std::shared_ptr<AWSMSKIAMAuth::OAuthBearerTokenRefreshContext> oauth_context TSA_GUARDED_BY(oauth_context_mutex);

    std::mutex consumers_mutex;
    std::condition_variable cv;
    std::vector<KeeperHandlingConsumerPtr> consumers TSA_GUARDED_BY(consumers_mutex);

    std::vector<std::shared_ptr<TaskContext>> tasks;
    bool thread_per_consumer = false;
    /// For memory accounting in the librdkafka threads.
    std::mutex thread_statuses_mutex;
    std::list<std::shared_ptr<ThreadStatus>> thread_statuses;
    /// If named_collection is specified.
    String collection_name;

    void scheduleStreamingTasksImpl() override;

    /// Number of background streaming threads currently consuming for materialized views.
    /// Prevents direct SELECTs from using consumers concurrently with MV streaming.
    /// Uses a counter instead of a boolean because with thread_per_consumer=1,
    /// multiple threads may stream simultaneously and each must be tracked independently.
    std::atomic<size_t> active_mv_streamers{0};
    /// Number of consumers used by direct SELECTs. Prevents MV streaming from starting
    /// while direct reads are in progress, avoiding concurrent consumer access.
    std::atomic<size_t> active_direct_readers{0};

    // Handling replica activation.
    std::atomic<bool> is_active = false;
    zkutil::EphemeralNodeHolderPtr replica_is_active_node;
    BackgroundSchedulePoolTaskHolder activating_task;
    String active_node_identifier;
    /// The Keeper session that created our current or latest `is_active` node. The identifier stored in the node
    /// is readable from Keeper and can be replayed by another client, the session cannot, so this is what tells a
    /// leftover of our own from a foreign node when the replica re-registers. Empty until the first registration
    /// of this process; `partialShutdown` deliberately leaves it in place, since the re-registration may take
    /// several attempts.
    std::optional<Int64> own_is_active_session_id;
    UInt64 consecutive_activate_failures = 0;

    bool activate();
    void activateAndReschedule();
    void partialShutdown();
    /// Drops the topic-partition locks held by the consumers that are not in use at the moment.
    void releaseConsumersLocks();

    void parsePartitionAffinitySettings();

    void assertActive() const;
    /// Whether the consumer could not poll because this replica has to be re-registered in Keeper, i.e. the
    /// activating task has to run again before any consumer of this table can make progress.
    static bool needsReactivation(KeeperHandlingConsumer::CannotPollReason reason);
    /// Asks the activating task to re-register this replica in Keeper as soon as possible.
    void scheduleReactivation(KeeperHandlingConsumer::CannotPollReason reason);
    KafkaConsumer2Ptr createKafkaConsumer(size_t consumer_number);
    // Returns full consumer related configuration, also the configuration
    // contains global kafka properties.
    cppkafka::Configuration getConsumerConfiguration(size_t consumer_number, IKafkaExceptionInfoSinkPtr exception_sink);
    // Returns full producer related configuration, also the configuration
    // contains global kafka properties.
    cppkafka::Configuration getProducerConfiguration();

    void threadFunc(size_t idx);

    size_t getPollMaxBatchSize() const;
    size_t getMaxBlockSize() const;
    size_t getPollTimeoutMillisecond() const;
    size_t getSchemaRegistrySkipBytes() const;

    enum class StallKind : uint8_t
    {
        ShortStall,
        LongStall,
    };

    std::optional<StallKind> streamToViews(size_t idx, UInt64 cycle_epoch);

    /// KeeperHandlingConsumer has to be acquired before polling it
    KeeperHandlingConsumerPtr acquireConsumer(size_t idx);
    void releaseConsumer(KeeperHandlingConsumerPtr && consumer_ptr);
    void cleanConsumers();

    std::optional<size_t> streamFromConsumer(KeeperHandlingConsumer & consumer_info, const Stopwatch & watch, UInt64 cycle_epoch);

    // Returns true if this is the first replica
    bool createTableIfNotExists();
    // Returns true if all of the nodes were cleaned up
    bool removeTableNodesFromZooKeeper(zkutil::ZooKeeperPtr keeper_to_use, const zkutil::EphemeralNodeHolder::Ptr & drop_lock);
    // Creates only the replica in ZooKeeper. Shouldn't be called on the first replica as it is created in createTableIfNotExists
    void createReplica();
    /// The data stored in the persistent `replicas/<replica_name>` znode: the shard num in affinity mode, empty otherwise.
    String getReplicaRegistrationData() const;
    /// True when the persistent `replicas/<replica_name>` znode is there with the expected data and carries our `is_active` node.
    bool isReplicaRegistrationValid(const zkutil::ZooKeeperPtr & keeper_to_use) const;
    /// Re-creates the persistent `replicas/<replica_name>` znode or restores its data when it drifted.
    void restoreReplicaRegistration(const zkutil::ZooKeeperPtr & keeper_to_use);
    void dropReplica();

    std::optional<BlocksAndGuard>
    pollConsumer(KeeperHandlingConsumer & consumer, const Stopwatch & watch, const ContextPtr & modified_context, size_t poll_max_block_size = 0);

    void setZooKeeper();
    zkutil::ZooKeeperPtr tryGetZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeperAndAssertActive() const;
    zkutil::ZooKeeperPtr getZooKeeperIfTableShutDown() const;

    static StallKind getStallKind(const KeeperHandlingConsumer::CannotPollReason & cannotPollReason);
};

}
