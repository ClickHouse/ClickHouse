#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Core/Block.h>
#include <Core/StreamingHandleErrorMode.h>
#include <Core/Types.h>
#include <Storages/IStorage.h>
#include <Storages/Kafka/KafkaConsumer2.h>
#include <Common/Macros.h>
#include <Common/SettingsChanges.h>
#include <Common/ThreadStatus.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <Poco/Semaphore.h>

#include <atomic>
#include <filesystem>
#include <list>
#include <mutex>
#include <rdkafka.h>

namespace cppkafka
{

class Configuration;

}

namespace DB
{

struct KafkaSettings;
template <typename TStorageKafka>
struct KafkaInterceptors;

using KafkaConsumer2Ptr = std::shared_ptr<KafkaConsumer2>;

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
class StorageKafka2 final : public IStorage, WithContext
{
    using KafkaInterceptors = KafkaInterceptors<StorageKafka2>;
    friend KafkaInterceptors;

public:
    StorageKafka2(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        const String & comment,
        std::unique_ptr<KafkaSettings> kafka_settings_,
        const String & collection_name_);

    ~StorageKafka2() override;

    std::string getName() const override { return "Kafka"; }

    bool noPushingToViews() const override { return true; }

    void startup() override;
    void shutdown(bool is_drop) override;

    void drop() override;

    Pipe read(
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

private:
    using TopicPartition = KafkaConsumer2::TopicPartition;
    using TopicPartitions = KafkaConsumer2::TopicPartitions;

    struct LockedTopicPartitionInfo
    {
        zkutil::EphemeralNodeHolderPtr lock;
        std::optional<int64_t> committed_offset;
        std::optional<int64_t> intent_size;
    };

    using TopicPartitionLocks = std::unordered_map<
        TopicPartition,
        LockedTopicPartitionInfo,
        KafkaConsumer2::OnlyTopicNameAndPartitionIdHash,
        KafkaConsumer2::OnlyTopicNameAndPartitionIdEquality>;

    struct ConsumerAndAssignmentInfo
    {
        KafkaConsumer2Ptr consumer;
        size_t consume_from_topic_partition_index{0};
        TopicPartitions topic_partitions{};
        zkutil::ZooKeeperPtr keeper;
        TopicPartitionLocks locks{};
        Stopwatch watch{CLOCK_MONOTONIC_COARSE};
    };

    struct PolledBatchInfo
    {
        BlocksList blocks;
        int64_t last_offset;
    };

    // Stream thread
    struct TaskContext
    {
        BackgroundSchedulePool::TaskHolder holder;
        std::atomic<bool> stream_cancelled{false};
        explicit TaskContext(BackgroundSchedulePool::TaskHolder && task_) : holder(std::move(task_)) { }
    };

    enum class AssignmentChange
    {
        NotChanged,
        Updated,
        Lost
    };

    // Configuration and state
    mutable std::mutex keeper_mutex;
    zkutil::ZooKeeperPtr keeper;
    String keeper_path;
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
    Poco::Semaphore semaphore;
    const SettingsChanges settings_adjustments;
    /// Can differ from num_consumers in case of exception in startup() (or if startup() hasn't been called).
    /// In this case we still need to be able to shutdown() properly.
    size_t num_created_consumers = 0; /// number of actually created consumers.
    std::vector<ConsumerAndAssignmentInfo> consumers;
    std::vector<std::shared_ptr<TaskContext>> tasks;
    bool thread_per_consumer = false;
    /// For memory accounting in the librdkafka threads.
    std::mutex thread_statuses_mutex;
    std::list<std::shared_ptr<ThreadStatus>> thread_statuses;
    /// If named_collection is specified.
    String collection_name;
    std::atomic<bool> shutdown_called = false;

    // Handling replica activation.
    std::atomic<bool> is_active = false;
    zkutil::EphemeralNodeHolderPtr replica_is_active_node;
    BackgroundSchedulePool::TaskHolder activating_task;
    String active_node_identifier;
    UInt64 consecutive_activate_failures = 0;
    bool activate();
    void activateAndReschedule();
    void partialShutdown();

    void assertActive() const;
    KafkaConsumer2Ptr createConsumer(size_t consumer_number);
    // Returns full consumer related configuration, also the configuration
    // contains global kafka properties.
    cppkafka::Configuration getConsumerConfiguration(size_t consumer_number);
    // Returns full producer related configuration, also the configuration
    // contains global kafka properties.
    cppkafka::Configuration getProducerConfiguration();

    void threadFunc(size_t idx);

    size_t getPollMaxBatchSize() const;
    size_t getMaxBlockSize() const;
    size_t getPollTimeoutMillisecond() const;

    enum class StallReason
    {
        NoAssignment,
        CouldNotAcquireLocks,
        NoPartitions,
        NoMessages,
        KeeperSessionEnded,
    };

    std::optional<StallReason> streamToViews(size_t idx);

    std::optional<size_t> streamFromConsumer(ConsumerAndAssignmentInfo & consumer_info);

    // Returns true if this is the first replica
    bool createTableIfNotExists();
    // Returns true if all of the nodes were cleaned up
    bool removeTableNodesFromZooKeeper(zkutil::ZooKeeperPtr keeper_to_use, const zkutil::EphemeralNodeHolder::Ptr & drop_lock);
    // Creates only the replica in ZooKeeper. Shouldn't be called on the first replica as it is created in createTableIfNotExists
    void createReplica();
    void dropReplica();

    // Takes lock over topic partitions and sets the committed offset in topic_partitions.
    std::optional<TopicPartitionLocks> lockTopicPartitions(zkutil::ZooKeeper & keeper_to_use, const TopicPartitions & topic_partitions);
    void saveCommittedOffset(zkutil::ZooKeeper & keeper_to_use, const TopicPartition & topic_partition);
    void saveIntent(zkutil::ZooKeeper & keeper_to_use, const TopicPartition & topic_partition, int64_t intent);

    PolledBatchInfo pollConsumer(
        KafkaConsumer2 & consumer,
        const TopicPartition & topic_partition,
        std::optional<int64_t> message_count,
        Stopwatch & watch,
        const ContextPtr & context);

    void setZooKeeper();
    zkutil::ZooKeeperPtr tryGetZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeperAndAssertActive() const;
    zkutil::ZooKeeperPtr getZooKeeperIfTableShutDown() const;


    std::filesystem::path getTopicPartitionPath(const TopicPartition & topic_partition);
};

}
