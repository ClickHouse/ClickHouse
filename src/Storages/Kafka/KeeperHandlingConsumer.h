#pragma once

#include <filesystem>
#include <mutex>
#include <IO/ReadBuffer.h>
#include <Storages/Kafka/KafkaConsumer2.h>
#include <Storages/Kafka/StorageKafkaUtils.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

/// Wrapper around KafkaConsumer2 that manages locks, committed offsets and intent sizes in Keeper.
///
/// It assigns a specific list of topic partitions to KafkaConsumer2 without relying on the rebalance logic of Kafka.
/// The general workflow how to use this class is quite error-prone, but a lot of this boils down to the quirks of
/// librdkafka:
///   1. If `isInUse()` returns false, `startUsing` must be called before calling `prepareToPoll` or `poll`. This will
/// create the librdkafka consumer inside KafkaConsumer2 if necessary
///   2. If `needsKeeper()` returns true, then `setKeeper` must be called to set the new Keeper session.
///   3. If `prepareToPoll` returns an empty optional, then it is possible to call `poll` to start to consume some
/// messages.
///   4. If the consumed messages are processed successfully, then the the batch must be committed using  the returned
/// `OffsetGuard`.
///
/// From thread-safety perspective the functions of this class falls into three categories:
///   a) Functions `startUsing`, `stopUsing` and `moveConsumer`: they need exclusive access to the object, so they
/// cannot be called simultaneously with other functions. No exceptions.
///   b) Function `getStat`: it can be called simultaneously with any other functions (including itself), except for
/// the functions of category a).
///   c) All other functions: they can be called simultaneously with `getStat`, but nothing else, not even each other
/// or themselves.
class KeeperHandlingConsumer
{
public:
    enum class CannotPollReason : uint8_t
    {
        NoMetadata,
        KeeperSessionEnded,
        NoPartitions,
    };

    class MessageInfo
    {
    public:
        explicit MessageInfo(const KafkaConsumer2 & kafka_consumer_);

        String currentTopic() const;
        int32_t currentPartition() const;
        int64_t currentOffset() const;
        String currentKey() const;
        String currentPayload() const;
        boost::optional<cppkafka::MessageTimestamp> currentTimestamp() const;
        const cppkafka::Message::HeaderListType & currentHeaderList() const;

    private:
        const KafkaConsumer2 & kafka_consumer;
    };

    class OffsetGuard
    {
    public:
        OffsetGuard(KeeperHandlingConsumer & consumer_, int64_t new_offset_);
        OffsetGuard(OffsetGuard && other) noexcept;
        ~OffsetGuard();

        void commit();
    private:
        bool needs_rollback{true};
        KeeperHandlingConsumer * consumer;
        int64_t new_offset;
    };

    using MessageSinkFunction
        = std::function<bool(ReadBufferPtr, const MessageInfo &, bool /*has_more_polled_messages*/, bool /*stalled*/)>;

    KeeperHandlingConsumer(
        const KafkaConsumer2Ptr & kafka_consumer_,
        const std::shared_ptr<zkutil::ZooKeeper> & keeper_,
        const std::filesystem::path & keeper_path_,
        const String & replica_name_,
        size_t idx_,
        const LoggerPtr & log_);

    /// It is important that the consumer is using the same Keeper session as the storage to make sure all ephemeral
    /// nodes are handled at the same time, e.g.: if a replica is deactivated because of session loss, all of its
    /// consumers should lose connection to keeper.
    bool needsNewKeeper() const;
    void setKeeper(const std::shared_ptr<zkutil::ZooKeeper> & keeper_);

    /// The concept of `prepareToPoll` and `poll` is quite a bit quirky, but I didn't find a better way to:
    ///   1. Separate the logic of converting messages to rows with virtual columns and everything else
    ///   2. Do not expose KafkaConsumer2, so KeeperHandlingConsumer has some kind of control over the offsets
    ///   3. Only do the heavy initialization once the consumer can actually poll messages
    /// 1: KeeperHandlingConsumer shouldn't be aware of the logic how a single message is turned into rows. It doesn't
    /// need to know about virtual columns, error streams, etc.
    /// 2: By polling `KafkaConsumer2` directly the user (StorageKafka2) should report back the intent size in order to
    /// be able to save it before pushing the block. Yes, the current solution does not solve this (in
    /// MessageSinkFunction KeeperHandlingConsumer has no control over the blocks) completely, but I think it is a good
    /// compromise. In the future we might add a similar class than `KafkaSource` to handle the conversion of messages
    /// to rows, but right now this is not the priority.
    /// 3: There are several reasons why KeeperHandlingConsumer might not be able to poll messages:
    ///   - Keeper session has ended
    ///   - Couldn't get the list of topic partitions from Kafka
    ///   - Couldn't lock any topic partitions
    /// In case of these reasons it doesn't make sense to set up the whole pipeline, which not the lightest operation.
    /// As a result we have the the MessageSink callback. In the end I think the extra complexity worth it.
    std::optional<CannotPollReason> prepareToPoll();

    /// Returns empty optional if polling it not possible or no messages were polled.
    /// Returns an OffsetGuard that rollbacks the offsets unless it is committed or deactivated.
    std::optional<OffsetGuard> poll(MessageSinkFunction & message_sink);

    using ConfigCreator = std::function<cppkafka::Configuration(IKafkaExceptionInfoSinkPtr)>;
    /// Returns true if a new consumer was created
    bool startUsing(const ConfigCreator & config_creator);
    void stopUsing();
    bool isInUse() const { return in_use; }
    UInt64 getLastUsedUsec() const { return last_used_usec; }
    bool hasConsumer() const { return kafka_consumer->hasConsumer(); }
    CppKafkaConsumerPtr moveConsumer() const;

    /// Can be called simultaneously with most of the functions, see the class comment for details.
    StorageKafkaUtils::ConsumerStatistics getStat() const;
    IKafkaExceptionInfoSinkPtr getExceptionInfoSink() const;
private:
    using TopicPartition = KafkaConsumer2::TopicPartition;
    using TopicPartitions = KafkaConsumer2::TopicPartitions;
    using TopicPartitionOffset = KafkaConsumer2::TopicPartitionOffset;
    using TopicPartitionOffsets = KafkaConsumer2::TopicPartitionOffsets;

    struct LockedTopicPartitionInfo
    {
        zkutil::EphemeralNodeHolderPtr lock;
        std::optional<int64_t> committed_offset;
        std::optional<uint64_t> intent_size;
    };

    using TopicPartitionLocks = std::
        unordered_map<TopicPartition, LockedTopicPartitionInfo, KafkaConsumer2::TopicPartitionHash, KafkaConsumer2::TopicPartitionEquality>;

    using TopicPartitionSet
        = std::unordered_set<TopicPartition, KafkaConsumer2::TopicPartitionHash, KafkaConsumer2::TopicPartitionEquality>;

    struct ActiveReplicasInfo
    {
        UInt64 active_replica_count{0};
        bool has_replica_without_locks{false};
    };

    std::filesystem::path keeper_path;
    const String replica_name;
    size_t idx;

    KafkaConsumer2Ptr kafka_consumer;
    zkutil::ZooKeeperPtr keeper;
    size_t topic_partition_index_to_consume_from{0};
    // TODO: handle KafkaAssignedPartitions and KafkaConsumersWithAssignment metric when assigned_topic_partitions is modified
    TopicPartitions assigned_topic_partitions{};
    size_t poll_count = 0;

    /// Protects access to the locks, because they are accessed simultaneously when statistics are collected
    mutable std::mutex topic_partition_locks_mutex;
    TopicPartitionLocks TSA_GUARDED_BY(topic_partition_locks_mutex) permanent_locks{};
    TopicPartitionLocks TSA_GUARDED_BY(topic_partition_locks_mutex) tmp_locks{};
    LoggerPtr log;

    // Quota, how many temporary locks can be taken in current round
    size_t tmp_locks_quota{};

    std::atomic<UInt64> last_poll_timestamp = 0;
    std::atomic<UInt64> last_commit_timestamp = 0;
    std::atomic<UInt64> num_commits = 0;
    std::atomic<bool> in_use = false;
    /// Last used time (for TTL)
    std::atomic<UInt64> last_used_usec = 0;


    std::pair<TopicPartitionSet, ActiveReplicasInfo> getLockedTopicPartitions();
    std::pair<TopicPartitions, ActiveReplicasInfo> getAvailableTopicPartitions(const TopicPartitionOffsets & all_topic_partitions);
    std::optional<LockedTopicPartitionInfo> createLocksInfoIfFree(const TopicPartition & partition_to_lock);

    LockedTopicPartitionInfo & getTopicPartitionLockLocked(const TopicPartition & topic_partition) TSA_REQUIRES(topic_partition_locks_mutex);
    void updatePermanentLocksLocked(const TopicPartitions & available_topic_partitions, size_t topic_partitions_count, size_t active_replica_count) TSA_REQUIRES(topic_partition_locks_mutex);
    void lockTemporaryLocksLocked(const TopicPartitions & available_topic_partitions, bool has_replica_without_locks) TSA_REQUIRES(topic_partition_locks_mutex);

    void rollbackToCommittedOffsets();

    void saveCommittedOffset(int64_t new_offset);
    void saveIntentSize(const KafkaConsumer2::TopicPartition & topic_partition, const std::optional<int64_t> & offset, uint64_t intent);
    // To save commit and intent nodes
    void writeTopicPartitionInfoToKeeper(const std::filesystem::path & keeper_path_to_data, const String & data);
    // Searches first in permanent_locks, then in tmp_locks.


    std::filesystem::path getTopicPartitionPath(const TopicPartition & topic_partition);
    std::filesystem::path getTopicPartitionLockPath(const TopicPartition & topic_partition);

    void appendToAssignedTopicPartitions(const TopicPartitionLocks & locks);
};
}
